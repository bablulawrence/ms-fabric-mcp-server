# ABOUTME: HTTP client for Microsoft Fabric API operations with retry and tracing.
# ABOUTME: Uses DefaultAzureCredential for authentication and provides centralized error handling.
"""HTTP client for Microsoft Fabric API operations with OpenTelemetry instrumentation."""

import logging
from typing import Optional, Dict, Any

import requests
from azure.identity import DefaultAzureCredential
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from .config import FabricConfig
from .exceptions import (
    FabricError,
    FabricAuthError,
    FabricAPIError,
    FabricConnectionError,
    FabricRateLimitError,
    FabricConfigError,
)

# Optional: OpenTelemetry instrumentation
try:
    from opentelemetry import trace
    from opentelemetry.trace import Status, StatusCode
    OTEL_AVAILABLE = True
except ImportError:
    OTEL_AVAILABLE = False


logger = logging.getLogger(__name__)


class FabricClient:
    """HTTP client for Microsoft Fabric API operations.
    
    This client provides authenticated HTTP access to Fabric REST API with:
    - Azure Identity authentication (DefaultAzureCredential)
    - Automatic retry with exponential backoff for transient failures
    - Request/response logging and tracing via OpenTelemetry
    - Thread-safe session management
    - Centralized error handling
    
    Example:
        ```python
        from ms_fabric_mcp_server.client import FabricConfig, FabricClient
        
        config = FabricConfig.from_environment()
        client = FabricClient(config)
        
        response = client.make_api_request("GET", "workspaces")
        workspaces = response.json()
        ```
    """
    
    def __init__(self, config: FabricConfig):
        """Initialize the Fabric client.
        
        Args:
            config: Configuration settings for the client
        """
        self.config = config
        self._credential = None
        self._session = None
        self._tracer = None
        
        # Initialize OpenTelemetry tracer if available
        if OTEL_AVAILABLE:
            self._tracer = trace.get_tracer(__name__)
        
        # Initialize session with retry strategy
        self._setup_session()
        
        # Initialize Azure credential
        self._setup_credential()
        
        logger.info("FabricClient initialized successfully")
    
    def _setup_session(self) -> None:
        """Set up HTTP session with retry strategy."""
        self._session = requests.Session()
        
        # Configure retry strategy
        retry_strategy = Retry(
            total=self.config.MAX_RETRIES,
            backoff_factor=self.config.RETRY_BACKOFF,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["HEAD", "GET", "OPTIONS", "POST", "PUT", "DELETE"]
        )
        
        adapter = HTTPAdapter(max_retries=retry_strategy)
        self._session.mount("http://", adapter)
        self._session.mount("https://", adapter)
        
        logger.debug(
            f"HTTP session configured with retry strategy "
            f"(max_retries={self.config.MAX_RETRIES}, backoff={self.config.RETRY_BACKOFF})"
        )
    
    def _setup_credential(self) -> None:
        """Set up Azure credential based on configuration."""
        try:
            # Use DefaultAzureCredential for consistency
            self._credential = DefaultAzureCredential()
            logger.debug("Using DefaultAzureCredential for authentication")
        except Exception as exc:
            logger.error(f"Failed to set up Azure credential: {exc}")
            raise FabricConfigError(f"Failed to configure authentication: {exc}")
    
    def get_auth_token(self) -> str:
        """Get fresh authentication token.
        
        Returns:
            Valid authentication token
            
        Raises:
            FabricAuthError: If authentication fails
        """
        try:
            logger.debug("Acquiring authentication token")
            token_result = self._credential.get_token(*self.config.SCOPES)
            
            logger.debug("Authentication token acquired successfully")
            return token_result.token
            
        except Exception as exc:
            logger.error(f"Authentication failed: {exc}")
            raise FabricAuthError(f"Failed to acquire authentication token: {exc}")
    
    def _get_auth_headers(self) -> Dict[str, str]:
        """Get authentication headers for API requests.
        
        Returns:
            Dictionary containing authorization and content-type headers
        """
        token = self.get_auth_token()
        return {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json"
        }
    
    def make_api_request(
        self,
        method: str,
        endpoint: str,
        payload: Optional[Dict[str, Any]] = None,
        timeout: Optional[int] = None,
        headers: Optional[Dict[str, str]] = None
    ) -> requests.Response:
        """Make authenticated API request with error handling and tracing.
        
        Args:
            method: HTTP method (GET, POST, PUT, DELETE)
            endpoint: API endpoint URL (absolute or relative to base URL)
            payload: Request payload for POST/PUT requests
            timeout: Request timeout in seconds (uses config default if not specified)
            headers: Additional headers to include
            
        Returns:
            Response object
            
        Raises:
            FabricAPIError: For API-related errors
            FabricConnectionError: For connection-related errors
            FabricRateLimitError: For rate limiting errors
            
        Example:
            ```python
            response = client.make_api_request("GET", "workspaces")
            response = client.make_api_request(
                "POST",
                "workspaces/abc-123/items",
                payload={"displayName": "My Notebook", "type": "Notebook"}
            )
            ```
        """
        # Build full URL if endpoint is relative
        if not endpoint.startswith("http"):
            endpoint = f"{self.config.BASE_URL.rstrip('/')}/{endpoint.lstrip('/')}"
        
        # Prepare headers
        request_headers = self._get_auth_headers()
        if headers:
            request_headers.update(headers)
        
        # Use default timeout if not specified
        request_timeout = timeout or self.config.API_CALL_TIMEOUT
        
        # Create span for tracing if available
        if self._tracer:
            with self._tracer.start_as_current_span(
                f"fabric.{method.lower()}",
                attributes={
                    "http.method": method.upper(),
                    "http.url": endpoint,
                    "fabric.api.version": "v1",
                }
            ) as span:
                return self._execute_request(
                    method, endpoint, payload, request_headers, request_timeout, span
                )
        else:
            return self._execute_request(
                method, endpoint, payload, request_headers, request_timeout
            )
    
    def _execute_request(
        self,
        method: str,
        endpoint: str,
        payload: Optional[Dict[str, Any]],
        headers: Dict[str, str],
        timeout: int,
        span = None
    ) -> requests.Response:
        """Execute HTTP request with error handling.
        
        Args:
            method: HTTP method
            endpoint: Full URL
            payload: Request payload
            headers: Request headers
            timeout: Request timeout
            span: Optional OpenTelemetry span for tracing
            
        Returns:
            Response object
        """
        logger.debug(f"Making {method} request to: {endpoint}")
        
        try:
            response = self._session.request(
                method=method.upper(),
                url=endpoint,
                json=payload,
                headers=headers,
                timeout=timeout
            )
            
            logger.debug(f"Response: {response.status_code}")
            
            # Add response attributes to span if available
            if span and OTEL_AVAILABLE:
                span.set_attribute("http.status_code", response.status_code)
                if response.ok:
                    span.set_status(Status(StatusCode.OK))
            
            # Handle the response
            self.handle_api_errors(response)
            return response
            
        except requests.exceptions.Timeout as exc:
            logger.error(f"Request timeout after {timeout}s: {exc}")
            if span and OTEL_AVAILABLE:
                span.set_status(Status(StatusCode.ERROR, "Request timeout"))
                span.record_exception(exc)
            raise FabricConnectionError(f"Request timeout after {timeout} seconds")
        
        except requests.exceptions.ConnectionError as exc:
            logger.error(f"Connection error: {exc}")
            if span and OTEL_AVAILABLE:
                span.set_status(Status(StatusCode.ERROR, "Connection error"))
                span.record_exception(exc)
            raise FabricConnectionError(f"Connection failed: {exc}")
        
        except (FabricAPIError, FabricRateLimitError) as exc:
            # Re-raise our custom exceptions
            if span and OTEL_AVAILABLE:
                span.set_status(Status(StatusCode.ERROR, str(exc)))
                span.record_exception(exc)
            raise
        
        except Exception as exc:
            logger.error(f"Unexpected error during API request: {exc}")
            if span and OTEL_AVAILABLE:
                span.set_status(Status(StatusCode.ERROR, "Unexpected error"))
                span.record_exception(exc)
            raise FabricError(f"Unexpected error: {exc}")
    
    def handle_api_errors(self, response: requests.Response) -> None:
        """Centralized API error handling and custom exceptions.
        
        Args:
            response: HTTP response object
            
        Raises:
            FabricRateLimitError: For 429 status codes
            FabricAPIError: For other error status codes
        """
        if response.status_code == 429:
            # Handle rate limiting
            retry_after = response.headers.get("Retry-After")
            retry_after_int = int(retry_after) if retry_after and retry_after.isdigit() else None
            logger.warning(f"Rate limit exceeded, retry after: {retry_after_int}")
            raise FabricRateLimitError(retry_after_int)
        
        if not response.ok:
            # Try to extract error message from response
            try:
                error_data = response.json()
                error_message = error_data.get("error", {}).get("message", response.text)
            except (ValueError, KeyError):
                error_message = response.text or f"HTTP {response.status_code}"
            
            logger.error(f"API error {response.status_code}: {error_message}")
            raise FabricAPIError(
                status_code=response.status_code,
                message=error_message,
                response_body=response.text[:1000]  # Limit response body size
            )
    
    def close(self) -> None:
        """Close the HTTP session and cleanup resources."""
        if self._session:
            self._session.close()
            logger.debug("HTTP session closed")
    
    def __enter__(self):
        """Context manager entry."""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()
