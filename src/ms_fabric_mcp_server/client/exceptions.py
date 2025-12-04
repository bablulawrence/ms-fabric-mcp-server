# ABOUTME: Exception hierarchy for Fabric operations.
# ABOUTME: Provides custom exceptions for authentication, API, Livy, and validation errors.
"""Exception hierarchy for Fabric operations."""

from typing import Optional


class FabricError(Exception):
    """Base exception for all Fabric operations."""
    pass


class FabricAuthError(FabricError):
    """Authentication-related errors."""
    
    def __init__(self, message: str = "Authentication failed"):
        super().__init__(message)


class FabricItemNotFoundError(FabricError):
    """Item not found errors."""
    
    def __init__(self, item_type: str, item_name: str, workspace_name: str):
        message = f"{item_type} '{item_name}' not found in workspace '{workspace_name}'"
        super().__init__(message)
        self.item_type = item_type
        self.item_name = item_name
        self.workspace_name = workspace_name


class FabricWorkspaceNotFoundError(FabricError):
    """Workspace not found errors."""
    
    def __init__(self, workspace_name: str):
        message = f"Workspace '{workspace_name}' not found"
        super().__init__(message)
        self.workspace_name = workspace_name


class FabricJobTimeoutError(FabricError):
    """Job execution timeout errors."""
    
    def __init__(self, timeout_minutes: int):
        message = f"Job timed out after {timeout_minutes} minutes"
        super().__init__(message)
        self.timeout_minutes = timeout_minutes


class FabricAPIError(FabricError):
    """API request errors."""
    
    def __init__(self, status_code: int, message: str, response_body: Optional[str] = None):
        full_message = f"API error {status_code}: {message}"
        if response_body:
            full_message += f" - Response: {response_body}"
        super().__init__(full_message)
        self.status_code = status_code
        self.response_body = response_body


class FabricValidationError(FabricError):
    """Validation errors for input parameters."""
    
    def __init__(self, field: str, value: str, message: str):
        full_message = f"Validation error for {field}='{value}': {message}"
        super().__init__(full_message)
        self.field = field
        self.value = value


class FabricConfigError(FabricError):
    """Configuration-related errors."""
    
    def __init__(self, message: str = "Configuration error"):
        super().__init__(message)


class FabricConnectionError(FabricError):
    """Network connection errors."""
    
    def __init__(self, message: str = "Connection failed"):
        super().__init__(message)


class FabricRateLimitError(FabricError):
    """Rate limiting errors."""
    
    def __init__(self, retry_after: Optional[int] = None):
        message = "Rate limit exceeded"
        if retry_after:
            message += f", retry after {retry_after} seconds"
        super().__init__(message)
        self.retry_after = retry_after


class FabricLivyError(FabricError):
    """Base exception for Livy-related errors."""
    pass


class FabricLivySessionError(FabricLivyError):
    """Livy session-related errors."""
    pass


class FabricLivyStatementError(FabricLivyError):
    """Livy statement execution errors."""
    pass


class FabricLivyTimeoutError(FabricLivyError):
    """Livy operation timeout errors."""
    
    def __init__(self, operation: str, timeout_seconds: int):
        message = f"Livy {operation} timed out after {timeout_seconds} seconds"
        super().__init__(message)
        self.operation = operation
        self.timeout_seconds = timeout_seconds


__all__ = [
    "FabricError",
    "FabricAuthError",
    "FabricItemNotFoundError",
    "FabricWorkspaceNotFoundError",
    "FabricJobTimeoutError",
    "FabricAPIError",
    "FabricValidationError",
    "FabricConfigError",
    "FabricConnectionError",
    "FabricRateLimitError",
    "FabricLivyError",
    "FabricLivySessionError",
    "FabricLivyStatementError",
    "FabricLivyTimeoutError",
]
