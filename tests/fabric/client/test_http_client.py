"""Tests for Fabric HTTP client."""

import pytest
from unittest.mock import Mock, patch, MagicMock
import requests


@pytest.mark.unit
class TestFabricClient:
    """Test suite for FabricClient class."""
    
    @patch("ms_fabric_mcp_server.client.http_client.DefaultAzureCredential")
    def test_client_initialization(self, mock_credential_class, mock_fabric_config):
        """Test basic client initialization."""
        from ms_fabric_mcp_server.client.http_client import FabricClient
        
        client = FabricClient(mock_fabric_config)
        
        assert client.config == mock_fabric_config
        mock_credential_class.assert_called_once()
    
    @patch("ms_fabric_mcp_server.client.http_client.DefaultAzureCredential")
    def test_client_setup_session(self, mock_credential_class, mock_fabric_config):
        """Test that HTTP session is configured with retry strategy."""
        from ms_fabric_mcp_server.client.http_client import FabricClient
        
        client = FabricClient(mock_fabric_config)
        
        assert client._session is not None
        assert isinstance(client._session, requests.Session)
    
    @patch("ms_fabric_mcp_server.client.http_client.DefaultAzureCredential")
    def test_get_auth_token(self, mock_credential_class, mock_fabric_config, mock_azure_credential):
        """Test authentication token retrieval."""
        from ms_fabric_mcp_server.client.http_client import FabricClient
        
        mock_credential_class.return_value = mock_azure_credential
        client = FabricClient(mock_fabric_config)
        
        token = client.get_auth_token()
        
        assert token == "mock_access_token_12345"
        mock_azure_credential.get_token.assert_called_once()
    
    @patch("ms_fabric_mcp_server.client.http_client.DefaultAzureCredential")
    def test_make_api_request_success(self, mock_credential_class, mock_fabric_config, 
                                       mock_azure_credential, mock_requests_response):
        """Test successful API request."""
        from ms_fabric_mcp_server.client.http_client import FabricClient
        
        mock_credential_class.return_value = mock_azure_credential
        client = FabricClient(mock_fabric_config)
        
        # Mock session.request
        response = mock_requests_response(200, {"value": []})
        client._session.request = Mock(return_value=response)
        
        result = client.make_api_request("GET", "workspaces")
        
        assert result.status_code == 200
        client._session.request.assert_called_once()
    
    @patch("ms_fabric_mcp_server.client.http_client.DefaultAzureCredential")
    def test_make_api_request_with_payload(self, mock_credential_class, mock_fabric_config,
                                            mock_azure_credential, mock_requests_response):
        """Test API request with JSON payload."""
        from ms_fabric_mcp_server.client.http_client import FabricClient
        
        mock_credential_class.return_value = mock_azure_credential
        client = FabricClient(mock_fabric_config)
        
        response = mock_requests_response(201, {"id": "new-item"})
        client._session.request = Mock(return_value=response)
        
        payload = {"displayName": "Test Item"}
        result = client.make_api_request("POST", "items", payload=payload)
        
        assert result.status_code == 201
        call_args = client._session.request.call_args
        assert call_args.kwargs['json'] == payload
    
    @patch("ms_fabric_mcp_server.client.http_client.DefaultAzureCredential")
    def test_handle_api_error_404(self, mock_credential_class, mock_fabric_config,
                                   mock_azure_credential, mock_requests_response):
        """Test handling 404 errors."""
        from ms_fabric_mcp_server.client.http_client import FabricClient
        from ms_fabric_mcp_server.client.exceptions import FabricAPIError
        
        mock_credential_class.return_value = mock_azure_credential
        client = FabricClient(mock_fabric_config)
        
        response = mock_requests_response(404, None)
        response.text = "Not found"
        client._session.request = Mock(return_value=response)
        
        with pytest.raises(FabricAPIError):
            client.make_api_request("GET", "workspaces/nonexistent")
    
    @patch("ms_fabric_mcp_server.client.http_client.DefaultAzureCredential")
    def test_handle_rate_limit_error(self, mock_credential_class, mock_fabric_config,
                                      mock_azure_credential):
        """Test handling rate limit (429) errors."""
        from ms_fabric_mcp_server.client.http_client import FabricClient
        from ms_fabric_mcp_server.client.exceptions import FabricRateLimitError
        
        mock_credential_class.return_value = mock_azure_credential
        client = FabricClient(mock_fabric_config)
        
        # Create 429 response
        response = Mock()
        response.status_code = 429
        response.ok = False
        response.headers = {"Retry-After": "60"}
        client._session.request = Mock(return_value=response)
        
        with pytest.raises(FabricRateLimitError):
            client.make_api_request("GET", "workspaces")
    
    @patch("ms_fabric_mcp_server.client.http_client.DefaultAzureCredential")
    def test_handle_connection_error(self, mock_credential_class, mock_fabric_config,
                                      mock_azure_credential):
        """Test handling connection errors."""
        from ms_fabric_mcp_server.client.http_client import FabricClient
        from ms_fabric_mcp_server.client.exceptions import FabricConnectionError
        
        mock_credential_class.return_value = mock_azure_credential
        client = FabricClient(mock_fabric_config)
        
        # Simulate connection error
        client._session.request = Mock(side_effect=requests.exceptions.ConnectionError("Network error"))
        
        with pytest.raises(FabricConnectionError):
            client.make_api_request("GET", "workspaces")
    
    @patch("ms_fabric_mcp_server.client.http_client.DefaultAzureCredential")
    def test_handle_timeout_error(self, mock_credential_class, mock_fabric_config,
                                   mock_azure_credential):
        """Test handling timeout errors."""
        from ms_fabric_mcp_server.client.http_client import FabricClient
        from ms_fabric_mcp_server.client.exceptions import FabricConnectionError
        
        mock_credential_class.return_value = mock_azure_credential
        client = FabricClient(mock_fabric_config)
        
        # Simulate timeout
        client._session.request = Mock(side_effect=requests.exceptions.Timeout("Request timeout"))
        
        with pytest.raises(FabricConnectionError):
            client.make_api_request("GET", "workspaces")
    
    @patch("ms_fabric_mcp_server.client.http_client.DefaultAzureCredential")
    def test_url_construction_relative(self, mock_credential_class, mock_fabric_config,
                                        mock_azure_credential, mock_requests_response):
        """Test URL construction for relative endpoints."""
        from ms_fabric_mcp_server.client.http_client import FabricClient
        
        mock_credential_class.return_value = mock_azure_credential
        client = FabricClient(mock_fabric_config)
        
        response = mock_requests_response(200, {})
        client._session.request = Mock(return_value=response)
        
        client.make_api_request("GET", "workspaces")
        
        call_args = client._session.request.call_args
        url = call_args.kwargs['url']
        assert url.startswith(mock_fabric_config.BASE_URL)
        assert "workspaces" in url
    
    @patch("ms_fabric_mcp_server.client.http_client.DefaultAzureCredential")
    def test_url_construction_absolute(self, mock_credential_class, mock_fabric_config,
                                        mock_azure_credential, mock_requests_response):
        """Test URL construction for absolute URLs."""
        from ms_fabric_mcp_server.client.http_client import FabricClient
        
        mock_credential_class.return_value = mock_azure_credential
        client = FabricClient(mock_fabric_config)
        
        response = mock_requests_response(200, {})
        client._session.request = Mock(return_value=response)
        
        absolute_url = "https://custom.api.com/resource"
        client.make_api_request("GET", absolute_url)
        
        call_args = client._session.request.call_args
        assert call_args.kwargs['url'] == absolute_url


@pytest.mark.unit
class TestFabricClientExports:
    """Test Fabric client exports."""
    
    def test_client_exported_from_fabric_module(self):
        """Test that FabricClient is exported from fabric module."""
        from ms_fabric_mcp_server import FabricClient
        
        assert FabricClient is not None
