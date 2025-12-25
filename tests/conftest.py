"""Pytest configuration and shared fixtures for ms-fabric-mcp-server tests."""

import sys
import pytest
import os
from pathlib import Path
from unittest.mock import Mock, MagicMock, patch
from typing import Dict, Any

# Ensure local src/ is used instead of any installed package.
PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_PATH = PROJECT_ROOT / "src"
if str(SRC_PATH) not in sys.path:
    sys.path.insert(0, str(SRC_PATH))


# ============================================================================
# Environment Setup
# ============================================================================

@pytest.fixture(autouse=True)
def mock_environment(monkeypatch):
    """Set up test environment variables for all tests."""
    # Clear any existing environment that might interfere
    env_vars_to_clear = [
        "APPLICATIONINSIGHTS_CONNECTION_STRING",
        "OTEL_SERVICE_NAME",
        "FABRIC_BASE_URL",
        "FABRIC_API_TIMEOUT",
    ]
    for var in env_vars_to_clear:
        monkeypatch.delenv(var, raising=False)
    
    # Set test defaults
    monkeypatch.setenv("FABRIC_BASE_URL", "https://api.fabric.microsoft.com/v1")
    monkeypatch.setenv("FABRIC_API_TIMEOUT", "30")


@pytest.fixture
def temp_repo_dir(tmp_path: Path) -> Path:
    """Create temporary repository directory for testing."""
    repo_dir = tmp_path / "test_repo"
    repo_dir.mkdir()
    return repo_dir


@pytest.fixture
def sample_llms_txt_content() -> str:
    """Sample llms.txt content for testing."""
    return """# Overview

This is the overview section.

## Architecture

This describes the architecture.

### Components

Details about components.

## Getting Started

Installation and setup instructions.

# Reference

API reference documentation.
"""


@pytest.fixture
def sample_llms_txt_file(temp_repo_dir: Path, sample_llms_txt_content: str) -> Path:
    """Create a sample llms.txt file in temp directory."""
    llms_file = temp_repo_dir / "llms.txt"
    llms_file.write_text(sample_llms_txt_content)
    return llms_file


# ============================================================================
# Fabric Client Fixtures
# ============================================================================

@pytest.fixture
def mock_fabric_config():
    """Mock Fabric configuration."""
    config = Mock()
    config.BASE_URL = "https://api.fabric.microsoft.com/v1"
    config.SCOPES = ["https://api.fabric.microsoft.com/.default"]
    config.API_CALL_TIMEOUT = 30
    config.MAX_RETRIES = 3
    config.RETRY_BACKOFF = 1.0
    config.LIVY_SESSION_WAIT_TIMEOUT = 300
    config.LIVY_STATEMENT_WAIT_TIMEOUT = 300
    config.LIVY_POLL_INTERVAL = 2
    return config


@pytest.fixture
def mock_azure_credential():
    """Mock Azure DefaultAzureCredential."""
    credential = Mock()
    token = Mock()
    token.token = "mock_access_token_12345"
    token.expires_on = 9999999999
    credential.get_token.return_value = token
    return credential


@pytest.fixture
def mock_fabric_client(mock_fabric_config, mock_azure_credential):
    """Mock FabricClient for testing services."""
    with patch("ms_fabric_mcp_server.client.http_client.DefaultAzureCredential", return_value=mock_azure_credential):
        from ms_fabric_mcp_server.client.http_client import FabricClient
        
        client = FabricClient(mock_fabric_config)
        
        # Mock the make_api_request method
        client.make_api_request = Mock()
        
        return client


@pytest.fixture
def mock_requests_response():
    """Factory for creating mock requests.Response objects."""
    def _create_response(status_code: int = 200, json_data: Dict[str, Any] = None, text: str = ""):
        response = Mock()
        response.status_code = status_code
        response.ok = 200 <= status_code < 300
        response.json.return_value = json_data or {}
        response.text = text
        response.headers = {}
        return response
    return _create_response


# ============================================================================
# Fabric Service Fixtures
# ============================================================================

@pytest.fixture
def sample_workspace_data() -> Dict[str, Any]:
    """Sample workspace data."""
    return {
        "id": "workspace-123",
        "displayName": "Test Workspace",
        "description": "A test workspace",
        "type": "Workspace",
        "capacityId": "capacity-456"
    }


@pytest.fixture
def sample_item_data() -> Dict[str, Any]:
    """Sample item data."""
    return {
        "id": "item-789",
        "displayName": "Test Notebook",
        "type": "Notebook",
        "workspaceId": "workspace-123",
        "description": "A test notebook"
    }


@pytest.fixture
def sample_notebook_definition() -> Dict[str, Any]:
    """Sample notebook definition."""
    return {
        "format": "ipynb",
        "parts": [
            {
                "path": "notebook-content.py",
                "payload": "eyJjZWxscyI6W119",  # Base64 encoded {"cells":[]}
                "payloadType": "InlineBase64"
            }
        ]
    }


@pytest.fixture
def sample_livy_session() -> Dict[str, Any]:
    """Sample Livy session data."""
    return {
        "id": 1,
        "name": None,
        "appId": None,
        "owner": None,
        "proxyUser": None,
        "state": "idle",
        "kind": "pyspark",
        "appInfo": {
            "driverLogUrl": None,
            "sparkUiUrl": None
        },
        "log": []
    }


@pytest.fixture
def sample_livy_statement() -> Dict[str, Any]:
    """Sample Livy statement data."""
    return {
        "id": 0,
        "code": "print('hello')",
        "state": "available",
        "output": {
            "status": "ok",
            "execution_count": 0,
            "data": {
                "text/plain": "hello"
            }
        },
        "progress": 1.0
    }


@pytest.fixture
def sample_job_instance() -> Dict[str, Any]:
    """Sample job instance data."""
    return {
        "id": "job-instance-123",
        "itemId": "item-789",
        "jobType": "RunNotebook",
        "invokeType": "Manual",
        "status": "Completed",
        "startTimeUtc": "2025-10-14T10:00:00Z",
        "endTimeUtc": "2025-10-14T10:05:00Z"
    }


# ============================================================================
# FastMCP Fixtures
# ============================================================================

@pytest.fixture
def mock_fastmcp():
    """Mock FastMCP server instance."""
    mcp = Mock()
    mcp.name = "test-server"
    mcp.instructions = "Test instructions"
    mcp.add_middleware = Mock()
    
    # Mock the tool decorator - it accepts kwargs (like title=) and returns a decorator
    def mock_tool_decorator(**kwargs):
        def decorator(func):
            return func
        return decorator
    
    mcp.tool = Mock(side_effect=mock_tool_decorator)
    return mcp


# ============================================================================
# Marker Configuration
# ============================================================================

def pytest_configure(config):
    """Register custom markers."""
    config.addinivalue_line("markers", "unit: Unit tests (fast, no external dependencies)")
    config.addinivalue_line("markers", "integration: Integration tests (require live services)")
    config.addinivalue_line("markers", "slow: Slow running tests")
