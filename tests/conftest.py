"""Pytest configuration and shared fixtures for ms-fabric-mcp-server tests."""

import asyncio
import os
import sys
import time
import uuid
import pytest
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
def mock_environment(monkeypatch, request):
    """Set up test environment variables for all tests."""
    if request.node.get_closest_marker("integration"):
        return
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


# ============================================================================
# Integration Test Fixtures
# ============================================================================

@pytest.fixture(autouse=True)
def integration_enabled(request):
    """Gate integration tests behind marker, -m integration, and env var."""
    if not request.node.get_closest_marker("integration"):
        return

    markexpr = request.config.option.markexpr or ""
    if "integration" not in markexpr:
        pytest.skip("Integration tests require -m integration")

    if os.getenv("FABRIC_INTEGRATION_TESTS") != "1":
        pytest.skip("Integration tests require FABRIC_INTEGRATION_TESTS=1")


def get_env_or_skip(name: str, allow_empty: bool = False) -> str:
    """Return env var value or skip with a clear message."""
    value = os.getenv(name)
    if value is None or (not allow_empty and not value.strip()):
        pytest.skip(f"Missing required environment variable: {name}")
    return value


def get_env_optional(name: str) -> str | None:
    """Return env var value or None if missing."""
    value = os.getenv(name)
    if value is None or not value.strip():
        return None
    return value


def unique_name(prefix: str) -> str:
    """Generate a unique name for live test items."""
    timestamp = time.strftime("%Y%m%d%H%M%S", time.gmtime())
    suffix = uuid.uuid4().hex[:8]
    return f"{prefix}_{timestamp}_{suffix}"


async def _poll_until(check_fn, timeout_seconds: int = 300, interval_seconds: int = 10):
    """Poll until check_fn returns a truthy value or timeout expires."""
    deadline = time.monotonic() + timeout_seconds
    last_result = None
    while time.monotonic() < deadline:
        last_result = await check_fn()
        if last_result:
            return last_result
        await asyncio.sleep(interval_seconds)
    return last_result


@pytest.fixture
def poll_until():
    """Fixture wrapper for poll_until helper."""
    return _poll_until


@pytest.fixture(scope="session")
def mcp_server():
    """Create a real FastMCP server with Fabric tools registered."""
    from fastmcp import FastMCP
    from ms_fabric_mcp_server import register_fabric_tools

    mcp = FastMCP("integration-tests")
    register_fabric_tools(mcp)
    return mcp


@pytest.fixture(scope="session")
async def tool_registry(mcp_server):
    """Return the registered tool mapping once per session."""
    return await mcp_server.get_tools()


@pytest.fixture
def call_tool(tool_registry):
    """Invoke a tool by name and return structured content."""
    async def _call(tool_name: str, **kwargs):
        tool = tool_registry.get(tool_name)
        if tool is None:
            raise AssertionError(f"Tool not registered: {tool_name}")
        result = await tool.run(kwargs)
        structured = result.structured_content
        if structured is None:
            raise AssertionError(f"Tool returned no structured content: {tool_name}")
        return structured

    return _call


@pytest.fixture
def workspace_name():
    """Configured workspace display name."""
    return get_env_or_skip("FABRIC_TEST_WORKSPACE_NAME")


@pytest.fixture
def lakehouse_name():
    """Configured lakehouse display name."""
    return get_env_or_skip("FABRIC_TEST_LAKEHOUSE_NAME")


@pytest.fixture
def sql_database():
    """Configured SQL database name."""
    return get_env_or_skip("FABRIC_TEST_SQL_DATABASE")


@pytest.fixture
def notebook_fixture_path() -> Path:
    """Path to the minimal notebook fixture."""
    return PROJECT_ROOT / "tests" / "fixtures" / "minimal_notebook.ipynb"


@pytest.fixture
async def workspace_id(call_tool, workspace_name):
    """Resolve workspace ID from display name."""
    result = await call_tool("list_workspaces")
    if result.get("status") != "success":
        raise AssertionError(f"Failed to list workspaces: {result}")
    for workspace in result.get("workspaces", []):
        if workspace.get("display_name") == workspace_name:
            return workspace.get("id")
    pytest.skip(f"Workspace not found: {workspace_name}")


@pytest.fixture
async def lakehouse_id(call_tool, workspace_name, lakehouse_name):
    """Resolve lakehouse ID from display name."""
    result = await call_tool("list_items", workspace_name=workspace_name, item_type="Lakehouse")
    if result.get("status") != "success":
        raise AssertionError(f"Failed to list lakehouses: {result}")
    for item in result.get("items", []):
        if item.get("display_name") == lakehouse_name:
            return item.get("id")
    pytest.skip(f"Lakehouse not found: {lakehouse_name}")


@pytest.fixture
def pipeline_copy_inputs():
    """Optional pipeline copy inputs from env vars."""
    source_connection_id = get_env_optional("FABRIC_TEST_SOURCE_CONNECTION_ID")
    source_type = get_env_optional("FABRIC_TEST_SOURCE_TYPE")
    source_schema = get_env_optional("FABRIC_TEST_SOURCE_SCHEMA")
    source_table = get_env_optional("FABRIC_TEST_SOURCE_TABLE")
    dest_connection_id = get_env_optional("FABRIC_TEST_DEST_CONNECTION_ID")
    dest_table = get_env_optional("FABRIC_TEST_DEST_TABLE_NAME") or source_table

    if not all([
        source_connection_id,
        source_type,
        source_schema,
        source_table,
        dest_connection_id,
        dest_table,
    ]):
        return None

    return {
        "source_connection_id": source_connection_id,
        "source_type": source_type,
        "source_schema": source_schema,
        "source_table": source_table,
        "destination_connection_id": dest_connection_id,
        "destination_table": dest_table,
    }


@pytest.fixture
def sql_dependencies_available(tool_registry):
    """Skip SQL tests if dependencies or tools are unavailable."""
    pyodbc = pytest.importorskip("pyodbc")
    drivers = [driver.lower() for driver in pyodbc.drivers()]
    if not any("odbc driver" in driver and "sql server" in driver for driver in drivers):
        pytest.skip("SQL tests require a SQL Server ODBC driver")
    if "get_sql_endpoint" not in tool_registry:
        pytest.skip("SQL tools are not registered (pyodbc missing?)")
    return True


@pytest.fixture
def delete_item_if_exists(call_tool, workspace_name):
    """Delete an item and ignore not-found errors."""
    async def _delete(item_display_name: str, item_type: str):
        result = await call_tool(
            "delete_item",
            workspace_name=workspace_name,
            item_display_name=item_display_name,
            item_type=item_type,
        )
        if result.get("status") == "error":
            message = (result.get("message") or "").lower()
            if "not found" not in message:
                raise AssertionError(f"Failed to delete {item_type} '{item_display_name}': {result}")
        return result

    return _delete
