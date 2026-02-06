"""Tests for dataflow MCP tools."""

from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from ms_fabric_mcp_server.models.item import FabricItem
from ms_fabric_mcp_server.models.results import RunJobResult
from ms_fabric_mcp_server.tools.dataflow_tools import register_dataflow_tools
from tests.fabric.tools.utils import capture_tools


@pytest.mark.unit
class TestDataflowTools:
    """Test suite for dataflow MCP tools."""

    @pytest.fixture
    def mock_services(self):
        """Create mock services for tool registration."""
        dataflow_service = Mock()
        workspace_service = Mock()
        item_service = Mock()

        # Configure workspace service
        workspace_service.resolve_workspace_id.return_value = "ws-123"

        # Configure item service
        item_service.resolve_folder_path.return_value = "folder-abc"
        item_service.get_item_by_name.return_value = FabricItem(
            id="df-123",
            display_name="TestDataflow",
            type="Dataflow",
            workspace_id="ws-123",
        )

        return dataflow_service, workspace_service, item_service

    def test_create_dataflow_success(self, mock_services):
        """Test successful dataflow creation."""
        dataflow_service, workspace_service, item_service = mock_services
        tools, mcp = capture_tools()

        dataflow_service.create_dataflow.return_value = "df-new-123"

        register_dataflow_tools(mcp, dataflow_service, workspace_service, item_service)

        result = tools["create_dataflow"](
            workspace_name="Analytics",
            dataflow_name="CustomerETL",
            mashup_content="section Section1; shared Query = 1;",
        )

        assert result["status"] == "success"
        assert result["dataflow_id"] == "df-new-123"
        assert result["dataflow_name"] == "CustomerETL"
        assert result["workspace_name"] == "Analytics"

        workspace_service.resolve_workspace_id.assert_called_once_with("Analytics")
        dataflow_service.create_dataflow.assert_called_once()

    def test_create_dataflow_with_folder(self, mock_services):
        """Test dataflow creation with folder path."""
        dataflow_service, workspace_service, item_service = mock_services
        tools, mcp = capture_tools()

        dataflow_service.create_dataflow.return_value = "df-folder-123"

        register_dataflow_tools(mcp, dataflow_service, workspace_service, item_service)

        result = tools["create_dataflow"](
            workspace_name="Analytics",
            dataflow_name="FolderDataflow",
            mashup_content="section Section1; shared Query = 1;",
            folder_path="etl/daily",
        )

        assert result["status"] == "success"
        item_service.resolve_folder_path.assert_called_once_with("ws-123", "etl/daily")
        call_kwargs = dataflow_service.create_dataflow.call_args[1]
        assert call_kwargs["folder_id"] == "folder-abc"

    def test_create_dataflow_with_metadata(self, mock_services):
        """Test dataflow creation with custom query metadata."""
        dataflow_service, workspace_service, item_service = mock_services
        tools, mcp = capture_tools()

        dataflow_service.create_dataflow.return_value = "df-meta-123"

        custom_metadata = {
            "formatVersion": "202502",
            "name": "CustomDF",
            "queriesMetadata": {},
        }

        register_dataflow_tools(mcp, dataflow_service, workspace_service, item_service)

        result = tools["create_dataflow"](
            workspace_name="Analytics",
            dataflow_name="CustomDF",
            mashup_content="section Section1; shared Q = 1;",
            query_metadata=custom_metadata,
        )

        assert result["status"] == "success"
        call_kwargs = dataflow_service.create_dataflow.call_args[1]
        assert call_kwargs["query_metadata"] == custom_metadata

    def test_get_dataflow_definition_success(self, mock_services):
        """Test successful dataflow definition retrieval."""
        dataflow_service, workspace_service, item_service = mock_services
        tools, mcp = capture_tools()

        query_metadata = {"name": "TestDF", "queriesMetadata": {}}
        mashup_content = "section Section1;\nshared Query = 1;"
        dataflow_service.get_dataflow_definition.return_value = (
            query_metadata,
            mashup_content,
        )

        register_dataflow_tools(mcp, dataflow_service, workspace_service, item_service)

        result = tools["get_dataflow_definition"](
            workspace_name="Analytics",
            dataflow_name="TestDataflow",
        )

        assert result["status"] == "success"
        assert result["dataflow_id"] == "df-123"
        assert result["dataflow_name"] == "TestDataflow"
        assert result["query_metadata"] == query_metadata
        assert result["mashup_content"] == mashup_content

        item_service.get_item_by_name.assert_called_once_with(
            workspace_id="ws-123",
            name="TestDataflow",
            item_type="Dataflow",
        )

    def test_run_dataflow_success(self, mock_services):
        """Test successful dataflow run trigger."""
        dataflow_service, workspace_service, item_service = mock_services
        tools, mcp = capture_tools()

        dataflow_service.run_dataflow.return_value = RunJobResult(
            status="success",
            message="Dataflow refresh started",
            job_instance_id="job-abc-123",
            location_url="https://api.fabric.microsoft.com/jobs/instances/job-abc-123",
            retry_after=30,
        )

        register_dataflow_tools(mcp, dataflow_service, workspace_service, item_service)

        result = tools["run_dataflow"](
            workspace_name="Analytics",
            dataflow_name="TestDataflow",
        )

        assert result["status"] == "success"
        assert result["job_instance_id"] == "job-abc-123"
        assert "job-abc-123" in result["location_url"]
        assert result["retry_after"] == 30

    def test_run_dataflow_with_params(self, mock_services):
        """Test dataflow run with execution parameters."""
        dataflow_service, workspace_service, item_service = mock_services
        tools, mcp = capture_tools()

        dataflow_service.run_dataflow.return_value = RunJobResult(
            status="success",
            message="Dataflow refresh started",
            job_instance_id="job-param-123",
            location_url="https://api.fabric.microsoft.com/jobs/instances/job-param-123",
            retry_after=60,
        )

        register_dataflow_tools(mcp, dataflow_service, workspace_service, item_service)

        execution_data = {"startDate": "2025-01-01"}
        result = tools["run_dataflow"](
            workspace_name="Analytics",
            dataflow_name="TestDataflow",
            execution_data=execution_data,
        )

        assert result["status"] == "success"
        call_kwargs = dataflow_service.run_dataflow.call_args[1]
        assert call_kwargs["execution_data"] == execution_data

    def test_tools_registration_count(self, mock_services):
        """Test that all 3 dataflow tools are registered."""
        dataflow_service, workspace_service, item_service = mock_services
        tools, mcp = capture_tools()

        register_dataflow_tools(mcp, dataflow_service, workspace_service, item_service)

        expected_tools = ["create_dataflow", "get_dataflow_definition", "run_dataflow"]
        for tool_name in expected_tools:
            assert tool_name in tools, f"Tool '{tool_name}' not registered"

        assert len(tools) == 3
