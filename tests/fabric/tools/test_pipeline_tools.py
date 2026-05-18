"""Tests for pipeline MCP tools."""

from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from ms_fabric_mcp_server.models.item import FabricItem
from ms_fabric_mcp_server.tools.pipeline_tools import register_pipeline_tools
from tests.fabric.tools.utils import capture_tools


@pytest.mark.unit
class TestPipelineTools:
    def test_pipeline_tools_smoke(self):
        tools, mcp = capture_tools()
        pipeline_service = Mock()
        workspace_service = Mock()
        item_service = Mock()

        workspace_service.resolve_workspace_id.return_value = "ws-1"
        workspace_service.get_workspace_by_id.return_value = SimpleNamespace(
            display_name="Workspace"
        )
        item_service.get_item_by_name.return_value = FabricItem(
            id="pipe-1",
            display_name="Pipe",
            type="DataPipeline",
            workspace_id="ws-1",
        )

        pipeline_service.create_blank_pipeline.return_value = "pipe-1"
        pipeline_service.add_copy_activity_to_pipeline.return_value = "pipe-1"
        pipeline_service.add_notebook_activity_to_pipeline.return_value = "pipe-1"
        pipeline_service.add_dataflow_activity_to_pipeline.return_value = "pipe-1"
        pipeline_service.add_activity_from_json.return_value = "pipe-1"
        pipeline_service.create_pipeline_with_definition.return_value = "pipe-2"
        pipeline_service.get_pipeline_definition.return_value = {
            "pipeline_content_json": {"properties": {"activities": []}},
            "platform": {"meta": "data"},
        }
        pipeline_service.update_pipeline_definition.return_value = None
        pipeline_service.add_activity_dependency.return_value = ("pipe-1", 1)
        pipeline_service.delete_activity_from_pipeline.return_value = "pipe-1"
        pipeline_service.remove_activity_dependency.return_value = ("pipe-1", 1)
        pipeline_service.get_pipeline_activity_runs.return_value = {
            "activities": [
                {
                    "activity_name": "CopyProducts",
                    "activity_type": "Copy",
                    "status": "Succeeded",
                    "duration_ms": 5230,
                    "rows_read": 1234,
                    "rows_written": 1234,
                    "error_message": None,
                }
            ],
            "activity_count": 1,
            "pipeline_name": "Copy_Data_Pipeline",
        }

        register_pipeline_tools(mcp, pipeline_service, workspace_service, item_service)

        assert (
            tools["create_pipeline"](workspace_name="Workspace", pipeline_name="Pipe")[
                "status"
            ]
            == "success"
        )

        assert (
            tools["add_copy_activity_to_pipeline"](
                workspace_name="Workspace",
                pipeline_name="Pipe",
                source_type="AzureSqlSource",
                source_connection_id="conn-1",
                source_table_schema="dbo",
                source_table_name="table",
                destination_lakehouse_id="lh-1",
                destination_connection_id="lh-conn",
                destination_table_name="table",
            )["status"]
            == "success"
        )

        assert (
            tools["add_notebook_activity_to_pipeline"](
                workspace_name="Workspace",
                pipeline_name="Pipe",
                notebook_name="Notebook",
            )["status"]
            == "success"
        )

        assert (
            tools["add_dataflow_activity_to_pipeline"](
                workspace_name="Workspace",
                pipeline_name="Pipe",
                dataflow_name="Dataflow",
            )["status"]
            == "success"
        )

        assert (
            tools["add_activity_to_pipeline"](
                workspace_name="Workspace",
                pipeline_name="Pipe",
                activity_json={"name": "Wait", "type": "Wait", "typeProperties": {}},
            )["status"]
            == "success"
        )

        assert (
            tools["create_pipeline"](
                workspace_name="Workspace",
                pipeline_name="Pipe2",
                pipeline_content_json={"properties": {"activities": []}},
            )["status"]
            == "success"
        )

        assert (
            tools["get_pipeline_definition"](
                workspace_name="Workspace",
                pipeline_name="Pipe",
            )["status"]
            == "success"
        )

        assert (
            tools["update_pipeline_definition"](
                workspace_name="Workspace",
                pipeline_name="Pipe",
                pipeline_content_json={"properties": {"activities": []}},
            )["status"]
            == "success"
        )

        assert (
            tools["add_activity_dependency"](
                workspace_name="Workspace",
                pipeline_name="Pipe",
                activity_name="A1",
                depends_on=["A0"],
            )["status"]
            == "success"
        )

        assert (
            tools["delete_activity_from_pipeline"](
                workspace_name="Workspace",
                pipeline_name="Pipe",
                activity_name="A1",
            )["status"]
            == "success"
        )

        assert (
            tools["remove_activity_dependency"](
                workspace_name="Workspace",
                pipeline_name="Pipe",
                activity_name="A1",
            )["status"]
            == "success"
        )

        assert (
            tools["get_pipeline_activity_runs"](
                workspace_name="Workspace",
                job_instance_id="job-123",
            )["status"]
            == "success"
        )

    def test_add_copy_activity_tool_forwards_destination_table_schema(self):
        """Issue #18: the MCP tool must forward destination_table_schema to the
        service so non-dbo Lakehouse schemas (e.g. medallion bronze) reach the
        produced Copy Activity sink."""
        tools, mcp = capture_tools()
        pipeline_service = Mock()
        workspace_service = Mock()
        item_service = Mock()
        workspace_service.resolve_workspace_id.return_value = "ws-1"
        workspace_service.get_workspace_by_id.return_value = SimpleNamespace(
            display_name="Workspace"
        )
        pipeline_service.add_copy_activity_to_pipeline.return_value = "pipe-1"

        register_pipeline_tools(mcp, pipeline_service, workspace_service, item_service)

        # Default: no destination_table_schema → service receives "dbo".
        tools["add_copy_activity_to_pipeline"](
            workspace_name="Workspace",
            pipeline_name="Pipe",
            source_type="AzureSqlSource",
            source_connection_id="conn-1",
            source_table_schema="dbo",
            source_table_name="table",
            destination_lakehouse_id="lh-1",
            destination_connection_id="lh-conn",
            destination_table_name="table",
        )
        _, default_kwargs = pipeline_service.add_copy_activity_to_pipeline.call_args
        assert default_kwargs["destination_table_schema"] == "dbo"

        # Caller-supplied medallion bronze schema must reach the service.
        tools["add_copy_activity_to_pipeline"](
            workspace_name="Workspace",
            pipeline_name="pl_chinook_bronze",
            source_type="AzureMySqlSource",
            source_connection_id="conn-1",
            source_table_schema="Chinook",
            source_table_name="customer",
            destination_lakehouse_id="lh-1",
            destination_connection_id="lh-conn",
            destination_table_name="customer",
            destination_table_schema="bronze",
        )
        _, bronze_kwargs = pipeline_service.add_copy_activity_to_pipeline.call_args
        assert bronze_kwargs["destination_table_schema"] == "bronze"

    def test_get_pipeline_definition_save_to_path(self):
        tools, mcp = capture_tools()
        pipeline_service = Mock()
        workspace_service = Mock()
        item_service = Mock()
        workspace_service.resolve_workspace_id.return_value = "ws-1"
        workspace_service.get_workspace_by_id.return_value = SimpleNamespace(
            display_name="Workspace"
        )
        item_service.get_item_by_name.return_value = FabricItem(
            id="pipe-1",
            display_name="Pipe",
            type="DataPipeline",
            workspace_id="ws-1",
        )
        pipeline_service.get_pipeline_definition.return_value = {
            "file_path": "/tmp/pipeline.json",
            "size_bytes": 1234,
        }
        register_pipeline_tools(mcp, pipeline_service, workspace_service, item_service)
        result = tools["get_pipeline_definition"](
            workspace_name="Workspace",
            pipeline_name="Pipe",
            save_to_path="/tmp/pipeline.json",
        )
        assert result["status"] == "success"
        assert result["file_path"] == "/tmp/pipeline.json"
        assert result["size_bytes"] == 1234
        assert "pipeline_content_json" not in result

    def test_create_pipeline_from_file(self):
        tools, mcp = capture_tools()
        pipeline_service = Mock()
        workspace_service = Mock()
        item_service = Mock()
        workspace_service.resolve_workspace_id.return_value = "ws-1"
        workspace_service.get_workspace_by_id.return_value = SimpleNamespace(
            display_name="Workspace"
        )
        pipeline_service._load_pipeline_from_file.return_value = {
            "properties": {"activities": []}
        }
        pipeline_service.create_pipeline_with_definition.return_value = "pipe-new"
        register_pipeline_tools(mcp, pipeline_service, workspace_service, item_service)
        result = tools["create_pipeline"](
            workspace_name="Workspace",
            pipeline_name="NewPipe",
            pipeline_file_path="/tmp/pipeline.json",
        )
        assert result["status"] == "success"
        assert result["pipeline_id"] == "pipe-new"
        pipeline_service._load_pipeline_from_file.assert_called_once_with(
            "/tmp/pipeline.json"
        )

    def test_create_pipeline_mutual_exclusion(self):
        tools, mcp = capture_tools()
        pipeline_service = Mock()
        workspace_service = Mock()
        item_service = Mock()
        register_pipeline_tools(mcp, pipeline_service, workspace_service, item_service)
        result = tools["create_pipeline"](
            workspace_name="Workspace",
            pipeline_name="Pipe",
            pipeline_content_json={"properties": {}},
            pipeline_file_path="/tmp/pipeline.json",
        )
        assert result["status"] == "error"

    def test_update_pipeline_definition_from_file(self):
        tools, mcp = capture_tools()
        pipeline_service = Mock()
        workspace_service = Mock()
        item_service = Mock()
        workspace_service.resolve_workspace_id.return_value = "ws-1"
        workspace_service.get_workspace_by_id.return_value = SimpleNamespace(
            display_name="Workspace"
        )
        item_service.get_item_by_name.return_value = FabricItem(
            id="pipe-1",
            display_name="Pipe",
            type="DataPipeline",
            workspace_id="ws-1",
        )
        pipeline_service._load_pipeline_from_file.return_value = {
            "properties": {"activities": []}
        }
        pipeline_service.update_pipeline_definition.return_value = None
        register_pipeline_tools(mcp, pipeline_service, workspace_service, item_service)
        result = tools["update_pipeline_definition"](
            workspace_name="Workspace",
            pipeline_name="Pipe",
            pipeline_file_path="/tmp/pipeline.json",
        )
        assert result["status"] == "success"
        pipeline_service._load_pipeline_from_file.assert_called_once_with(
            "/tmp/pipeline.json"
        )

    def test_update_pipeline_definition_mutual_exclusion(self):
        tools, mcp = capture_tools()
        pipeline_service = Mock()
        workspace_service = Mock()
        item_service = Mock()
        register_pipeline_tools(mcp, pipeline_service, workspace_service, item_service)
        result = tools["update_pipeline_definition"](
            workspace_name="Workspace",
            pipeline_name="Pipe",
            pipeline_content_json={"properties": {}},
            pipeline_file_path="/tmp/pipeline.json",
        )
        assert result["status"] == "error"

    def test_update_pipeline_definition_neither_provided(self):
        tools, mcp = capture_tools()
        pipeline_service = Mock()
        workspace_service = Mock()
        item_service = Mock()
        register_pipeline_tools(mcp, pipeline_service, workspace_service, item_service)
        result = tools["update_pipeline_definition"](
            workspace_name="Workspace", pipeline_name="Pipe"
        )
        assert result["status"] == "error"
