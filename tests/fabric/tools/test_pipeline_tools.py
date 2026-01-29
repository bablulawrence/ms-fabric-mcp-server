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

        register_pipeline_tools(mcp, pipeline_service, workspace_service, item_service)

        assert tools["create_blank_pipeline"](
            workspace_name="Workspace", pipeline_name="Pipe"
        )["status"] == "success"

        assert tools["add_copy_activity_to_pipeline"](
            workspace_name="Workspace",
            pipeline_name="Pipe",
            source_type="AzureSqlSource",
            source_connection_id="conn-1",
            source_table_schema="dbo",
            source_table_name="table",
            destination_lakehouse_id="lh-1",
            destination_connection_id="lh-conn",
            destination_table_name="table",
        )["status"] == "success"

        assert tools["add_notebook_activity_to_pipeline"](
            workspace_name="Workspace",
            pipeline_name="Pipe",
            notebook_name="Notebook",
        )["status"] == "success"

        assert tools["add_dataflow_activity_to_pipeline"](
            workspace_name="Workspace",
            pipeline_name="Pipe",
            dataflow_name="Dataflow",
        )["status"] == "success"

        assert tools["add_activity_to_pipeline"](
            workspace_name="Workspace",
            pipeline_name="Pipe",
            activity_json={"name": "Wait", "type": "Wait", "typeProperties": {}},
        )["status"] == "success"

        assert tools["create_pipeline_with_definition"](
            workspace_name="Workspace",
            display_name="Pipe2",
            pipeline_content_json={"properties": {"activities": []}},
        )["status"] == "success"

        assert tools["get_pipeline_definition"](
            workspace_name="Workspace",
            pipeline_name="Pipe",
        )["status"] == "success"

        assert tools["update_pipeline_definition"](
            workspace_name="Workspace",
            pipeline_name="Pipe",
            pipeline_content_json={"properties": {"activities": []}},
        )["status"] == "success"

        assert tools["add_activity_dependency"](
            workspace_name="Workspace",
            pipeline_name="Pipe",
            activity_name="A1",
            depends_on=["A0"],
        )["status"] == "success"

        assert tools["delete_activity_from_pipeline"](
            workspace_name="Workspace",
            pipeline_name="Pipe",
            activity_name="A1",
        )["status"] == "success"

        assert tools["remove_activity_dependency"](
            workspace_name="Workspace",
            pipeline_name="Pipe",
            activity_name="A1",
        )["status"] == "success"
