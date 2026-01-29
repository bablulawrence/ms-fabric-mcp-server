"""Tests for semantic model MCP tools."""

from unittest.mock import Mock

import pytest

from ms_fabric_mcp_server.models.item import FabricItem
from ms_fabric_mcp_server.models.semantic_model import SemanticModelColumn, SemanticModelMeasure, DataType
from ms_fabric_mcp_server.services.semantic_model import SemanticModelReference
from ms_fabric_mcp_server.tools.semantic_model_tools import register_semantic_model_tools
from tests.fabric.tools.utils import capture_tools


@pytest.mark.unit
class TestSemanticModelTools:
    def test_semantic_model_tools_smoke(self):
        tools, mcp = capture_tools()
        semantic_service = Mock()
        semantic_service.create_semantic_model.return_value = SemanticModelReference(
            workspace_id="ws-1", id="sm-1"
        )
        semantic_service.add_table_to_semantic_model.return_value = SemanticModelReference(
            workspace_id="ws-1", id="sm-1"
        )
        semantic_service.add_measures_to_semantic_model.return_value = SemanticModelReference(
            workspace_id="ws-1", id="sm-1"
        )
        semantic_service.delete_measures_from_semantic_model.return_value = SemanticModelReference(
            workspace_id="ws-1", id="sm-1"
        )
        semantic_service.get_semantic_model_details.return_value = FabricItem(
            id="sm-1",
            display_name="Model",
            type="SemanticModel",
            workspace_id="ws-1",
        )
        semantic_service.get_semantic_model_definition.return_value = (
            FabricItem(
                id="sm-1",
                display_name="Model",
                type="SemanticModel",
                workspace_id="ws-1",
            ),
            {"definition": {}},
        )
        semantic_service.add_relationship_to_semantic_model.return_value = SemanticModelReference(
            workspace_id="ws-1", id="sm-1"
        )

        register_semantic_model_tools(mcp, semantic_service)

        result = tools["create_semantic_model"](
            workspace_name="Workspace",
            semantic_model_name="Model",
        )
        assert result["status"] == "success"

        columns = [SemanticModelColumn(name="id", data_type=DataType.INT64)]
        assert tools["add_table_to_semantic_model"](
            workspace_name="Workspace",
            semantic_model_name="Model",
            lakehouse_name="Lakehouse",
            table_name="Table",
            columns=columns,
        )["status"] == "success"

        measures = [SemanticModelMeasure(name="m1", expression="SUM(Table[id])")]
        assert tools["add_measures_to_semantic_model"](
            workspace_name="Workspace",
            table_name="Table",
            measures=measures,
            semantic_model_name="Model",
        )["status"] == "success"

        assert tools["delete_measures_from_semantic_model"](
            workspace_name="Workspace",
            table_name="Table",
            measure_names=["m1"],
            semantic_model_name="Model",
        )["status"] == "success"

        assert tools["get_semantic_model_details"](
            workspace_name="Workspace",
            semantic_model_name="Model",
        )["status"] == "success"

        assert tools["get_semantic_model_definition"](
            workspace_name="Workspace",
            semantic_model_name="Model",
        )["status"] == "success"

        assert tools["add_relationship_to_semantic_model"](
            workspace_name="Workspace",
            semantic_model_name="Model",
            from_table="A",
            from_column="id",
            to_table="B",
            to_column="id",
        )["status"] == "success"
