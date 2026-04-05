"""Tests for semantic model MCP tools."""

from unittest.mock import Mock

import pytest

from ms_fabric_mcp_server.models.item import FabricItem
from ms_fabric_mcp_server.models.semantic_model import (DataType,
                                                        SemanticModelColumn,
                                                        SemanticModelMeasure)
from ms_fabric_mcp_server.services.semantic_model import SemanticModelReference
from ms_fabric_mcp_server.tools.semantic_model_tools import \
    register_semantic_model_tools
from tests.fabric.tools.utils import capture_tools


@pytest.mark.unit
class TestSemanticModelTools:
    def test_semantic_model_tools_smoke(self):
        tools, mcp = capture_tools()
        semantic_service = Mock()
        semantic_service.create_semantic_model.return_value = SemanticModelReference(
            workspace_id="ws-1", id="sm-1"
        )
        semantic_service.add_table_to_semantic_model.return_value = (
            SemanticModelReference(workspace_id="ws-1", id="sm-1")
        )
        semantic_service.add_measures_to_semantic_model.return_value = (
            SemanticModelReference(workspace_id="ws-1", id="sm-1")
        )
        semantic_service.delete_measures_from_semantic_model.return_value = (
            SemanticModelReference(workspace_id="ws-1", id="sm-1")
        )
        semantic_service.delete_table_from_semantic_model.return_value = (
            SemanticModelReference(workspace_id="ws-1", id="sm-1"),
            1,
        )
        semantic_service.delete_relationship_from_semantic_model.return_value = (
            SemanticModelReference(workspace_id="ws-1", id="sm-1"),
            1,
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
        semantic_service.add_relationship_to_semantic_model.return_value = (
            SemanticModelReference(workspace_id="ws-1", id="sm-1")
        )

        register_semantic_model_tools(mcp, semantic_service)

        result = tools["create_semantic_model"](
            workspace_name="Workspace",
            semantic_model_name="Model",
        )
        assert result["status"] == "success"

        columns = [SemanticModelColumn(name="id", data_type=DataType.INT64)]
        assert (
            tools["add_table_to_semantic_model"](
                workspace_name="Workspace",
                semantic_model_name="Model",
                lakehouse_name="Lakehouse",
                table_name="Table",
                columns=columns,
                table_schema="gold",
                model_table_name="FactSales",
            )["status"]
            == "success"
        )

        measures = [SemanticModelMeasure(name="m1", expression="SUM(Table[id])")]
        assert (
            tools["add_measures_to_semantic_model"](
                workspace_name="Workspace",
                table_name="Table",
                measures=measures,
                semantic_model_name="Model",
            )["status"]
            == "success"
        )

        assert (
            tools["delete_measures_from_semantic_model"](
                workspace_name="Workspace",
                table_name="Table",
                measure_names=["m1"],
                semantic_model_name="Model",
            )["status"]
            == "success"
        )

        assert (
            tools["delete_table_from_semantic_model"](
                workspace_name="Workspace",
                table_name="Table",
                semantic_model_name="Model",
            )["status"]
            == "success"
        )

        assert (
            tools["delete_relationship_from_semantic_model"](
                workspace_name="Workspace",
                semantic_model_name="Model",
                relationship_name="rel-1",
            )["status"]
            == "success"
        )

        assert (
            tools["get_semantic_model_details"](
                workspace_name="Workspace",
                semantic_model_name="Model",
            )["status"]
            == "success"
        )

        assert (
            tools["get_semantic_model_definition"](
                workspace_name="Workspace",
                semantic_model_name="Model",
            )["status"]
            == "success"
        )

        assert (
            tools["add_relationship_to_semantic_model"](
                workspace_name="Workspace",
                semantic_model_name="Model",
                from_table="A",
                from_column="id",
                to_table="B",
                to_column="id",
            )["status"]
            == "success"
        )

    def test_get_semantic_model_definition_save_to_path(self):
        tools, mcp = capture_tools()
        semantic_service = Mock()
        semantic_service.get_semantic_model_definition.return_value = (
            FabricItem(
                id="sm-1",
                display_name="Model",
                type="SemanticModel",
                workspace_id="ws-1",
            ),
            {"file_path": "/tmp/model.json", "size_bytes": 4567},
        )
        register_semantic_model_tools(mcp, semantic_service)
        result = tools["get_semantic_model_definition"](
            workspace_name="Workspace",
            semantic_model_name="Model",
            save_to_path="/tmp/model.json",
        )
        assert result["status"] == "success"
        assert result["file_path"] == "/tmp/model.json"
        assert result["size_bytes"] == 4567
        assert "definition" not in result

    def test_update_semantic_model_definition_inline(self):
        tools, mcp = capture_tools()
        semantic_service = Mock()
        semantic_service.update_semantic_model_definition.return_value = (
            SemanticModelReference(workspace_id="ws-1", id="sm-1")
        )
        register_semantic_model_tools(mcp, semantic_service)
        result = tools["update_semantic_model_definition"](
            workspace_name="Workspace",
            semantic_model_name="Model",
            definition={"definition": {"parts": []}},
        )
        assert result["status"] == "success"
        assert result["semantic_model_id"] == "sm-1"

    def test_update_semantic_model_definition_from_file(self):
        tools, mcp = capture_tools()
        semantic_service = Mock()
        semantic_service._load_definition_from_file.return_value = {
            "definition": {"parts": []}
        }
        semantic_service.update_semantic_model_definition.return_value = (
            SemanticModelReference(workspace_id="ws-1", id="sm-1")
        )
        register_semantic_model_tools(mcp, semantic_service)
        result = tools["update_semantic_model_definition"](
            workspace_name="Workspace",
            semantic_model_name="Model",
            definition_file_path="/tmp/model.json",
        )
        assert result["status"] == "success"
        semantic_service._load_definition_from_file.assert_called_once_with(
            "/tmp/model.json"
        )

    def test_update_semantic_model_definition_mutual_exclusion(self):
        tools, mcp = capture_tools()
        semantic_service = Mock()
        register_semantic_model_tools(mcp, semantic_service)
        result = tools["update_semantic_model_definition"](
            workspace_name="Workspace",
            semantic_model_name="Model",
            definition={"definition": {}},
            definition_file_path="/tmp/model.json",
        )
        assert result["status"] == "error"

    def test_update_semantic_model_definition_neither_provided(self):
        tools, mcp = capture_tools()
        semantic_service = Mock()
        register_semantic_model_tools(mcp, semantic_service)
        result = tools["update_semantic_model_definition"](
            workspace_name="Workspace", semantic_model_name="Model"
        )
        assert result["status"] == "error"
