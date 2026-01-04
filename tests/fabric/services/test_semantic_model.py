"""Unit tests for FabricSemanticModelService."""

import base64
import json
from unittest.mock import Mock

import pytest

from ms_fabric_mcp_server.client.exceptions import FabricValidationError
from ms_fabric_mcp_server.models.item import FabricItem
from ms_fabric_mcp_server.models.semantic_model import SemanticModelColumn, DataType
from ms_fabric_mcp_server.services.semantic_model import FabricSemanticModelService


def _encode(definition: dict) -> str:
    return base64.b64encode(json.dumps(definition).encode("utf-8")).decode("utf-8")


def _decode(payload: str) -> dict:
    return json.loads(base64.b64decode(payload).decode("utf-8"))


@pytest.mark.unit
class TestFabricSemanticModelService:
    @pytest.fixture
    def mock_workspace_service(self):
        return Mock()

    @pytest.fixture
    def mock_item_service(self):
        return Mock()

    @pytest.fixture
    def semantic_model_service(self, mock_workspace_service, mock_item_service):
        return FabricSemanticModelService(mock_workspace_service, mock_item_service)

    def test_create_semantic_model_success(
        self, semantic_model_service, mock_workspace_service, mock_item_service
    ):
        mock_workspace_service.resolve_workspace_id.return_value = "ws-1"
        mock_item_service.create_item.return_value = FabricItem(
            id="sm-1",
            display_name="Model",
            type="SemanticModel",
            workspace_id="ws-1",
        )

        result = semantic_model_service.create_semantic_model("ws", "Model")

        assert result.id == "sm-1"
        args, kwargs = mock_item_service.create_item.call_args
        item_definition = args[1]
        assert item_definition["type"] == "SemanticModel"
        parts = item_definition["definition"]["parts"]
        pbism = _decode(parts[0]["payload"])
        bim = _decode(parts[1]["payload"])
        assert pbism["version"] == "4.2"
        assert bim["compatibilityLevel"] == 1604

    def test_add_table_to_semantic_model_success(
        self, semantic_model_service, mock_workspace_service, mock_item_service
    ):
        mock_workspace_service.resolve_workspace_id.return_value = "ws-1"
        mock_item_service.get_item_by_name.side_effect = [
            FabricItem(id="sm-1", display_name="Model", type="SemanticModel", workspace_id="ws-1"),
            FabricItem(id="lh-1", display_name="Lake", type="Lakehouse", workspace_id="ws-1"),
        ]
        definition = {
            "definition": {
                "parts": [
                    {"path": "definition.pbism", "payload": _encode({"version": "4.2"}), "payloadType": "InlineBase64"},
                    {"path": "model.bim", "payload": _encode({"model": {}}), "payloadType": "InlineBase64"},
                ]
            }
        }
        mock_item_service.get_item_definition.return_value = definition

        columns = [
            SemanticModelColumn(name="id", data_type=DataType.INT64),
            SemanticModelColumn(name="name", data_type=DataType.STRING),
        ]

        semantic_model_service.add_table_to_semantic_model(
            workspace_name="Workspace",
            semantic_model_name="Model",
            lakehouse_name="Lake",
            table_name="Customers",
            columns=columns,
        )

        args, kwargs = mock_item_service.update_item_definition.call_args
        update_payload = args[2]
        model_payload = update_payload["definition"]["parts"][1]["payload"]
        bim = _decode(model_payload)
        model = bim["model"]
        assert any(expr["name"] == "DirectLake - Lake" for expr in model["expressions"])
        table = next(t for t in model["tables"] if t["name"] == "Customers")
        assert len(table["columns"]) == 2
        assert table["columns"][0]["dataType"] == "int64"

    def test_add_table_to_semantic_model_duplicate_table(
        self, semantic_model_service, mock_workspace_service, mock_item_service
    ):
        mock_workspace_service.resolve_workspace_id.return_value = "ws-1"
        mock_item_service.get_item_by_name.side_effect = [
            FabricItem(id="sm-1", display_name="Model", type="SemanticModel", workspace_id="ws-1"),
            FabricItem(id="lh-1", display_name="Lake", type="Lakehouse", workspace_id="ws-1"),
        ]
        definition = {
            "definition": {
                "parts": [
                    {"path": "definition.pbism", "payload": _encode({"version": "4.2"}), "payloadType": "InlineBase64"},
                    {"path": "model.bim", "payload": _encode({"model": {"tables": [{"name": "Customers"}]}}), "payloadType": "InlineBase64"},
                ]
            }
        }
        mock_item_service.get_item_definition.return_value = definition

        columns = [SemanticModelColumn(name="id", data_type=DataType.INT64)]

        with pytest.raises(FabricValidationError):
            semantic_model_service.add_table_to_semantic_model(
                workspace_name="Workspace",
                semantic_model_name="Model",
                lakehouse_name="Lake",
                table_name="Customers",
                columns=columns,
            )

    def test_add_relationship_to_semantic_model_success(
        self, semantic_model_service, mock_workspace_service, mock_item_service
    ):
        mock_workspace_service.resolve_workspace_id.return_value = "ws-1"
        mock_item_service.get_item_by_name.return_value = FabricItem(
            id="sm-1",
            display_name="Model",
            type="SemanticModel",
            workspace_id="ws-1",
        )
        definition = {
            "definition": {
                "parts": [
                    {"path": "definition.pbism", "payload": _encode({"version": "4.2"}), "payloadType": "InlineBase64"},
                    {"path": "model.bim", "payload": _encode({"model": {}}), "payloadType": "InlineBase64"},
                ]
            }
        }
        mock_item_service.get_item_definition.return_value = definition

        semantic_model_service.add_relationships_to_semantic_model(
            workspace_name="Workspace",
            semantic_model_name="Model",
            from_table="Orders",
            from_column="CustomerId",
            to_table="Customers",
            to_column="Id",
            cardinality="manyToOne",
            cross_filter_direction="oneDirection",
            is_active=True,
        )

        args, kwargs = mock_item_service.update_item_definition.call_args
        update_payload = args[2]
        model_payload = update_payload["definition"]["parts"][1]["payload"]
        bim = _decode(model_payload)
        rel = bim["model"]["relationships"][0]
        assert rel["fromCardinality"] == "many"
        assert rel["toCardinality"] == "one"
        assert rel["crossFilteringBehavior"] == "oneDirection"
        assert rel["isActive"] is True

    def test_add_relationship_to_semantic_model_invalid_params(
        self, semantic_model_service, mock_workspace_service, mock_item_service
    ):
        mock_workspace_service.resolve_workspace_id.return_value = "ws-1"
        mock_item_service.get_item_by_name.return_value = FabricItem(
            id="sm-1",
            display_name="Model",
            type="SemanticModel",
            workspace_id="ws-1",
        )
        definition = {
            "definition": {
                "parts": [
                    {"path": "definition.pbism", "payload": _encode({"version": "4.2"}), "payloadType": "InlineBase64"},
                    {"path": "model.bim", "payload": _encode({"model": {}}), "payloadType": "InlineBase64"},
                ]
            }
        }
        mock_item_service.get_item_definition.return_value = definition

        with pytest.raises(FabricValidationError):
            semantic_model_service.add_relationships_to_semantic_model(
                workspace_name="Workspace",
                semantic_model_name="Model",
                from_table="Orders",
                from_column="CustomerId",
                to_table="Customers",
                to_column="Id",
                cardinality="invalid",
                cross_filter_direction="oneDirection",
                is_active=True,
            )
