"""Unit tests for FabricSemanticModelService."""

import base64
import json
from unittest.mock import Mock

import pytest

from ms_fabric_mcp_server.client.exceptions import FabricValidationError
from ms_fabric_mcp_server.models.item import FabricItem
from ms_fabric_mcp_server.models.semantic_model import (
    SemanticModelColumn,
    SemanticModelMeasure,
    DataType,
)
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
        mock_item_service.resolve_folder_id_from_path.return_value = "folder-1"
        mock_item_service.create_item.return_value = FabricItem(
            id="sm-1",
            display_name="Model",
            type="SemanticModel",
            workspace_id="ws-1",
        )

        result = semantic_model_service.create_semantic_model(
            "ws", "Model", folder_path="Models/Finance"
        )

        assert result.id == "sm-1"
        args, kwargs = mock_item_service.create_item.call_args
        item_definition = args[1]
        assert item_definition["type"] == "SemanticModel"
        assert item_definition["folderId"] == "folder-1"
        parts = item_definition["definition"]["parts"]
        pbism = _decode(parts[0]["payload"])
        bim = _decode(parts[1]["payload"])
        assert pbism["version"] == "4.2"
        assert bim["compatibilityLevel"] == 1604

    def test_create_semantic_model_invalid_name(self, semantic_model_service):
        """Names with separators raise validation error."""
        with pytest.raises(FabricValidationError):
            semantic_model_service.create_semantic_model("ws", "Bad/Name")

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

    def test_add_table_to_semantic_model_with_schema(
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
            table_name="fact_table",
            columns=columns,
            table_schema="gold",
            model_table_name="FactSales",
        )

        args, kwargs = mock_item_service.update_item_definition.call_args
        update_payload = args[2]
        model_payload = update_payload["definition"]["parts"][1]["payload"]
        bim = _decode(model_payload)
        model = bim["model"]
        table = next(t for t in model["tables"] if t["name"] == "FactSales")
        partition_source = table["partitions"][0]["source"]
        assert partition_source["entityName"] == "fact_table"
        assert partition_source["schemaName"] == "gold"
        assert partition_source["expressionSource"] == "DirectLake - Lake"

    def test_add_table_to_semantic_model_schema_requires_unqualified_table(
        self, semantic_model_service
    ):
        columns = [SemanticModelColumn(name="id", data_type=DataType.INT64)]
        with pytest.raises(FabricValidationError):
            semantic_model_service.add_table_to_semantic_model(
                workspace_name="Workspace",
                semantic_model_name="Model",
                lakehouse_name="Lake",
                table_name="gold.fact_table",
                columns=columns,
                table_schema="gold",
            )

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

    def test_get_semantic_model_details_by_name(
        self, semantic_model_service, mock_workspace_service, mock_item_service
    ):
        mock_workspace_service.resolve_workspace_id.return_value = "ws-1"
        mock_item_service.get_item_by_name.return_value = FabricItem(
            id="sm-1",
            display_name="Model",
            type="SemanticModel",
            workspace_id="ws-1",
        )

        result = semantic_model_service.get_semantic_model_details(
            workspace_name="Workspace",
            semantic_model_name="Model",
        )

        assert result.id == "sm-1"
        mock_item_service.get_item_by_name.assert_called_once_with(
            "ws-1", "Model", "SemanticModel"
        )

    def test_get_semantic_model_definition_by_name(
        self, semantic_model_service, mock_workspace_service, mock_item_service
    ):
        mock_workspace_service.resolve_workspace_id.return_value = "ws-1"
        mock_item_service.get_item_by_name.return_value = FabricItem(
            id="sm-1",
            display_name="Model",
            type="SemanticModel",
            workspace_id="ws-1",
        )
        definition = {"definition": {"parts": []}}
        mock_item_service.get_item_definition.return_value = definition

        semantic_model, result = semantic_model_service.get_semantic_model_definition(
            workspace_name="Workspace",
            semantic_model_name="Model",
            format="TMSL",
        )

        assert semantic_model.id == "sm-1"
        assert result == definition
        mock_item_service.get_item_definition.assert_called_once_with(
            "ws-1", "sm-1", format="TMSL"
        )

    def test_get_semantic_model_definition_invalid_format(
        self, semantic_model_service, mock_workspace_service, mock_item_service
    ):
        mock_workspace_service.resolve_workspace_id.return_value = "ws-1"
        mock_item_service.get_item_by_name.return_value = FabricItem(
            id="sm-1",
            display_name="Model",
            type="SemanticModel",
            workspace_id="ws-1",
        )

        with pytest.raises(FabricValidationError):
            semantic_model_service.get_semantic_model_definition(
                workspace_name="Workspace",
                semantic_model_name="Model",
                format="invalid",
            )

    def test_decode_model_bim(self, semantic_model_service):
        definition = {
            "definition": {
                "parts": [
                    {
                        "path": "model.bim",
                        "payload": _encode({"model": {"tables": []}}),
                        "payloadType": "InlineBase64",
                    }
                ]
            }
        }

        result = semantic_model_service.decode_model_bim(definition)

        assert result == {"model": {"tables": []}}

    def test_add_measures_to_semantic_model_success(
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
                    {"path": "model.bim", "payload": _encode({"model": {"tables": [{"name": "Sales"}]}}), "payloadType": "InlineBase64"},
                ]
            }
        }
        mock_item_service.get_item_definition.return_value = definition

        measures = [
            SemanticModelMeasure(
                name="Total Sales",
                expression="SUM(Sales[Amount])",
                format_string="0.00",
                display_folder="Sales Metrics",
                description="Total sales amount",
            )
        ]

        semantic_model_service.add_measures_to_semantic_model(
            workspace_name="Workspace",
            semantic_model_name="Model",
            semantic_model_id=None,
            table_name="Sales",
            measures=measures,
        )

        args, kwargs = mock_item_service.update_item_definition.call_args
        update_payload = args[2]
        model_payload = update_payload["definition"]["parts"][1]["payload"]
        bim = _decode(model_payload)
        table = next(t for t in bim["model"]["tables"] if t["name"] == "Sales")
        added = table["measures"][0]
        assert added["name"] == "Total Sales"
        assert added["expression"] == "SUM(Sales[Amount])"
        assert added["formatString"] == "0.00"
        assert added["displayFolder"] == "Sales Metrics"
        assert added["description"] == "Total sales amount"

    def test_add_measures_duplicate_name_raises(
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
                    {
                        "path": "model.bim",
                        "payload": _encode({"model": {"tables": [{"name": "Sales", "measures": [{"name": "Total Sales"}]}]}}),
                        "payloadType": "InlineBase64",
                    },
                ]
            }
        }
        mock_item_service.get_item_definition.return_value = definition

        measures = [SemanticModelMeasure(name="Total Sales", expression="1")]

        with pytest.raises(FabricValidationError):
            semantic_model_service.add_measures_to_semantic_model(
                workspace_name="Workspace",
                semantic_model_name="Model",
                semantic_model_id=None,
                table_name="Sales",
                measures=measures,
            )

    def test_delete_measures_success(
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
                    {
                        "path": "model.bim",
                        "payload": _encode({"model": {"tables": [{"name": "Sales", "measures": [{"name": "Total Sales"}, {"name": "Count Sales"}]}]}}),
                        "payloadType": "InlineBase64",
                    },
                ]
            }
        }
        mock_item_service.get_item_definition.return_value = definition

        semantic_model_service.delete_measures_from_semantic_model(
            workspace_name="Workspace",
            semantic_model_name="Model",
            semantic_model_id=None,
            table_name="Sales",
            measure_names=["Total Sales"],
        )

        args, kwargs = mock_item_service.update_item_definition.call_args
        update_payload = args[2]
        model_payload = update_payload["definition"]["parts"][1]["payload"]
        bim = _decode(model_payload)
        table = next(t for t in bim["model"]["tables"] if t["name"] == "Sales")
        remaining = [m["name"] for m in table["measures"]]
        assert remaining == ["Count Sales"]

    def test_delete_measures_missing_raises(
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
                    {"path": "model.bim", "payload": _encode({"model": {"tables": [{"name": "Sales", "measures": [{"name": "Total Sales"}]}]}}), "payloadType": "InlineBase64"},
                ]
            }
        }
        mock_item_service.get_item_definition.return_value = definition

        with pytest.raises(FabricValidationError):
            semantic_model_service.delete_measures_from_semantic_model(
                workspace_name="Workspace",
                semantic_model_name="Model",
                semantic_model_id=None,
                table_name="Sales",
                measure_names=["Missing Measure"],
            )

    def test_list_semantic_model_tables(
        self, semantic_model_service, mock_workspace_service, mock_item_service
    ):
        mock_workspace_service.resolve_workspace_id.return_value = "ws-1"
        mock_item_service.get_item_by_name.return_value = FabricItem(
            id="sm-1", display_name="Model", type="SemanticModel", workspace_id="ws-1"
        )
        definition = {
            "definition": {
                "parts": [
                    {"path": "definition.pbism", "payload": _encode({"version": "4.2"}), "payloadType": "InlineBase64"},
                    {
                        "path": "model.bim",
                        "payload": _encode(
                            {
                                "model": {
                                    "tables": [
                                        {
                                            "name": "Customers",
                                            "columns": [
                                                {"name": "id", "dataType": "int64"},
                                                {"name": "name", "dataType": "string"},
                                            ],
                                        }
                                    ]
                                }
                            }
                        ),
                        "payloadType": "InlineBase64",
                    },
                ]
            }
        }
        mock_item_service.get_item_definition.return_value = definition

        tables = semantic_model_service.list_semantic_model_tables(
            workspace_name="Workspace",
            semantic_model_name="Model",
        )

        assert tables == [
            {
                "name": "Customers",
                "columns": [
                    {"name": "id", "data_type": "int64"},
                    {"name": "name", "data_type": "string"},
                ],
            }
        ]

    def test_list_semantic_model_relationships(
        self, semantic_model_service, mock_workspace_service, mock_item_service
    ):
        mock_workspace_service.resolve_workspace_id.return_value = "ws-1"
        mock_item_service.get_item_by_name.return_value = FabricItem(
            id="sm-1", display_name="Model", type="SemanticModel", workspace_id="ws-1"
        )
        definition = {
            "definition": {
                "parts": [
                    {"path": "definition.pbism", "payload": _encode({"version": "4.2"}), "payloadType": "InlineBase64"},
                    {
                        "path": "model.bim",
                        "payload": _encode(
                            {
                                "model": {
                                    "relationships": [
                                        {
                                            "name": "rel-1",
                                            "fromTable": "fact",
                                            "fromColumn": "id",
                                            "toTable": "dim",
                                            "toColumn": "id",
                                            "cardinality": "manyToOne",
                                            "crossFilteringBehavior": "oneDirection",
                                            "isActive": True,
                                        }
                                    ]
                                }
                            }
                        ),
                        "payloadType": "InlineBase64",
                    },
                ]
            }
        }
        mock_item_service.get_item_definition.return_value = definition

        relationships = semantic_model_service.list_semantic_model_relationships(
            workspace_name="Workspace",
            semantic_model_name="Model",
        )

        assert relationships == [
            {
                "name": "rel-1",
                "from_table": "fact",
                "from_column": "id",
                "to_table": "dim",
                "to_column": "id",
                "cardinality": "manyToOne",
                "cross_filter_direction": "oneDirection",
                "is_active": True,
            }
        ]

    def test_delete_table_from_semantic_model_removes_relationships(
        self, semantic_model_service, mock_workspace_service, mock_item_service
    ):
        mock_workspace_service.resolve_workspace_id.return_value = "ws-1"
        mock_item_service.get_item_by_name.return_value = FabricItem(
            id="sm-1", display_name="Model", type="SemanticModel", workspace_id="ws-1"
        )
        definition = {
            "definition": {
                "parts": [
                    {"path": "definition.pbism", "payload": _encode({"version": "4.2"}), "payloadType": "InlineBase64"},
                    {
                        "path": "model.bim",
                        "payload": _encode(
                            {
                                "model": {
                                    "tables": [
                                        {"name": "fact", "columns": []},
                                        {"name": "dim", "columns": []},
                                    ],
                                    "relationships": [
                                        {
                                            "name": "rel-1",
                                            "fromTable": "fact",
                                            "fromColumn": "id",
                                            "toTable": "dim",
                                            "toColumn": "id",
                                        }
                                    ],
                                }
                            }
                        ),
                        "payloadType": "InlineBase64",
                    },
                ]
            }
        }
        mock_item_service.get_item_definition.return_value = definition

        _, removed = semantic_model_service.delete_table_from_semantic_model(
            workspace_name="Workspace",
            semantic_model_name="Model",
            semantic_model_id=None,
            table_name="fact",
            remove_relationships=True,
        )

        assert removed == 1
        args, kwargs = mock_item_service.update_item_definition.call_args
        update_payload = args[2]
        model_payload = update_payload["definition"]["parts"][1]["payload"]
        bim = _decode(model_payload)
        model = bim["model"]
        assert [t["name"] for t in model["tables"]] == ["dim"]
        assert model["relationships"] == []

    def test_delete_relationship_by_name(
        self, semantic_model_service, mock_workspace_service, mock_item_service
    ):
        mock_workspace_service.resolve_workspace_id.return_value = "ws-1"
        mock_item_service.get_item_by_name.return_value = FabricItem(
            id="sm-1", display_name="Model", type="SemanticModel", workspace_id="ws-1"
        )
        definition = {
            "definition": {
                "parts": [
                    {"path": "definition.pbism", "payload": _encode({"version": "4.2"}), "payloadType": "InlineBase64"},
                    {
                        "path": "model.bim",
                        "payload": _encode(
                            {
                                "model": {
                                    "relationships": [
                                        {
                                            "name": "rel-1",
                                            "fromTable": "a",
                                            "fromColumn": "id",
                                            "toTable": "b",
                                            "toColumn": "id",
                                        }
                                    ]
                                }
                            }
                        ),
                        "payloadType": "InlineBase64",
                    },
                ]
            }
        }
        mock_item_service.get_item_definition.return_value = definition

        _, removed = semantic_model_service.delete_relationship_from_semantic_model(
            workspace_name="Workspace",
            semantic_model_name="Model",
            semantic_model_id=None,
            relationship_name="rel-1",
        )

        assert removed == 1
        args, kwargs = mock_item_service.update_item_definition.call_args
        update_payload = args[2]
        model_payload = update_payload["definition"]["parts"][1]["payload"]
        bim = _decode(model_payload)
        assert bim["model"]["relationships"] == []

    def test_delete_relationship_by_keys_validation(self, semantic_model_service):
        with pytest.raises(FabricValidationError):
            semantic_model_service.delete_relationship_from_semantic_model(
                workspace_name="Workspace",
                semantic_model_name="Model",
                semantic_model_id=None,
            )
