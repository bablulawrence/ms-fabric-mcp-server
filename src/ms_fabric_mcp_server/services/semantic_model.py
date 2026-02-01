# ABOUTME: Service for Fabric semantic model operations.
# ABOUTME: Provides TMSL-based creation and updates for semantic models.
"""Service for Microsoft Fabric semantic model operations."""

import base64
import json
import logging
import uuid
import time
from typing import Any, Dict, List, Optional, Tuple

from ms_fabric_mcp_server.client.exceptions import (
    FabricAPIError,
    FabricError,
    FabricValidationError,
)
from ms_fabric_mcp_server.models.semantic_model import (
    SemanticModelColumn,
    SemanticModelMeasure,
)
from ms_fabric_mcp_server.services.item import FabricItemService
from ms_fabric_mcp_server.services.workspace import FabricWorkspaceService

logger = logging.getLogger(__name__)


class SemanticModelReference:
    """Lightweight reference to a semantic model."""

    def __init__(self, workspace_id: str, id: str):
        self.workspace_id = workspace_id
        self.id = id


class FabricSemanticModelService:
    """Service for Microsoft Fabric semantic model operations."""

    def __init__(
        self,
        workspace_service: FabricWorkspaceService,
        item_service: FabricItemService,
    ):
        self.workspace_service = workspace_service
        self.item_service = item_service
        logger.debug("FabricSemanticModelService initialized")

    def create_semantic_model(
        self,
        workspace_name: str,
        semantic_model_name: str,
        folder_path: Optional[str] = None,
    ) -> SemanticModelReference:
        """Create an empty Fabric semantic model."""
        logger.info(
            f"Creating semantic model '{semantic_model_name}' in workspace {workspace_name}"
        )

        if not semantic_model_name or not semantic_model_name.strip():
            raise FabricValidationError(
                "semantic_model_name",
                semantic_model_name,
                "Semantic model name cannot be empty",
            )
        if "/" in semantic_model_name or "\\" in semantic_model_name:
            raise FabricValidationError(
                "semantic_model_name",
                semantic_model_name,
                "Semantic model name cannot include path separators. Use folder_path instead.",
            )

        try:
            workspace_id = self.workspace_service.resolve_workspace_id(workspace_name)
            folder_id = self.item_service.resolve_folder_id_from_path(
                workspace_id, folder_path, create_missing=True
            )

            item_definition = {
                "displayName": semantic_model_name,
                "type": "SemanticModel",
                "definition": {
                    "format": "TMSL",
                    "parts": [
                        {
                            "path": "definition.pbism",
                            "payloadType": "InlineBase64",
                            "payload": self._encode_definition(
                                {
                                    "$schema": "https://developer.microsoft.com/json-schemas/fabric/item/semanticModel/definitionProperties/1.0.0/schema.json",
                                    "version": "4.2",
                                }
                            ),
                        },
                        {
                            "path": "model.bim",
                            "payloadType": "InlineBase64",
                            "payload": self._encode_definition(
                                {
                                    "compatibilityLevel": 1604,
                                    "model": {
                                        "culture": "en-US",
                                        "dataAccessOptions": {
                                            "legacyRedirects": True,
                                            "returnErrorValuesAsNull": True,
                                        },
                                        "defaultPowerBIDataSourceVersion": "powerBI_V3",
                                    },
                                }
                            ),
                        },
                    ],
                },
            }
            if folder_id:
                item_definition["folderId"] = folder_id

            created_item = self.item_service.create_item(workspace_id, item_definition)

            logger.info(
                f"Successfully created semantic model with ID: {created_item.id}"
            )
            return SemanticModelReference(workspace_id, created_item.id)

        except FabricValidationError:
            raise
        except FabricAPIError:
            raise
        except Exception as exc:
            logger.error(f"Failed to create semantic model: {exc}")
            raise FabricError(f"Failed to create semantic model: {exc}")

    def add_table_to_semantic_model(
        self,
        workspace_name: str,
        semantic_model_name: str,
        lakehouse_name: str,
        table_name: str,
        columns: List[SemanticModelColumn],
        table_schema: Optional[str] = None,
        model_table_name: Optional[str] = None,
    ) -> SemanticModelReference:
        """Add a table from a lakehouse to an existing semantic model."""
        logger.info(
            f"Adding table '{table_name}' from lakehouse '{lakehouse_name}' to semantic model '{semantic_model_name}' in workspace '{workspace_name}'"
        )

        if not columns:
            raise FabricValidationError(
                "columns", "empty", "Columns list cannot be empty"
            )
        if table_schema is not None and not table_schema.strip():
            raise FabricValidationError(
                "table_schema", table_schema, "Table schema cannot be empty"
            )
        if model_table_name is not None and not model_table_name.strip():
            raise FabricValidationError(
                "model_table_name", model_table_name, "Model table name cannot be empty"
            )
        if table_schema and ("/" in table_name or "\\" in table_name or "." in table_name):
            raise FabricValidationError(
                "table_name",
                table_name,
                "Table name must be unqualified when table_schema is provided",
            )

        try:
            resolved_table_name = model_table_name or table_name
            workspace_id = self.workspace_service.resolve_workspace_id(workspace_name)
            semantic_model_id = self.item_service.get_item_by_name(
                workspace_id, semantic_model_name, "SemanticModel"
            ).id
            lakehouse_id = self.item_service.get_item_by_name(
                workspace_id, lakehouse_name, "Lakehouse"
            ).id
            definition = self._get_definition_with_retry(
                workspace_id, semantic_model_id, format="TMSL"
            )

            bim = self._get_bim(definition)
            model = bim.setdefault("model", {})
            expressions = model.setdefault("expressions", [])
            tables = model.setdefault("tables", [])

            expression_name = f"DirectLake - {lakehouse_name}"
            if not self._find_list_item(expressions, "name", expression_name):
                expressions.append(
                    {
                        "name": expression_name,
                        "expression": [
                            "let",
                            f'    Source = AzureStorage.DataLake("https://onelake.dfs.fabric.microsoft.com/{workspace_id}/{lakehouse_id}", [HierarchicalNavigation=true])',
                            "in",
                            "    Source",
                        ],
                        "kind": "m",
                        "lineageTag": str(uuid.uuid4()),
                    }
                )

            if self._find_list_item(tables, "name", resolved_table_name):
                raise FabricValidationError(
                    "model_table_name" if model_table_name else "table_name",
                    resolved_table_name,
                    f"Table '{resolved_table_name}' already exists in semantic model '{semantic_model_name}'",
                )

            partition_source = {
                "entityName": table_name,
                "expressionSource": expression_name,
                "type": "entity",
            }
            if table_schema:
                partition_source["schemaName"] = table_schema

            tables.append(
                {
                    "name": resolved_table_name,
                    "columns": [
                        {
                            "name": column.name,
                            "sourceColumn": column.name,
                            "dataType": column.data_type.value,
                            "sourceLineageTag": column.name,
                            "lineageTag": str(uuid.uuid4()),
                        }
                        for column in columns
                    ],
                    "partitions": [
                        {
                            "name": resolved_table_name,
                            "mode": "directLake",
                            "source": partition_source,
                        }
                    ],
                    "lineageTag": str(uuid.uuid4()),
                }
            )

            self._update_definition(workspace_id, semantic_model_id, definition, bim)
            return SemanticModelReference(workspace_id, semantic_model_id)

        except FabricValidationError:
            raise
        except FabricAPIError:
            raise
        except Exception as exc:
            logger.error(f"Failed to add table to semantic model: {exc}")
            raise FabricError(f"Failed to add table to semantic model: {exc}")

    def add_relationships_to_semantic_model(
        self,
        workspace_name: str,
        semantic_model_name: str,
        from_table: str,
        from_column: str,
        to_table: str,
        to_column: str,
        cardinality: str = "manyToOne",
        cross_filter_direction: str = "oneDirection",
        is_active: bool = True,
    ) -> SemanticModelReference:
        """Add a relationship to an existing semantic model."""
        logger.info(
            f"Adding relationship from '{from_table}.{from_column}' to '{to_table}.{to_column}' in semantic model '{semantic_model_name}'"
        )

        self._validate_relationship_params(cardinality, cross_filter_direction)

        workspace_id = self.workspace_service.resolve_workspace_id(workspace_name)
        semantic_model_id = self.item_service.get_item_by_name(
            workspace_id, semantic_model_name, "SemanticModel"
        ).id
        definition = self._get_definition_with_retry(
            workspace_id, semantic_model_id, format="TMSL"
        )
        bim = self._get_bim(definition)
        model = bim.setdefault("model", {})
        relationships = model.setdefault("relationships", [])

        if any(
            relationship.get("fromTable", "").lower() == from_table.lower()
            and relationship.get("fromColumn", "").lower() == from_column.lower()
            and relationship.get("toTable", "").lower() == to_table.lower()
            and relationship.get("toColumn", "").lower() == to_column.lower()
            for relationship in relationships
        ):
            raise FabricValidationError(
                "relationship",
                f"{from_table}.{from_column}->{to_table}.{to_column}",
                "Relationship already exists in semantic model",
            )

        from_cardinality, to_cardinality = self._map_cardinality(cardinality)
        relationships.append(
            {
                "name": str(uuid.uuid4()),
                "fromTable": from_table,
                "fromColumn": from_column,
                "toTable": to_table,
                "toColumn": to_column,
                "fromCardinality": from_cardinality,
                "toCardinality": to_cardinality,
                "crossFilteringBehavior": cross_filter_direction,
                "isActive": is_active,
            }
        )

        self._update_definition(workspace_id, semantic_model_id, definition, bim)
        return SemanticModelReference(workspace_id, semantic_model_id)

    def add_measures_to_semantic_model(
        self,
        workspace_name: str,
        semantic_model_name: Optional[str],
        semantic_model_id: Optional[str],
        table_name: str,
        measures: List[SemanticModelMeasure],
    ) -> SemanticModelReference:
        """Add measures to a table in an existing semantic model."""
        if not measures:
            raise FabricValidationError("measures", "empty", "Measures list cannot be empty")

        workspace_id = self.workspace_service.resolve_workspace_id(workspace_name)
        semantic_model = self._resolve_semantic_model(
            workspace_id, semantic_model_name, semantic_model_id
        )
        definition = self._get_definition_with_retry(
            workspace_id, semantic_model.id, format="TMSL"
        )
        bim = self._get_bim(definition)
        model = bim.setdefault("model", {})
        tables = model.setdefault("tables", [])
        table = self._find_list_item(tables, "name", table_name)
        if not table:
            raise FabricValidationError(
                "table_name",
                table_name,
                f"Table '{table_name}' not found in semantic model",
            )

        table_measures = table.setdefault("measures", [])
        existing_names = {m.get("name", "").lower() for m in table_measures}

        for measure in measures:
            if measure.name.lower() in existing_names:
                raise FabricValidationError(
                    "measure_name",
                    measure.name,
                    f"Measure '{measure.name}' already exists in table '{table_name}'",
                )

            entry = {
                "name": measure.name,
                "expression": measure.expression,
                "lineageTag": str(uuid.uuid4()),
            }
            if measure.format_string:
                entry["formatString"] = measure.format_string
            if measure.display_folder:
                entry["displayFolder"] = measure.display_folder
            if measure.description:
                entry["description"] = measure.description

            table_measures.append(entry)
            existing_names.add(measure.name.lower())

        self._update_definition(workspace_id, semantic_model.id, definition, bim)
        return SemanticModelReference(workspace_id, semantic_model.id)

    def delete_table_from_semantic_model(
        self,
        workspace_name: str,
        semantic_model_name: Optional[str],
        semantic_model_id: Optional[str],
        table_name: str,
        remove_relationships: bool = True,
    ) -> Tuple[SemanticModelReference, int]:
        """Delete a table from a semantic model."""
        if not table_name or not table_name.strip():
            raise FabricValidationError(
                "table_name", table_name, "Table name cannot be empty"
            )

        workspace_id = self.workspace_service.resolve_workspace_id(workspace_name)
        semantic_model = self._resolve_semantic_model(
            workspace_id, semantic_model_name, semantic_model_id
        )
        definition = self._get_definition_with_retry(
            workspace_id, semantic_model.id, format="TMSL"
        )
        bim = self._get_bim(definition)
        model = bim.setdefault("model", {})
        tables = model.setdefault("tables", [])

        table = self._find_list_item(tables, "name", table_name)
        if not table:
            raise FabricValidationError(
                "table_name",
                table_name,
                f"Table '{table_name}' not found in semantic model",
            )

        model["tables"] = [t for t in tables if t is not table]

        removed_relationships = 0
        if remove_relationships:
            relationships = model.get("relationships", [])
            kept: List[Dict[str, Any]] = []
            for relationship in relationships:
                if relationship.get("fromTable") == table_name or relationship.get(
                    "toTable"
                ) == table_name:
                    removed_relationships += 1
                    continue
                kept.append(relationship)
            model["relationships"] = kept

        self._update_definition(workspace_id, semantic_model.id, definition, bim)
        return SemanticModelReference(workspace_id, semantic_model.id), removed_relationships

    def delete_relationship_from_semantic_model(
        self,
        workspace_name: str,
        semantic_model_name: Optional[str],
        semantic_model_id: Optional[str],
        relationship_name: Optional[str] = None,
        from_table: Optional[str] = None,
        from_column: Optional[str] = None,
        to_table: Optional[str] = None,
        to_column: Optional[str] = None,
    ) -> Tuple[SemanticModelReference, int]:
        """Delete relationship(s) from a semantic model."""
        if relationship_name:
            match_by_name = True
        else:
            match_by_name = False
            required = [from_table, from_column, to_table, to_column]
            if not all(required):
                raise FabricValidationError(
                    "relationship",
                    "",
                    "Provide relationship_name or from_table/from_column/to_table/to_column",
                )

        workspace_id = self.workspace_service.resolve_workspace_id(workspace_name)
        semantic_model = self._resolve_semantic_model(
            workspace_id, semantic_model_name, semantic_model_id
        )
        definition = self._get_definition_with_retry(
            workspace_id, semantic_model.id, format="TMSL"
        )
        bim = self._get_bim(definition)
        model = bim.setdefault("model", {})
        relationships = model.setdefault("relationships", [])

        kept: List[Dict[str, Any]] = []
        removed = 0
        for relationship in relationships:
            if match_by_name:
                if relationship.get("name") == relationship_name:
                    removed += 1
                    continue
            else:
                if (
                    relationship.get("fromTable") == from_table
                    and relationship.get("fromColumn") == from_column
                    and relationship.get("toTable") == to_table
                    and relationship.get("toColumn") == to_column
                ):
                    removed += 1
                    continue
            kept.append(relationship)

        if removed == 0:
            raise FabricValidationError(
                "relationship",
                relationship_name or "",
                "Relationship not found in semantic model",
            )

        model["relationships"] = kept
        self._update_definition(workspace_id, semantic_model.id, definition, bim)
        return SemanticModelReference(workspace_id, semantic_model.id), removed

    def delete_measures_from_semantic_model(
        self,
        workspace_name: str,
        semantic_model_name: Optional[str],
        semantic_model_id: Optional[str],
        table_name: str,
        measure_names: List[str],
    ) -> SemanticModelReference:
        """Delete measures from a table in an existing semantic model."""
        if not measure_names:
            raise FabricValidationError(
                "measure_names", "empty", "Measure names list cannot be empty"
            )

        workspace_id = self.workspace_service.resolve_workspace_id(workspace_name)
        semantic_model = self._resolve_semantic_model(
            workspace_id, semantic_model_name, semantic_model_id
        )
        definition = self._get_definition_with_retry(
            workspace_id, semantic_model.id, format="TMSL"
        )
        bim = self._get_bim(definition)
        model = bim.setdefault("model", {})
        tables = model.setdefault("tables", [])
        table = self._find_list_item(tables, "name", table_name)
        if not table:
            raise FabricValidationError(
                "table_name",
                table_name,
                f"Table '{table_name}' not found in semantic model",
            )

        table_measures = table.get("measures", [])
        names_lower = {name.lower() for name in measure_names}
        existing_names = {m.get("name", "").lower() for m in table_measures}
        missing = names_lower - existing_names
        if missing:
            missing_list = ", ".join(sorted(missing))
            raise FabricValidationError(
                "measure_names",
                missing_list,
                f"Measure(s) not found in table '{table_name}': {missing_list}",
            )

        table["measures"] = [
            measure
            for measure in table_measures
            if measure.get("name", "").lower() not in names_lower
        ]

        self._update_definition(workspace_id, semantic_model.id, definition, bim)
        return SemanticModelReference(workspace_id, semantic_model.id)

    def get_semantic_model_details(
        self,
        workspace_name: str,
        semantic_model_name: Optional[str] = None,
        semantic_model_id: Optional[str] = None,
    ):
        """Get semantic model metadata by name or ID."""
        workspace_id = self.workspace_service.resolve_workspace_id(workspace_name)
        semantic_model = self._resolve_semantic_model(
            workspace_id, semantic_model_name, semantic_model_id
        )
        return semantic_model

    def get_semantic_model_definition(
        self,
        workspace_name: str,
        semantic_model_name: Optional[str] = None,
        semantic_model_id: Optional[str] = None,
        format: str = "TMSL",
    ) -> tuple[Any, Dict[str, Any]]:
        """Get the semantic model definition in the requested format."""
        workspace_id = self.workspace_service.resolve_workspace_id(workspace_name)
        semantic_model = self._resolve_semantic_model(
            workspace_id, semantic_model_name, semantic_model_id
        )
        normalized_format = format.upper() if format else "TMSL"
        self._validate_definition_format(normalized_format)
        definition = self.item_service.get_item_definition(
            workspace_id, semantic_model.id, format=normalized_format
        )
        return semantic_model, definition

    def decode_model_bim(self, definition: Dict[str, Any]) -> Dict[str, Any]:
        """Decode the model.bim payload from a TMSL definition."""
        return self._get_bim(definition)

    def _validate_relationship_params(
        self, cardinality: str, cross_filter_direction: str
    ) -> None:
        valid_cardinality = {"manyToOne", "oneToMany", "oneToOne", "manyToMany"}
        valid_cross_filter = {"oneDirection", "bothDirections"}

        if cardinality not in valid_cardinality:
            raise FabricValidationError(
                "cardinality",
                cardinality,
                f"Invalid cardinality. Supported values: {', '.join(sorted(valid_cardinality))}",
            )

        if cross_filter_direction not in valid_cross_filter:
            raise FabricValidationError(
                "cross_filter_direction",
                cross_filter_direction,
                f"Invalid cross_filter_direction. Supported values: {', '.join(sorted(valid_cross_filter))}",
            )

    def _map_cardinality(self, cardinality: str) -> Tuple[str, str]:
        mapping = {
            "manyToOne": ("many", "one"),
            "oneToMany": ("one", "many"),
            "oneToOne": ("one", "one"),
            "manyToMany": ("many", "many"),
        }
        return mapping[cardinality]

    def _resolve_semantic_model(
        self,
        workspace_id: str,
        semantic_model_name: Optional[str],
        semantic_model_id: Optional[str],
    ):
        if semantic_model_id:
            semantic_model = self.item_service.get_item_by_id(
                workspace_id, semantic_model_id
            )
            if semantic_model.type != "SemanticModel":
                raise FabricValidationError(
                    "semantic_model_id",
                    semantic_model_id,
                    f"Item type '{semantic_model.type}' is not SemanticModel",
                )
            return semantic_model

        if not semantic_model_name or not semantic_model_name.strip():
            raise FabricValidationError(
                "semantic_model_name",
                semantic_model_name or "",
                "Semantic model name cannot be empty",
            )

        return self.item_service.get_item_by_name(
            workspace_id, semantic_model_name, "SemanticModel"
        )

    def _validate_definition_format(self, format: str) -> None:
        valid_formats = {"TMSL", "TMDL"}
        if format not in valid_formats:
            raise FabricValidationError(
                "format",
                format,
                f"Invalid format. Supported values: {', '.join(sorted(valid_formats))}",
            )

    def _get_bim(self, definition: Dict[str, Any]) -> Dict[str, Any]:
        parts = definition.get("definition", {}).get("parts", [])
        part = self._find_list_item(parts, "path", "model.bim")
        if not part:
            raise FabricError("Semantic model definition missing model.bim part")
        return self._decode_definition(part["payload"])

    def _get_definition_with_retry(
        self,
        workspace_id: str,
        semantic_model_id: str,
        format: str = "TMSL",
        retries: int = 12,
        interval_seconds: int = 10,
    ) -> Dict[str, Any]:
        for attempt in range(retries):
            definition = self.item_service.get_item_definition(
                workspace_id, semantic_model_id, format=format
            )
            parts = definition.get("definition", {}).get("parts") if definition else None
            if definition and parts:
                return definition
            if attempt < retries - 1:
                time.sleep(interval_seconds)
        raise FabricError(
            f"Semantic model definition not available after {retries} attempts"
        )

    def _update_definition(
        self,
        workspace_id: str,
        semantic_model_id: str,
        definition: Dict[str, Any],
        bim: Dict[str, Any],
    ) -> None:
        parts = definition.get("definition", {}).get("parts", [])
        pbism_part = self._find_list_item(parts, "path", "definition.pbism")
        if not pbism_part:
            raise FabricError("Semantic model definition missing definition.pbism part")

        update_payload = {
            "definition": {
                "format": "TMSL",
                "parts": [
                    pbism_part,
                    {
                        "path": "model.bim",
                        "payload": self._encode_definition(bim),
                        "payloadType": "InlineBase64",
                    },
                ],
            }
        }

        self.item_service.update_item_definition(
            workspace_id, semantic_model_id, update_payload
        )

    def _find_list_item(
        self, items: List[Dict[str, Any]], key: str, value: Any
    ) -> Optional[Dict[str, Any]]:
        for item in items:
            if item.get(key) == value:
                return item
        return None

    def _encode_definition(self, definition: Dict[str, Any]) -> str:
        json_str = json.dumps(definition)
        return base64.b64encode(json_str.encode("utf-8")).decode("utf-8")

    def _decode_definition(self, payload: str) -> Dict[str, Any]:
        decoded_bytes = base64.b64decode(payload)
        return json.loads(decoded_bytes.decode("utf-8"))
