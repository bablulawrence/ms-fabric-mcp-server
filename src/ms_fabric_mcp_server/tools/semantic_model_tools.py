# ABOUTME: Semantic model management MCP tools.
# ABOUTME: Provides tools to create semantic models and add tables/relationships.
"""Semantic model management MCP tools."""

from typing import Optional, TYPE_CHECKING
import logging

from ms_fabric_mcp_server.client.exceptions import FabricValidationError
from ms_fabric_mcp_server.models.semantic_model import (
    SemanticModelColumn,
    SemanticModelMeasure,
)
from ms_fabric_mcp_server.services.semantic_model import FabricSemanticModelService

if TYPE_CHECKING:
    from fastmcp import FastMCP

from .base import handle_tool_errors, log_tool_invocation

logger = logging.getLogger(__name__)


def register_semantic_model_tools(
    mcp: "FastMCP", semantic_model_service: FabricSemanticModelService
):
    """Register semantic model management MCP tools."""

    @mcp.tool(title="Create Semantic Model")
    @handle_tool_errors
    def create_semantic_model(
        workspace_name: str,
        semantic_model_name: str,
        folder_path: Optional[str] = None,
    ) -> dict:
        """Create an empty Fabric semantic model.

        Parameters:
            workspace_name: The display name of the workspace.
            semantic_model_name: Name for the semantic model (no folder separators).
            folder_path: Optional folder path (e.g., "models/finance") to place the model.
                         Defaults to the workspace root when omitted.
        """
        log_tool_invocation(
            "create_semantic_model",
            workspace_name=workspace_name,
            semantic_model_name=semantic_model_name,
            folder_path=folder_path,
        )

        semantic_model = semantic_model_service.create_semantic_model(
            workspace_name=workspace_name,
            semantic_model_name=semantic_model_name,
            folder_path=folder_path,
        )

        result = {
            "status": "success",
            "semantic_model_id": semantic_model.id,
            "semantic_model_name": semantic_model_name,
            "workspace_name": workspace_name,
            "workspace_id": semantic_model.workspace_id,
            "message": f"Semantic model '{semantic_model_name}' created successfully",
        }

        logger.info(
            f"Semantic model created successfully: {semantic_model_name} in workspace {workspace_name}"
        )
        return result

    @mcp.tool(title="Add Table to Semantic Model")
    @handle_tool_errors
    def add_table_to_semantic_model(
        workspace_name: str,
        semantic_model_name: str,
        lakehouse_name: str,
        table_name: str,
        columns: list[SemanticModelColumn],
        table_schema: Optional[str] = None,
        model_table_name: Optional[str] = None,
    ) -> dict:
        """Add a table from a lakehouse to an existing semantic model.

        Parameters:
            workspace_name: The display name of the workspace.
            semantic_model_name: Name of the semantic model to update.
            lakehouse_name: Name of the source lakehouse.
            table_name: Source table name (unqualified when table_schema is provided).
            columns: Column definitions for the table.
            table_schema: Optional schema name for the source table.
            model_table_name: Optional table name to use in the semantic model.

        Notes:
            - Schema-less DirectLake tables: omit table_schema and pass table_name only.
            - Schema-based DirectLake tables: set table_schema and pass an unqualified table_name.
        """
        log_tool_invocation(
            "add_table_to_semantic_model",
            workspace_name=workspace_name,
            semantic_model_name=semantic_model_name,
            lakehouse_name=lakehouse_name,
            table_name=table_name,
            table_schema=table_schema,
            model_table_name=model_table_name,
        )

        model = semantic_model_service.add_table_to_semantic_model(
            workspace_name=workspace_name,
            semantic_model_name=semantic_model_name,
            lakehouse_name=lakehouse_name,
            table_name=table_name,
            columns=columns,
            table_schema=table_schema,
            model_table_name=model_table_name,
        )

        result = {
            "status": "success",
            "semantic_model_id": model.id,
            "semantic_model_name": semantic_model_name,
            "workspace_name": workspace_name,
            "workspace_id": model.workspace_id,
            "message": f"Table successfully added to semantic model '{semantic_model_name}'",
        }

        logger.info(
            f"Table '{table_name}' added successfully to semantic model '{semantic_model_name}' in workspace '{workspace_name}'"
        )
        return result

    @mcp.tool(title="Add Measures to Semantic Model")
    @handle_tool_errors
    def add_measures_to_semantic_model(
        workspace_name: str,
        table_name: str,
        measures: list[SemanticModelMeasure],
        semantic_model_name: Optional[str] = None,
        semantic_model_id: Optional[str] = None,
    ) -> dict:
        """Add measures to a table in an existing semantic model."""
        log_tool_invocation(
            "add_measures_to_semantic_model",
            workspace_name=workspace_name,
            semantic_model_name=semantic_model_name,
            semantic_model_id=semantic_model_id,
            table_name=table_name,
        )

        model = semantic_model_service.add_measures_to_semantic_model(
            workspace_name=workspace_name,
            semantic_model_name=semantic_model_name,
            semantic_model_id=semantic_model_id,
            table_name=table_name,
            measures=measures,
        )

        result = {
            "status": "success",
            "semantic_model_id": model.id,
            "semantic_model_name": semantic_model_name,
            "workspace_name": workspace_name,
            "workspace_id": model.workspace_id,
            "table_name": table_name,
            "measures_added": [measure.name for measure in measures],
        }

        logger.info(
            f"Measures added successfully to semantic model in workspace '{workspace_name}'"
        )
        return result

    @mcp.tool(title="List Semantic Model Tables")
    @handle_tool_errors
    def list_semantic_model_tables(
        workspace_name: str,
        semantic_model_name: Optional[str] = None,
        semantic_model_id: Optional[str] = None,
    ) -> dict:
        """List tables in a semantic model.

        Parameters:
            workspace_name: The display name of the workspace.
            semantic_model_name: Name of the semantic model to query.
            semantic_model_id: Semantic model ID (optional).
        """
        log_tool_invocation(
            "list_semantic_model_tables",
            workspace_name=workspace_name,
            semantic_model_name=semantic_model_name,
            semantic_model_id=semantic_model_id,
        )

        tables = semantic_model_service.list_semantic_model_tables(
            workspace_name=workspace_name,
            semantic_model_name=semantic_model_name,
            semantic_model_id=semantic_model_id,
        )

        return {
            "status": "success",
            "tables": tables,
        }

    @mcp.tool(title="Delete Table from Semantic Model")
    @handle_tool_errors
    def delete_table_from_semantic_model(
        workspace_name: str,
        table_name: str,
        semantic_model_name: Optional[str] = None,
        semantic_model_id: Optional[str] = None,
        remove_relationships: bool = True,
    ) -> dict:
        """Delete a table from a semantic model.

        Parameters:
            workspace_name: The display name of the workspace.
            table_name: Name of the table to delete.
            semantic_model_name: Name of the semantic model to update.
            semantic_model_id: Semantic model ID (optional).
            remove_relationships: Remove relationships that reference the table.
        """
        log_tool_invocation(
            "delete_table_from_semantic_model",
            workspace_name=workspace_name,
            semantic_model_name=semantic_model_name,
            semantic_model_id=semantic_model_id,
            table_name=table_name,
        )

        model, removed_relationships = (
            semantic_model_service.delete_table_from_semantic_model(
                workspace_name=workspace_name,
                semantic_model_name=semantic_model_name,
                semantic_model_id=semantic_model_id,
                table_name=table_name,
                remove_relationships=remove_relationships,
            )
        )

        return {
            "status": "success",
            "semantic_model_id": model.id,
            "semantic_model_name": semantic_model_name,
            "workspace_name": workspace_name,
            "workspace_id": model.workspace_id,
            "table_name": table_name,
            "removed_relationships": removed_relationships,
        }

    @mcp.tool(title="List Semantic Model Relationships")
    @handle_tool_errors
    def list_semantic_model_relationships(
        workspace_name: str,
        semantic_model_name: Optional[str] = None,
        semantic_model_id: Optional[str] = None,
    ) -> dict:
        """List relationships in a semantic model."""
        log_tool_invocation(
            "list_semantic_model_relationships",
            workspace_name=workspace_name,
            semantic_model_name=semantic_model_name,
            semantic_model_id=semantic_model_id,
        )

        relationships = semantic_model_service.list_semantic_model_relationships(
            workspace_name=workspace_name,
            semantic_model_name=semantic_model_name,
            semantic_model_id=semantic_model_id,
        )

        return {
            "status": "success",
            "relationships": relationships,
        }

    @mcp.tool(title="Delete Relationship from Semantic Model")
    @handle_tool_errors
    def delete_relationship_from_semantic_model(
        workspace_name: str,
        semantic_model_name: Optional[str] = None,
        semantic_model_id: Optional[str] = None,
        relationship_name: Optional[str] = None,
        from_table: Optional[str] = None,
        from_column: Optional[str] = None,
        to_table: Optional[str] = None,
        to_column: Optional[str] = None,
    ) -> dict:
        """Delete relationship(s) from a semantic model."""
        log_tool_invocation(
            "delete_relationship_from_semantic_model",
            workspace_name=workspace_name,
            semantic_model_name=semantic_model_name,
            semantic_model_id=semantic_model_id,
            relationship_name=relationship_name,
        )

        model, removed = semantic_model_service.delete_relationship_from_semantic_model(
            workspace_name=workspace_name,
            semantic_model_name=semantic_model_name,
            semantic_model_id=semantic_model_id,
            relationship_name=relationship_name,
            from_table=from_table,
            from_column=from_column,
            to_table=to_table,
            to_column=to_column,
        )

        return {
            "status": "success",
            "semantic_model_id": model.id,
            "semantic_model_name": semantic_model_name,
            "workspace_name": workspace_name,
            "workspace_id": model.workspace_id,
            "relationships_removed": removed,
        }

    @mcp.tool(title="Delete Measures from Semantic Model")
    @handle_tool_errors
    def delete_measures_from_semantic_model(
        workspace_name: str,
        table_name: str,
        measure_names: list[str],
        semantic_model_name: Optional[str] = None,
        semantic_model_id: Optional[str] = None,
    ) -> dict:
        """Delete measures from a table in an existing semantic model."""
        log_tool_invocation(
            "delete_measures_from_semantic_model",
            workspace_name=workspace_name,
            semantic_model_name=semantic_model_name,
            semantic_model_id=semantic_model_id,
            table_name=table_name,
        )

        model = semantic_model_service.delete_measures_from_semantic_model(
            workspace_name=workspace_name,
            semantic_model_name=semantic_model_name,
            semantic_model_id=semantic_model_id,
            table_name=table_name,
            measure_names=measure_names,
        )

        result = {
            "status": "success",
            "semantic_model_id": model.id,
            "semantic_model_name": semantic_model_name,
            "workspace_name": workspace_name,
            "workspace_id": model.workspace_id,
            "table_name": table_name,
            "measures_deleted": measure_names,
        }

        logger.info(
            f"Measures deleted successfully from semantic model in workspace '{workspace_name}'"
        )
        return result

    @mcp.tool(title="Get Semantic Model Details")
    @handle_tool_errors
    def get_semantic_model_details(
        workspace_name: str,
        semantic_model_name: Optional[str] = None,
        semantic_model_id: Optional[str] = None,
    ) -> dict:
        """Get semantic model metadata by name or ID."""
        log_tool_invocation(
            "get_semantic_model_details",
            workspace_name=workspace_name,
            semantic_model_name=semantic_model_name,
            semantic_model_id=semantic_model_id,
        )

        semantic_model = semantic_model_service.get_semantic_model_details(
            workspace_name=workspace_name,
            semantic_model_name=semantic_model_name,
            semantic_model_id=semantic_model_id,
        )

        result = {
            "status": "success",
            "workspace_name": workspace_name,
            "workspace_id": semantic_model.workspace_id,
            "semantic_model_id": semantic_model.id,
            "semantic_model_name": semantic_model.display_name,
            "description": semantic_model.description,
            "type": semantic_model.type,
            "created_date": semantic_model.created_date,
            "modified_date": semantic_model.modified_date,
        }

        logger.info(
            f"Semantic model details retrieved for '{semantic_model.display_name}' in workspace '{workspace_name}'"
        )
        return result

    @mcp.tool(title="Get Semantic Model Definition")
    @handle_tool_errors
    def get_semantic_model_definition(
        workspace_name: str,
        semantic_model_name: Optional[str] = None,
        semantic_model_id: Optional[str] = None,
        format: str = "TMSL",
        decode_model_bim: bool = False,
    ) -> dict:
        """Get semantic model definition parts in the requested format."""
        log_tool_invocation(
            "get_semantic_model_definition",
            workspace_name=workspace_name,
            semantic_model_name=semantic_model_name,
            semantic_model_id=semantic_model_id,
            format=format,
            decode_model_bim=decode_model_bim,
        )

        semantic_model, definition = semantic_model_service.get_semantic_model_definition(
            workspace_name=workspace_name,
            semantic_model_name=semantic_model_name,
            semantic_model_id=semantic_model_id,
            format=format,
        )

        result = {
            "status": "success",
            "workspace_name": workspace_name,
            "semantic_model_name": semantic_model.display_name,
            "semantic_model_id": semantic_model.id,
            "definition": definition,
        }

        if decode_model_bim:
            if (format or "TMSL").upper() != "TMSL":
                raise FabricValidationError(
                    "decode_model_bim",
                    format,
                    "decode_model_bim is only supported for TMSL format",
                )
            result["model_bim_json"] = semantic_model_service.decode_model_bim(
                definition
            )

        logger.info(
            f"Semantic model definition retrieved in workspace '{workspace_name}'"
        )
        return result

    @mcp.tool(title="Add Relationship to Semantic Model")
    @handle_tool_errors
    def add_relationship_to_semantic_model(
        workspace_name: str,
        semantic_model_name: str,
        from_table: str,
        from_column: str,
        to_table: str,
        to_column: str,
        cardinality: str = "manyToOne",
        cross_filter_direction: str = "oneDirection",
        is_active: bool = True,
    ) -> dict:
        """Add a relationship between two tables in an existing semantic model."""
        log_tool_invocation(
            "add_relationship_to_semantic_model",
            workspace_name=workspace_name,
            semantic_model_name=semantic_model_name,
            from_table=from_table,
            from_column=from_column,
            to_table=to_table,
            to_column=to_column,
            cardinality=cardinality,
            cross_filter_direction=cross_filter_direction,
            is_active=is_active,
        )

        model = semantic_model_service.add_relationships_to_semantic_model(
            workspace_name=workspace_name,
            semantic_model_name=semantic_model_name,
            from_table=from_table,
            from_column=from_column,
            to_table=to_table,
            to_column=to_column,
            cardinality=cardinality,
            cross_filter_direction=cross_filter_direction,
            is_active=is_active,
        )

        result = {
            "status": "success",
            "semantic_model_id": model.id,
            "semantic_model_name": semantic_model_name,
            "workspace_name": workspace_name,
            "workspace_id": model.workspace_id,
            "message": f"Relationship added successfully to semantic model '{semantic_model_name}'",
        }

        logger.info(
            f"Relationship added successfully to semantic model '{semantic_model_name}' in workspace '{workspace_name}'"
        )
        return result
