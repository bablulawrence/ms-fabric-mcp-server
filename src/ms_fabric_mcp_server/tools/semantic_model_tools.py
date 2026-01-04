# ABOUTME: Semantic model management MCP tools.
# ABOUTME: Provides tools to create semantic models and add tables/relationships.
"""Semantic model management MCP tools."""

from typing import TYPE_CHECKING
import logging

from ms_fabric_mcp_server.models.semantic_model import SemanticModelColumn
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
    ) -> dict:
        """Create an empty Fabric semantic model."""
        log_tool_invocation(
            "create_semantic_model",
            workspace_name=workspace_name,
            semantic_model_name=semantic_model_name,
        )

        semantic_model = semantic_model_service.create_semantic_model(
            workspace_name=workspace_name,
            semantic_model_name=semantic_model_name,
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
    ) -> dict:
        """Add a table from a lakehouse to an existing semantic model."""
        log_tool_invocation(
            "add_table_to_semantic_model",
            workspace_name=workspace_name,
            semantic_model_name=semantic_model_name,
            lakehouse_name=lakehouse_name,
            table_name=table_name,
        )

        model = semantic_model_service.add_table_to_semantic_model(
            workspace_name=workspace_name,
            semantic_model_name=semantic_model_name,
            lakehouse_name=lakehouse_name,
            table_name=table_name,
            columns=columns,
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
