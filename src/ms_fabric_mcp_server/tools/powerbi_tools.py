# ABOUTME: Power BI REST MCP tools for semantic model refresh and DAX queries.
# ABOUTME: Provides refresh_semantic_model and execute_dax_query tools.
"""Power BI REST MCP tools."""

from typing import Optional, TYPE_CHECKING
import logging

from ms_fabric_mcp_server.services.powerbi import FabricPowerBIService
from .base import handle_tool_errors, log_tool_invocation

if TYPE_CHECKING:
    from fastmcp import FastMCP

logger = logging.getLogger(__name__)


def register_powerbi_tools(mcp: "FastMCP", powerbi_service: FabricPowerBIService):
    """Register Power BI REST MCP tools."""

    @mcp.tool(title="Refresh Semantic Model")
    @handle_tool_errors
    def refresh_semantic_model(
        workspace_name: str,
        semantic_model_name: Optional[str] = None,
        semantic_model_id: Optional[str] = None,
        refresh_type: Optional[str] = None,
        objects: Optional[list[dict]] = None,
    ) -> dict:
        """Refresh a semantic model and wait for completion."""
        log_tool_invocation(
            "refresh_semantic_model",
            workspace_name=workspace_name,
            semantic_model_name=semantic_model_name,
            semantic_model_id=semantic_model_id,
            refresh_type=refresh_type,
        )

        result = powerbi_service.refresh_semantic_model(
            workspace_name=workspace_name,
            semantic_model_name=semantic_model_name,
            semantic_model_id=semantic_model_id,
            refresh_type=refresh_type,
            objects=objects,
        )

        logger.info(
            f"Semantic model refresh completed for workspace '{workspace_name}'"
        )
        return result

    @mcp.tool(title="Execute DAX Query")
    @handle_tool_errors
    def execute_dax_query(
        workspace_name: str,
        query: str,
        semantic_model_name: Optional[str] = None,
        semantic_model_id: Optional[str] = None,
    ) -> dict:
        """Execute a DAX query and return the raw Power BI response."""
        log_tool_invocation(
            "execute_dax_query",
            workspace_name=workspace_name,
            semantic_model_name=semantic_model_name,
            semantic_model_id=semantic_model_id,
        )

        response = powerbi_service.execute_dax_query(
            workspace_name=workspace_name,
            semantic_model_name=semantic_model_name,
            semantic_model_id=semantic_model_id,
            query=query,
        )

        result = {
            "status": "success",
            "response": response,
        }

        logger.info(f"DAX query executed successfully in workspace '{workspace_name}'")
        return result

    logger.info("Power BI tools registered successfully (2 tools)")
