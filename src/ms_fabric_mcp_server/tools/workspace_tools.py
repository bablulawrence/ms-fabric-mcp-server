# ABOUTME: Workspace management MCP tools for Microsoft Fabric.
# ABOUTME: Provides list_workspaces tool.
"""Workspace management MCP tools.

This module provides MCP tools for Microsoft Fabric workspace operations including
listing workspaces.
"""

from typing import TYPE_CHECKING
import logging

if TYPE_CHECKING:
    from fastmcp import FastMCP

from ..services import FabricWorkspaceService
from .base import handle_tool_errors, format_success_response, format_error_response, log_tool_invocation

logger = logging.getLogger(__name__)


def register_workspace_tools(mcp: "FastMCP", workspace_service: FabricWorkspaceService):
    """Register workspace management MCP tools.
    
    This function registers workspace-related tools:
    - list_workspaces: List all accessible workspaces
    
    Args:
        mcp: FastMCP server instance to register tools on.
        workspace_service: Initialized FabricWorkspaceService instance.
        
    Example:
        ```python
        from ms_fabric_mcp_server.client import FabricConfig, FabricClient
        from ms_fabric_mcp_server.services import FabricWorkspaceService
        from ms_fabric_mcp_server.tools import register_workspace_tools
        
        config = FabricConfig.from_environment()
        client = FabricClient(config)
        workspace_service = FabricWorkspaceService(client)
        
        register_workspace_tools(mcp, workspace_service)
        ```
    """
    
    @mcp.tool(title="List Workspaces")
    @handle_tool_errors
    def list_workspaces() -> dict:
        """List all accessible Fabric workspaces.
        
        Returns a list of all workspaces the authenticated user has access to,
        including workspace ID, name, description, type, state, and capacity ID.

        Parameters:
            None
        
        Returns:
            Dictionary with status, workspace_count, and list of workspaces.
            Each workspace contains: id, display_name, description, type, state, capacity_id.

        Example:
            ```python
            result = list_workspaces()
            ```
        """
        log_tool_invocation("list_workspaces")
        logger.info("Listing all workspaces")
        
        workspaces = workspace_service.list_workspaces()
        
        result = {
            "status": "success",
            "workspace_count": len(workspaces),
            "workspaces": [
                {
                    "id": ws.id,
                    "display_name": ws.display_name,
                    "description": ws.description,
                    "type": ws.type,
                    "state": ws.state,
                    "capacity_id": ws.capacity_id,
                }
                for ws in workspaces
            ]
        }
        
        logger.info(f"Found {len(workspaces)} workspaces")
        return result

    logger.info("Workspace tools registered successfully (1 tool)")
