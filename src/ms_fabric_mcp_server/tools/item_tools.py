# ABOUTME: Item management MCP tools for Microsoft Fabric.
# ABOUTME: Provides list_items and delete_item tools.
"""Item management MCP tools.

This module provides MCP tools for generic Fabric item operations including
listing and deleting items across all supported item types.
"""

from typing import Optional, TYPE_CHECKING
import logging

if TYPE_CHECKING:
    from fastmcp import FastMCP

from ..services import FabricItemService, FabricWorkspaceService
from ..client.exceptions import FabricItemNotFoundError
from .base import handle_tool_errors, log_tool_invocation

logger = logging.getLogger(__name__)


def register_item_tools(
    mcp: "FastMCP",
    item_service: FabricItemService,
    workspace_service: FabricWorkspaceService
):
    """Register item management MCP tools.
    
    This function registers two item-related tools:
    - list_items: List items in a workspace with optional type filter
    - delete_item: Delete an item by name and type
    
    Args:
        mcp: FastMCP server instance to register tools on.
        item_service: Initialized FabricItemService instance.
        workspace_service: Initialized FabricWorkspaceService instance for name resolution.
        
    Example:
        ```python
        from ms_fabric_mcp_server import (
            FabricConfig, FabricClient,
            FabricWorkspaceService, FabricItemService
        )
        from ms_fabric_mcp_server.tools import register_item_tools
        
        config = FabricConfig.from_environment()
        client = FabricClient(config)
        workspace_service = FabricWorkspaceService(client)
        item_service = FabricItemService(client)
        
        register_item_tools(mcp, item_service, workspace_service)
        ```
    """
    
    @mcp.tool(title="List Items in Workspace")
    @handle_tool_errors
    def list_items(
        workspace_name: str,
        item_type: Optional[str] = None
    ) -> dict:
        """List all items in a Fabric workspace, optionally filtered by type.
        
        Returns all items in the specified workspace. If item_type is provided,
        only items of that type are returned. Supported types include: Notebook,
        Lakehouse, Warehouse, Pipeline, DataPipeline, Report, SemanticModel,
        Dashboard, Dataflow, Dataset, and 40+ other Fabric item types.
        
        Parameters:
            workspace_name: The display name of the workspace.
            item_type: Optional item type filter (e.g., "Notebook", "Lakehouse").
                      If not provided, all items are returned.
                      
        Returns:
            Dictionary with status, workspace_name, item_type_filter, item_count,
            and list of items. Each item contains: id, display_name, type, description,
            created_date, modified_date.
            
        Example:
            ```python
            # List all items
            result = list_items("My Workspace")
            
            # List only notebooks
            result = list_items("My Workspace", item_type="Notebook")
            ```
        """
        log_tool_invocation("list_items", workspace_name=workspace_name, item_type=item_type)
        logger.info(f"Listing items in workspace '{workspace_name}'" + 
                   (f" (type: {item_type})" if item_type else " (all types)"))
        
        # Resolve workspace ID
        workspace_id = workspace_service.resolve_workspace_id(workspace_name)
        
        # Get items
        items = item_service.list_items(workspace_id, item_type)
        
        result = {
            "status": "success",
            "workspace_name": workspace_name,
            "item_type_filter": item_type,
            "item_count": len(items),
            "items": [
                {
                    "id": item.id,
                    "display_name": item.display_name,
                    "type": item.type,
                    "description": item.description,
                    "created_date": item.created_date,
                    "modified_date": item.modified_date,
                }
                for item in items
            ]
        }
        
        type_filter_msg = f" of type '{item_type}'" if item_type else ""
        logger.info(f"Found {len(items)} items{type_filter_msg} in workspace '{workspace_name}'")
        return result

    @mcp.tool(title="Delete Item from Workspace")
    @handle_tool_errors
    def delete_item(
        workspace_name: str,
        item_display_name: str,
        item_type: str
    ) -> dict:
        """Delete an item from a Fabric workspace.
        
        Deletes the specified item from the workspace. The item is identified by
        its display name and type. Common item types include: Notebook, Lakehouse,
        Warehouse, Pipeline, Report, SemanticModel, Dashboard, etc.
        
        Parameters:
            workspace_name: The display name of the workspace.
            item_display_name: Name of the item to delete.
            item_type: Type of the item to delete (e.g., "Notebook", "Lakehouse").
                      Supported types: Notebook, Lakehouse, Warehouse, Pipeline,
                      DataPipeline, Report, SemanticModel, Dashboard, Dataflow, Dataset.
                      
        Returns:
            Dictionary with status and success/error message.
            
        Example:
            ```python
            result = delete_item(
                workspace_name="My Workspace",
                item_display_name="Old Notebook",
                item_type="Notebook"
            )
            ```
        """
        log_tool_invocation("delete_item", workspace_name=workspace_name,
                          item_display_name=item_display_name, item_type=item_type)
        logger.info(f"Deleting {item_type} '{item_display_name}' from workspace '{workspace_name}'")
        
        try:
            # Resolve workspace ID
            workspace_id = workspace_service.resolve_workspace_id(workspace_name)
            
            # Find the item
            item = item_service.get_item_by_name(workspace_id, item_display_name, item_type)
            
            # Delete the item
            item_service.delete_item(workspace_id, item.id)
            
            logger.info(f"Successfully deleted {item_type} '{item_display_name}'")
            return {
                "status": "success",
                "message": f"Successfully deleted {item_type} '{item_display_name}'"
            }
            
        except FabricItemNotFoundError:
            error_msg = f"{item_type} '{item_display_name}' not found in workspace '{workspace_name}'"
            logger.error(error_msg)
            return {
                "status": "error",
                "message": error_msg
            }
    
    logger.info("Item tools registered successfully (2 tools)")
