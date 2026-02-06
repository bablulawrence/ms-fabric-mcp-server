# ABOUTME: Item management MCP tools for Microsoft Fabric.
# ABOUTME: Provides item and folder management tools.
"""Item management MCP tools.

This module provides MCP tools for generic Fabric item and folder operations.
"""

from typing import Optional, TYPE_CHECKING
import logging

if TYPE_CHECKING:
    from fastmcp import FastMCP

from ..services import FabricItemService, FabricWorkspaceService
from ..client.exceptions import FabricItemNotFoundError, FabricValidationError
from .base import handle_tool_errors, log_tool_invocation

logger = logging.getLogger(__name__)


def register_item_tools(
    mcp: "FastMCP",
    item_service: FabricItemService,
    workspace_service: FabricWorkspaceService
):
    """Register item management MCP tools.
    
    This function registers item-related tools:
    - list_items: List items in a workspace with optional type filter
    - get_item: Get an item by ID or display name/type
    - list_folders: List folders in a workspace
    - create_folder: Create a folder in a workspace
    - move_folder: Move a folder to a new parent
    - delete_folder: Delete a folder
    - create_lakehouse: Create a lakehouse in a workspace
    - delete_item: Delete an item by name and type
    - rename_item: Rename an item by ID
    - move_item_to_folder: Move an item to a folder by ID
    
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
        item_type: Optional[str] = None,
        root_folder_id: Optional[str] = None,
        root_folder_path: Optional[str] = None,
        recursive: bool = True,
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
            root_folder_id: Optional folder ID to scope the listing.
            root_folder_path: Optional folder path to scope the listing (e.g., "team/etl").
                              Defaults to the workspace root when omitted.
            recursive: Whether to include items in subfolders (default True).
                      
        Returns:
            Dictionary with status, workspace_name, item_type_filter, item_count,
            and list of items. Each item contains: id, display_name, type, description,
            folder_id, created_date, modified_date.
            
        Example:
            ```python
            # List all items
            result = list_items("My Workspace")
            
            # List only notebooks
            result = list_items("My Workspace", item_type="Notebook")
            ```
        """
        log_tool_invocation(
            "list_items",
            workspace_name=workspace_name,
            item_type=item_type,
            root_folder_id=root_folder_id,
            root_folder_path=root_folder_path,
            recursive=recursive,
        )
        logger.info(f"Listing items in workspace '{workspace_name}'" + 
                   (f" (type: {item_type})" if item_type else " (all types)"))
        
        # Resolve workspace ID
        workspace_id = workspace_service.resolve_workspace_id(workspace_name)
        
        if root_folder_id and root_folder_path:
            raise FabricValidationError(
                "root_folder_path",
                root_folder_path,
                "Provide either root_folder_id or root_folder_path, not both.",
            )

        if root_folder_path:
            root_folder_id = item_service.resolve_folder_id_from_path(
                workspace_id, root_folder_path, create_missing=False
            )

        # Get items
        items = item_service.list_items(
            workspace_id,
            item_type,
            root_folder_id=root_folder_id,
            recursive=recursive,
        )
        
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
                    "folder_id": item.folder_id,
                    "created_date": item.created_date,
                    "modified_date": item.modified_date,
                }
                for item in items
            ]
        }
        
        type_filter_msg = f" of type '{item_type}'" if item_type else ""
        logger.info(f"Found {len(items)} items{type_filter_msg} in workspace '{workspace_name}'")
        return result

    @mcp.tool(title="Get Item")
    @handle_tool_errors
    def get_item(
        workspace_name: str,
        item_id: Optional[str] = None,
        item_name: Optional[str] = None,
        item_type: Optional[str] = None,
    ) -> dict:
        """Get a Fabric item by ID or display name/type.

        Parameters:
            workspace_name: The display name of the workspace.
            item_id: ID of the item to fetch.
            item_name: Display name of the item to fetch.
            item_type: Type of the item when using display name.

        Returns:
            Dictionary with status and item metadata.
        """
        log_tool_invocation(
            "get_item",
            workspace_name=workspace_name,
            item_id=item_id,
            item_name=item_name,
            item_type=item_type,
        )

        workspace_id = workspace_service.resolve_workspace_id(workspace_name)

        if item_id:
            item = item_service.get_item_by_id(workspace_id, item_id)
        else:
            if not item_name or not item_type:
                raise FabricValidationError(
                    "item_name",
                    item_name,
                    "Provide item_name and item_type when item_id is not supplied.",
                )
            item = item_service.get_item_by_name(workspace_id, item_name, item_type)

        return {
            "status": "success",
            "workspace_name": workspace_name,
            "workspace_id": workspace_id,
            "item": {
                "id": item.id,
                "display_name": item.display_name,
                "type": item.type,
                "description": item.description,
                "folder_id": item.folder_id,
                "created_date": item.created_date,
                "modified_date": item.modified_date,
            },
        }

    @mcp.tool(title="List Folders")
    @handle_tool_errors
    def list_folders(
        workspace_name: str,
        root_folder_id: Optional[str] = None,
        root_folder_path: Optional[str] = None,
        recursive: bool = True,
    ) -> dict:
        """List folders in a Fabric workspace.

        Parameters:
            workspace_name: The display name of the workspace.
            root_folder_id: Optional folder ID to scope the listing.
            root_folder_path: Optional folder path to scope the listing (e.g., "team/etl").
                              Defaults to the workspace root when omitted.
            recursive: Whether to include folders in subfolders (default True).

        Returns:
            Dictionary with status, folder_count, and list of folders.
        """
        log_tool_invocation(
            "list_folders",
            workspace_name=workspace_name,
            root_folder_id=root_folder_id,
            root_folder_path=root_folder_path,
            recursive=recursive,
        )

        workspace_id = workspace_service.resolve_workspace_id(workspace_name)

        if root_folder_id and root_folder_path:
            raise FabricValidationError(
                "root_folder_path",
                root_folder_path,
                "Provide either root_folder_id or root_folder_path, not both.",
            )

        if root_folder_path:
            root_folder_id = item_service.resolve_folder_id_from_path(
                workspace_id, root_folder_path, create_missing=False
            )

        folders = item_service.list_folders(
            workspace_id,
            root_folder_id=root_folder_id,
            recursive=recursive,
        )

        return {
            "status": "success",
            "workspace_name": workspace_name,
            "workspace_id": workspace_id,
            "folder_count": len(folders),
            "folders": [
                {
                    "id": folder.get("id"),
                    "display_name": folder.get("displayName"),
                    "parent_folder_id": folder.get("parentFolderId"),
                    "created_date": folder.get("createdDate"),
                    "modified_date": folder.get("modifiedDate"),
                }
                for folder in folders
            ],
        }

    @mcp.tool(title="Create Folder")
    @handle_tool_errors
    def create_folder(
        workspace_name: str,
        folder_name: str,
        parent_folder_id: Optional[str] = None,
        parent_folder_path: Optional[str] = None,
    ) -> dict:
        """Create a folder in a Fabric workspace.

        Parameters:
            workspace_name: The display name of the workspace.
            folder_name: Display name for the new folder.
            parent_folder_id: Optional parent folder ID.
            parent_folder_path: Optional parent folder path (creates missing parents).

        Returns:
            Dictionary with status and folder metadata.
        """
        log_tool_invocation(
            "create_folder",
            workspace_name=workspace_name,
            folder_name=folder_name,
            parent_folder_id=parent_folder_id,
            parent_folder_path=parent_folder_path,
        )

        if "/" in folder_name or "\\" in folder_name:
            raise FabricValidationError(
                "folder_name",
                folder_name,
                "Folder name cannot include path separators. Use parent_folder_path instead.",
            )

        workspace_id = workspace_service.resolve_workspace_id(workspace_name)

        if parent_folder_id and parent_folder_path:
            raise FabricValidationError(
                "parent_folder_path",
                parent_folder_path,
                "Provide either parent_folder_id or parent_folder_path, not both.",
            )

        if parent_folder_path:
            parent_folder_id = item_service.resolve_folder_id_from_path(
                workspace_id, parent_folder_path, create_missing=True
            )

        folder = item_service.create_folder(
            workspace_id=workspace_id,
            display_name=folder_name,
            parent_folder_id=parent_folder_id,
        )

        return {
            "status": "success",
            "workspace_name": workspace_name,
            "workspace_id": workspace_id,
            "folder_id": folder.get("id"),
            "folder_name": folder.get("displayName"),
            "parent_folder_id": folder.get("parentFolderId"),
            "message": f"Folder '{folder.get('displayName')}' created successfully",
        }

    @mcp.tool(title="Move Folder")
    @handle_tool_errors
    def move_folder(
        workspace_name: str,
        folder_id: str,
        target_folder_id: Optional[str] = None,
        target_folder_path: Optional[str] = None,
    ) -> dict:
        """Move a folder to a new parent.

        Parameters:
            workspace_name: The display name of the workspace.
            folder_id: Folder ID to move.
            target_folder_id: Target parent folder ID. Use "root" or omit to move to workspace root.
            target_folder_path: Target parent folder path (must exist).

        Returns:
            Dictionary with status and folder metadata.
        """
        log_tool_invocation(
            "move_folder",
            workspace_name=workspace_name,
            folder_id=folder_id,
            target_folder_id=target_folder_id,
            target_folder_path=target_folder_path,
        )

        workspace_id = workspace_service.resolve_workspace_id(workspace_name)

        if target_folder_id and target_folder_path:
            raise FabricValidationError(
                "target_folder_path",
                target_folder_path,
                "Provide either target_folder_id or target_folder_path, not both.",
            )

        # Accept "root" as sentinel for workspace root
        if isinstance(target_folder_id, str) and target_folder_id.lower() == "root":
            target_folder_id = None

        if target_folder_path:
            target_folder_id = item_service.resolve_folder_id_from_path(
                workspace_id, target_folder_path, create_missing=False
            )

        folder = item_service.move_folder(
            workspace_id=workspace_id,
            folder_id=folder_id,
            target_folder_id=target_folder_id,
        )

        return {
            "status": "success",
            "workspace_name": workspace_name,
            "workspace_id": workspace_id,
            "folder_id": folder.get("id"),
            "folder_name": folder.get("displayName"),
            "parent_folder_id": folder.get("parentFolderId"),
            "message": f"Folder '{folder.get('displayName')}' moved successfully",
        }

    @mcp.tool(title="Delete Folder")
    @handle_tool_errors
    def delete_folder(
        workspace_name: str,
        folder_id: Optional[str] = None,
        folder_path: Optional[str] = None,
    ) -> dict:
        """Delete a folder from a workspace.

        Parameters:
            workspace_name: The display name of the workspace.
            folder_id: Folder ID to delete.
            folder_path: Folder path to delete (e.g., "team/etl").

        Returns:
            Dictionary with status and folder metadata.
        """
        log_tool_invocation(
            "delete_folder",
            workspace_name=workspace_name,
            folder_id=folder_id,
            folder_path=folder_path,
        )

        workspace_id = workspace_service.resolve_workspace_id(workspace_name)

        if folder_id and folder_path:
            raise FabricValidationError(
                "folder_path",
                folder_path,
                "Provide either folder_id or folder_path, not both.",
            )
        if not folder_id and not folder_path:
            raise FabricValidationError(
                "folder_id",
                folder_id,
                "Provide either folder_id or folder_path.",
            )

        if folder_path:
            folder_id = item_service.resolve_folder_id_from_path(
                workspace_id, folder_path, create_missing=False
            )

        folder = item_service.delete_folder(workspace_id=workspace_id, folder_id=folder_id)

        return {
            "status": "success",
            "workspace_name": workspace_name,
            "workspace_id": workspace_id,
            "folder_id": folder.get("id", folder_id),
            "message": "Folder deleted successfully",
        }

    @mcp.tool(title="Create Lakehouse")
    @handle_tool_errors
    def create_lakehouse(
        workspace_name: str,
        lakehouse_name: str,
        description: Optional[str] = None,
        enable_schemas: bool = True,
    ) -> dict:
        """Create a lakehouse in a Fabric workspace.

        Parameters:
            workspace_name: The display name of the workspace.
            lakehouse_name: Display name for the new lakehouse.
            description: Optional description for the lakehouse.
            enable_schemas: Whether to enable schemas (default: True).

        Returns:
            Dictionary with status and lakehouse metadata.
        """
        log_tool_invocation(
            "create_lakehouse",
            workspace_name=workspace_name,
            lakehouse_name=lakehouse_name,
            enable_schemas=enable_schemas,
        )

        workspace_id = workspace_service.resolve_workspace_id(workspace_name)
        lakehouse = item_service.create_lakehouse(
            workspace_id=workspace_id,
            display_name=lakehouse_name,
            description=description,
            enable_schemas=enable_schemas,
        )

        return {
            "status": "success",
            "workspace_name": workspace_name,
            "workspace_id": workspace_id,
            "lakehouse_id": lakehouse.id,
            "lakehouse_name": lakehouse.display_name,
            "description": lakehouse.description,
            "enable_schemas": lakehouse.enable_schemas,
            "message": f"Lakehouse '{lakehouse.display_name}' created successfully",
        }


    @mcp.tool(title="Delete Item from Workspace")
    @handle_tool_errors
    def delete_item(
        workspace_name: str,
        item_name: str,
        item_type: str
    ) -> dict:
        """Delete an item from a Fabric workspace.
        
        Deletes the specified item from the workspace. The item is identified by
        its display name and type. Common item types include: Notebook, Lakehouse,
        Warehouse, Pipeline, Report, SemanticModel, Dashboard, etc.
        
        Parameters:
            workspace_name: The display name of the workspace.
            item_name: Name of the item to delete.
            item_type: Type of the item to delete (e.g., "Notebook", "Lakehouse").
                      Supported types: Notebook, Lakehouse, Warehouse, Pipeline,
                      DataPipeline, Report, SemanticModel, Dashboard, Dataflow, Dataset.
                      
        Returns:
            Dictionary with status and success/error message.
            
        Example:
            ```python
            result = delete_item(
                workspace_name="My Workspace",
                item_name="Old Notebook",
                item_type="Notebook"
            )
            ```
        """
        log_tool_invocation("delete_item", workspace_name=workspace_name,
                          item_name=item_name, item_type=item_type)
        logger.info(f"Deleting {item_type} '{item_name}' from workspace '{workspace_name}'")
        
        try:
            # Resolve workspace ID
            workspace_id = workspace_service.resolve_workspace_id(workspace_name)
            
            # Find the item
            item = item_service.get_item_by_name(workspace_id, item_name, item_type)
            
            # Delete the item
            item_service.delete_item(workspace_id, item.id)
            
            logger.info(f"Successfully deleted {item_type} '{item_name}'")
            return {
                "status": "success",
                "message": f"Successfully deleted {item_type} '{item_name}'"
            }
            
        except FabricItemNotFoundError:
            error_msg = f"{item_type} '{item_name}' not found in workspace '{workspace_name}'"
            logger.error(error_msg)
            return {
                "status": "error",
                "message": error_msg
            }
    
    @mcp.tool(title="Rename Item")
    @handle_tool_errors
    def rename_item(
        workspace_name: str,
        item_id: str,
        new_display_name: str,
        description: Optional[str] = None,
    ) -> dict:
        """Rename an item in a Fabric workspace.

        Parameters:
            workspace_name: The display name of the workspace.
            item_id: ID of the item to rename.
            new_display_name: New display name for the item.
            description: Optional description to update.

        Returns:
            Dictionary with status and updated item metadata.
        """
        log_tool_invocation(
            "rename_item",
            workspace_name=workspace_name,
            item_id=item_id,
            new_display_name=new_display_name,
        )
        logger.info(
            f"Renaming item '{item_id}' in workspace '{workspace_name}' to '{new_display_name}'"
        )

        workspace_id = workspace_service.resolve_workspace_id(workspace_name)
        item = item_service.rename_item(
            workspace_id=workspace_id,
            item_id=item_id,
            new_display_name=new_display_name,
            description=description,
        )

        return {
            "status": "success",
            "item": {
                "id": item.id,
                "display_name": item.display_name,
                "type": item.type,
                "description": item.description,
                "folder_id": item.folder_id,
                "created_date": item.created_date,
                "modified_date": item.modified_date,
            },
            "workspace_id": workspace_id,
            "workspace_name": workspace_name,
            "message": f"Item '{item.id}' renamed successfully",
        }

    @mcp.tool(title="Move Item to Folder")
    @handle_tool_errors
    def move_item_to_folder(
        workspace_name: str,
        item_id: str,
        target_folder_id: Optional[str] = None,
        target_folder_path: Optional[str] = None,
    ) -> dict:
        """Move an item to a folder in a Fabric workspace.

        Parameters:
            workspace_name: The display name of the workspace.
            item_id: ID of the item to move.
            target_folder_id: Folder ID to move the item into. Use "root" or omit to move to workspace root.
            target_folder_path: Folder path to move the item into. Omit to move to root.

        Returns:
            Dictionary with status and moved item metadata.
        """
        log_tool_invocation(
            "move_item_to_folder",
            workspace_name=workspace_name,
            item_id=item_id,
            target_folder_id=target_folder_id,
            target_folder_path=target_folder_path,
        )
        logger.info(
            f"Moving item '{item_id}' to folder '{target_folder_id}' in workspace '{workspace_name}'"
        )

        workspace_id = workspace_service.resolve_workspace_id(workspace_name)
        if target_folder_id and target_folder_path:
            raise FabricValidationError(
                "target_folder_path",
                target_folder_path,
                "Provide either target_folder_id or target_folder_path, not both.",
            )
        # Accept "root" as sentinel for workspace root
        if isinstance(target_folder_id, str) and target_folder_id.lower() == "root":
            target_folder_id = None
        if target_folder_path:
            target_folder_id = item_service.resolve_folder_id_from_path(
                workspace_id, target_folder_path, create_missing=False
            )
        item = item_service.move_item_to_folder(
            workspace_id=workspace_id,
            item_id=item_id,
            target_folder_id=target_folder_id,
        )

        return {
            "status": "success",
            "item": {
                "id": item.id,
                "display_name": item.display_name,
                "type": item.type,
                "description": item.description,
                "folder_id": item.folder_id,
                "created_date": item.created_date,
                "modified_date": item.modified_date,
            },
            "workspace_id": workspace_id,
            "workspace_name": workspace_name,
            "message": f"Item '{item.id}' moved successfully",
        }

    logger.info("Item tools registered successfully (10 tools)")
