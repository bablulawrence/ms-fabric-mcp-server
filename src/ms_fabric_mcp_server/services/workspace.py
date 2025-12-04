# ABOUTME: Service for Microsoft Fabric workspace operations.
# ABOUTME: Handles listing, creating, deleting workspaces and resolving workspace IDs.
"""Service for Microsoft Fabric workspace operations."""

import logging
from typing import List, Optional

from ..client.http_client import FabricClient
from ..client.exceptions import (
    FabricWorkspaceNotFoundError,
    FabricAPIError,
    FabricError,
)
from ..models.workspace import FabricWorkspace


logger = logging.getLogger(__name__)


class FabricWorkspaceService:
    """Service for Microsoft Fabric workspace operations.
    
    This service provides high-level operations for managing workspaces:
    - List all accessible workspaces
    - Find workspace by name or ID
    - Create new workspaces
    - Delete workspaces
    - Resolve workspace identifiers (name or ID) to workspace IDs
    
    Example:
        ```python
        from ms_fabric_mcp_server import FabricConfig, FabricClient, FabricWorkspaceService
        
        config = FabricConfig.from_environment()
        client = FabricClient(config)
        workspace_service = FabricWorkspaceService(client)
        
        # List all workspaces
        workspaces = workspace_service.list_workspaces()
        
        # Find workspace by name
        workspace = workspace_service.get_workspace_by_name("My Workspace")
        
        # Resolve workspace identifier (name or ID) to ID
        workspace_id = workspace_service.resolve_workspace_id("My Workspace")
        ```
    """
    
    def __init__(self, client: FabricClient):
        """Initialize the workspace service.
        
        Args:
            client: FabricClient instance for API communication
        """
        self.client = client
        
        logger.debug("FabricWorkspaceService initialized")
    
    def list_workspaces(self) -> List[FabricWorkspace]:
        """Get all accessible workspaces.
            
        Returns:
            List of FabricWorkspace objects
            
        Raises:
            FabricAPIError: If API request fails
            FabricError: For other errors
        """
        logger.info("Fetching workspace list from Fabric API")
        
        try:
            response = self.client.make_api_request("GET", "workspaces")
            workspace_data = response.json().get("value", [])
            
            # Convert to FabricWorkspace objects
            workspaces = []
            for ws_data in workspace_data:
                workspace = FabricWorkspace(
                    id=ws_data["id"],
                    display_name=ws_data["displayName"],
                    description=ws_data.get("description"),
                    type=ws_data.get("type", "Workspace"),
                    state=ws_data.get("state"),
                    capacity_id=ws_data.get("capacityId")
                )
                workspaces.append(workspace)
            
            logger.info(f"Successfully fetched {len(workspaces)} workspaces")
            return workspaces
            
        except FabricAPIError:
            # Re-raise API errors
            raise
        except Exception as exc:
            logger.error(f"Unexpected error fetching workspaces: {exc}")
            raise FabricError(f"Failed to fetch workspaces: {exc}")
    
    def get_workspace_by_name(self, name: str) -> FabricWorkspace:
        """Find workspace by display name.
        
        Args:
            name: Display name of the workspace
            
        Returns:
            FabricWorkspace object
            
        Raises:
            FabricWorkspaceNotFoundError: If workspace not found
            FabricAPIError: If API request fails
        """
        logger.debug(f"Looking up workspace by name: '{name}'")
        
        # Fetch workspace list
        workspaces = self.list_workspaces()
        
        # Find workspace by name
        for workspace in workspaces:
            if workspace.display_name == name:
                logger.info(f"Found workspace '{name}' with ID: {workspace.id}")
                return workspace
        
        # Not found
        logger.warning(f"Workspace '{name}' not found")
        raise FabricWorkspaceNotFoundError(name)
    
    def get_workspace_by_id(self, workspace_id: str) -> FabricWorkspace:
        """Get workspace by ID.
        
        Args:
            workspace_id: Unique identifier of the workspace
            
        Returns:
            FabricWorkspace object
            
        Raises:
            FabricWorkspaceNotFoundError: If workspace not found
            FabricAPIError: If API request fails
        """
        logger.debug(f"Looking up workspace by ID: {workspace_id}")
        
        # Try to fetch specific workspace
        try:
            response = self.client.make_api_request("GET", f"workspaces/{workspace_id}")
            ws_data = response.json()
            
            workspace = FabricWorkspace(
                id=ws_data["id"],
                display_name=ws_data["displayName"],
                description=ws_data.get("description"),
                type=ws_data.get("type", "Workspace"),
                state=ws_data.get("state"),
                capacity_id=ws_data.get("capacityId")
            )
            
            logger.info(f"Found workspace '{workspace.display_name}' with ID: {workspace_id}")
            return workspace
            
        except FabricAPIError as exc:
            if exc.status_code == 404:
                logger.warning(f"Workspace ID '{workspace_id}' not found")
                raise FabricWorkspaceNotFoundError(workspace_id)
            raise
        except Exception as exc:
            logger.error(f"Unexpected error fetching workspace {workspace_id}: {exc}")
            raise FabricError(f"Failed to fetch workspace: {exc}")
    
    def resolve_workspace_id(self, workspace_identifier: str) -> str:
        """Resolve workspace identifier to workspace ID.
        
        This method accepts either a workspace ID or display name
        and returns the workspace ID.
        
        Args:
            workspace_identifier: Workspace ID or display name
            
        Returns:
            Workspace ID
            
        Raises:
            FabricWorkspaceNotFoundError: If workspace not found
        """
        # If it looks like a UUID, treat as ID
        if len(workspace_identifier) == 36 and workspace_identifier.count('-') == 4:
            try:
                workspace = self.get_workspace_by_id(workspace_identifier)
                return workspace.id
            except FabricWorkspaceNotFoundError:
                # Maybe it's not an ID after all, try as name
                pass
        
        # Try as display name
        workspace = self.get_workspace_by_name(workspace_identifier)
        return workspace.id
    
    def create_workspace(
        self,
        display_name: str,
        description: Optional[str] = None,
        capacity_id: Optional[str] = None
    ) -> FabricWorkspace:
        """Create a new workspace in Microsoft Fabric.
        
        Args:
            display_name: Name for the new workspace
            description: Optional description for the workspace
            capacity_id: Optional capacity ID to assign to the workspace
            
        Returns:
            The created workspace object
            
        Raises:
            FabricAPIError: If workspace creation fails
        """
        logger.info(f"Creating workspace '{display_name}'")
        
        # Prepare payload
        payload = {"displayName": display_name}
        if description:
            payload["description"] = description
        if capacity_id:
            payload["capacityId"] = capacity_id
        
        try:
            response = self.client.make_api_request(
                "POST",
                "workspaces",
                payload=payload,
                timeout=60
            )
            
            workspace_data = response.json()
            
            # Map the response fields to match our Pydantic model
            workspace = FabricWorkspace(
                id=workspace_data["id"],
                display_name=workspace_data["displayName"],
                description=workspace_data.get("description"),
                type=workspace_data.get("type", "Workspace"),
                state=workspace_data.get("state"),
                capacity_id=workspace_data.get("capacityId")
            )
            
            logger.info(f"Successfully created workspace '{display_name}' with ID: {workspace.id}")
            return workspace
            
        except FabricAPIError:
            # Re-raise API errors as-is
            raise
        except Exception as exc:
            logger.error(f"Failed to create workspace '{display_name}': {exc}")
            raise FabricAPIError(500, f"Workspace creation failed: {exc}")

    def delete_workspace(self, workspace_identifier: str) -> None:
        """Delete a workspace in Microsoft Fabric.
        
        WARNING: This operation permanently deletes the workspace and ALL items within it.
        
        Args:
            workspace_identifier: Workspace ID or display name to delete
            
        Raises:
            FabricWorkspaceNotFoundError: If workspace not found
            FabricAPIError: If workspace deletion fails
        """
        # Resolve workspace identifier to ID
        workspace_id = self.resolve_workspace_id(workspace_identifier)
        
        logger.info(f"Deleting workspace with ID: {workspace_id}")
        
        try:
            self.client.make_api_request(
                "DELETE",
                f"workspaces/{workspace_id}",
                timeout=60
            )
            
            logger.info(f"Successfully deleted workspace with ID: {workspace_id}")
            
        except FabricAPIError:
            # Re-raise API errors as-is
            raise
        except Exception as exc:
            logger.error(f"Failed to delete workspace {workspace_identifier}: {exc}")
            raise FabricAPIError(500, f"Workspace deletion failed: {exc}")
