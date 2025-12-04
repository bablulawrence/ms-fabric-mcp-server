# ABOUTME: Generic service for Fabric item operations.
# ABOUTME: Handles listing, creating, updating, and deleting Fabric items.
"""Generic service for Fabric item operations."""

import logging
import time
from typing import List, Dict, Optional, Any, TYPE_CHECKING

if TYPE_CHECKING:
    from ..models.lakehouse import FabricLakehouse

from ..client.http_client import FabricClient
from ..client.exceptions import (
    FabricItemNotFoundError,
    FabricAPIError,
    FabricError,
    FabricValidationError,
)
from ..models.item import FabricItem


logger = logging.getLogger(__name__)


class FabricItemService:
    """Generic service for Fabric item operations.
    
    This service provides high-level operations for managing Fabric items:
    - List items in a workspace (with optional type filter)
    - Find item by name or ID
    - Create new items
    - Update existing items
    - Delete items
    - Get item definitions
    
    Supported item types include: Notebook, Lakehouse, Warehouse, Pipeline,
    Report, SemanticModel, Dashboard, Dataflow, and many others.
    
    Example:
        ```python
        from ms_fabric_mcp_server import FabricConfig, FabricClient, FabricItemService
        
        config = FabricConfig.from_environment()
        client = FabricClient(config)
        item_service = FabricItemService(client)
        
        # List all notebooks in a workspace
        notebooks = item_service.list_items(workspace_id, "Notebook")
        
        # Find specific notebook
        notebook = item_service.get_item_by_name(workspace_id, "My Notebook", "Notebook")
        ```
    """
    
    # Supported Fabric item types (from official Microsoft documentation, July 2025)
    SUPPORTED_ITEM_TYPES = {
        "Notebook",
        "Lakehouse",
        "Warehouse",
        "Pipeline",
        "DataPipeline",
        "Report",
        "SemanticModel",
        "Dashboard",
        "Dataflow",
        "Dataset",
        "Datamart",
        "PaginatedReport",
        "KQLDashboard",
        "KQLDatabase",
        "KQLQueryset",
        "ApacheAirflowJob",
        "CopyJob",
        "DigitalTwinBuilder",
        "DigitalTwinBuilderFlow",
        "Environment",
        "Eventhouse",
        "Eventstream",
        "GraphQLApi",
        "MLExperiment",
        "MLModel",
        "MirroredAzureDatabricksCatalog",
        "MirroredDatabase",
        "MirroredWarehouse",
        "MountedDataFactory",
        "Reflex",
        "SQLDatabase",
        "SQLEndpoint",
        "SparkJobDefinition",
        "VariableLibrary",
        "WarehouseSnapshot"
    }
    
    def __init__(self, client: FabricClient):
        """Initialize the item service.
        
        Args:
            client: FabricClient instance for API communication
        """
        self.client = client
        
        logger.debug("FabricItemService initialized")
    
    def _validate_item_type(self, item_type: str) -> None:
        """Validate that item type is supported.
        
        Args:
            item_type: Type of Fabric item
            
        Raises:
            FabricValidationError: If item type is not supported
        """
        if item_type not in self.SUPPORTED_ITEM_TYPES:
            raise FabricValidationError(
                "item_type", 
                item_type, 
                f"Unsupported item type. Supported types: {', '.join(sorted(self.SUPPORTED_ITEM_TYPES))}"
            )
    
    def list_items(
        self, 
        workspace_id: str, 
        item_type: Optional[str] = None
    ) -> List[FabricItem]:
        """List items in workspace, optionally filtered by type.
        
        Args:
            workspace_id: Workspace ID
            item_type: Optional item type filter
            
        Returns:
            List of FabricItem objects
            
        Raises:
            FabricValidationError: If item_type is invalid
            FabricAPIError: If API request fails
        """
        if item_type:
            self._validate_item_type(item_type)
        
        logger.info(f"Fetching items from workspace {workspace_id}")
        
        try:
            # Build endpoint with optional type filter
            endpoint = f"workspaces/{workspace_id}/items"
            params = {}
            if item_type:
                params["type"] = item_type
            
            # Add query parameters if any
            if params:
                param_str = "&".join([f"{k}={v}" for k, v in params.items()])
                endpoint = f"{endpoint}?{param_str}"
            
            response = self.client.make_api_request("GET", endpoint)
            items_data = response.json().get("value", [])
            
            # Convert to FabricItem objects
            items = []
            for item_data in items_data:
                item = FabricItem(
                    id=item_data["id"],
                    display_name=item_data["displayName"],
                    type=item_data["type"],
                    workspace_id=workspace_id,
                    description=item_data.get("description"),
                    created_date=item_data.get("createdDate"),
                    modified_date=item_data.get("modifiedDate")
                )
                items.append(item)
            
            logger.info(f"Successfully fetched {len(items)} items from workspace {workspace_id}")
            return items
            
        except FabricAPIError:
            # Re-raise API errors
            raise
        except Exception as exc:
            logger.error(f"Unexpected error fetching items: {exc}")
            raise FabricError(f"Failed to fetch items: {exc}")
    
    def get_item_by_name(
        self, 
        workspace_id: str, 
        name: str, 
        item_type: str
    ) -> FabricItem:
        """Find item by name and type.
        
        Args:
            workspace_id: Workspace ID
            name: Display name of the item
            item_type: Type of the item
            
        Returns:
            FabricItem object
            
        Raises:
            FabricItemNotFoundError: If item not found
            FabricValidationError: If item_type is invalid
        """
        self._validate_item_type(item_type)
        
        logger.debug(f"Looking up {item_type} '{name}' in workspace {workspace_id}")
        
        # Fetch items of this type
        items = self.list_items(workspace_id, item_type)
        
        # Find item by name
        for item in items:
            if item.display_name == name:
                logger.info(f"Found {item_type} '{name}' with ID: {item.id}")
                return item
        
        # Not found
        logger.warning(f"{item_type} '{name}' not found in workspace {workspace_id}")
        raise FabricItemNotFoundError(item_type, name, workspace_id)
    
    def get_item_by_id(self, workspace_id: str, item_id: str) -> FabricItem:
        """Get item by ID.
        
        Args:
            workspace_id: Workspace ID
            item_id: Item ID
            
        Returns:
            FabricItem object
            
        Raises:
            FabricItemNotFoundError: If item not found
            FabricAPIError: If API request fails
        """
        logger.debug(f"Looking up item by ID: {item_id}")
        
        try:
            response = self.client.make_api_request("GET", f"workspaces/{workspace_id}/items/{item_id}")
            item_data = response.json()
            
            item = FabricItem(
                id=item_data["id"],
                display_name=item_data["displayName"],
                type=item_data["type"],
                workspace_id=workspace_id,
                description=item_data.get("description"),
                created_date=item_data.get("createdDate"),
                modified_date=item_data.get("modifiedDate")
            )
            
            logger.info(f"Found item '{item.display_name}' with ID: {item_id}")
            return item
            
        except FabricAPIError as exc:
            if exc.status_code == 404:
                logger.warning(f"Item ID '{item_id}' not found")
                raise FabricItemNotFoundError("Item", item_id, workspace_id)
            raise
        except Exception as exc:
            logger.error(f"Unexpected error fetching item {item_id}: {exc}")
            raise FabricError(f"Failed to fetch item: {exc}")
    
    def get_item_definition(self, workspace_id: str, item_id: str) -> Dict[str, Any]:
        """Get the full definition of an item, including content.
        
        Args:
            workspace_id: Workspace ID
            item_id: Item ID
            
        Returns:
            Dictionary containing item definition
            
        Raises:
            FabricAPIError: If API request fails
        """
        logger.debug(f"Fetching definition for item {item_id}")
        
        try:
            response = self.client.make_api_request(
                "POST", 
                f"workspaces/{workspace_id}/items/{item_id}/getDefinition"
            )
            definition = response.json()
            
            logger.info(f"Successfully fetched definition for item {item_id}")
            return definition
            
        except FabricAPIError:
            raise
        except Exception as exc:
            logger.error(f"Unexpected error fetching item definition: {exc}")
            raise FabricError(f"Failed to fetch item definition: {exc}")
    
    def create_item(self, workspace_id: str, item_definition: Dict[str, Any]) -> FabricItem:
        """Create new item in workspace.
        
        Args:
            workspace_id: Workspace ID
            item_definition: Item definition dictionary
            
        Returns:
            Created FabricItem object
            
        Raises:
            FabricValidationError: If item definition is invalid
            FabricAPIError: If API request fails
        """
        # Basic validation
        required_fields = ["displayName", "type"]
        for field in required_fields:
            if field not in item_definition:
                raise FabricValidationError(
                    field, "missing", f"Required field '{field}' missing from item definition"
                )
        
        item_type = item_definition["type"]
        self._validate_item_type(item_type)
        
        logger.info(f"Creating {item_type} '{item_definition['displayName']}' in workspace {workspace_id}")
        
        try:
            response = self.client.make_api_request(
                "POST",
                f"workspaces/{workspace_id}/items",
                payload=item_definition
            )
            
            # Handle different response formats
            if response.status_code == 201:
                item_data = response.json()
                item = FabricItem(
                    id=item_data["id"],
                    display_name=item_data["displayName"],
                    type=item_data["type"],
                    workspace_id=workspace_id,
                    description=item_data.get("description"),
                    created_date=item_data.get("createdDate"),
                    modified_date=item_data.get("modifiedDate")
                )
                
                logger.info(f"Successfully created {item_type} with ID: {item.id}")
                return item
            
            elif response.status_code == 202:
                # Accepted - operation is async
                logger.info("Item creation accepted (async operation)")
                try:
                    response_data = response.json()
                    item_id = response_data.get("id")
                    if item_id:
                        # Wait and fetch the created item
                        time.sleep(2)
                        return self.get_item_by_id(workspace_id, item_id)
                except:
                    pass
                
                # Return minimal item object
                return FabricItem(
                    id="unknown",
                    display_name=item_definition["displayName"],
                    type=item_definition["type"],
                    workspace_id=workspace_id,
                    description=item_definition.get("description")
                )
            
            else:
                raise FabricAPIError(
                    response.status_code, 
                    "Unexpected response status for item creation"
                )
                
        except FabricAPIError:
            raise
        except Exception as exc:
            logger.error(f"Unexpected error creating item: {exc}")
            raise FabricError(f"Failed to create item: {exc}")
    
    def update_item(
        self, 
        workspace_id: str, 
        item_id: str, 
        updates: Dict[str, Any]
    ) -> FabricItem:
        """Update existing item.
        
        Args:
            workspace_id: Workspace ID
            item_id: Item ID
            updates: Dictionary of fields to update
            
        Returns:
            Updated FabricItem object
            
        Raises:
            FabricAPIError: If API request fails
        """
        logger.info(f"Updating item {item_id}")
        
        try:
            response = self.client.make_api_request(
                "PATCH",
                f"workspaces/{workspace_id}/items/{item_id}",
                payload=updates
            )
            item_data = response.json()
            
            item = FabricItem(
                id=item_data["id"],
                display_name=item_data["displayName"],
                type=item_data["type"],
                workspace_id=workspace_id,
                description=item_data.get("description"),
                created_date=item_data.get("createdDate"),
                modified_date=item_data.get("modifiedDate")
            )
            
            logger.info(f"Successfully updated item {item_id}")
            return item
            
        except FabricAPIError:
            raise
        except Exception as exc:
            logger.error(f"Unexpected error updating item: {exc}")
            raise FabricError(f"Failed to update item: {exc}")
    
    def delete_item(self, workspace_id: str, item_id: str) -> None:
        """Delete item from workspace.
        
        Args:
            workspace_id: Workspace ID
            item_id: Item ID
            
        Raises:
            FabricAPIError: If API request fails
        """
        logger.info(f"Deleting item {item_id}")
        
        try:
            self.client.make_api_request("DELETE", f"workspaces/{workspace_id}/items/{item_id}")
            logger.info(f"Successfully deleted item {item_id}")
            
        except FabricAPIError:
            raise
        except Exception as exc:
            logger.error(f"Unexpected error deleting item: {exc}")
            raise FabricError(f"Failed to delete item: {exc}")
    
    def create_lakehouse(
        self,
        workspace_id: str,
        display_name: str,
        description: Optional[str] = None,
        enable_schemas: bool = True
    ) -> "FabricLakehouse":
        """Create a new lakehouse in workspace.
        
        Args:
            workspace_id: Workspace ID
            display_name: Name for the new lakehouse
            description: Optional description for the lakehouse
            enable_schemas: Whether to enable schemas (default: True)
            
        Returns:
            The created FabricLakehouse object
            
        Raises:
            FabricAPIError: If lakehouse creation fails
        """
        from ..models.lakehouse import FabricLakehouse
        
        logger.info(f"Creating lakehouse '{display_name}' in workspace {workspace_id}")
        
        # Prepare payload according to Fabric API spec
        payload = {
            "displayName": display_name,
            "type": "Lakehouse"
        }
        
        if description:
            payload["description"] = description
        
        # Add creation payload with enableSchemas
        payload["creationPayload"] = {
            "enableSchemas": enable_schemas
        }
        
        try:
            response = self.client.make_api_request(
                "POST",
                f"workspaces/{workspace_id}/lakehouses",
                payload=payload,
                timeout=60
            )
            
            lakehouse_data = response.json()
            
            # Map the response fields to our Pydantic model
            lakehouse = FabricLakehouse(
                id=lakehouse_data["id"],
                display_name=lakehouse_data["displayName"],
                description=lakehouse_data.get("description"),
                workspace_id=workspace_id,
                enable_schemas=enable_schemas,
                type="Lakehouse",
                created_date=lakehouse_data.get("createdDate"),
                modified_date=lakehouse_data.get("modifiedDate")
            )
            
            logger.info(f"Successfully created lakehouse '{display_name}' with ID: {lakehouse.id}")
            return lakehouse
            
        except FabricAPIError:
            # Re-raise API errors as-is
            raise
        except Exception as exc:
            logger.error(f"Failed to create lakehouse '{display_name}': {exc}")
            raise FabricAPIError(500, f"Lakehouse creation failed: {exc}")
