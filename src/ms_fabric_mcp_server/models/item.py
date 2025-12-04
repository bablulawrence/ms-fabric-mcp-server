# ABOUTME: Item-related data models for Microsoft Fabric.
# ABOUTME: Provides FabricItem model for generic Fabric items (notebooks, lakehouses, etc).
"""Item-related data models for Microsoft Fabric."""

from typing import Optional, Dict, Any
from pydantic import BaseModel, ConfigDict, Field


class FabricItem(BaseModel):
    """Generic Fabric item model.
    
    Represents any item in a Fabric workspace (Notebook, Lakehouse, Warehouse,
    Pipeline, Report, SemanticModel, etc.).
    
    Attributes:
        id: Unique identifier for the item (GUID)
        display_name: Display name of the item (shown in Fabric UI)
        type: Type of item (Notebook, Lakehouse, Warehouse, Pipeline, etc.)
        workspace_id: ID of the workspace containing this item
        description: Optional description of the item
        created_date: Creation timestamp (ISO 8601 format)
        modified_date: Last modification timestamp (ISO 8601 format)
        definition: Item definition details (type-specific structure)
    
    Example:
        ```python
        notebook = FabricItem(
            id="item-123-abc",
            display_name="My Notebook",
            type="Notebook",
            workspace_id="ws-456-def",
            description="Data analysis notebook"
        )
        ```
    """
    
    id: str = Field(description="Unique identifier for the item")
    display_name: str = Field(description="Display name of the item")
    type: str = Field(description="Type of item (Notebook, Lakehouse, Warehouse, etc.)")
    workspace_id: str = Field(description="ID of the workspace containing this item")
    description: Optional[str] = Field(default=None, description="Description of the item")
    created_date: Optional[str] = Field(default=None, description="Creation timestamp")
    modified_date: Optional[str] = Field(default=None, description="Last modification timestamp")
    definition: Optional[Dict[str, Any]] = Field(default=None, description="Item definition details")
    
    model_config = ConfigDict(from_attributes=True)
