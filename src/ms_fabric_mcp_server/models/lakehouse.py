# ABOUTME: Lakehouse-related data models for Microsoft Fabric.
# ABOUTME: Provides FabricLakehouse model for lakehouse representation.
"""Lakehouse-related data models for Microsoft Fabric."""

from typing import Optional
from pydantic import BaseModel, ConfigDict, Field


class FabricLakehouse(BaseModel):
    """Fabric lakehouse model.
    
    Represents a Microsoft Fabric lakehouse - a data architecture platform
    combining data lake storage with warehouse capabilities.
    
    Attributes:
        id: Unique identifier for the lakehouse (GUID)
        display_name: Display name of the lakehouse (shown in Fabric UI)
        description: Optional description of the lakehouse
        workspace_id: ID of the workspace containing this lakehouse
        enable_schemas: Whether schemas are enabled in the lakehouse
        type: Type of item (always "Lakehouse")
        created_date: Creation timestamp (ISO 8601 format)
        modified_date: Last modification timestamp (ISO 8601 format)
    
    Example:
        ```python
        lakehouse = FabricLakehouse(
            id="lh-123-abc",
            display_name="Bronze Lakehouse",
            description="Raw data landing zone",
            workspace_id="ws-456-def",
            enable_schemas=True,
            type="Lakehouse"
        )
        ```
    """
    
    id: str = Field(description="Unique identifier for the lakehouse")
    display_name: str = Field(description="Display name of the lakehouse")
    description: Optional[str] = Field(default=None, description="Description of the lakehouse")
    workspace_id: str = Field(description="ID of the workspace containing this lakehouse")
    enable_schemas: bool = Field(default=True, description="Whether schemas are enabled")
    type: str = Field(default="Lakehouse", description="Type of item (always Lakehouse)")
    created_date: Optional[str] = Field(default=None, description="Creation timestamp")
    modified_date: Optional[str] = Field(default=None, description="Last modification timestamp")
    
    model_config = ConfigDict(from_attributes=True)
