# ABOUTME: Workspace-related data models for Microsoft Fabric.
# ABOUTME: Provides FabricWorkspace model for workspace representation.
"""Workspace-related data models for Microsoft Fabric."""

from typing import Optional
from pydantic import BaseModel, ConfigDict, Field


class FabricWorkspace(BaseModel):
    """Fabric workspace model.
    
    Represents a Microsoft Fabric workspace containing items like notebooks,
    lakehouses, warehouses, and other artifacts.
    
    Attributes:
        id: Unique identifier for the workspace (GUID)
        display_name: Display name of the workspace (shown in Fabric UI)
        description: Optional description of the workspace
        type: Type of workspace (typically "Workspace")
        state: Current state of the workspace (e.g., "Active")
        capacity_id: Capacity ID if assigned to a capacity
    
    Example:
        ```python
        workspace = FabricWorkspace(
            id="abc-123-def-456",
            display_name="My Workspace",
            description="Development workspace",
            type="Workspace",
            state="Active"
        )
        ```
    """
    
    id: str = Field(description="Unique identifier for the workspace")
    display_name: str = Field(description="Display name of the workspace")
    description: Optional[str] = Field(default=None, description="Description of the workspace")
    type: str = Field(description="Type of workspace")
    state: Optional[str] = Field(default=None, description="Current state of the workspace")
    capacity_id: Optional[str] = Field(default=None, description="Capacity ID if assigned")
    
    model_config = ConfigDict(from_attributes=True)
