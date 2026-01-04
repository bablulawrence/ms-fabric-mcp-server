# ABOUTME: Models module - Pydantic models for Fabric API objects.
# ABOUTME: Provides models for workspaces, items, jobs, lakehouses, and operation results.
"""Fabric data models - Pydantic models for Fabric API objects."""

# Workspace models
from ms_fabric_mcp_server.models.workspace import FabricWorkspace

# Item models
from ms_fabric_mcp_server.models.item import FabricItem

# Lakehouse models
from ms_fabric_mcp_server.models.lakehouse import FabricLakehouse

# Job models
from ms_fabric_mcp_server.models.job import FabricJob

# Semantic model models
from ms_fabric_mcp_server.models.semantic_model import SemanticModelColumn, DataType

# Result models
from ms_fabric_mcp_server.models.results import (
    FabricOperationResult,
    ImportNotebookResult,
    AttachLakehouseResult,
    ExecuteNotebookResult,
    CreateItemResult,
    QueryResult,
    RunJobRequest,
    RunJobResult,
    JobStatusResult,
    OperationResult,
)

__all__ = [
    # Workspace
    "FabricWorkspace",
    # Items
    "FabricItem",
    # Lakehouse
    "FabricLakehouse",
    # Jobs
    "FabricJob",
    # Semantic Models
    "SemanticModelColumn",
    "DataType",
    # Results
    "FabricOperationResult",
    "ImportNotebookResult",
    "AttachLakehouseResult",
    "ExecuteNotebookResult",
    "CreateItemResult",
    "QueryResult",
    "RunJobRequest",
    "RunJobResult",
    "JobStatusResult",
    "OperationResult",
]
