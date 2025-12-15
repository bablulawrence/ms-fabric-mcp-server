# ABOUTME: Main package exports for ms-fabric-mcp-server.
# ABOUTME: Provides register_fabric_tools, create_fabric_server, and version info.
"""Microsoft Fabric MCP Server.

A Model Context Protocol (MCP) server for Microsoft Fabric that exposes 
Fabric operations as MCP tools for AI agents.

Example:
    ```python
    from ms_fabric_mcp_server import create_fabric_server
    
    server = create_fabric_server()
    server.run()
    ```
    
    Or use programmatically:
    
    ```python
    from fastmcp import FastMCP
    from ms_fabric_mcp_server import register_fabric_tools
    
    mcp = FastMCP("my-server")
    register_fabric_tools(mcp)
    mcp.run()
    ```
"""

from importlib.metadata import version, PackageNotFoundError

try:
    __version__ = version("ms-fabric-mcp-server")
except PackageNotFoundError:
    # Package is not installed (development mode)
    __version__ = "0.0.0.dev"

# Main exports
from .server import create_fabric_server
from .tools import register_fabric_tools

# Client exports
from .client import (
    FabricConfig,
    FabricClient,
    FabricError,
    FabricAuthError,
    FabricAPIError,
    FabricItemNotFoundError,
    FabricWorkspaceNotFoundError,
    FabricValidationError,
    FabricConnectionError,
    FabricLivyError,
)

# Service exports
from .services import (
    FabricWorkspaceService,
    FabricItemService,
    FabricNotebookService,
    FabricJobService,
    FabricSQLService,
    FabricLivyService,
    FabricPipelineService,
)

# Model exports
from .models import (
    FabricWorkspace,
    FabricItem,
    FabricLakehouse,
    FabricJob,
)

__all__ = [
    # Version
    "__version__",
    
    # Main functions
    "create_fabric_server",
    "register_fabric_tools",
    
    # Client
    "FabricConfig",
    "FabricClient",
    
    # Exceptions
    "FabricError",
    "FabricAuthError",
    "FabricAPIError",
    "FabricItemNotFoundError",
    "FabricWorkspaceNotFoundError",
    "FabricValidationError",
    "FabricConnectionError",
    "FabricLivyError",
    
    # Services
    "FabricWorkspaceService",
    "FabricItemService",
    "FabricNotebookService",
    "FabricJobService",
    "FabricSQLService",
    "FabricLivyService",
    "FabricPipelineService",
    
    # Models
    "FabricWorkspace",
    "FabricItem",
    "FabricLakehouse",
    "FabricJob",
]
