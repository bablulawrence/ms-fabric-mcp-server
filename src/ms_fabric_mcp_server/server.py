# ABOUTME: Main MCP server setup for Microsoft Fabric.
# ABOUTME: Provides create_fabric_server() to create a pre-configured FastMCP server with all Fabric tools.
"""Main MCP server setup for Microsoft Fabric.

This module provides the server factory function that creates a FastMCP server
pre-configured with all Fabric tools.
"""

import logging
import os
from typing import Optional

from dotenv import load_dotenv
from fastmcp import FastMCP

from .tools import register_fabric_tools

logger = logging.getLogger(__name__)


def create_fabric_server(
    name: Optional[str] = None,
    log_level: Optional[str] = None
) -> FastMCP:
    """Create a FastMCP server pre-configured with all Fabric tools.
    
    This is the main factory function for creating a ready-to-use MCP server
    with all 28 Fabric tools registered.
    
    Args:
        name: Server name (default: from MCP_SERVER_NAME env var or 'ms-fabric-mcp-server')
        log_level: Logging level (default: from MCP_LOG_LEVEL env var or 'INFO')
        
    Returns:
        Configured FastMCP server instance
        
    Example:
        ```python
        from ms_fabric_mcp_server import create_fabric_server
        
        server = create_fabric_server()
        server.run()
        ```
    """
    # Load .env file if present
    load_dotenv()
    
    # Configure logging
    log_level = log_level or os.getenv("MCP_LOG_LEVEL", "INFO")
    logging.basicConfig(
        level=getattr(logging, log_level.upper(), logging.INFO),
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    
    # Get server name
    server_name = name or os.getenv("MCP_SERVER_NAME", "ms-fabric-mcp-server")
    
    logger.info(f"Creating Fabric MCP server: {server_name}")
    
    # Create FastMCP server
    mcp = FastMCP(server_name)
    
    # Register all Fabric tools
    register_fabric_tools(mcp)
    
    logger.info(f"Fabric MCP server '{server_name}' created successfully")
    
    return mcp
