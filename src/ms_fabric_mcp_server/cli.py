# ABOUTME: CLI entry point for the ms-fabric-mcp-server package.
# ABOUTME: Provides argument parsing and runs the MCP server.
"""CLI entry point for ms-fabric-mcp-server.

This module provides the command-line interface for running the Fabric MCP server.
"""

import argparse
import logging
import sys

from . import __version__
from .server import create_fabric_server

logger = logging.getLogger(__name__)


def parse_args(args=None):
    """Parse command-line arguments.
    
    Args:
        args: Optional list of arguments (defaults to sys.argv)
        
    Returns:
        Parsed arguments namespace
    """
    parser = argparse.ArgumentParser(
        prog="ms-fabric-mcp-server",
        description="MCP server for Microsoft Fabric - exposes Fabric operations as MCP tools"
    )
    
    parser.add_argument(
        "--version",
        action="version",
        version=f"%(prog)s {__version__}"
    )
    
    parser.add_argument(
        "--log-level",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        default=None,
        help="Override MCP_LOG_LEVEL environment variable"
    )
    
    return parser.parse_args(args)


def main(args=None):
    """Main entry point for the CLI.
    
    Args:
        args: Optional list of arguments (defaults to sys.argv)
    """
    parsed_args = parse_args(args)
    
    try:
        # Create and run the server
        server = create_fabric_server(log_level=parsed_args.log_level)
        server.run()
        
    except KeyboardInterrupt:
        logger.info("Server stopped by user")
        sys.exit(0)
        
    except Exception as exc:
        logger.error(f"Server error: {exc}")
        sys.exit(1)


if __name__ == "__main__":
    main()
