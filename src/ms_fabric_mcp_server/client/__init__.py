# ABOUTME: Client module - HTTP client and configuration for Fabric API.
# ABOUTME: Provides FabricClient, FabricConfig, and exception classes.
"""Fabric client module - HTTP client and configuration."""

from ms_fabric_mcp_server.client.config import FabricConfig
from ms_fabric_mcp_server.client.exceptions import (
    FabricAPIError, FabricAuthError, FabricConfigError, FabricConnectionError,
    FabricError, FabricItemNotFoundError, FabricJobTimeoutError,
    FabricLivyError, FabricLivySessionError, FabricLivyStatementError,
    FabricLivyTimeoutError, FabricRateLimitError, FabricValidationError,
    FabricWorkspaceNotFoundError)
from ms_fabric_mcp_server.client.http_client import FabricClient

__all__ = [
    "FabricConfig",
    "FabricClient",
    # Exceptions
    "FabricError",
    "FabricAuthError",
    "FabricItemNotFoundError",
    "FabricWorkspaceNotFoundError",
    "FabricJobTimeoutError",
    "FabricAPIError",
    "FabricValidationError",
    "FabricConfigError",
    "FabricConnectionError",
    "FabricRateLimitError",
    "FabricLivyError",
    "FabricLivySessionError",
    "FabricLivyStatementError",
    "FabricLivyTimeoutError",
]
