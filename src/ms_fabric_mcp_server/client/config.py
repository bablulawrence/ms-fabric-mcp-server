# ABOUTME: Configuration settings for Fabric API integration.
# ABOUTME: Loads settings from environment variables with sensible defaults.
"""Configuration settings for Fabric API integration."""

import os
from typing import Optional
from pydantic import BaseModel, Field


class FabricConfig(BaseModel):
    """Configuration settings for Fabric API integration.
    
    This configuration can be loaded from environment variables using
    the from_environment() class method.
    
    Environment Variables:
        FABRIC_BASE_URL: Base URL for Fabric API (default: https://api.fabric.microsoft.com/v1)
        FABRIC_SCOPES: OAuth scopes (default: https://api.fabric.microsoft.com/.default)
        FABRIC_API_CALL_TIMEOUT: API call timeout in seconds (default: 30)
        FABRIC_MAX_RETRIES: Maximum retry attempts (default: 3)
        FABRIC_RETRY_BACKOFF: Retry backoff factor (default: 2.0)
        LIVY_API_CALL_TIMEOUT: Livy API timeout in seconds (default: 120)
        LIVY_POLL_INTERVAL: Livy polling interval in seconds (default: 2.0)
        LIVY_STATEMENT_WAIT_TIMEOUT: Livy statement wait timeout in seconds (default: 10)
        LIVY_SESSION_WAIT_TIMEOUT: Livy session wait timeout in seconds (default: 240)
    """
    
    # API Configuration
    BASE_URL: str = Field(
        default="https://api.fabric.microsoft.com/v1",
        description="Base URL for Fabric API"
    )
    SCOPES: list[str] = Field(
        default_factory=lambda: ["https://api.fabric.microsoft.com/.default"],
        description="OAuth scopes for authentication"
    )
    
    # Timeout and Retry Configuration
    API_CALL_TIMEOUT: int = Field(
        default=30,
        description="API call timeout in seconds",
        ge=1
    )
    MAX_RETRIES: int = Field(
        default=3,
        description="Maximum number of retry attempts",
        ge=0
    )
    RETRY_BACKOFF: float = Field(
        default=2.0,
        description="Exponential backoff factor for retries",
        gt=0.0
    )
    
    # Livy Configuration
    LIVY_API_CALL_TIMEOUT: int = Field(
        default=120,
        description="Livy API call timeout in seconds",
        ge=1
    )
    LIVY_POLL_INTERVAL: float = Field(
        default=2.0,
        description="Polling interval for Livy operations in seconds",
        gt=0.0
    )
    LIVY_STATEMENT_WAIT_TIMEOUT: int = Field(
        default=10,
        description="Timeout for waiting on Livy statement completion in seconds",
        ge=1
    )
    LIVY_SESSION_WAIT_TIMEOUT: int = Field(
        default=240,
        description="Timeout for waiting on Livy session startup in seconds",
        ge=1
    )
    
    @classmethod
    def from_environment(cls) -> 'FabricConfig':
        """Load configuration from environment variables.
        
        Returns:
            FabricConfig instance with values from environment
        """
        scopes_str = os.getenv("FABRIC_SCOPES", "https://api.fabric.microsoft.com/.default")
        scopes = [s.strip() for s in scopes_str.split(",")]
        
        return cls(
            BASE_URL=os.getenv("FABRIC_BASE_URL", "https://api.fabric.microsoft.com/v1"),
            SCOPES=scopes,
            API_CALL_TIMEOUT=int(os.getenv("FABRIC_API_CALL_TIMEOUT", "30")),
            MAX_RETRIES=int(os.getenv("FABRIC_MAX_RETRIES", "3")),
            RETRY_BACKOFF=float(os.getenv("FABRIC_RETRY_BACKOFF", "2.0")),
            LIVY_API_CALL_TIMEOUT=int(os.getenv("LIVY_API_CALL_TIMEOUT", "120")),
            LIVY_POLL_INTERVAL=float(os.getenv("LIVY_POLL_INTERVAL", "2.0")),
            LIVY_STATEMENT_WAIT_TIMEOUT=int(os.getenv("LIVY_STATEMENT_WAIT_TIMEOUT", "10")),
            LIVY_SESSION_WAIT_TIMEOUT=int(os.getenv("LIVY_SESSION_WAIT_TIMEOUT", "240")),
        )
    
    def get_endpoints(self) -> dict[str, str]:
        """Get common API endpoint templates.
        
        Returns:
            Dictionary of endpoint templates with placeholders
        """
        return {
            "workspaces": f"{self.BASE_URL}/workspaces",
            "workspace_items": f"{self.BASE_URL}/workspaces/{{workspace_id}}/items",
            "item_definition": f"{self.BASE_URL}/workspaces/{{workspace_id}}/items/{{item_id}}/getDefinition",
            "notebook_jobs": f"{self.BASE_URL}/workspaces/{{workspace_id}}/items/{{item_id}}/jobs/instances",
            "job_status": f"{self.BASE_URL}/workspaces/{{workspace_id}}/items/{{item_id}}/jobs/instances/{{job_instance_id}}",
            "livy_sessions": f"{self.BASE_URL}/workspaces/{{workspace_id}}/lakehouses/{{lakehouse_id}}/livyapi/versions/2023-12-01/sessions",
            "livy_session": f"{self.BASE_URL}/workspaces/{{workspace_id}}/lakehouses/{{lakehouse_id}}/livyapi/versions/2023-12-01/sessions/{{session_id}}",
            "livy_session_log": f"{self.BASE_URL}/workspaces/{{workspace_id}}/lakehouses/{{lakehouse_id}}/livyapi/versions/2023-12-01/sessions/{{session_id}}/log",
            "livy_statements": f"{self.BASE_URL}/workspaces/{{workspace_id}}/lakehouses/{{lakehouse_id}}/livyapi/versions/2023-12-01/sessions/{{session_id}}/statements",
            "livy_statement": f"{self.BASE_URL}/workspaces/{{workspace_id}}/lakehouses/{{lakehouse_id}}/livyapi/versions/2023-12-01/sessions/{{session_id}}/statements/{{statement_id}}",
        }


__all__ = ["FabricConfig"]
