# ABOUTME: Main entry point for registering Microsoft Fabric MCP tools.
# ABOUTME: Provides register_fabric_tools() to add all 52 Fabric tools to an MCP server.
"""Fabric MCP tools - Modular tool registration.

This module provides the main entry point for registering Microsoft Fabric MCP tools.
Tools can be registered all at once or selectively by category.
"""

from typing import TYPE_CHECKING
import logging

if TYPE_CHECKING:
    from fastmcp import FastMCP

from ..client import FabricConfig, FabricClient
from ..services import (
    FabricWorkspaceService,
    FabricItemService,
    FabricNotebookService,
    FabricJobService,
    FabricSQLService,
    FabricLivyService,
    FabricPipelineService,
    FabricSemanticModelService,
    FabricPowerBIService,
)
from .workspace_tools import register_workspace_tools
from .item_tools import register_item_tools
from .notebook_tools import register_notebook_tools
from .job_tools import register_job_tools
from .sql_tools import register_sql_tools
from .livy_tools import register_livy_tools
from .pipeline_tools import register_pipeline_tools
from .semantic_model_tools import register_semantic_model_tools
from .powerbi_tools import register_powerbi_tools

logger = logging.getLogger(__name__)


def register_fabric_tools(mcp: "FastMCP"):
    """Register all Fabric MCP tools (workspace, item, notebook, job, SQL, Livy, pipeline).
    
    This is the main registration function that sets up all 52 Fabric tools.
    It initializes the service hierarchy and registers all tool categories.
    
    Tool Categories:
    - Workspace tools (1): list_workspaces
    - Item tools (10): list_items, get_item, list_folders, create_folder, move_folder,
      delete_folder, create_lakehouse,
      delete_item, rename_item, move_item_to_folder
    - Notebook tools (6): import_notebook_to_fabric, get_notebook_content, attach_lakehouse_to_notebook, get_notebook_execution_details, list_notebook_executions, get_notebook_driver_logs
    - Job tools (4): run_on_demand_job, get_job_status, get_job_status_by_url, get_operation_result
    - SQL tools (3): get_sql_endpoint, execute_sql_query, execute_sql_statement
    - Livy tools (8): Session and statement management for Spark
    - Pipeline tools (11): create_blank_pipeline, add_copy_activity_to_pipeline, add_notebook_activity_to_pipeline,
      add_dataflow_activity_to_pipeline, add_activity_to_pipeline, delete_activity_from_pipeline,
      remove_activity_dependency, add_activity_dependency, create_pipeline_with_definition,
      get_pipeline_definition, update_pipeline_definition
    - Semantic model tools (7): create_semantic_model, add_table_to_semantic_model,
      add_relationship_to_semantic_model, get_semantic_model_details,
      get_semantic_model_definition, add_measures_to_semantic_model,
      delete_measures_from_semantic_model
    - Power BI tools (2): refresh_semantic_model, execute_dax_query
    
    Args:
        mcp: FastMCP server instance to register tools on.
        
    Raises:
        FabricError: If service initialization fails.
        
    Example:
        ```python
        from fastmcp import FastMCP
        from ms_fabric_mcp_server import register_fabric_tools
        
        mcp = FastMCP("my-server")
        register_fabric_tools(mcp)
        
        if __name__ == "__main__":
            mcp.run()
        ```
    """
    logger.info("Initializing Fabric services for tool registration")
    
    try:
        # Initialize service hierarchy
        config = FabricConfig.from_environment()
        fabric_client = FabricClient(config)
        
        workspace_service = FabricWorkspaceService(fabric_client)
        item_service = FabricItemService(fabric_client)
        notebook_service = FabricNotebookService(fabric_client, item_service, workspace_service, repo_root=None)
        job_service = FabricJobService(fabric_client, workspace_service, item_service)
        livy_service = FabricLivyService(fabric_client)
        pipeline_service = FabricPipelineService(fabric_client, workspace_service, item_service)
        semantic_model_service = FabricSemanticModelService(workspace_service, item_service)
        powerbi_service = FabricPowerBIService(
            fabric_client,
            workspace_service,
            item_service,
            refresh_poll_interval=config.POWERBI_REFRESH_POLL_INTERVAL,
            refresh_wait_timeout=config.POWERBI_REFRESH_WAIT_TIMEOUT,
        )
        
        # SQL service is optional (requires pyodbc)
        sql_service = None
        try:
            sql_service = FabricSQLService(fabric_client, workspace_service, item_service)
        except ImportError as sql_exc:
            logger.warning(f"SQL tools disabled: {sql_exc}")
        
        logger.info("Fabric services initialized successfully")
        
    except Exception as exc:
        logger.error(f"Failed to initialize Fabric services: {exc}")
        raise
    
    # Register all tool categories
    logger.info("Registering all Fabric tool categories")
    
    register_workspace_tools(mcp, workspace_service)
    register_item_tools(mcp, item_service, workspace_service)
    register_notebook_tools(mcp, notebook_service)
    register_job_tools(mcp, job_service)
    if sql_service:
        register_sql_tools(mcp, sql_service)
    else:
        logger.info("SQL tools not registered (pyodbc not available)")
    register_livy_tools(mcp, livy_service)
    register_pipeline_tools(mcp, pipeline_service, workspace_service, item_service)
    register_semantic_model_tools(mcp, semantic_model_service)
    register_powerbi_tools(mcp, powerbi_service)
    
    tool_count = 52 if sql_service else 49  # 3 SQL tools
    logger.info(f"All Fabric tools registered successfully ({tool_count} tools)")


# Export individual registration functions for selective tool loading
__all__ = [
    "register_fabric_tools",
    "register_workspace_tools",
    "register_item_tools", 
    "register_notebook_tools",
    "register_job_tools",
    "register_sql_tools",
    "register_livy_tools",
    "register_pipeline_tools",
    "register_semantic_model_tools",
    "register_powerbi_tools",
]
