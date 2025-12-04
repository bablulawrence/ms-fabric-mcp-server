# ABOUTME: Pipeline management MCP tools for Microsoft Fabric.
# ABOUTME: Provides tools to create pipelines and add activities for data ingestion.
"""Pipeline management MCP tools.

This module provides MCP tools for Microsoft Fabric pipeline operations including
creating pipelines with Copy Activities for data ingestion.
"""

from typing import Optional, TYPE_CHECKING
import logging

if TYPE_CHECKING:
    from fastmcp import FastMCP

from ..services import (
    FabricPipelineService,
    FabricWorkspaceService,
    FabricItemService,
)
from .base import handle_tool_errors, format_success_response, format_error_response, log_tool_invocation

logger = logging.getLogger(__name__)


def register_pipeline_tools(
    mcp: "FastMCP",
    pipeline_service: FabricPipelineService,
    workspace_service: FabricWorkspaceService,
    item_service: FabricItemService,
):
    """Register pipeline management MCP tools.
    
    This function registers pipeline-related tools:
    - create_blank_pipeline: Create a blank pipeline with no activities
    - add_copy_activity_to_pipeline: Add a Copy Activity to an existing pipeline
    - add_activity_to_pipeline: Add any activity from JSON template to an existing pipeline
    
    Args:
        mcp: FastMCP server instance to register tools on.
        pipeline_service: Initialized FabricPipelineService instance.
        workspace_service: Initialized FabricWorkspaceService instance.
        item_service: Initialized FabricItemService instance.
        
    Example:
        ```python
        from ms_fabric_mcp_server import FabricConfig, FabricClient
        from ms_fabric_mcp_server.services import (
            FabricPipelineService,
            FabricWorkspaceService,
            FabricItemService
        )
        from ms_fabric_mcp_server.tools import register_pipeline_tools
        
        config = FabricConfig.from_environment()
        client = FabricClient(config)
        workspace_service = FabricWorkspaceService(client)
        item_service = FabricItemService(client)
        pipeline_service = FabricPipelineService(client, workspace_service, item_service)
        
        register_pipeline_tools(mcp, pipeline_service, workspace_service, item_service)
        ```
    """
    
    @mcp.tool(title="Create Blank Pipeline")
    @handle_tool_errors
    def create_blank_pipeline(
        workspace_name: str,
        pipeline_name: str,
        description: Optional[str] = None
    ) -> dict:
        """Create a blank Fabric pipeline with no activities.
        
        Creates a Data Pipeline in the specified workspace with an empty activities
        array, ready to be populated with activities later using the
        add_copy_activity_to_pipeline tool.
        
        Parameters:
            workspace_name: The display name of the workspace where the pipeline will be created.
            pipeline_name: Name for the new pipeline (must be unique in workspace).
            description: Optional description for the pipeline.
            
        Returns:
            Dictionary with status, pipeline_id, pipeline_name, workspace_name, and message.
            
        Example:
            ```python
            # Create a blank pipeline
            result = create_blank_pipeline(
                workspace_name="Analytics Workspace",
                pipeline_name="My_Data_Integration_Pipeline",
                description="Pipeline for data integration workflows"
            )
            
            # Later, add activities to it
            add_copy_activity_to_pipeline(
                workspace_name="Analytics Workspace",
                pipeline_name="My_Data_Integration_Pipeline",
                source_type="AzurePostgreSqlSource",
                source_connection_id=conn_id,
                source_table_schema="public",
                source_table_name="customers",
                destination_lakehouse_id=lakehouse_id,
                destination_connection_id=lakehouse_conn_id,
                destination_table_name="customers"
            )
            ```
        """
        log_tool_invocation(
            "create_blank_pipeline",
            workspace_name=workspace_name,
            pipeline_name=pipeline_name
        )
        
        logger.info(f"Creating blank pipeline '{pipeline_name}' in workspace '{workspace_name}'")
        
        # Resolve workspace ID
        workspace_id = workspace_service.resolve_workspace_id(workspace_name)
        workspace = workspace_service.get_workspace_by_id(workspace_id)
        
        # Create the blank pipeline
        pipeline_id = pipeline_service.create_blank_pipeline(
            workspace_id=workspace_id,
            pipeline_name=pipeline_name,
            description=description
        )
        
        result = {
            "status": "success",
            "pipeline_id": pipeline_id,
            "pipeline_name": pipeline_name,
            "workspace_name": workspace.display_name,
            "workspace_id": workspace_id,
            "message": f"Blank pipeline '{pipeline_name}' created successfully"
        }
        
        logger.info(f"Blank pipeline created successfully: {pipeline_id} in workspace {workspace.display_name}")
        return result
    
    @mcp.tool(title="Add Copy Activity to Pipeline")
    @handle_tool_errors
    def add_copy_activity_to_pipeline(
        workspace_name: str,
        pipeline_name: str,
        source_type: str,
        source_connection_id: str,
        source_table_schema: str,
        source_table_name: str,
        destination_lakehouse_id: str,
        destination_connection_id: str,
        destination_table_name: str,
        activity_name: Optional[str] = None,
        table_action_option: str = "Append",
        apply_v_order: bool = True,
        timeout: str = "0.12:00:00",
        retry: int = 0,
        retry_interval_seconds: int = 30
    ) -> dict:
        """Add a Copy Activity to an existing Fabric pipeline.
        
        Retrieves an existing pipeline, adds a Copy Activity to it, and updates
        the pipeline definition. The Copy Activity will be appended to any existing
        activities in the pipeline.
        
        **Use this tool when:**
        - You have an existing pipeline and want to add a new Copy Activity
        - You're building complex pipelines with multiple data copy operations
        - You want to incrementally build a pipeline
        
        Parameters:
            workspace_name: The display name of the workspace containing the pipeline.
            pipeline_name: Name of the existing pipeline to update.
            source_type: Type of source (e.g., "AzurePostgreSqlSource", "AzureSqlSource", "SqlServerSource").
            source_connection_id: Fabric workspace connection ID for source database.
            source_table_schema: Schema name of the source table (e.g., "public", "dbo").
            source_table_name: Name of the source table (e.g., "movie").
            destination_lakehouse_id: Workspace artifact ID of the destination Lakehouse.
            destination_connection_id: Fabric workspace connection ID for destination Lakehouse.
            destination_table_name: Name for the destination table in Lakehouse.
            activity_name: Optional custom name for the activity (default: auto-generated).
            table_action_option: Table action option (default: "Append", options: "Append", "Overwrite").
            apply_v_order: Apply V-Order optimization (default: True).
            timeout: Activity timeout (default: "0.12:00:00").
            retry: Number of retry attempts (default: 0).
            retry_interval_seconds: Retry interval in seconds (default: 30).
            
        Returns:
            Dictionary with status, pipeline_id, pipeline_name, activity_name, workspace_name, and message.
            
        Example:
            ```python
            # First, get the lakehouse and connection IDs
            lakehouses = list_items(workspace_name="Analytics", item_type="Lakehouse")
            lakehouse_id = lakehouses["items"][0]["id"]
            lakehouse_conn_id = "a216973e-47d7-4224-bb56-2c053bac6831"
            
            # Add a Copy Activity to an existing pipeline
            result = add_copy_activity_to_pipeline(
                workspace_name="Analytics Workspace",
                pipeline_name="My_Existing_Pipeline",
                source_type="AzurePostgreSqlSource",
                source_connection_id="12345678-1234-1234-1234-123456789abc",
                source_table_schema="public",
                source_table_name="orders",
                destination_lakehouse_id=lakehouse_id,
                destination_connection_id=lakehouse_conn_id,
                destination_table_name="orders",
                activity_name="CopyOrdersData",
                table_action_option="Overwrite"
            )
            
            # Add another Copy Activity to the same pipeline
            result = add_copy_activity_to_pipeline(
                workspace_name="Analytics Workspace",
                pipeline_name="My_Existing_Pipeline",
                source_type="AzurePostgreSqlSource",
                source_connection_id="12345678-1234-1234-1234-123456789abc",
                source_table_schema="public",
                source_table_name="customers",
                destination_lakehouse_id=lakehouse_id,
                destination_connection_id=lakehouse_conn_id,
                destination_table_name="customers",
                activity_name="CopyCustomersData"
            )
            ```
        """
        log_tool_invocation(
            "add_copy_activity_to_pipeline",
            workspace_name=workspace_name,
            pipeline_name=pipeline_name,
            source_type=source_type,
            source_table=f"{source_table_schema}.{source_table_name}",
            destination_table=destination_table_name,
            activity_name=activity_name or f"CopyDataToLakehouse_{destination_table_name}"
        )
        
        logger.info(
            f"Adding Copy Activity to pipeline '{pipeline_name}' in workspace '{workspace_name}' "
            f"to copy {source_table_schema}.{source_table_name} ({source_type}) to {destination_table_name}"
        )
        
        # Resolve workspace ID
        workspace_id = workspace_service.resolve_workspace_id(workspace_name)
        workspace = workspace_service.get_workspace_by_id(workspace_id)
        
        # Add the Copy Activity to the pipeline
        pipeline_id = pipeline_service.add_copy_activity_to_pipeline(
            workspace_id=workspace_id,
            pipeline_name=pipeline_name,
            source_type=source_type,
            source_connection_id=source_connection_id,
            source_schema=source_table_schema,
            source_table=source_table_name,
            destination_lakehouse_id=destination_lakehouse_id,
            destination_connection_id=destination_connection_id,
            destination_table=destination_table_name,
            activity_name=activity_name,
            table_action_option=table_action_option,
            apply_v_order=apply_v_order,
            timeout=timeout,
            retry=retry,
            retry_interval_seconds=retry_interval_seconds
        )
        
        final_activity_name = activity_name or f"CopyDataToLakehouse_{destination_table_name}"
        
        result = {
            "status": "success",
            "pipeline_id": pipeline_id,
            "pipeline_name": pipeline_name,
            "activity_name": final_activity_name,
            "workspace_name": workspace.display_name,
            "workspace_id": workspace_id,
            "message": f"Copy Activity '{final_activity_name}' added successfully to pipeline '{pipeline_name}'"
        }
        
        logger.info(
            f"Copy Activity '{final_activity_name}' added successfully to pipeline {pipeline_id}"
        )
        return result
    
    @mcp.tool(title="Add Activity to Pipeline from JSON")
    @handle_tool_errors
    def add_activity_to_pipeline(
        workspace_name: str,
        pipeline_name: str,
        activity_json: dict
    ) -> dict:
        """Add a generic activity to an existing Fabric pipeline from a JSON template.
        
        Retrieves an existing pipeline, adds an activity from the provided JSON template,
        and updates the pipeline definition. This is a more general-purpose tool compared
        to add_copy_activity_to_pipeline, allowing you to add any type of Fabric pipeline
        activity by providing its complete JSON definition.
        
        **Use this tool when:**
        - You have a custom activity JSON template to add
        - You want to add activity types beyond Copy (e.g., Notebook, Script, Web, etc.)
        - You need full control over the activity definition
        - You're working with complex activity configurations
        
        **Activity JSON Requirements:**
        - Must be a valid dictionary/object
        - Must include a "name" field (string)
        - Must include a "type" field (e.g., "Copy", "Notebook", "Script", "Web", etc.)
        - Should include all required properties for the specific activity type
        - Common fields: "dependsOn", "policy", "typeProperties"
        
        Parameters:
            workspace_name: The display name of the workspace containing the pipeline.
            pipeline_name: Name of the existing pipeline to update.
            activity_json: Complete JSON dictionary representing the activity definition.
                          Must include "name", "type", and all required properties.
            
        Returns:
            Dictionary with status, pipeline_id, pipeline_name, activity_name, 
            activity_type, workspace_name, and message.
            
        Example:
            ```python
            # Example 1: Add a Copy Activity from JSON template
            copy_activity = {
                "name": "CopyCustomData",
                "type": "Copy",
                "dependsOn": [],
                "policy": {
                    "timeout": "0.12:00:00",
                    "retry": 0,
                    "retryIntervalInSeconds": 30,
                    "secureOutput": False,
                    "secureInput": False
                },
                "typeProperties": {
                    "source": {
                        "type": "AzurePostgreSqlSource",
                        "partitionOption": "None",
                        "queryTimeout": "02:00:00",
                        "datasetSettings": {
                            "type": "AzurePostgreSqlTable",
                            "schema": [],
                            "typeProperties": {
                                "schema": "public",
                                "table": "products"
                            },
                            "externalReferences": {
                                "connection": "12345678-1234-1234-1234-123456789abc"
                            }
                        }
                    },
                    "sink": {
                        "type": "LakehouseTableSink",
                        "tableActionOption": "Overwrite",
                        "applyVOrder": True,
                        "datasetSettings": {
                            "type": "LakehouseTable",
                            "typeProperties": {
                                "table": "products"
                            }
                        }
                    }
                }
            }
            
            result = add_activity_to_pipeline(
                workspace_name="Analytics Workspace",
                pipeline_name="My_Pipeline",
                activity_json=copy_activity
            )
            
            # Example 2: Add a Notebook Activity
            notebook_activity = {
                "name": "RunTransformation",
                "type": "Notebook",
                "dependsOn": [
                    {
                        "activity": "CopyCustomData",
                        "dependencyConditions": ["Succeeded"]
                    }
                ],
                "policy": {
                    "timeout": "1.00:00:00",
                    "retry": 0
                },
                "typeProperties": {
                    "notebookPath": "/Notebooks/TransformData",
                    "parameters": {
                        "table_name": "products"
                    }
                }
            }
            
            result = add_activity_to_pipeline(
                workspace_name="Analytics Workspace",
                pipeline_name="My_Pipeline",
                activity_json=notebook_activity
            )
            ```
        """
        log_tool_invocation(
            "add_activity_to_pipeline",
            workspace_name=workspace_name,
            pipeline_name=pipeline_name,
            activity_name=activity_json.get("name", "UnnamedActivity"),
            activity_type=activity_json.get("type", "Unknown")
        )
        
        activity_name = activity_json.get("name", "UnnamedActivity")
        activity_type = activity_json.get("type", "Unknown")
        
        logger.info(
            f"Adding {activity_type} activity '{activity_name}' to pipeline '{pipeline_name}' "
            f"in workspace '{workspace_name}'"
        )
        
        # Resolve workspace ID
        workspace_id = workspace_service.resolve_workspace_id(workspace_name)
        workspace = workspace_service.get_workspace_by_id(workspace_id)
        
        # Add the activity from JSON to the pipeline
        pipeline_id = pipeline_service.add_activity_from_json(
            workspace_id=workspace_id,
            pipeline_name=pipeline_name,
            activity_json=activity_json
        )
        
        result = {
            "status": "success",
            "pipeline_id": pipeline_id,
            "pipeline_name": pipeline_name,
            "activity_name": activity_name,
            "activity_type": activity_type,
            "workspace_name": workspace.display_name,
            "workspace_id": workspace_id,
            "message": f"{activity_type} activity '{activity_name}' added successfully to pipeline '{pipeline_name}'"
        }
        
        logger.info(
            f"{activity_type} activity '{activity_name}' added successfully to pipeline {pipeline_id}"
        )
        return result
    
    logger.info("Pipeline tools registered successfully (3 tools)")
