# ABOUTME: Pipeline management MCP tools for Microsoft Fabric.
# ABOUTME: Provides tools to create pipelines and add activities for data ingestion.
"""Pipeline management MCP tools.

This module provides MCP tools for Microsoft Fabric pipeline operations including
creating pipelines with Copy Activities for data ingestion.
"""

import logging
from typing import TYPE_CHECKING, Any, Dict, Optional

if TYPE_CHECKING:
    from fastmcp import FastMCP

from ..services import (FabricItemService, FabricPipelineService,
                        FabricWorkspaceService)
from .base import (format_error_response, format_success_response,
                   handle_tool_errors, log_tool_invocation)

logger = logging.getLogger(__name__)


def register_pipeline_tools(
    mcp: "FastMCP",
    pipeline_service: FabricPipelineService,
    workspace_service: FabricWorkspaceService,
    item_service: FabricItemService,
):
    """Register pipeline management MCP tools.

    This function registers pipeline-related tools:
    - create_pipeline: Create a pipeline (blank or from definition)
    - add_copy_activity_to_pipeline: Add a Copy Activity to an existing pipeline
    - add_notebook_activity_to_pipeline: Add a Notebook Activity to an existing pipeline
    - add_dataflow_activity_to_pipeline: Add a Dataflow Activity to an existing pipeline
    - add_activity_to_pipeline: Add any activity from JSON template to an existing pipeline
    - delete_activity_from_pipeline: Delete an activity from an existing pipeline
    - remove_activity_dependency: Remove dependsOn entries referencing an activity
    - add_activity_dependency: Add dependsOn entries to an activity
    - get_pipeline_definition: Fetch and decode pipeline definition JSON
    - update_pipeline_definition: Update pipeline definition JSON
    - get_pipeline_activity_runs: Get per-activity execution details for a pipeline run

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

    @mcp.tool(title="Create Pipeline")
    @handle_tool_errors
    def create_pipeline(
        workspace_name: str,
        pipeline_name: str,
        description: Optional[str] = None,
        folder_path: Optional[str] = None,
        pipeline_content_json: Optional[Dict[str, Any]] = None,
        platform: Optional[Dict[str, Any]] = None,
    ) -> dict:
        """Create a Fabric pipeline.

        Creates either a blank pipeline (when no definition is provided) or
        a pipeline based on a supplied definition.

        Parameters:
            workspace_name: The display name of the workspace where the pipeline will be created.
            pipeline_name: Name for the new pipeline (must be unique in workspace).
            description: Optional description for the pipeline.
            folder_path: Optional folder path (e.g., "pipelines/daily") to place the pipeline.
                         Defaults to the workspace root when omitted.
            pipeline_content_json: Optional pipeline definition JSON.
            platform: Optional .platform JSON definition.

        Returns:
            Dictionary with status, pipeline_id, pipeline_name, workspace_name, and message.
        """
        log_tool_invocation(
            "create_pipeline",
            workspace_name=workspace_name,
            pipeline_name=pipeline_name,
            folder_path=folder_path,
        )

        logger.info(
            f"Creating pipeline '{pipeline_name}' in workspace '{workspace_name}'"
        )

        workspace_id = workspace_service.resolve_workspace_id(workspace_name)
        workspace = workspace_service.get_workspace_by_id(workspace_id)

        if pipeline_content_json is None:
            pipeline_id = pipeline_service.create_blank_pipeline(
                workspace_id=workspace_id,
                pipeline_name=pipeline_name,
                description=description,
                folder_path=folder_path,
            )
        else:
            pipeline_id = pipeline_service.create_pipeline_with_definition(
                workspace_id=workspace_id,
                display_name=pipeline_name,
                pipeline_content_json=pipeline_content_json,
                platform=platform,
                description=description,
                folder_path=folder_path,
            )

        result = {
            "status": "success",
            "pipeline_id": pipeline_id,
            "pipeline_name": pipeline_name,
            "workspace_name": workspace.display_name,
            "workspace_id": workspace_id,
            "message": f"Pipeline '{pipeline_name}' created successfully",
        }

        logger.info(
            f"Pipeline created successfully: {pipeline_id} in workspace {workspace.display_name}"
        )
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
        source_access_mode: str = "direct",
        source_sql_query: Optional[str] = None,
        table_action_option: str = "Append",
        apply_v_order: bool = True,
        timeout: str = "0.12:00:00",
        retry: int = 0,
        retry_interval_seconds: int = 30,
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
            source_access_mode: Source access mode ("direct" or "sql"). Default is "direct".
            source_sql_query: Optional SQL query for sql access mode.
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

            # SQL fallback mode (use when direct Lakehouse copy fails with
            # "datasource type Lakehouse is invalid" error):
            result = add_copy_activity_to_pipeline(
                workspace_name="Analytics Workspace",
                pipeline_name="My_Existing_Pipeline",
                source_type="LakehouseTableSource",
                source_connection_id=sql_endpoint_conn_id,  # SQL analytics endpoint connection
                source_table_schema="dbo",
                source_table_name="fact_sale",
                destination_lakehouse_id=lakehouse_id,
                destination_connection_id=lakehouse_conn_id,
                destination_table_name="fact_sale_copy",
                source_access_mode="sql",
                source_sql_query="SELECT * FROM dbo.fact_sale"  # optional
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
            activity_name=activity_name
            or f"CopyDataToLakehouse_{destination_table_name}",
            source_access_mode=source_access_mode,
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
            source_access_mode=source_access_mode,
            source_sql_query=source_sql_query,
            table_action_option=table_action_option,
            apply_v_order=apply_v_order,
            timeout=timeout,
            retry=retry,
            retry_interval_seconds=retry_interval_seconds,
        )

        final_activity_name = (
            activity_name or f"CopyDataToLakehouse_{destination_table_name}"
        )

        result = {
            "status": "success",
            "pipeline_id": pipeline_id,
            "pipeline_name": pipeline_name,
            "activity_name": final_activity_name,
            "workspace_name": workspace.display_name,
            "workspace_id": workspace_id,
            "message": f"Copy Activity '{final_activity_name}' added successfully to pipeline '{pipeline_name}'",
        }

        logger.info(
            f"Copy Activity '{final_activity_name}' added successfully to pipeline {pipeline_id}"
        )
        return result

    @mcp.tool(title="Add Notebook Activity to Pipeline")
    @handle_tool_errors
    def add_notebook_activity_to_pipeline(
        workspace_name: str,
        pipeline_name: str,
        notebook_name: str,
        notebook_workspace_name: Optional[str] = None,
        activity_name: Optional[str] = None,
        depends_on_activity_name: Optional[str] = None,
        session_tag: Optional[str] = None,
        parameters: Optional[Dict[str, Any]] = None,
        timeout: str = "0.12:00:00",
        retry: int = 0,
        retry_interval_seconds: int = 30,
    ) -> dict:
        """Add a Notebook Activity to an existing Fabric pipeline.

        Retrieves an existing pipeline, adds a Notebook Activity to it, and updates
        the pipeline definition. The Notebook Activity will be appended to any existing
        activities in the pipeline.

        **Use this tool when:**
        - You have an existing pipeline and want to add a new Notebook Activity
        - You're building complex pipelines with multiple activities
        - You want to incrementally build a pipeline

        Parameters:
            workspace_name: The display name of the workspace containing the pipeline.
            pipeline_name: Name of the existing pipeline to update.
            notebook_name: Name of the notebook to run.
            notebook_workspace_name: Optional name of the workspace containing the notebook.
            activity_name: Optional custom name for the activity (default: auto-generated).
            depends_on_activity_name: Optional name of an existing activity this one depends on.
            session_tag: Optional session tag for the notebook execution.
            parameters: Optional parameters to pass to the notebook.
            timeout: Activity timeout (default: "0.12:00:00").
            retry: Number of retry attempts (default: 0).
            retry_interval_seconds: Retry interval in seconds (default: 30).

        Returns:
            Dictionary with status, pipeline_id, pipeline_name, activity_name, workspace_name, and message.
        """
        activity_name = activity_name or f"RunNotebook_{notebook_name}"
        log_tool_invocation(
            "add_notebook_activity_to_pipeline",
            workspace_name=workspace_name,
            pipeline_name=pipeline_name,
            notebook_name=notebook_name,
            activity_name=activity_name,
            depends_on_activity_name=depends_on_activity_name,
        )

        logger.info(
            f"Adding Notebook Activity to pipeline '{pipeline_name}' in workspace '{workspace_name}' "
            f"to run {notebook_name}"
        )

        # Resolve workspace IDs
        workspace_id = workspace_service.resolve_workspace_id(workspace_name)
        workspace = workspace_service.get_workspace_by_id(workspace_id)
        notebook_workspace_id = (
            workspace_service.resolve_workspace_id(notebook_workspace_name)
            if notebook_workspace_name
            else workspace_id
        )

        pipeline_id = pipeline_service.add_notebook_activity_to_pipeline(
            workspace_id=workspace_id,
            pipeline_name=pipeline_name,
            notebook_name=notebook_name,
            activity_name=activity_name,
            notebook_workspace_id=notebook_workspace_id,
            depends_on_activity_name=depends_on_activity_name,
            session_tag=session_tag,
            parameters=parameters,
            timeout=timeout,
            retry=retry,
            retry_interval_seconds=retry_interval_seconds,
        )

        result = {
            "status": "success",
            "pipeline_id": pipeline_id,
            "pipeline_name": pipeline_name,
            "activity_name": activity_name,
            "workspace_name": workspace.display_name,
            "workspace_id": workspace_id,
            "message": f"Notebook Activity '{activity_name}' added successfully to pipeline '{pipeline_name}'",
        }

        logger.info(
            f"Notebook Activity '{activity_name}' added successfully to pipeline {pipeline_id}"
        )
        return result

    @mcp.tool(title="Add Dataflow Activity to Pipeline")
    @handle_tool_errors
    def add_dataflow_activity_to_pipeline(
        workspace_name: str,
        pipeline_name: str,
        dataflow_name: str,
        dataflow_workspace_name: Optional[str] = None,
        activity_name: Optional[str] = None,
        depends_on_activity_name: Optional[str] = None,
        timeout: str = "0.12:00:00",
        retry: int = 0,
        retry_interval_seconds: int = 30,
    ) -> dict:
        """Add a Dataflow Activity to an existing Fabric pipeline.

        Retrieves an existing pipeline, adds a Dataflow Activity to it, and updates
        the pipeline definition. The Dataflow Activity will be appended to any existing
        activities in the pipeline.

        **Use this tool when:**
        - You have an existing pipeline and want to add a new Dataflow Activity
        - You're building complex pipelines with multiple activities
        - You want to incrementally build a pipeline

        Parameters:
            workspace_name: The display name of the workspace containing the pipeline.
            pipeline_name: Name of the existing pipeline to update.
            dataflow_name: Name of the Dataflow to run.
            dataflow_workspace_name: Optional name of the workspace containing the Dataflow.
            activity_name: Optional custom name for the activity (default: auto-generated).
            depends_on_activity_name: Optional name of an existing activity this one depends on.
            timeout: Activity timeout (default: "0.12:00:00").
            retry: Number of retry attempts (default: 0).
            retry_interval_seconds: Retry interval in seconds (default: 30).

        Returns:
            Dictionary with status, pipeline_id, pipeline_name, activity_name, workspace_name, and message.
        """
        activity_name = activity_name or f"RunDataflow_{dataflow_name}"
        log_tool_invocation(
            "add_dataflow_activity_to_pipeline",
            workspace_name=workspace_name,
            pipeline_name=pipeline_name,
            dataflow_name=dataflow_name,
            activity_name=activity_name,
            depends_on_activity_name=depends_on_activity_name,
        )

        logger.info(
            f"Adding Dataflow Activity to pipeline '{pipeline_name}' in workspace '{workspace_name}' "
            f"to run {dataflow_name}"
        )

        # Resolve workspace IDs
        workspace_id = workspace_service.resolve_workspace_id(workspace_name)
        workspace = workspace_service.get_workspace_by_id(workspace_id)
        dataflow_workspace_id = (
            workspace_service.resolve_workspace_id(dataflow_workspace_name)
            if dataflow_workspace_name
            else workspace_id
        )

        pipeline_id = pipeline_service.add_dataflow_activity_to_pipeline(
            workspace_id=workspace_id,
            pipeline_name=pipeline_name,
            dataflow_name=dataflow_name,
            activity_name=activity_name,
            dataflow_workspace_id=dataflow_workspace_id,
            depends_on_activity_name=depends_on_activity_name,
            timeout=timeout,
            retry=retry,
            retry_interval_seconds=retry_interval_seconds,
        )

        result = {
            "status": "success",
            "pipeline_id": pipeline_id,
            "pipeline_name": pipeline_name,
            "activity_name": activity_name,
            "workspace_name": workspace.display_name,
            "workspace_id": workspace_id,
            "message": f"Dataflow Activity '{activity_name}' added successfully to pipeline '{pipeline_name}'",
        }

        logger.info(
            f"Dataflow Activity '{activity_name}' added successfully to pipeline {pipeline_id}"
        )
        return result

    @mcp.tool(title="Add Activity to Pipeline from JSON")
    @handle_tool_errors
    def add_activity_to_pipeline(
        workspace_name: str, pipeline_name: str, activity_json: dict
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
            activity_type=activity_json.get("type", "Unknown"),
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
            activity_json=activity_json,
        )

        result = {
            "status": "success",
            "pipeline_id": pipeline_id,
            "pipeline_name": pipeline_name,
            "activity_name": activity_name,
            "activity_type": activity_type,
            "workspace_name": workspace.display_name,
            "workspace_id": workspace_id,
            "message": f"{activity_type} activity '{activity_name}' added successfully to pipeline '{pipeline_name}'",
        }

        logger.info(
            f"{activity_type} activity '{activity_name}' added successfully to pipeline {pipeline_id}"
        )
        return result

    @mcp.tool(title="Get Pipeline Definition")
    @handle_tool_errors
    def get_pipeline_definition(
        workspace_name: str,
        pipeline_name: str,
        format: Optional[str] = None,
    ) -> dict:
        """Get a Fabric pipeline definition (decoded JSON).

        Parameters:
            workspace_name: The display name of the workspace containing the pipeline.
            pipeline_name: Name of the pipeline.
            format: Optional format hint for the definition (pass-through).

        Returns:
            Dictionary with status, pipeline metadata, pipeline_content_json, and optional platform.
        """
        log_tool_invocation(
            "get_pipeline_definition",
            workspace_name=workspace_name,
            pipeline_name=pipeline_name,
            format=format,
        )

        logger.info(
            f"Fetching definition for pipeline '{pipeline_name}' in workspace '{workspace_name}'"
        )

        workspace_id = workspace_service.resolve_workspace_id(workspace_name)
        workspace = workspace_service.get_workspace_by_id(workspace_id)
        pipeline = item_service.get_item_by_name(
            workspace_id, pipeline_name, "DataPipeline"
        )

        definition_result = pipeline_service.get_pipeline_definition(
            workspace_id=workspace_id,
            pipeline_id=pipeline.id,
            format=format,
        )

        result = {
            "status": "success",
            "pipeline_id": pipeline.id,
            "pipeline_name": pipeline.display_name,
            "workspace_name": workspace.display_name,
            "workspace_id": workspace_id,
            "pipeline_content_json": definition_result["pipeline_content_json"],
        }

        platform = definition_result.get("platform")
        if platform is not None:
            result["platform"] = platform

        logger.info(
            f"Pipeline definition fetched successfully for {pipeline.display_name} ({pipeline.id})"
        )
        return result

    @mcp.tool(title="Update Pipeline Definition")
    @handle_tool_errors
    def update_pipeline_definition(
        workspace_name: str,
        pipeline_name: str,
        pipeline_content_json: dict,
        platform: Optional[dict] = None,
        update_metadata: bool = False,
    ) -> dict:
        """Update a Fabric pipeline definition (decoded JSON input).

        Parameters:
            workspace_name: The display name of the workspace containing the pipeline.
            pipeline_name: Name of the pipeline.
            pipeline_content_json: Pipeline definition JSON to upload.
            platform: Optional .platform JSON definition.
            update_metadata: Whether to apply metadata updates (requires platform).

        Returns:
            Dictionary with status and pipeline metadata.
        """
        log_tool_invocation(
            "update_pipeline_definition",
            workspace_name=workspace_name,
            pipeline_name=pipeline_name,
            update_metadata=update_metadata,
        )

        logger.info(
            f"Updating definition for pipeline '{pipeline_name}' in workspace '{workspace_name}'"
        )

        workspace_id = workspace_service.resolve_workspace_id(workspace_name)
        workspace = workspace_service.get_workspace_by_id(workspace_id)
        pipeline = item_service.get_item_by_name(
            workspace_id, pipeline_name, "DataPipeline"
        )

        pipeline_service.update_pipeline_definition(
            workspace_id=workspace_id,
            pipeline_id=pipeline.id,
            pipeline_content_json=pipeline_content_json,
            platform=platform,
            update_metadata=update_metadata,
        )

        result = {
            "status": "success",
            "pipeline_id": pipeline.id,
            "pipeline_name": pipeline.display_name,
            "workspace_name": workspace.display_name,
            "workspace_id": workspace_id,
            "message": f"Pipeline '{pipeline.display_name}' definition updated successfully",
        }

        logger.info(
            f"Pipeline definition updated successfully for {pipeline.display_name} ({pipeline.id})"
        )
        return result

    @mcp.tool(title="Add Activity Dependency")
    @handle_tool_errors
    def add_activity_dependency(
        workspace_name: str,
        pipeline_name: str,
        activity_name: str,
        depends_on: list[str],
    ) -> dict:
        """Add dependsOn dependencies to a pipeline activity.

        Parameters:
            workspace_name: The display name of the workspace containing the pipeline.
            pipeline_name: Name of the pipeline.
            activity_name: Name of the activity to update.
            depends_on: List of activity names this activity depends on.

        Returns:
            Dictionary with status, pipeline metadata, and added dependency count.
        """
        log_tool_invocation(
            "add_activity_dependency",
            workspace_name=workspace_name,
            pipeline_name=pipeline_name,
            activity_name=activity_name,
            depends_on=depends_on,
        )

        logger.info(
            f"Adding dependencies to '{activity_name}' in pipeline '{pipeline_name}' "
            f"within workspace '{workspace_name}'"
        )

        workspace_id = workspace_service.resolve_workspace_id(workspace_name)
        workspace = workspace_service.get_workspace_by_id(workspace_id)

        pipeline_id, added_count = pipeline_service.add_activity_dependency(
            workspace_id=workspace_id,
            pipeline_name=pipeline_name,
            activity_name=activity_name,
            depends_on=depends_on,
        )

        result = {
            "status": "success",
            "pipeline_id": pipeline_id,
            "pipeline_name": pipeline_name,
            "activity_name": activity_name,
            "added_count": added_count,
            "workspace_name": workspace.display_name,
            "workspace_id": workspace_id,
            "message": f"Added {added_count} dependencies to '{activity_name}' in pipeline '{pipeline_name}'",
        }

        logger.info(
            f"Added {added_count} dependencies to '{activity_name}' in pipeline {pipeline_id}"
        )
        return result

    @mcp.tool(title="Delete Activity from Pipeline")
    @handle_tool_errors
    def delete_activity_from_pipeline(
        workspace_name: str,
        pipeline_name: str,
        activity_name: str,
    ) -> dict:
        """Delete an activity from an existing Fabric pipeline.

        Removes the specified activity from the pipeline definition. This will
        fail if any other activity depends on it. Use remove_activity_dependency
        to remove dependencies first.

        Parameters:
            workspace_name: The display name of the workspace containing the pipeline.
            pipeline_name: Name of the existing pipeline to update.
            activity_name: Name of the activity to delete.

        Returns:
            Dictionary with status, pipeline_id, pipeline_name, activity_name, workspace_name, and message.
        """
        log_tool_invocation(
            "delete_activity_from_pipeline",
            workspace_name=workspace_name,
            pipeline_name=pipeline_name,
            activity_name=activity_name,
        )

        logger.info(
            f"Deleting activity '{activity_name}' from pipeline '{pipeline_name}' "
            f"in workspace '{workspace_name}'"
        )

        workspace_id = workspace_service.resolve_workspace_id(workspace_name)
        workspace = workspace_service.get_workspace_by_id(workspace_id)

        pipeline_id = pipeline_service.delete_activity_from_pipeline(
            workspace_id=workspace_id,
            pipeline_name=pipeline_name,
            activity_name=activity_name,
        )

        result = {
            "status": "success",
            "pipeline_id": pipeline_id,
            "pipeline_name": pipeline_name,
            "activity_name": activity_name,
            "workspace_name": workspace.display_name,
            "workspace_id": workspace_id,
            "message": (
                f"Activity '{activity_name}' deleted successfully "
                f"from pipeline '{pipeline_name}'"
            ),
        }

        logger.info(
            f"Activity '{activity_name}' deleted successfully from pipeline {pipeline_id}"
        )
        return result

    @mcp.tool(title="Remove Activity Dependency")
    @handle_tool_errors
    def remove_activity_dependency(
        workspace_name: str,
        pipeline_name: str,
        activity_name: str,
        from_activity_name: Optional[str] = None,
    ) -> dict:
        """Remove dependsOn references to a target activity.

        Removes dependsOn edges pointing to the target activity. If from_activity_name
        is provided, only removes edges from that activity.

        Parameters:
            workspace_name: The display name of the workspace containing the pipeline.
            pipeline_name: Name of the existing pipeline to update.
            activity_name: Name of the activity being depended on.
            from_activity_name: Optional activity to remove dependencies from.

        Returns:
            Dictionary with status, pipeline_id, pipeline_name, activity_name,
            removed_count, workspace_name, and message.
        """
        log_tool_invocation(
            "remove_activity_dependency",
            workspace_name=workspace_name,
            pipeline_name=pipeline_name,
            activity_name=activity_name,
            from_activity_name=from_activity_name,
        )

        logger.info(
            f"Removing dependencies on '{activity_name}' from pipeline '{pipeline_name}' "
            f"in workspace '{workspace_name}'"
        )

        workspace_id = workspace_service.resolve_workspace_id(workspace_name)
        workspace = workspace_service.get_workspace_by_id(workspace_id)

        pipeline_id, removed_count = pipeline_service.remove_activity_dependency(
            workspace_id=workspace_id,
            pipeline_name=pipeline_name,
            activity_name=activity_name,
            from_activity_name=from_activity_name,
        )

        result = {
            "status": "success",
            "pipeline_id": pipeline_id,
            "pipeline_name": pipeline_name,
            "activity_name": activity_name,
            "from_activity_name": from_activity_name,
            "removed_count": removed_count,
            "workspace_name": workspace.display_name,
            "workspace_id": workspace_id,
            "message": (
                f"Removed {removed_count} dependencies on '{activity_name}' "
                f"from pipeline '{pipeline_name}'"
            ),
        }

        logger.info(
            f"Removed {removed_count} dependencies on '{activity_name}' "
            f"from pipeline {pipeline_id}"
        )
        return result

    @mcp.tool(title="Get Pipeline Activity Runs")
    @handle_tool_errors
    def get_pipeline_activity_runs(
        workspace_name: str,
        job_instance_id: str,
        status_filter: Optional[str] = None,
        activity_name: Optional[str] = None,
    ) -> dict:
        """Get activity runs for a pipeline job execution.

        Retrieves per-activity execution details for a given pipeline run, including
        timing, status, and output metrics (row counts for Copy activities).

        **Use this tool when:**
        - A pipeline run completed and you need to see which activities succeeded/failed
        - You want to diagnose why a pipeline failed by examining activity errors
        - You need row counts or timing details for Copy activities

        Parameters:
            workspace_name: The display name of the workspace containing the pipeline.
            job_instance_id: The job instance ID from run_on_demand_job or get_job_status.
            status_filter: Optional filter by status: "Succeeded", "Failed", "InProgress", "Cancelled".
            activity_name: Optional filter to a specific activity by name.

        Returns:
            Dictionary with status, activity_count, pipeline_name, and activities list.
            Each activity contains: activity_name, activity_type, status, duration_ms,
            start_time, end_time, rows_read (Copy only), rows_written (Copy only),
            error_message (if failed).

        Example:
            ```python
            # After running a pipeline
            job_result = run_on_demand_job(
                workspace_name="Analytics",
                item_name="Copy_Data_Pipeline",
                item_type="DataPipeline",
                job_type="Pipeline"
            )

            # Check activity details
            activities = get_pipeline_activity_runs(
                workspace_name="Analytics",
                job_instance_id=job_result["job_instance_id"]
            )

            # Filter to failed activities only
            failed = get_pipeline_activity_runs(
                workspace_name="Analytics",
                job_instance_id=job_result["job_instance_id"],
                status_filter="Failed"
            )
            ```
        """
        log_tool_invocation(
            "get_pipeline_activity_runs",
            workspace_name=workspace_name,
            job_instance_id=job_instance_id,
            status_filter=status_filter,
            activity_name=activity_name,
        )

        logger.info(
            f"Getting activity runs for job '{job_instance_id}' "
            f"in workspace '{workspace_name}'"
        )

        workspace_id = workspace_service.resolve_workspace_id(workspace_name)
        workspace = workspace_service.get_workspace_by_id(workspace_id)

        result_data = pipeline_service.get_pipeline_activity_runs(
            workspace_id=workspace_id,
            job_instance_id=job_instance_id,
            status_filter=status_filter,
            activity_name_filter=activity_name,
        )

        result = {
            "status": "success",
            "message": f"Retrieved {result_data['activity_count']} activity runs",
            "workspace_name": workspace.display_name,
            "workspace_id": workspace_id,
            "job_instance_id": job_instance_id,
            "pipeline_name": result_data.get("pipeline_name"),
            "activity_count": result_data["activity_count"],
            "activities": result_data["activities"],
        }

        logger.info(
            f"Retrieved {result_data['activity_count']} activity runs "
            f"for job {job_instance_id}"
        )
        return result

    logger.info("Pipeline tools registered successfully (11 tools)")
