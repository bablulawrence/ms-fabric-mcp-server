# ABOUTME: Dataflow Gen2 MCP tools for Microsoft Fabric.
# ABOUTME: Provides tools for creating, inspecting, and running dataflows.
"""Dataflow Gen2 MCP tools.

This module provides MCP tools for Microsoft Fabric Dataflow Gen2 operations
including creating dataflows, retrieving definitions, and running refreshes.
"""

import logging
from typing import TYPE_CHECKING, Optional

if TYPE_CHECKING:
    from fastmcp import FastMCP

from ..services.dataflow import FabricDataflowService
from ..services.item import FabricItemService
from ..services.workspace import FabricWorkspaceService
from .base import handle_tool_errors, log_tool_invocation

logger = logging.getLogger(__name__)


def register_dataflow_tools(
    mcp: "FastMCP",
    dataflow_service: FabricDataflowService,
    workspace_service: FabricWorkspaceService,
    item_service: FabricItemService,
):
    """Register Dataflow Gen2 MCP tools.

    This function registers dataflow-related tools for creating, inspecting,
    and running Dataflow Gen2 items.

    Args:
        mcp: FastMCP server instance to register tools on.
        dataflow_service: Initialized FabricDataflowService instance.
        workspace_service: Initialized FabricWorkspaceService instance.
        item_service: Initialized FabricItemService instance.

    Example:
        ```python
        from ms_fabric_mcp_server import (
            FabricConfig, FabricClient,
            FabricWorkspaceService, FabricItemService
        )
        from ms_fabric_mcp_server.services.dataflow import FabricDataflowService
        from ms_fabric_mcp_server.tools import register_dataflow_tools

        config = FabricConfig.from_environment()
        client = FabricClient(config)
        workspace_service = FabricWorkspaceService(client)
        item_service = FabricItemService(client)
        dataflow_service = FabricDataflowService(client, workspace_service, item_service)

        register_dataflow_tools(mcp, dataflow_service, workspace_service, item_service)
        ```
    """

    @mcp.tool(title="Create Dataflow")
    @handle_tool_errors
    def create_dataflow(
        workspace_name: str,
        dataflow_name: str,
        mashup_content: str,
        query_metadata: Optional[dict] = None,
        description: Optional[str] = None,
        folder_path: Optional[str] = None,
    ) -> dict:
        """Create a Dataflow Gen2 item with Power Query M code.

        Creates a new Dataflow Gen2 in the specified workspace with the provided
        Power Query M code. The dataflow can transform data from various sources.

        **Use this tool when:**
        - You need to create a new dataflow for data transformation
        - You want to define Power Query M transformations programmatically
        - You're building ETL pipelines that require Power Query logic

        Parameters:
            workspace_name: The display name of the target workspace.
            dataflow_name: Display name for the new dataflow.
            mashup_content: Power Query M code in section format (mashup.pq content).
                           Must include a section declaration and shared queries.
            query_metadata: Optional queryMetadata.json as dict. If omitted, it will
                           be auto-generated from the mashup content.
            description: Optional description for the dataflow.
            folder_path: Optional folder path (e.g., "etl/daily") to place the dataflow.
                        Defaults to the workspace root when omitted.

        Returns:
            Dictionary with status, dataflow_id, dataflow_name, and workspace_name.

        Example:
            ```python
            result = create_dataflow(
                workspace_name="Analytics",
                dataflow_name="CustomerETL",
                mashup_content=\"\"\"
section Section1;
shared Customers = let
    Source = Lakehouse.Contents([]),
    Nav1 = Source{[workspaceId = "..."]}[Data],
    Result = Nav1{[lakehouseId = "..."]}[Data]
in
    Result;
\"\"\",
                description="Customer data transformation"
            )
            ```
        """
        log_tool_invocation(
            "create_dataflow",
            workspace_name=workspace_name,
            dataflow_name=dataflow_name,
            description=description,
            folder_path=folder_path,
        )
        logger.info(
            f"Creating dataflow '{dataflow_name}' in workspace '{workspace_name}'"
        )

        # Resolve workspace ID
        workspace_id = workspace_service.resolve_workspace_id(workspace_name)

        # Resolve folder ID if folder_path provided
        folder_id = None
        if folder_path:
            folder_id = item_service.resolve_folder_path(workspace_id, folder_path)

        dataflow_id = dataflow_service.create_dataflow(
            workspace_id=workspace_id,
            dataflow_name=dataflow_name,
            mashup_content=mashup_content,
            query_metadata=query_metadata,
            description=description,
            folder_id=folder_id,
        )

        logger.info(f"Dataflow created successfully: {dataflow_id}")
        return {
            "status": "success",
            "message": f"Dataflow '{dataflow_name}' created successfully",
            "dataflow_id": dataflow_id,
            "dataflow_name": dataflow_name,
            "workspace_name": workspace_name,
        }

    @mcp.tool(title="Get Dataflow Definition")
    @handle_tool_errors
    def get_dataflow_definition(
        workspace_name: str,
        dataflow_name: str,
    ) -> dict:
        """Retrieve the full definition of an existing dataflow.

        Gets the Power Query M code and query metadata for an existing
        Dataflow Gen2 item.

        **Use this tool when:**
        - You need to inspect the current definition of a dataflow
        - You want to understand what transformations a dataflow performs
        - You're troubleshooting or modifying an existing dataflow

        Parameters:
            workspace_name: The display name of the workspace containing the dataflow.
            dataflow_name: Name of the dataflow to retrieve.

        Returns:
            Dictionary with status, dataflow_id, dataflow_name, workspace_name,
            query_metadata (parsed queryMetadata.json), and mashup_content
            (raw Power Query M code).

        Example:
            ```python
            result = get_dataflow_definition(
                workspace_name="Analytics",
                dataflow_name="CustomerETL"
            )

            if result["status"] == "success":
                print(result["mashup_content"])  # Power Query M code
                print(result["query_metadata"])  # Query configuration
            ```
        """
        log_tool_invocation(
            "get_dataflow_definition",
            workspace_name=workspace_name,
            dataflow_name=dataflow_name,
        )
        logger.info(
            f"Getting definition for dataflow '{dataflow_name}' in workspace '{workspace_name}'"
        )

        # Resolve workspace ID
        workspace_id = workspace_service.resolve_workspace_id(workspace_name)

        # Get dataflow item
        dataflow = item_service.get_item_by_name(
            workspace_id=workspace_id,
            name=dataflow_name,
            item_type="Dataflow",
        )

        query_metadata, mashup_content = dataflow_service.get_dataflow_definition(
            workspace_id=workspace_id,
            dataflow_id=dataflow.id,
        )

        logger.info(f"Successfully retrieved definition for dataflow '{dataflow_name}'")
        return {
            "status": "success",
            "dataflow_id": dataflow.id,
            "dataflow_name": dataflow_name,
            "workspace_name": workspace_name,
            "query_metadata": query_metadata,
            "mashup_content": mashup_content,
        }

    @mcp.tool(title="Run Dataflow")
    @handle_tool_errors
    def run_dataflow(
        workspace_name: str,
        dataflow_name: str,
        execution_data: Optional[dict] = None,
    ) -> dict:
        """Trigger an on-demand dataflow refresh.

        Starts a dataflow refresh job using the dedicated Dataflow Execute API.
        The job runs asynchronously; use get_job_status_by_url to poll for completion.

        **Use this tool when:**
        - You need to manually trigger a dataflow refresh
        - You want to run a dataflow as part of an orchestration workflow
        - You need to test dataflow execution

        Parameters:
            workspace_name: The display name of the workspace containing the dataflow.
            dataflow_name: Name of the dataflow to run.
            execution_data: Optional parameters for parameterized dataflows.

        Returns:
            Dictionary with status, message, job_instance_id, location_url
            (for polling), and retry_after (suggested poll interval).

        Example:
            ```python
            # Start the dataflow
            result = run_dataflow(
                workspace_name="Analytics",
                dataflow_name="CustomerETL"
            )

            if result["status"] == "success":
                # Poll for completion
                status = get_job_status_by_url(result["location_url"])
            ```
        """
        log_tool_invocation(
            "run_dataflow",
            workspace_name=workspace_name,
            dataflow_name=dataflow_name,
        )
        logger.info(
            f"Running dataflow '{dataflow_name}' in workspace '{workspace_name}'"
        )

        # Resolve workspace ID
        workspace_id = workspace_service.resolve_workspace_id(workspace_name)

        # Get dataflow item
        dataflow = item_service.get_item_by_name(
            workspace_id=workspace_id,
            name=dataflow_name,
            item_type="Dataflow",
        )

        result = dataflow_service.run_dataflow(
            workspace_id=workspace_id,
            dataflow_id=dataflow.id,
            execution_data=execution_data,
        )

        logger.info(
            f"Dataflow refresh started with job instance: {result.job_instance_id}"
        )
        return {
            "status": result.status,
            "message": result.message,
            "job_instance_id": result.job_instance_id,
            "location_url": result.location_url,
            "retry_after": result.retry_after,
        }

    logger.info("Dataflow tools registered successfully (3 tools)")
