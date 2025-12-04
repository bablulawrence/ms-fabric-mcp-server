# ABOUTME: Job management MCP tools for Microsoft Fabric.
# ABOUTME: Provides tools for running and monitoring Fabric jobs.
"""Job execution and monitoring MCP tools.

This module provides MCP tools for running and monitoring Fabric jobs including
notebooks, pipelines, and other executable items.
"""

from typing import Optional, TYPE_CHECKING
import logging

if TYPE_CHECKING:
    from fastmcp import FastMCP

from ..services import FabricJobService
from .base import handle_tool_errors, log_tool_invocation

logger = logging.getLogger(__name__)


def register_job_tools(mcp: "FastMCP", job_service: FabricJobService):
    """Register job execution and monitoring MCP tools.
    
    This function registers four job-related tools:
    - run_on_demand_job: Execute a job for an item (notebook, pipeline, etc.)
    - get_job_status: Get job status by job instance ID
    - get_job_status_by_url: Get job status from location URL
    - get_operation_result: Get result of long-running operations
    
    Args:
        mcp: FastMCP server instance to register tools on.
        job_service: Initialized FabricJobService instance.
        
    Example:
        ```python
        from ms_fabric_mcp_server import (
            FabricConfig, FabricClient,
            FabricWorkspaceService, FabricItemService, FabricJobService
        )
        from ms_fabric_mcp_server.tools import register_job_tools
        
        config = FabricConfig.from_environment()
        client = FabricClient(config)
        workspace_service = FabricWorkspaceService(client)
        item_service = FabricItemService(client)
        job_service = FabricJobService(client, workspace_service, item_service)
        
        register_job_tools(mcp, job_service)
        ```
    """
    
    @mcp.tool(title="Run On-Demand Job")
    @handle_tool_errors
    def run_on_demand_job(
        workspace_name: str,
        item_name: str,
        item_type: str,
        job_type: str,
        execution_data: Optional[dict] = None,
    ) -> dict:
        """Run an on-demand job for a Fabric item.
        
        Executes a job for the specified item. Common job types include:
        - RunNotebook: Execute a notebook
        - Pipeline: Run a data pipeline
        - DefaultJob: Default job type for the item
        
        The job runs asynchronously. Use get_job_status or get_job_status_by_url
        to check the job's progress and result.
        
        Parameters:
            workspace_name: The display name of the workspace.
            item_name: Name of the item to run job for.
            item_type: Type of the item (Notebook, Pipeline, Lakehouse, Warehouse, etc.).
            job_type: Type of job to run (RunNotebook, DefaultJob, Pipeline, etc.).
            execution_data: Optional execution data payload for the job (e.g., notebook parameters).
            
        Returns:
            Dictionary with status, message, job_instance_id, location_url, and retry_after.
            
        Example:
            ```python
            # Run a notebook
            result = run_on_demand_job(
                workspace_name="My Workspace",
                item_name="analysis_notebook",
                item_type="Notebook",
                job_type="RunNotebook",
                execution_data={"parameters": {"start_date": "2025-01-01"}}
            )
            
            # Use the location URL to check status
            job_status = get_job_status_by_url(result["location_url"])
            ```
        """
        log_tool_invocation(
            "run_on_demand_job",
            workspace_name=workspace_name,
            item_name=item_name,
            item_type=item_type,
            job_type=job_type,
            execution_data=execution_data
        )
        logger.info(f"Running {job_type} job for {item_type} '{item_name}' in workspace '{workspace_name}'")
        
        result = job_service.run_on_demand_job(
            workspace_name=workspace_name,
            item_name=item_name,
            item_type=item_type,
            job_type=job_type,
            execution_data=execution_data
        )
        
        response = {
            "status": result.status,
            "message": result.message,
        }
        
        if result.status == "success":
            response.update({
                "job_instance_id": result.job_instance_id,
                "location_url": result.location_url,
                "retry_after": result.retry_after,
            })
            logger.info(f"Job started successfully: {result.job_instance_id}")
        else:
            logger.error(f"Job start failed: {result.message}")
            
        return response

    @mcp.tool(title="Get Job Status")  
    @handle_tool_errors
    def get_job_status(
        workspace_name: str,
        item_name: str,
        item_type: str,
        job_instance_id: str,
    ) -> dict:
        """Get status of a specific job instance.
        
        Retrieves the current status and details of a running or completed job.
        The job state includes: NotStarted, InProgress, Completed, Failed, Cancelled.
        
        Parameters:
            workspace_name: The display name of the workspace.
            item_name: Name of the item.
            item_type: Type of the item (Notebook, Pipeline, etc.).
            job_instance_id: ID of the job instance to check.
            
        Returns:
            Dictionary with status, message, and job details including:
            - job_instance_id, item_id, job_type, job_status
            - invoke_type, root_activity_id, start_time_utc, end_time_utc
            - failure_reason (if failed)
            - is_terminal, is_successful, is_failed, is_running flags
            
        Example:
            ```python
            result = get_job_status(
                workspace_name="My Workspace",
                item_name="analysis_notebook",
                item_type="Notebook",
                job_instance_id="12345678-1234-1234-1234-123456789abc"
            )
            
            if result["job"]["is_terminal"]:
                if result["job"]["is_successful"]:
                    print("Job completed successfully!")
                else:
                    print(f"Job failed: {result['job']['failure_reason']}")
            ```
        """
        log_tool_invocation(
            "get_job_status",
            workspace_name=workspace_name,
            item_name=item_name,
            item_type=item_type,
            job_instance_id=job_instance_id
        )
        logger.info(f"Getting status for job instance {job_instance_id}")
        
        result = job_service.get_job_status(
            workspace_name=workspace_name,
            item_name=item_name,
            item_type=item_type,
            job_instance_id=job_instance_id
        )
        
        response = {
            "status": result.status,
            "message": result.message,
        }
        
        if result.status == "success" and result.job:
            response.update({
                "job": {
                    "job_instance_id": result.job.job_instance_id,
                    "item_id": result.job.item_id,
                    "job_type": result.job.job_type,
                    "job_status": result.job.status,
                    "invoke_type": result.job.invoke_type,
                    "root_activity_id": result.job.root_activity_id,
                    "start_time_utc": result.job.start_time_utc,
                    "end_time_utc": result.job.end_time_utc,
                    "failure_reason": result.job.failure_reason,
                    "is_terminal": result.job.is_terminal_state(),
                    "is_successful": result.job.is_successful(),
                    "is_failed": result.job.is_failed(),
                    "is_running": result.job.is_running(),
                }
            })
            logger.info(f"Job status retrieved: {result.job.status}")
        else:
            logger.error(f"Failed to get job status: {result.message}")
            
        return response

    @mcp.tool(title="Get Job Status by URL")
    @handle_tool_errors
    def get_job_status_by_url(location_url: str) -> dict:
        """Get job status using the location URL from run_on_demand_job.
        
        Retrieves job status using the location URL returned when the job was created.
        This is convenient when you have the location URL but not the individual
        workspace/item/job identifiers.
        
        Parameters:
            location_url: The location URL returned from job creation.
            
        Returns:
            Dictionary with status, message, and job details (same structure as get_job_status).
            
        Example:
            ```python
            # Start a job
            start_result = run_on_demand_job(...)
            
            # Check status using the location URL
            status_result = get_job_status_by_url(start_result["location_url"])
            ```
        """
        log_tool_invocation("get_job_status_by_url", location_url=location_url)
        logger.info(f"Getting job status from URL: {location_url}")
        
        result = job_service.get_job_status_by_url(location_url)
        
        response = {
            "status": result.status,
            "message": result.message,
        }
        
        if result.status == "success" and result.job:
            response.update({
                "job": {
                    "job_instance_id": result.job.job_instance_id,
                    "item_id": result.job.item_id,
                    "job_type": result.job.job_type,
                    "job_status": result.job.status,
                    "invoke_type": result.job.invoke_type,
                    "root_activity_id": result.job.root_activity_id,
                    "start_time_utc": result.job.start_time_utc,
                    "end_time_utc": result.job.end_time_utc,
                    "failure_reason": result.job.failure_reason,
                    "is_terminal": result.job.is_terminal_state(),
                    "is_successful": result.job.is_successful(),
                    "is_failed": result.job.is_failed(),
                    "is_running": result.job.is_running(),
                }
            })
            logger.info(f"Job status retrieved: {result.job.status}")
        else:
            logger.error(f"Failed to get job status: {result.message}")
            
        return response

    @mcp.tool(title="Get Operation Result")
    @handle_tool_errors
    def get_operation_result(operation_id: str) -> dict:
        """Get the result of a long-running operation.
        
        Retrieves the result of an asynchronous operation using its operation ID.
        Operation IDs are typically returned in the x-ms-operation-id header from
        API calls that return 202 Accepted responses.
        
        Parameters:
            operation_id: The operation ID (from x-ms-operation-id header).
            
        Returns:
            Dictionary with status, operation_id, message, and operation result.
            
        Example:
            ```python
            result = get_operation_result("12345678-1234-1234-1234-123456789abc")
            
            if result["status"] == "success":
                operation_result = result["result"]
                # Process operation result
            ```
        """
        log_tool_invocation("get_operation_result", operation_id=operation_id)
        logger.info(f"Getting operation result for ID: {operation_id}")
        
        result = job_service.get_operation_result(operation_id)
        
        response = {
            "status": result.status,
            "operation_id": result.operation_id,
            "message": result.message,
        }
        
        if result.status == "success":
            response["result"] = result.result
            logger.info(f"Operation result retrieved successfully")
        else:
            logger.error(f"Failed to get operation result: {result.message}")
            
        return response
    
    logger.info("Job tools registered successfully (4 tools)")
