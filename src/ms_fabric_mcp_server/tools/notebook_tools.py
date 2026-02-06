# ABOUTME: Notebook management MCP tools for Microsoft Fabric.
# ABOUTME: Provides tools for creating, updating, and executing notebooks.
"""Notebook management MCP tools.

This module provides MCP tools for Microsoft Fabric notebook operations including
creating notebooks and retrieving notebook definitions.
"""

from typing import Optional, TYPE_CHECKING
import logging

if TYPE_CHECKING:
    from fastmcp import FastMCP

from ..services import FabricNotebookService
from .base import handle_tool_errors, format_success_response, format_error_response, log_tool_invocation

logger = logging.getLogger(__name__)


def register_notebook_tools(mcp: "FastMCP", notebook_service: FabricNotebookService):
    """Register notebook management MCP tools.
    
    This function registers notebook-related tools for creating, updating, and
    inspecting notebooks, plus run history and driver logs.
    
    Args:
        mcp: FastMCP server instance to register tools on.
        notebook_service: Initialized FabricNotebookService instance.
        
    Example:
        ```python
        from ms_fabric_mcp_server import (
            FabricConfig, FabricClient,
            FabricWorkspaceService, FabricItemService, FabricNotebookService
        )
        from ms_fabric_mcp_server.tools import register_notebook_tools
        
        config = FabricConfig.from_environment()
        client = FabricClient(config)
        workspace_service = FabricWorkspaceService(client)
        item_service = FabricItemService(client)
        notebook_service = FabricNotebookService(client, item_service, workspace_service)
        
        register_notebook_tools(mcp, notebook_service)
        ```
    """
    
    @mcp.tool(title="Create Notebook")
    @handle_tool_errors
    def create_notebook(
        workspace_name: str,
        notebook_name: str,
        notebook_content: dict,
        description: Optional[str] = None,
        folder_path: Optional[str] = None,
        default_lakehouse_name: Optional[str] = None,
        lakehouse_workspace_name: Optional[str] = None,
    ) -> dict:
        """Create a notebook in a Fabric workspace.
        
        Parameters:
            workspace_name: The display name of the target workspace.
            notebook_name: Desired name inside Fabric (no folder separators).
            notebook_content: Notebook definition in ipynb JSON format.
            description: Optional description for the notebook.
            folder_path: Optional folder path (e.g., "demos/etl") to place the notebook.
                         Defaults to the workspace root when omitted.
            default_lakehouse_name: Optional default lakehouse to attach.
            lakehouse_workspace_name: Optional workspace name for the lakehouse.
            
        Returns:
            Dictionary with status, message, and notebook_id if successful.
        """
        log_tool_invocation(
            "create_notebook",
            workspace_name=workspace_name,
            notebook_name=notebook_name,
            description=description,
            folder_path=folder_path,
            default_lakehouse_name=default_lakehouse_name,
            lakehouse_workspace_name=lakehouse_workspace_name,
        )
        logger.info(
            f"Creating notebook '{notebook_name}' in workspace '{workspace_name}'"
        )
        
        result = notebook_service.create_notebook(
            workspace_name=workspace_name,
            notebook_name=notebook_name,
            notebook_content=notebook_content,
            description=description,
            folder_path=folder_path,
            default_lakehouse_name=default_lakehouse_name,
            lakehouse_workspace_name=lakehouse_workspace_name,
        )
        
        if result.status == "success":
            logger.info(f"Notebook created successfully: {result.notebook_id}")
            return {
                "status": "success",
                "message": result.message,
                "notebook_id": result.notebook_id,
            }
        logger.error(f"Notebook create failed: {result.message}")
        return {
            "status": "error",
            "message": result.message,
        }

    @mcp.tool(title="Get Notebook Definition")
    @handle_tool_errors
    def get_notebook_definition(
        workspace_name: str,
        notebook_name: str,
    ) -> dict:
        """Get the notebook definition (ipynb content)."""
        log_tool_invocation(
            "get_notebook_definition",
            workspace_name=workspace_name,
            notebook_name=notebook_name,
        )
        logger.info(
            f"Getting definition for notebook '{notebook_name}' in workspace '{workspace_name}'"
        )
        
        content = notebook_service.get_notebook_definition(workspace_name, notebook_name)
        
        result = {
            "status": "success",
            "workspace_name": workspace_name,
            "notebook_name": notebook_name,
            "definition": content,
        }
        
        logger.info("Successfully retrieved notebook definition")
        return result

    @mcp.tool(title="Update Notebook Definition")
    @handle_tool_errors
    def update_notebook_definition(
        workspace_name: str,
        notebook_name: str,
        notebook_content: Optional[dict] = None,
        default_lakehouse_name: Optional[str] = None,
        lakehouse_workspace_name: Optional[str] = None,
    ) -> dict:
        """Update notebook definition in Fabric.

        If notebook_content is omitted, the existing notebook definition is loaded
        and only metadata changes (e.g., default lakehouse) are applied.
        """
        log_tool_invocation(
            "update_notebook_definition",
            workspace_name=workspace_name,
            notebook_name=notebook_name,
            default_lakehouse_name=default_lakehouse_name,
            lakehouse_workspace_name=lakehouse_workspace_name,
        )
        logger.info(
            f"Updating notebook '{notebook_name}' in workspace '{workspace_name}'"
        )

        result = notebook_service.update_notebook_definition(
            workspace_name=workspace_name,
            notebook_name=notebook_name,
            notebook_content=notebook_content,
            default_lakehouse_name=default_lakehouse_name,
            lakehouse_workspace_name=lakehouse_workspace_name,
        )

        if result.status == "success":
            logger.info(f"Notebook updated successfully: {notebook_name}")
            return {
                "status": "success",
                "message": result.message,
                "notebook_id": result.notebook_id,
                "notebook_name": result.notebook_name,
                "workspace_id": result.workspace_id,
            }
        logger.error(f"Notebook update failed: {result.message}")
        return {
            "status": "error",
            "message": result.message,
        }
    
    @mcp.tool(title="Get Notebook Run Details")
    @handle_tool_errors
    def get_notebook_run_details(
        workspace_name: str,
        notebook_name: str,
        job_instance_id: str
    ) -> dict:
        """Get detailed run information for a notebook job instance.
        
        Retrieves execution metadata from the Fabric Notebook Livy Sessions API,
        which provides detailed timing, resource usage, and execution state information.
        
        **Use this tool when:**
        - You want to check the status and timing of a completed notebook run
        - You need to verify resource allocation for a notebook execution
        - You want to analyze execution performance (queue time, run time)
        
        **Note:** This method returns execution metadata (timing, state, resource usage).
        Cell-level outputs are only available for active sessions. Once a notebook job
        completes, individual cell outputs cannot be retrieved via the REST API. To
        capture cell outputs, use `mssparkutils.notebook.exit()` in your notebook and
        access the exitValue through Data Pipeline activities.
        
        Parameters:
            workspace_name: The display name of the workspace containing the notebook.
            notebook_name: Name of the notebook.
            job_instance_id: The job instance ID from execute_notebook or run_on_demand_job result.
            
        Returns:
            Dictionary with:
            - status: "success" or "error"
            - message: Description of the result
            - session: Full Livy session details (state, timing, resources)
            - execution_summary: Summarized execution information including:
                - state: Execution state (Success, Failed, Cancelled, etc.)
                - spark_application_id: Spark application identifier
                - queued_duration_seconds: Time spent in queue
                - running_duration_seconds: Actual execution time
                - total_duration_seconds: Total end-to-end time
                - driver_memory, driver_cores, executor_memory, etc.
            
        Example:
            ```python
            # After executing a notebook
            exec_result = run_on_demand_job(
                workspace_name="Analytics",
                item_name="ETL_Pipeline",
                item_type="Notebook",
                job_type="RunNotebook"
            )
            
            # Get detailed execution information
            details = get_notebook_run_details(
                workspace_name="Analytics",
                notebook_name="ETL_Pipeline",
                job_instance_id=exec_result["job_instance_id"]
            )
            
            if details["status"] == "success":
                summary = details["execution_summary"]
                print(f"State: {summary['state']}")
                print(f"Duration: {summary['total_duration_seconds']}s")
                print(f"Spark App ID: {summary['spark_application_id']}")
            ```
        """
        log_tool_invocation(
            "get_notebook_run_details",
            workspace_name=workspace_name,
            notebook_name=notebook_name,
            job_instance_id=job_instance_id
        )
        logger.info(
            f"Getting run details for notebook '{notebook_name}' "
            f"job instance '{job_instance_id}'"
        )
        
        result = notebook_service.get_notebook_run_details(
            workspace_name=workspace_name,
            notebook_name=notebook_name,
            job_instance_id=job_instance_id
        )
        
        if result.get("status") == "success":
            logger.info(f"Successfully retrieved run details for job instance '{job_instance_id}'")
        else:
            logger.error(f"Failed to get run details: {result.get('message')}")
        
        return result
    
    @mcp.tool(title="List Notebook Runs")
    @handle_tool_errors
    def list_notebook_runs(
        workspace_name: str,
        notebook_name: str,
        limit: Optional[int] = None
    ) -> dict:
        """List all Livy sessions (run history) for a notebook.
        
        Retrieves a list of all Livy sessions associated with a notebook, providing
        an execution history with job instance IDs, states, and timing information.
        
        **Use this tool when:**
        - You want to see the execution history of a notebook
        - You need to find a job instance ID for a past execution
        - You want to analyze execution patterns over time
        
        Parameters:
            workspace_name: The display name of the workspace containing the notebook.
            notebook_name: Name of the notebook.
            limit: Optional maximum number of sessions to return.
            
        Returns:
            Dictionary with:
            - status: "success" or "error"
            - message: Description of the result
            - sessions: List of session summaries, each containing:
                - job_instance_id: Unique identifier for the job
                - livy_id: Livy session identifier
                - state: Execution state (Success, Failed, Cancelled, etc.)
                - operation_name: Type of operation (Notebook Scheduled Run, etc.)
                - spark_application_id: Spark application identifier
                - submitted_time_utc: When the job was submitted
                - start_time_utc: When execution started
                - end_time_utc: When execution ended
                - total_duration_seconds: Total execution time
            - total_count: Total number of sessions found
            
        Example:
            ```python
            history = list_notebook_runs(
                workspace_name="Analytics",
                notebook_name="ETL_Pipeline",
                limit=10
            )
            
            if history["status"] == "success":
                print(f"Found {history['total_count']} executions")
                for session in history["sessions"]:
                    print(f"{session['job_instance_id']}: {session['state']}")
            ```
        """
        log_tool_invocation(
            "list_notebook_runs",
            workspace_name=workspace_name,
            notebook_name=notebook_name,
            limit=limit
        )
        logger.info(
            f"Listing runs for notebook '{notebook_name}' "
            f"in workspace '{workspace_name}'"
        )
        
        result = notebook_service.list_notebook_runs(
            workspace_name=workspace_name,
            notebook_name=notebook_name,
            limit=limit
        )
        
        if result.get("status") == "success":
            logger.info(f"Found {result.get('total_count', 0)} runs for notebook '{notebook_name}'")
        else:
            logger.error(f"Failed to list runs: {result.get('message')}")
        
        return result
    
    @mcp.tool(title="Get Notebook Driver Logs")
    @handle_tool_errors
    def get_notebook_driver_logs(
        workspace_name: str,
        notebook_name: str,
        job_instance_id: str,
        log_type: str = "stdout",
        max_lines: Optional[int] = 500
    ) -> dict:
        """Get Spark driver logs for a notebook execution.
        
        Retrieves the driver logs (stdout or stderr) from a completed notebook run.
        This is particularly useful for getting detailed error messages and Python
        tracebacks when a notebook fails.
        
        **Important Notes**:
        - Python exceptions and tracebacks appear in `stdout`, not `stderr`
        - `stderr` contains Spark/system logs (typically larger)
        - For failed notebooks, check `stdout` first for the Python error
        - Look for "Error", "Exception", "Traceback" in the output
        
        **Use this tool when:**
        - A notebook execution failed and you need to see the Python error
        - You want to debug notebook issues by examining driver logs
        - You need to analyze Spark driver behavior (stderr)
        
        Parameters:
            workspace_name: The display name of the workspace containing the notebook.
            notebook_name: Name of the notebook.
            job_instance_id: The job instance ID from execute_notebook or run_on_demand_job result.
            log_type: Type of log to retrieve - "stdout" (default) or "stderr".
                     Use "stdout" for Python errors and print statements.
                     Use "stderr" for Spark/system logs.
            max_lines: Maximum number of lines to return (default: 500, None for all).
                      Returns the last N lines (most recent, where errors typically are).
            
        Returns:
            Dictionary with:
            - status: "success" or "error"
            - message: Description of the result
            - log_type: Type of log retrieved
            - log_content: The actual log content as a string
            - log_size_bytes: Total size of the log file
            - truncated: Whether the log was truncated
            - spark_application_id: The Spark application ID
            - livy_id: The Livy session ID
            
        Example:
            ```python
            # Get Python error from a failed notebook
            result = get_notebook_driver_logs(
                workspace_name="Analytics",
                notebook_name="ETL_Pipeline",
                job_instance_id="12345678-1234-1234-1234-123456789abc",
                log_type="stdout"  # Python errors are in stdout!
            )
            
            if result["status"] == "success":
                print(result["log_content"])
                # Output will include Python traceback like:
                # ZeroDivisionError: division by zero
                # Traceback (most recent call last):
                #   Cell In[11], line 2
                #     result = x / 0
            ```
        """
        log_tool_invocation(
            "get_notebook_driver_logs",
            workspace_name=workspace_name,
            notebook_name=notebook_name,
            job_instance_id=job_instance_id,
            log_type=log_type,
            max_lines=max_lines
        )
        logger.info(
            f"Getting driver logs ({log_type}) for notebook '{notebook_name}' "
            f"job instance '{job_instance_id}'"
        )
        
        result = notebook_service.get_notebook_driver_logs(
            workspace_name=workspace_name,
            notebook_name=notebook_name,
            job_instance_id=job_instance_id,
            log_type=log_type,
            max_lines=max_lines
        )
        
        if result.get("status") == "success":
            logger.info(
                f"Successfully retrieved {log_type} logs "
                f"({result.get('log_size_bytes', 0)} bytes) "
                f"for job instance '{job_instance_id}'"
            )
        else:
            logger.error(f"Failed to get driver logs: {result.get('message')}")
        
        return result
    
    logger.info("Notebook tools registered successfully (6 tools)")
