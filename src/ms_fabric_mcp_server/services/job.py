# ABOUTME: Service for Fabric job operations.
# ABOUTME: Handles job execution, status monitoring, and result retrieval.
"""Service for Fabric job operations."""

import base64
import logging
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Optional
from urllib.parse import urlparse

from ..client.exceptions import FabricAPIError, FabricItemNotFoundError
from ..client.http_client import FabricClient
from ..models.job import FabricJob
from ..models.results import JobStatusResult, OperationResult, RunJobResult

logger = logging.getLogger(__name__)


class FabricJobService:
    """Service for Fabric job operations.
    
    This service handles job execution, status monitoring, and result retrieval
    for Fabric items such as notebooks, pipelines, and other executable items.
    
    Example:
        ```python
        from ms_fabric_mcp_server import FabricConfig, FabricClient
        from ms_fabric_mcp_server.services import FabricWorkspaceService, FabricItemService, FabricJobService
        
        config = FabricConfig.from_environment()
        client = FabricClient(config)
        workspace_service = FabricWorkspaceService(client)
        item_service = FabricItemService(client, workspace_service)
        job_service = FabricJobService(client, workspace_service, item_service)
        
        # Run a notebook job
        result = job_service.run_notebook_job(
            workspace_name="MyWorkspace",
            notebook_name="MyNotebook",
            parameters={"param1": "value1"},
            wait=True
        )
        
        if result.job and result.job.is_successful():
            print("Notebook executed successfully!")
        ```
    """
    
    def __init__(
        self,
        client: FabricClient,
        workspace_service: "FabricWorkspaceService",
        item_service: "FabricItemService",
    ):
        """Initialize the job service.
        
        Args:
            client: FabricClient instance for API requests
            workspace_service: FabricWorkspaceService for workspace operations
            item_service: FabricItemService for item operations
        """
        self.client = client
        self.workspace_service = workspace_service
        self.item_service = item_service
        
        logger.debug("FabricJobService initialized")
    
    def run_on_demand_job(
        self,
        workspace_name: str,
        item_name: str,
        item_type: str,
        job_type: str,
        execution_data: Optional[Dict[str, Any]] = None,
    ) -> RunJobResult:
        """Run an on-demand job for a Fabric item.
        
        Args:
            workspace_name: Name of the workspace
            item_name: Name of the item to run job for
            item_type: Type of the item (Notebook, Pipeline, etc.)
            job_type: Type of job to run (RunNotebook, DefaultJob, etc.)
            execution_data: Optional execution data payload
            
        Returns:
            RunJobResult with job creation status and location URL
            
        Raises:
            FabricItemNotFoundError: If workspace or item not found
            FabricAPIError: If API request fails
            
        Example:
            ```python
            result = job_service.run_on_demand_job(
                workspace_name="MyWorkspace",
                item_name="MyNotebook",
                item_type="Notebook",
                job_type="RunNotebook",
                execution_data={"parameters": {"param1": "value1"}}
            )
            
            if result.status == "success":
                print(f"Job started: {result.job_instance_id}")
                print(f"Status URL: {result.location_url}")
            ```
        """
        logger.info(
            f"Running {job_type} job for {item_type} '{item_name}' "
            f"in workspace '{workspace_name}'"
        )
        
        try:
            # Resolve workspace and item IDs
            workspace_id = self.workspace_service.resolve_workspace_id(workspace_name)
            item = self.item_service.get_item_by_name(workspace_id, item_name, item_type)
            
            # Prepare request payload
            payload = {}
            if execution_data:
                payload["executionData"] = execution_data
            
            # Make API request
            url = f"workspaces/{workspace_id}/items/{item.id}/jobs/instances?jobType={job_type}"
            
            response = self.client.make_api_request(
                "POST",
                url,
                payload=payload if payload else None,
                timeout=60
            )
            
            # Parse response headers
            location_url = response.headers.get("Location", "").strip()
            retry_after = None
            if "Retry-After" in response.headers:
                try:
                    retry_after = int(response.headers["Retry-After"])
                except (ValueError, TypeError):
                    retry_after = None

            if not location_url:
                return RunJobResult(
                    status="error",
                    message="No Location header returned from job creation"
                )
            
            # Extract job instance ID from location URL
            job_instance_id = None
            if location_url:
                try:
                    job_instance_id = location_url.rstrip("/").split("/")[-1]
                except (IndexError, AttributeError):
                    logger.warning(
                        f"Could not extract job instance ID from location: {location_url}"
                    )
            
            logger.info(f"Job started successfully with ID: {job_instance_id}")
            return RunJobResult(
                status="success",
                job_instance_id=job_instance_id,
                location_url=location_url,
                retry_after=retry_after,
                message=f"Job {job_type} started successfully"
            )
            
        except (FabricItemNotFoundError, FabricAPIError) as exc:
            logger.error(f"Failed to run job: {exc}")
            return RunJobResult(
                status="error",
                message=str(exc)
            )
        except Exception as exc:
            logger.error(f"Unexpected error running job: {exc}")
            return RunJobResult(
                status="error",
                message=f"Unexpected error: {exc}"
            )
    
    def get_job_status(
        self,
        workspace_name: str,
        item_name: str,
        item_type: str,
        job_instance_id: str,
    ) -> JobStatusResult:
        """Get status of a specific job instance.
        
        Args:
            workspace_name: Name of the workspace
            item_name: Name of the item
            item_type: Type of the item
            job_instance_id: ID of the job instance
            
        Returns:
            JobStatusResult with current job status and details
            
        Raises:
            FabricItemNotFoundError: If workspace or item not found
            FabricAPIError: If API request fails
            
        Example:
            ```python
            result = job_service.get_job_status(
                workspace_name="MyWorkspace",
                item_name="MyNotebook",
                item_type="Notebook",
                job_instance_id="abc-123-def-456"
            )
            
            if result.job:
                print(f"Status: {result.job.status}")
                if result.job.is_failed():
                    print(f"Failure reason: {result.job.failure_reason}")
            ```
        """
        logger.info(f"Getting status for job instance {job_instance_id}")
        
        try:
            # Resolve workspace and item IDs
            workspace_id = self.workspace_service.resolve_workspace_id(workspace_name)
            item = self.item_service.get_item_by_name(workspace_id, item_name, item_type)
            
            # Make API request
            url = f"workspaces/{workspace_id}/items/{item.id}/jobs/instances/{job_instance_id}"
            
            response = self.client.make_api_request("GET", url)
            job_data = response.json()
            
            # Create FabricJob object
            job = FabricJob(
                job_instance_id=job_data.get("id", job_instance_id),
                item_id=job_data.get("itemId", item.id),
                job_type=job_data.get("jobType", ""),
                status=job_data.get("status", "Unknown"),
                invoke_type=job_data.get("invokeType"),
                root_activity_id=job_data.get("rootActivityId"),
                start_time_utc=job_data.get("startTimeUtc"),
                end_time_utc=job_data.get("endTimeUtc"),
                failure_reason=job_data.get("failureReason"),
            )
            
            logger.info(f"Job status: {job.status}")
            return JobStatusResult(
                status="success",
                job=job,
                message="Job status retrieved successfully"
            )
            
        except (FabricItemNotFoundError, FabricAPIError) as exc:
            logger.error(f"Failed to get job status: {exc}")
            return JobStatusResult(
                status="error",
                message=str(exc)
            )
        except Exception as exc:
            logger.error(f"Unexpected error getting job status: {exc}")
            return JobStatusResult(
                status="error",
                message=f"Unexpected error: {exc}"
            )
    
    def get_job_status_by_url(self, location_url: str) -> JobStatusResult:
        """Get job status using the location URL from run_on_demand_job.
        
        Args:
            location_url: The location URL returned from job creation
            
        Returns:
            JobStatusResult with current job status
            
        Raises:
            FabricAPIError: If API request fails
            
        Example:
            ```python
            # After starting a job
            run_result = job_service.run_on_demand_job(...)
            
            # Check status using location URL
            status_result = job_service.get_job_status_by_url(run_result.location_url)
            ```
        """
        logger.info(f"Getting job status from URL: {location_url}")
        
        try:
            # Extract the relative path from the URL
            parsed_url = urlparse(location_url)
            relative_path = (parsed_url.path or location_url).lstrip("/")
            
            # Remove API version prefix if present
            if relative_path.startswith("v1/"):
                relative_path = relative_path[3:]

            parts = [part for part in relative_path.split("/") if part]
            if (
                len(parts) != 7
                or parts[0] != "workspaces"
                or parts[2] != "items"
                or parts[4] != "jobs"
                or parts[5] != "instances"
            ):
                return JobStatusResult(
                    status="error",
                    message="Invalid job status URL"
                )
            
            response = self.client.make_api_request("GET", relative_path)
            job_data = response.json()
            
            # Create FabricJob object
            job = FabricJob(
                job_instance_id=job_data.get("id", ""),
                item_id=job_data.get("itemId", ""),
                job_type=job_data.get("jobType", ""),
                status=job_data.get("status", "Unknown"),
                invoke_type=job_data.get("invokeType"),
                root_activity_id=job_data.get("rootActivityId"),
                start_time_utc=job_data.get("startTimeUtc"),
                end_time_utc=job_data.get("endTimeUtc"),
                failure_reason=job_data.get("failureReason"),
            )
            
            logger.info(f"Job status: {job.status}")
            return JobStatusResult(
                status="success",
                job=job,
                message="Job status retrieved successfully"
            )
            
        except FabricAPIError as exc:
            logger.error(f"Failed to get job status: {exc}")
            return JobStatusResult(
                status="error",
                message=str(exc)
            )
        except Exception as exc:
            logger.error(f"Unexpected error getting job status: {exc}")
            return JobStatusResult(
                status="error",
                message=f"Unexpected error: {exc}"
            )
    
    def wait_for_job_completion(
        self,
        workspace_name: str,
        item_name: str,
        item_type: str,
        job_instance_id: str,
        poll_interval: int = 15,
        timeout_minutes: int = 30,
    ) -> JobStatusResult:
        """Wait for a job to complete by polling its status.
        
        Args:
            workspace_name: Name of the workspace
            item_name: Name of the item
            item_type: Type of the item
            job_instance_id: ID of the job instance
            poll_interval: Seconds between status checks (default: 15)
            timeout_minutes: Maximum time to wait in minutes (default: 30)
            
        Returns:
            JobStatusResult with final job status
            
        Example:
            ```python
            result = job_service.wait_for_job_completion(
                workspace_name="MyWorkspace",
                item_name="MyNotebook",
                item_type="Notebook",
                job_instance_id="abc-123",
                timeout_minutes=60
            )
            
            if result.job and result.job.is_successful():
                print("Job completed successfully!")
            ```
        """
        logger.info(
            f"Waiting for job {job_instance_id} to complete "
            f"(timeout: {timeout_minutes} min)"
        )
        
        deadline = datetime.now(timezone.utc) + timedelta(minutes=timeout_minutes)
        
        while datetime.now(timezone.utc) < deadline:
            result = self.get_job_status(
                workspace_name, item_name, item_type, job_instance_id
            )
            
            if result.status == "error":
                return result
            
            if result.job and result.job.is_terminal_state():
                logger.info(f"Job completed with status: {result.job.status}")
                return result
            
            logger.debug(
                f"Job status: {result.job.status if result.job else 'Unknown'}, "
                f"waiting {poll_interval}s..."
            )
            time.sleep(poll_interval)
        
        # Timeout - get final status
        final_result = self.get_job_status(
            workspace_name, item_name, item_type, job_instance_id
        )
        if final_result.status == "success" and final_result.job:
            final_result.message = (
                f"Timed out after {timeout_minutes} minutes. "
                f"Final status: {final_result.job.status}"
            )
        else:
            final_result.message = (
                f"Timed out after {timeout_minutes} minutes. "
                "Could not retrieve final status."
            )
        
        return final_result
    
    def wait_for_job_completion_by_url(
        self,
        location_url: str,
        poll_interval: int = 15,
        timeout_minutes: int = 30,
    ) -> JobStatusResult:
        """Wait for a job to complete using the location URL.
        
        Args:
            location_url: The location URL returned from job creation
            poll_interval: Seconds between status checks (default: 15)
            timeout_minutes: Maximum time to wait in minutes (default: 30)
            
        Returns:
            JobStatusResult with final job status
            
        Example:
            ```python
            run_result = job_service.run_on_demand_job(...)
            
            # Wait for completion using URL
            status_result = job_service.wait_for_job_completion_by_url(
                location_url=run_result.location_url,
                timeout_minutes=60
            )
            ```
        """
        logger.info(
            f"Waiting for job to complete using URL (timeout: {timeout_minutes} min)"
        )
        
        deadline = datetime.now(timezone.utc) + timedelta(minutes=timeout_minutes)
        
        while datetime.now(timezone.utc) < deadline:
            result = self.get_job_status_by_url(location_url)
            
            if result.status == "error":
                return result
            
            if result.job and result.job.is_terminal_state():
                logger.info(f"Job completed with status: {result.job.status}")
                return result
            
            logger.debug(
                f"Job status: {result.job.status if result.job else 'Unknown'}, "
                f"waiting {poll_interval}s..."
            )
            time.sleep(poll_interval)
        
        # Timeout - get final status
        final_result = self.get_job_status_by_url(location_url)
        if final_result.status == "success" and final_result.job:
            final_result.message = (
                f"Timed out after {timeout_minutes} minutes. "
                f"Final status: {final_result.job.status}"
            )
        else:
            final_result.message = (
                f"Timed out after {timeout_minutes} minutes. "
                "Could not retrieve final status."
            )
        
        return final_result
    
    def get_operation_result(self, operation_id: str) -> OperationResult:
        """Get the result of a long-running operation.
        
        Args:
            operation_id: The operation ID (from x-ms-operation-id header)
            
        Returns:
            OperationResult with operation data
            
        Raises:
            FabricAPIError: If API request fails
            
        Example:
            ```python
            # After an async operation
            result = job_service.get_operation_result("operation-id-123")
            
            if result.status == "success":
                print(f"Operation data: {result.result}")
            ```
        """
        logger.info(f"Getting operation result for ID: {operation_id}")
        
        try:
            url = f"operations/{operation_id}/result"
            
            response = self.client.make_api_request("GET", url)
            
            # The response could be JSON or binary data
            content_type = response.headers.get("Content-Type", "").lower()
            
            if "application/json" in content_type:
                result_data = response.json()
            else:
                # For binary data, we'll store it as base64
                result_data = {
                    "content_type": content_type,
                    "data": base64.b64encode(response.content).decode('utf-8'),
                    "size": len(response.content)
                }
            
            logger.info("Operation result retrieved successfully")
            return OperationResult(
                status="success",
                operation_id=operation_id,
                result=result_data,
                message="Operation result retrieved successfully"
            )
            
        except FabricAPIError as exc:
            logger.error(f"Failed to get operation result: {exc}")
            return OperationResult(
                status="error",
                operation_id=operation_id,
                message=str(exc)
            )
        except Exception as exc:
            logger.error(f"Unexpected error getting operation result: {exc}")
            return OperationResult(
                status="error",
                operation_id=operation_id,
                message=f"Unexpected error: {exc}"
            )
    
    def run_notebook_job(
        self,
        workspace_name: str,
        notebook_name: str,
        parameters: Optional[Dict[str, Any]] = None,
        wait: bool = True,
        poll_interval: int = 15,
        timeout_minutes: int = 30,
    ) -> JobStatusResult:
        """Run a notebook job with parameters.
        
        This is a convenience method that combines run_on_demand_job and
        wait_for_job_completion for notebook execution.
        
        Args:
            workspace_name: Name of the workspace
            notebook_name: Name of the notebook
            parameters: Optional parameters for notebook execution
            wait: Whether to wait for completion (default: True)
            poll_interval: Seconds between status checks (default: 15)
            timeout_minutes: Maximum time to wait in minutes (default: 30)
            
        Returns:
            JobStatusResult with execution status
            
        Example:
            ```python
            # Run notebook and wait for completion
            result = job_service.run_notebook_job(
                workspace_name="MyWorkspace",
                notebook_name="ETL_Pipeline",
                parameters={"date": "2025-01-01", "mode": "production"},
                wait=True,
                timeout_minutes=60
            )
            
            if result.job and result.job.is_successful():
                print("Notebook executed successfully!")
            elif result.job and result.job.is_failed():
                print(f"Notebook failed: {result.job.failure_reason}")
            ```
        """
        logger.info(
            f"Running notebook job for '{notebook_name}' "
            f"in workspace '{workspace_name}'"
        )
        
        # Prepare execution data
        execution_data = {}
        if parameters:
            execution_data["parameters"] = parameters
        
        # Start the job
        run_result = self.run_on_demand_job(
            workspace_name=workspace_name,
            item_name=notebook_name,
            item_type="Notebook",
            job_type="RunNotebook",
            execution_data=execution_data if execution_data else None
        )
        
        if run_result.status == "error":
            return JobStatusResult(
                status="error",
                message=run_result.message
            )
        
        if not wait:
            # Return immediate status
            return JobStatusResult(
                status="success",
                message=f"Notebook job started. Job ID: {run_result.job_instance_id}"
            )
        
        # Wait for completion
        if run_result.location_url:
            return self.wait_for_job_completion_by_url(
                location_url=run_result.location_url,
                poll_interval=poll_interval,
                timeout_minutes=timeout_minutes
            )
        else:
            return JobStatusResult(
                status="error",
                message="No location URL returned from job creation"
            )
