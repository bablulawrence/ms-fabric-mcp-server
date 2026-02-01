# ABOUTME: Service for Fabric notebook operations.
# ABOUTME: Handles notebook creation, update, execution, and metadata management.
"""Service for Fabric notebook operations."""

import base64
import copy
import json
import logging
import time
from typing import Any, Dict, List, Optional

from ..client.exceptions import (
    FabricAPIError,
    FabricError,
    FabricItemNotFoundError,
    FabricValidationError,
)
from ..client.http_client import FabricClient
from ..models.item import FabricItem
from ..models.results import ExecuteNotebookResult, CreateNotebookResult, UpdateNotebookResult

logger = logging.getLogger(__name__)


class FabricNotebookService:
    """Service for Fabric notebook operations.
    
    This service handles notebook creation, update, execution, and metadata management.
    
    Example:
        ```python
        from ms_fabric_mcp_server import FabricConfig, FabricClient
        from ms_fabric_mcp_server.services import (
            FabricWorkspaceService,
            FabricItemService,
            FabricNotebookService
        )
        
        config = FabricConfig.from_environment()
        client = FabricClient(config)
        workspace_service = FabricWorkspaceService(client)
        item_service = FabricItemService(client, workspace_service)
        notebook_service = FabricNotebookService(
            client,
            item_service,
            workspace_service,
            repo_root="/path/to/repo"
        )
        
        # Create a notebook
        result = notebook_service.create_notebook(
            workspace_name="MyWorkspace",
            notebook_name="MyNotebook",
            notebook_content={"cells": [], "metadata": {}, "nbformat": 4, "nbformat_minor": 5},
            description="ETL Pipeline"
        )
        
        if result.status == "success":
            print(f"Notebook created with ID: {result.notebook_id}")
        ```
    """
    
    def __init__(
        self,
        client: FabricClient,
        item_service: "FabricItemService",
        workspace_service: "FabricWorkspaceService",
        repo_root: Optional[str] = None,
    ):
        """Initialize the notebook service.
        
        Args:
            client: FabricClient instance for API requests
            item_service: FabricItemService instance for generic item operations
            workspace_service: FabricWorkspaceService instance for workspace operations
            repo_root: Optional repository root path for resolving relative paths
        """
        self.client = client
        self.item_service = item_service
        self.workspace_service = workspace_service
        self.repo_root = repo_root
        
        logger.debug("FabricNotebookService initialized")
    
    def _encode_notebook_content(self, notebook_content: Dict[str, Any]) -> str:
        """Encode notebook content to base64."""
        if not isinstance(notebook_content, dict) or not notebook_content:
            raise FabricValidationError(
                "notebook_content",
                str(type(notebook_content)),
                "notebook_content must be a non-empty dictionary",
            )

        try:
            payload = json.dumps(notebook_content)
            encoded_content = base64.b64encode(payload.encode("utf-8")).decode("utf-8")
            logger.debug(
                f"Notebook content encoded: {len(encoded_content)} base64 characters"
            )
            return encoded_content
        except Exception as exc:
            logger.error(f"Failed to encode notebook content: {exc}")
            raise FabricError(f"Failed to encode notebook content: {exc}")
    
    def _create_notebook_definition(
        self,
        notebook_name: str,
        notebook_content: Dict[str, Any],
        description: Optional[str] = None,
        folder_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Create notebook definition for API request.
        
        Args:
            notebook_name: Display name for the notebook
            notebook_content: Notebook definition (ipynb JSON)
            description: Optional description
            
        Returns:
            Notebook definition dictionary
        """
        encoded_content = self._encode_notebook_content(notebook_content)
        
        definition = {
            "displayName": notebook_name,
            "type": "Notebook",
            "definition": {
                "format": "ipynb",
                "parts": [
                    {
                        "path": f"{notebook_name}.ipynb",
                        "payload": encoded_content,
                        "payloadType": "InlineBase64",
                    }
                ],
            },
        }
        
        if description:
            definition["description"] = description
        if folder_id:
            definition["folderId"] = folder_id
        
        return definition

    @staticmethod
    def _parse_retry_after(headers: Dict[str, Any], default: int = 5) -> int:
        """Safely parse Retry-After header with fallback default."""
        value = headers.get("Retry-After", default)
        try:
            return int(value)
        except (TypeError, ValueError):
            return default

    def _get_notebook_definition_response(
        self, workspace_id: str, notebook_id: str
    ) -> Dict[str, Any]:
        response = self.client.make_api_request(
            "POST",
            f"workspaces/{workspace_id}/items/{notebook_id}/getDefinition?format=ipynb",
        )

        if response.status_code == 202:
            location = response.headers.get("Location")
            retry_after = self._parse_retry_after(response.headers, 5)

            if not location:
                raise FabricError("No Location header in 202 response")

            max_retries = 30
            for _ in range(max_retries):
                time.sleep(retry_after)
                poll_response = self.client.make_api_request("GET", location)
                if poll_response.status_code == 200:
                    op_result = poll_response.json()
                    if op_result.get("status") == "Succeeded":
                        result_response = self.client.make_api_request(
                            "GET",
                            f"{location}/result",
                        )
                        if result_response.status_code == 200:
                            return result_response.json()
                        raise FabricAPIError(
                            result_response.status_code,
                            f"Failed to get definition result: {result_response.text}",
                        )
                    if op_result.get("status") == "Failed":
                        error_msg = op_result.get("error", {}).get(
                            "message", "Unknown error"
                        )
                        raise FabricError(f"Operation failed: {error_msg}")

                    retry_after = self._parse_retry_after(poll_response.headers, 5)
                    continue

                if poll_response.status_code == 202:
                    retry_after = self._parse_retry_after(poll_response.headers, 5)
                    continue

                raise FabricAPIError(
                    poll_response.status_code,
                    f"Failed to poll operation: {poll_response.text}",
                )

            raise FabricError("Timeout waiting for notebook definition")

        definition_response = response.json()
        if definition_response is None:
            raise FabricError("Empty response when getting notebook definition")
        return definition_response

    @staticmethod
    def _extract_ipynb_content(
        definition_response: Dict[str, Any],
    ) -> tuple[Optional[Dict[str, Any]], Optional[str]]:
        parts = definition_response.get("definition", {}).get("parts", [])
        for part in parts:
            path = part.get("path", "")
            if not path.endswith(".ipynb"):
                continue
            payload = part.get("payload", "")
            if not payload:
                raise FabricError("Notebook content payload missing")
            notebook_content = json.loads(base64.b64decode(payload).decode("utf-8"))
            return notebook_content, path
        return None, None

    @staticmethod
    def _apply_lakehouse_dependency(
        notebook_content: Dict[str, Any],
        lakehouse: FabricItem,
        lakehouse_workspace_id: str,
    ) -> None:
        metadata = notebook_content.setdefault("metadata", {})
        dependencies = metadata.setdefault("dependencies", {})
        dependencies["lakehouse"] = {
            "default_lakehouse": lakehouse.id,
            "default_lakehouse_name": lakehouse.display_name,
            "default_lakehouse_workspace_id": lakehouse_workspace_id,
            "known_lakehouses": [{"id": lakehouse.id}],
        }
    
    def create_notebook(
        self,
        workspace_name: str,
        notebook_name: str,
        notebook_content: Dict[str, Any],
        description: Optional[str] = None,
        folder_path: Optional[str] = None,
        default_lakehouse_name: Optional[str] = None,
        lakehouse_workspace_name: Optional[str] = None,
    ) -> CreateNotebookResult:
        """Create a notebook in Fabric workspace.
        
        Args:
            workspace_name: Name of the target workspace
            notebook_name: Display name for the notebook in Fabric
            notebook_content: Notebook content (ipynb JSON)
            description: Optional description for the notebook
            default_lakehouse_name: Optional default lakehouse name to attach
            lakehouse_workspace_name: Optional workspace name for the lakehouse
            
        Returns:
            CreateNotebookResult with operation status and notebook ID
            
        Example:
            ```python
            result = notebook_service.create_notebook(
                workspace_name="Analytics",
                notebook_name="ETL_Pipeline",
                notebook_content=notebook_definition,
                description="Daily ETL processing"
            )
            
            if result.status == "success":
                print(f"Notebook ID: {result.notebook_id}")
            ```
        """
        logger.info(
            f"Creating notebook '{notebook_name}' in workspace '{workspace_name}'"
        )

        if "/" in notebook_name or "\\" in notebook_name:
            raise FabricValidationError(
                "notebook_name",
                notebook_name,
                "Notebook name cannot include path separators. "
                "Use folder_path to place notebooks in folders.",
            )
        
        try:
            # Resolve workspace ID
            workspace_id = self.workspace_service.resolve_workspace_id(workspace_name)
            
            folder_id = self.item_service.resolve_folder_id_from_path(
                workspace_id, folder_path, create_missing=True
            )

            notebook_payload = copy.deepcopy(notebook_content)
            if default_lakehouse_name:
                lakehouse_workspace = lakehouse_workspace_name or workspace_name
                lakehouse_workspace_id = self.workspace_service.resolve_workspace_id(
                    lakehouse_workspace
                )
                lakehouse = self.item_service.get_item_by_name(
                    lakehouse_workspace_id, default_lakehouse_name, "Lakehouse"
                )
                self._apply_lakehouse_dependency(
                    notebook_payload, lakehouse, lakehouse_workspace_id
                )

            # Create notebook definition
            notebook_definition = self._create_notebook_definition(
                notebook_name, notebook_payload, description, folder_id=folder_id
            )
            
            # Create the notebook item
            created_item = self.item_service.create_item(workspace_id, notebook_definition)
            
            logger.info(f"Successfully created notebook with ID: {created_item.id}")
            return CreateNotebookResult(
                status="success",
                notebook_id=created_item.id,
                message=f"Notebook '{notebook_name}' created successfully"
            )
            
        except (
            FabricItemNotFoundError,
            FabricValidationError,
            FabricAPIError,
        ) as exc:
            logger.error(f"Create notebook failed: {exc}")
            return CreateNotebookResult(
                status="error",
                message=str(exc)
            )
        except Exception as exc:
            logger.error(f"Unexpected error during notebook creation: {exc}")
            return CreateNotebookResult(
                status="error",
                message=f"Unexpected error: {exc}"
            )
    
    def get_notebook_definition(
        self,
        workspace_name: str,
        notebook_name: str,
    ) -> Dict[str, Any]:
        """Get notebook definition (ipynb content)."""
        logger.info(
            f"Fetching definition for notebook '{notebook_name}' "
            f"in workspace '{workspace_name}'"
        )

        workspace_id = self.workspace_service.resolve_workspace_id(workspace_name)
        notebook = self.item_service.get_item_by_name(
            workspace_id, notebook_name, "Notebook"
        )

        definition_response = self._get_notebook_definition_response(
            workspace_id, notebook.id
        )
        notebook_content, _ = self._extract_ipynb_content(definition_response)
        if notebook_content is not None:
            logger.info(
                f"Successfully fetched notebook definition for {notebook_name}"
            )
            return notebook_content

        logger.warning(f"No .ipynb content found for notebook {notebook_name}")
        return definition_response

    def update_notebook_content(
        self,
        workspace_name: str,
        notebook_name: str,
        notebook_content: Optional[Dict[str, Any]] = None,
        default_lakehouse_name: Optional[str] = None,
        lakehouse_workspace_name: Optional[str] = None,
    ) -> UpdateNotebookResult:
        """Update notebook content using the updateDefinition endpoint."""
        logger.info(
            f"Updating notebook '{notebook_name}' in workspace '{workspace_name}'"
        )

        try:
            workspace_id = self.workspace_service.resolve_workspace_id(workspace_name)
            notebook = self.item_service.get_item_by_name(
                workspace_id, notebook_name, "Notebook"
            )

            definition_response = self._get_notebook_definition_response(
                workspace_id, notebook.id
            )
            existing_content, notebook_path = self._extract_ipynb_content(
                definition_response
            )
            if existing_content is None:
                raise FabricError("Could not find notebook content in definition")

            if notebook_content is None:
                updated_content = copy.deepcopy(existing_content)
            else:
                updated_content = copy.deepcopy(notebook_content)

            existing_dependencies = (
                existing_content.get("metadata", {}).get("dependencies")
                if isinstance(existing_content, dict)
                else None
            )

            if default_lakehouse_name:
                if existing_dependencies is not None:
                    updated_content.setdefault("metadata", {})
                    updated_content["metadata"]["dependencies"] = copy.deepcopy(
                        existing_dependencies
                    )

                lakehouse_workspace = lakehouse_workspace_name or workspace_name
                lakehouse_workspace_id = self.workspace_service.resolve_workspace_id(
                    lakehouse_workspace
                )
                lakehouse = self.item_service.get_item_by_name(
                    lakehouse_workspace_id, default_lakehouse_name, "Lakehouse"
                )
                self._apply_lakehouse_dependency(
                    updated_content, lakehouse, lakehouse_workspace_id
                )
            elif existing_dependencies is not None:
                updated_content.setdefault("metadata", {})
                updated_content["metadata"].setdefault(
                    "dependencies", copy.deepcopy(existing_dependencies)
                )

            encoded_content = self._encode_notebook_content(updated_content)
            update_payload = {
                "definition": {
                    "format": "ipynb",
                    "parts": [
                        {
                            "path": notebook_path or f"{notebook_name}.ipynb",
                            "payload": encoded_content,
                            "payloadType": "InlineBase64",
                        }
                    ],
                }
            }

            self.client.make_api_request(
                "POST",
                f"workspaces/{workspace_id}/items/{notebook.id}/updateDefinition",
                payload=update_payload,
            )

            logger.info(
                f"Successfully updated notebook '{notebook_name}' in workspace '{workspace_name}'"
            )
            return UpdateNotebookResult(
                status="success",
                message=f"Notebook '{notebook_name}' updated successfully",
                notebook_id=notebook.id,
                notebook_name=notebook_name,
                workspace_id=workspace_id,
            )

        except (FabricItemNotFoundError, FabricValidationError, FabricAPIError) as exc:
            logger.error(f"Notebook update failed: {exc}")
            return UpdateNotebookResult(status="error", message=str(exc))
        except Exception as exc:
            logger.error(f"Unexpected error during notebook update: {exc}")
            return UpdateNotebookResult(
                status="error",
                message=f"Unexpected error: {exc}",
            )
    
    def list_notebooks(self, workspace_name: str) -> List[FabricItem]:
        """List all notebooks in workspace.
        
        Args:
            workspace_name: Name of the workspace
            
        Returns:
            List of FabricItem objects representing notebooks
            
        Example:
            ```python
            notebooks = notebook_service.list_notebooks("MyWorkspace")
            
            for notebook in notebooks:
                print(f"{notebook.display_name}: {notebook.id}")
            ```
        """
        logger.info(f"Listing notebooks in workspace '{workspace_name}'")
        
        # Resolve workspace ID
        workspace_id = self.workspace_service.resolve_workspace_id(workspace_name)
        
        # Get notebooks
        notebooks = self.item_service.list_items(workspace_id, "Notebook")
        
        logger.info(f"Found {len(notebooks)} notebooks in workspace '{workspace_name}'")
        return notebooks
    
    def get_notebook_by_name(
        self,
        workspace_name: str,
        notebook_name: str
    ) -> FabricItem:
        """Get a notebook by name.
        
        Args:
            workspace_name: Name of the workspace
            notebook_name: Name of the notebook
            
        Returns:
            FabricItem representing the notebook
            
        Raises:
            FabricItemNotFoundError: If notebook not found
            
        Example:
            ```python
            notebook = notebook_service.get_notebook_by_name(
                workspace_name="Analytics",
                notebook_name="ETL_Pipeline"
            )
            
            print(f"Notebook ID: {notebook.id}")
            print(f"Created: {notebook.created_date}")
            ```
        """
        logger.debug(
            f"Getting notebook '{notebook_name}' from workspace '{workspace_name}'"
        )
        
        # Resolve workspace ID
        workspace_id = self.workspace_service.resolve_workspace_id(workspace_name)
        
        # Find the notebook
        notebook = self.item_service.get_item_by_name(
            workspace_id, notebook_name, "Notebook"
        )
        
        return notebook
    
    def update_notebook_metadata(
        self,
        workspace_name: str,
        notebook_name: str,
        updates: Dict[str, Any]
    ) -> FabricItem:
        """Update notebook metadata (display name, description, etc.).
        
        Args:
            workspace_name: Name of the workspace
            notebook_name: Current name of the notebook
            updates: Dictionary of fields to update
            
        Returns:
            Updated FabricItem
            
        Raises:
            FabricItemNotFoundError: If notebook not found
            FabricAPIError: If API request fails
            
        Example:
            ```python
            updated_notebook = notebook_service.update_notebook_metadata(
                workspace_name="Analytics",
                notebook_name="ETL_Pipeline",
                updates={
                    "displayName": "ETL_Pipeline_v2",
                    "description": "Updated ETL pipeline with new features"
                }
            )
            
            print(f"Updated: {updated_notebook.display_name}")
            ```
        """
        logger.info(
            f"Updating metadata for notebook '{notebook_name}' "
            f"in workspace '{workspace_name}'"
        )
        
        # Resolve workspace ID
        workspace_id = self.workspace_service.resolve_workspace_id(workspace_name)
        
        # Find the notebook
        notebook = self.item_service.get_item_by_name(
            workspace_id, notebook_name, "Notebook"
        )
        
        # Update the notebook
        updated_notebook = self.item_service.update_item(
            workspace_id, notebook.id, updates
        )
        
        logger.info("Successfully updated notebook metadata")
        return updated_notebook
    
    def delete_notebook(self, workspace_name: str, notebook_name: str) -> None:
        """Delete a notebook from the workspace.
        
        Args:
            workspace_name: Name of the workspace
            notebook_name: Name of the notebook to delete
            
        Raises:
            FabricItemNotFoundError: If notebook not found
            FabricAPIError: If API request fails
            
        Example:
            ```python
            notebook_service.delete_notebook(
                workspace_name="Analytics",
                notebook_name="Old_Pipeline"
            )
            
            print("Notebook deleted successfully")
            ```
        """
        logger.info(
            f"Deleting notebook '{notebook_name}' from workspace '{workspace_name}'"
        )
        
        # Resolve workspace ID
        workspace_id = self.workspace_service.resolve_workspace_id(workspace_name)
        
        # Find the notebook
        notebook = self.item_service.get_item_by_name(
            workspace_id, notebook_name, "Notebook"
        )
        
        # Delete the notebook
        self.item_service.delete_item(workspace_id, notebook.id)
        
        logger.info(f"Successfully deleted notebook '{notebook_name}'")
    
    def execute_notebook(
        self,
        workspace_name: str,
        notebook_name: str,
        parameters: Optional[Dict[str, Any]] = None,
        wait: bool = True,
        poll_interval: int = 15,
        timeout_minutes: int = 30,
    ) -> ExecuteNotebookResult:
        """Execute notebook with parameters.
        
        Args:
            workspace_name: Name of the workspace
            notebook_name: Name of the notebook to execute
            parameters: Optional parameters for notebook execution
            wait: Whether to wait for completion (default: True)
            poll_interval: Seconds between status checks (default: 15)
            timeout_minutes: Maximum time to wait in minutes (default: 30)
            
        Returns:
            ExecuteNotebookResult with execution status
            
        Example:
            ```python
            result = notebook_service.execute_notebook(
                workspace_name="Analytics",
                notebook_name="ETL_Pipeline",
                parameters={"date": "2025-01-01", "mode": "production"},
                wait=True,
                timeout_minutes=60
            )
            
            if result.status == "success":
                print(f"Job status: {result.job_status}")
                print(f"Started: {result.start_time_utc}")
                print(f"Ended: {result.end_time_utc}")
            ```
        """
        logger.info(
            f"Executing notebook '{notebook_name}' in workspace '{workspace_name}'"
        )
        
        try:
            # Import job service here to avoid circular imports
            from .job import FabricJobService
            
            # Create job service instance
            job_service = FabricJobService(
                client=self.client,
                workspace_service=self.workspace_service,
                item_service=self.item_service
            )
            
            # Use the job service to run the notebook
            job_result = job_service.run_notebook_job(
                workspace_name=workspace_name,
                notebook_name=notebook_name,
                parameters=parameters,
                wait=wait,
                poll_interval=poll_interval,
                timeout_minutes=timeout_minutes
            )
            
            # Convert JobStatusResult to ExecuteNotebookResult for backward compatibility
            if job_result.status == "success":
                if job_result.job:
                    return ExecuteNotebookResult(
                        status="success",
                        job_instance_id=job_result.job.job_instance_id,
                        item_id=job_result.job.item_id,
                        job_type=job_result.job.job_type,
                        invoke_type=job_result.job.invoke_type,
                        job_status=job_result.job.status,
                        root_activity_id=job_result.job.root_activity_id,
                        start_time_utc=job_result.job.start_time_utc,
                        end_time_utc=job_result.job.end_time_utc,
                        failure_reason=job_result.job.failure_reason,
                        message=job_result.message,
                        # Legacy fields for backward compatibility
                        final_state=job_result.job.status,
                        started_utc=job_result.job.start_time_utc,
                        finished_utc=job_result.job.end_time_utc
                    )

                return ExecuteNotebookResult(
                    status="success",
                    job_instance_id=job_result.job_instance_id,
                    message=job_result.message
                )

            return ExecuteNotebookResult(
                status="error",
                message=job_result.message or "Unknown error occurred"
            )
            
        except Exception as exc:
            logger.error(f"Unexpected error during notebook execution: {exc}")
            return ExecuteNotebookResult(
                status="error",
                message=f"Unexpected error: {exc}"
            )
    
    def get_notebook_run_details(
        self,
        workspace_name: str,
        notebook_name: str,
        job_instance_id: str
    ) -> Dict[str, Any]:
        """Get detailed run information for a notebook job instance.
        
        Retrieves execution metadata from the Fabric Notebook Livy Sessions API,
        which provides detailed timing, resource usage, and execution state information.
        
        **Note**: This method returns execution metadata (timing, state, resource usage).
        Cell-level outputs are only available for active sessions. Once a notebook job
        completes, the session is terminated and individual cell outputs cannot be
        retrieved via the REST API. To capture cell outputs, use `mssparkutils.notebook.exit()`
        in your notebook and access the exitValue through Data Pipeline activities.
        
        Args:
            workspace_name: Name of the workspace containing the notebook
            notebook_name: Name of the notebook
            job_instance_id: The job instance ID from execute_notebook result
            
        Returns:
            Dictionary with execution details including:
            - status: "success" or "error"
            - message: Description of the result
            - session: Full Livy session details if found
            - execution_summary: Summarized execution information
            
        Example:
            ```python
            # After executing a notebook
            exec_result = notebook_service.execute_notebook(
                workspace_name="Analytics",
                notebook_name="ETL_Pipeline"
            )
            
            # Get detailed execution information
            details = notebook_service.get_notebook_run_details(
                workspace_name="Analytics",
                notebook_name="ETL_Pipeline",
                job_instance_id=exec_result.job_instance_id
            )
            
            if details["status"] == "success":
                summary = details["execution_summary"]
                print(f"State: {summary['state']}")
                print(f"Duration: {summary['total_duration_seconds']}s")
                print(f"Spark App ID: {summary['spark_application_id']}")
            ```
        """
        logger.info(
            f"Getting run details for notebook '{notebook_name}' "
            f"job instance '{job_instance_id}'"
        )
        
        try:
            # Resolve workspace ID
            workspace_id = self.workspace_service.resolve_workspace_id(workspace_name)
            
            # Find the notebook
            notebook = self.item_service.get_item_by_name(
                workspace_id, notebook_name, "Notebook"
            )
            
            # List Livy sessions for the notebook
            response = self.client.make_api_request(
                "GET",
                f"workspaces/{workspace_id}/notebooks/{notebook.id}/livySessions"
            )
            
            sessions = response.json().get("value", [])
            
            # Find the session matching the job instance ID
            matching_session = None
            for session in sessions:
                if session.get("jobInstanceId") == job_instance_id:
                    matching_session = session
                    break
            
            if not matching_session:
                return {
                    "status": "error",
                    "message": f"No Livy session found for job instance ID: {job_instance_id}",
                    "available_sessions": len(sessions)
                }
            
            # Get detailed session info
            livy_id = matching_session.get("livyId")
            detail_response = self.client.make_api_request(
                "GET",
                f"workspaces/{workspace_id}/notebooks/{notebook.id}/livySessions/{livy_id}"
            )
            session_details = detail_response.json()
            
            # Also get failure details from Job Scheduler API if available
            failure_reason = None
            try:
                job_response = self.client.make_api_request(
                    "GET",
                    f"workspaces/{workspace_id}/items/{notebook.id}/jobs/instances/{job_instance_id}"
                )
                job_data = job_response.json()
                failure_reason = job_data.get("failureReason")
            except Exception as e:
                logger.debug(f"Could not get job failure details: {e}")
            
            # Build execution summary
            execution_summary = {
                "state": session_details.get("state"),
                "spark_application_id": session_details.get("sparkApplicationId"),
                "livy_id": session_details.get("livyId"),
                "job_instance_id": session_details.get("jobInstanceId"),
                "operation_name": session_details.get("operationName"),
                "submitted_time_utc": session_details.get("submittedDateTime"),
                "start_time_utc": session_details.get("startDateTime"),
                "end_time_utc": session_details.get("endDateTime"),
                "queued_duration_seconds": session_details.get("queuedDuration", {}).get("value"),
                "running_duration_seconds": session_details.get("runningDuration", {}).get("value"),
                "total_duration_seconds": session_details.get("totalDuration", {}).get("value"),
                "driver_memory": session_details.get("driverMemory"),
                "driver_cores": session_details.get("driverCores"),
                "executor_memory": session_details.get("executorMemory"),
                "executor_cores": session_details.get("executorCores"),
                "num_executors": session_details.get("numExecutors"),
                "dynamic_allocation_enabled": session_details.get("isDynamicAllocationEnabled"),
                "runtime_version": session_details.get("runtimeVersion"),
                "cancellation_reason": session_details.get("cancellationReason"),
                "failure_reason": failure_reason,
            }
            
            logger.info(
                f"Successfully retrieved run details for job instance '{job_instance_id}'"
            )
            
            return {
                "status": "success",
                "message": f"Run details retrieved for job instance {job_instance_id}",
                "workspace_name": workspace_name,
                "notebook_name": notebook_name,
                "notebook_id": notebook.id,
                "session": session_details,
                "execution_summary": execution_summary
            }
            
        except FabricItemNotFoundError as exc:
            logger.error(f"Item not found: {exc}")
            return {
                "status": "error",
                "message": str(exc)
            }
        except FabricAPIError as exc:
            logger.error(f"API error getting run details: {exc}")
            return {
                "status": "error",
                "message": str(exc)
            }
        except Exception as exc:
            logger.error(f"Unexpected error getting run details: {exc}")
            return {
                "status": "error",
                "message": f"Unexpected error: {exc}"
            }
    
    def list_notebook_runs(
        self,
        workspace_name: str,
        notebook_name: str,
        limit: Optional[int] = None
    ) -> Dict[str, Any]:
        """List all Livy sessions (run history) for a notebook.
        
        Retrieves a list of all Livy sessions associated with a notebook, providing
        an execution history with job instance IDs, states, and timing information.
        
        Args:
            workspace_name: Name of the workspace containing the notebook
            notebook_name: Name of the notebook
            limit: Optional maximum number of sessions to return
            
        Returns:
            Dictionary with:
            - status: "success" or "error"
            - message: Description of the result
            - sessions: List of session summaries
            - total_count: Total number of sessions
            
        Example:
            ```python
            history = notebook_service.list_notebook_runs(
                workspace_name="Analytics",
                notebook_name="ETL_Pipeline",
                limit=10
            )
            
            if history["status"] == "success":
                for session in history["sessions"]:
                    print(f"{session['job_instance_id']}: {session['state']}")
            ```
        """
        logger.info(
            f"Listing runs for notebook '{notebook_name}' "
            f"in workspace '{workspace_name}'"
        )
        
        try:
            # Resolve workspace ID
            workspace_id = self.workspace_service.resolve_workspace_id(workspace_name)
            
            # Find the notebook
            notebook = self.item_service.get_item_by_name(
                workspace_id, notebook_name, "Notebook"
            )
            
            # List Livy sessions for the notebook
            response = self.client.make_api_request(
                "GET",
                f"workspaces/{workspace_id}/notebooks/{notebook.id}/livySessions"
            )
            
            sessions = response.json().get("value", [])
            
            # Build session summaries
            session_summaries = []
            for session in sessions:
                summary = {
                    "job_instance_id": session.get("jobInstanceId"),
                    "livy_id": session.get("livyId"),
                    "state": session.get("state"),
                    "operation_name": session.get("operationName"),
                    "spark_application_id": session.get("sparkApplicationId"),
                    "submitted_time_utc": session.get("submittedDateTime"),
                    "start_time_utc": session.get("startDateTime"),
                    "end_time_utc": session.get("endDateTime"),
                    "total_duration_seconds": session.get("totalDuration", {}).get("value"),
                }
                session_summaries.append(summary)
            
            # Apply limit if specified
            if limit and limit > 0:
                session_summaries = session_summaries[:limit]
            
            logger.info(
                f"Found {len(session_summaries)} runs for notebook '{notebook_name}'"
            )
            
            return {
                "status": "success",
                "message": f"Found {len(session_summaries)} runs",
                "workspace_name": workspace_name,
                "notebook_name": notebook_name,
                "notebook_id": notebook.id,
                "sessions": session_summaries,
                "total_count": len(sessions)
            }
            
        except FabricItemNotFoundError as exc:
            logger.error(f"Item not found: {exc}")
            return {
                "status": "error",
                "message": str(exc)
            }
        except FabricAPIError as exc:
            logger.error(f"API error listing runs: {exc}")
            return {
                "status": "error",
                "message": str(exc)
            }
        except Exception as exc:
            logger.error(f"Unexpected error listing runs: {exc}")
            return {
                "status": "error",
                "message": f"Unexpected error: {exc}"
            }
    
    def get_notebook_driver_logs(
        self,
        workspace_name: str,
        notebook_name: str,
        job_instance_id: str,
        log_type: str = "stdout",
        max_lines: Optional[int] = 500
    ) -> Dict[str, Any]:
        """Get Spark driver logs for a notebook execution.
        
        Retrieves the driver logs (stdout or stderr) from a completed notebook run.
        This is particularly useful for getting detailed error messages and Python
        tracebacks when a notebook fails.
        
        **Important Notes**:
        - Python exceptions appear in `stdout`, not `stderr`
        - `stderr` contains Spark/system logs (typically larger)
        - For failed notebooks, check `stdout` first for the Python error
        
        Args:
            workspace_name: Name of the workspace containing the notebook
            notebook_name: Name of the notebook
            job_instance_id: The job instance ID from execute_notebook result
            log_type: Type of log to retrieve - "stdout" (default) or "stderr"
            max_lines: Maximum number of lines to return (default: 500, None for all)
            
        Returns:
            Dictionary with:
            - status: "success" or "error"
            - message: Description of the result
            - log_type: Type of log retrieved
            - log_content: The actual log content as a string
            - log_size_bytes: Total size of the log file
            - truncated: Whether the log was truncated
            
        Example:
            ```python
            # Get error details from a failed notebook
            result = notebook_service.get_notebook_driver_logs(
                workspace_name="Analytics",
                notebook_name="ETL_Pipeline",
                job_instance_id="12345678-1234-1234-1234-123456789abc",
                log_type="stdout"  # Python errors are in stdout!
            )
            
            if result["status"] == "success":
                print(result["log_content"])
                # Look for "Error", "Exception", "Traceback" in the output
            ```
        """
        logger.info(
            f"Getting driver logs ({log_type}) for notebook '{notebook_name}' "
            f"job instance '{job_instance_id}'"
        )
        
        try:
            # Validate log_type
            if log_type not in ("stdout", "stderr"):
                return {
                    "status": "error",
                    "message": f"Invalid log_type '{log_type}'. Must be 'stdout' or 'stderr'."
                }
            
            # Resolve workspace ID
            workspace_id = self.workspace_service.resolve_workspace_id(workspace_name)
            
            # Find the notebook
            notebook = self.item_service.get_item_by_name(
                workspace_id, notebook_name, "Notebook"
            )
            
            # First, get the run details to find the Livy session and Spark app ID
            exec_details = self.get_notebook_run_details(
                workspace_name, notebook_name, job_instance_id
            )
            
            if exec_details.get("status") != "success":
                return exec_details
            
            summary = exec_details.get("execution_summary", {})
            livy_id = summary.get("livy_id")
            spark_app_id = summary.get("spark_application_id")
            
            if not livy_id or not spark_app_id:
                return {
                    "status": "error",
                    "message": f"Could not find Livy session or Spark application ID for job {job_instance_id}"
                }
            
            # First, get log metadata to check file size
            meta_response = self.client.make_api_request(
                "GET",
                f"workspaces/{workspace_id}/notebooks/{notebook.id}/livySessions/{livy_id}"
                f"/applications/{spark_app_id}/logs?type=driver&meta=true&fileName={log_type}"
            )
            
            log_metadata = meta_response.json()
            log_size = log_metadata.get("sizeInBytes", 0)
            
            # Now fetch the actual log content
            log_response = self.client.make_api_request(
                "GET",
                f"workspaces/{workspace_id}/notebooks/{notebook.id}/livySessions/{livy_id}"
                f"/applications/{spark_app_id}/logs?type=driver&fileName={log_type}&isDownload=true"
            )
            
            log_content = log_response.text
            
            # Optionally truncate to max_lines
            truncated = False
            if max_lines and max_lines > 0:
                lines = log_content.split('\n')
                if len(lines) > max_lines:
                    # Keep last N lines (most recent, where errors typically are)
                    lines = lines[-max_lines:]
                    log_content = '\n'.join(lines)
                    truncated = True
            
            logger.info(
                f"Successfully retrieved {log_type} logs ({log_size} bytes) "
                f"for job instance '{job_instance_id}'"
            )
            
            return {
                "status": "success",
                "message": f"Successfully retrieved {log_type} logs",
                "workspace_name": workspace_name,
                "notebook_name": notebook_name,
                "job_instance_id": job_instance_id,
                "spark_application_id": spark_app_id,
                "livy_id": livy_id,
                "log_type": log_type,
                "log_content": log_content,
                "log_size_bytes": log_size,
                "truncated": truncated,
                "max_lines": max_lines
            }
            
        except FabricItemNotFoundError as exc:
            logger.error(f"Item not found: {exc}")
            return {
                "status": "error",
                "message": str(exc)
            }
        except FabricAPIError as exc:
            logger.error(f"API error getting driver logs: {exc}")
            return {
                "status": "error",
                "message": str(exc)
            }
        except Exception as exc:
            logger.error(f"Unexpected error getting driver logs: {exc}")
            return {
                "status": "error",
                "message": f"Unexpected error: {exc}"
            }
