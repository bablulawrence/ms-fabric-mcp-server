# ABOUTME: Service for Fabric notebook operations.
# ABOUTME: Handles notebook import, export, execution, and metadata management.
"""Service for Fabric notebook operations."""

import base64
import logging
import os
from pathlib import Path
from typing import Any, Dict, List, Optional

from ..client.exceptions import (
    FabricAPIError,
    FabricError,
    FabricItemNotFoundError,
    FabricValidationError,
)
from ..client.http_client import FabricClient
from ..models.item import FabricItem
from ..models.results import ExecuteNotebookResult, ImportNotebookResult, AttachLakehouseResult

logger = logging.getLogger(__name__)


class FabricNotebookService:
    """Service for Fabric notebook operations.
    
    This service handles notebook import, export, execution, and metadata management.
    
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
        
        # Import a notebook
        result = notebook_service.import_notebook(
            workspace_name="MyWorkspace",
            notebook_name="MyNotebook",
            local_path="notebooks/my_notebook.ipynb",
            description="ETL Pipeline"
        )
        
        if result.status == "success":
            print(f"Notebook imported with ID: {result.artifact_id}")
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
    
    def _resolve_notebook_path(self, notebook_path: str) -> str:
        """Resolve notebook path using repo root if configured.
        
        Args:
            notebook_path: Path to the notebook file (relative or absolute)
            
        Returns:
            Resolved absolute path to the notebook file
        """
        # If path is already absolute, return as-is
        if os.path.isabs(notebook_path):
            return notebook_path
        
        # If repo root is configured, resolve relative to repo root
        if self.repo_root:
            resolved_path = os.path.join(self.repo_root, notebook_path)
            logger.debug(f"Resolved notebook path: {notebook_path} -> {resolved_path}")
            return resolved_path
        
        # Otherwise, resolve relative to current working directory
        return os.path.abspath(notebook_path)
    
    def _encode_notebook_file(self, notebook_path: str) -> str:
        """Encode notebook file to base64.
        
        Args:
            notebook_path: Path to the notebook file
            
        Returns:
            Base64 encoded notebook content
            
        Raises:
            FileNotFoundError: If notebook file doesn't exist
            ValueError: If notebook file is empty
            FabricError: For other encoding errors
        """
        # Resolve the notebook path using repo root if configured
        resolved_path = self._resolve_notebook_path(notebook_path)
        logger.debug(f"Encoding notebook file: {resolved_path}")
        
        # Check if file exists and validate size
        p = Path(resolved_path)
        if not p.exists():
            raise FileNotFoundError(f"Notebook not found: {p}")
        
        size = p.stat().st_size
        if size == 0:
            raise ValueError(
                f"Notebook is empty (0 bytes): {p}. "
                "Please ensure notebook is saved before uploading."
            )
        
        try:
            with open(resolved_path, "rb") as file:
                content = file.read()
                encoded_content = base64.b64encode(content).decode('utf-8')
            
            logger.debug(f"Notebook encoded: {len(encoded_content)} base64 characters")
            return encoded_content
            
        except Exception as exc:
            logger.error(f"Failed to encode notebook: {exc}")
            raise FabricError(f"Failed to encode notebook file: {exc}")
    
    def _create_notebook_definition(
        self,
        notebook_name: str,
        notebook_path: str,
        description: Optional[str] = None
    ) -> Dict[str, Any]:
        """Create notebook definition for API request.
        
        Args:
            notebook_name: Display name for the notebook
            notebook_path: Path to the notebook file
            description: Optional description
            
        Returns:
            Notebook definition dictionary
        """
        encoded_content = self._encode_notebook_file(notebook_path)
        
        definition = {
            "displayName": notebook_name,
            "type": "Notebook",
            "definition": {
                "format": "ipynb",
                "parts": [
                    {
                        "path": os.path.basename(notebook_path),
                        "payload": encoded_content,
                        "payloadType": "InlineBase64",
                    }
                ],
            },
        }
        
        if description:
            definition["description"] = description
        
        return definition

    @staticmethod
    def _parse_retry_after(headers: Dict[str, Any], default: int = 5) -> int:
        """Safely parse Retry-After header with fallback default."""
        value = headers.get("Retry-After", default)
        try:
            return int(value)
        except (TypeError, ValueError):
            return default
    
    def import_notebook(
        self,
        workspace_name: str,
        notebook_name: str,
        local_path: str,
        description: Optional[str] = None
    ) -> ImportNotebookResult:
        """Import local notebook to Fabric workspace.
        
        Args:
            workspace_name: Name of the target workspace
            notebook_name: Display name for the notebook in Fabric
            local_path: Path to the local notebook file
            description: Optional description for the notebook
            
        Returns:
            ImportNotebookResult with operation status and notebook ID
            
        Example:
            ```python
            result = notebook_service.import_notebook(
                workspace_name="Analytics",
                notebook_name="ETL_Pipeline",
                local_path="notebooks/etl.ipynb",
                description="Daily ETL processing"
            )
            
            if result.status == "success":
                print(f"Notebook ID: {result.artifact_id}")
            ```
        """
        logger.info(
            f"Importing notebook '{notebook_name}' to workspace '{workspace_name}'"
        )
        
        try:
            # Resolve workspace ID
            workspace_id = self.workspace_service.resolve_workspace_id(workspace_name)
            
            # Create notebook definition
            notebook_definition = self._create_notebook_definition(
                notebook_name, local_path, description
            )
            
            # Create the notebook item
            created_item = self.item_service.create_item(workspace_id, notebook_definition)
            
            logger.info(f"Successfully imported notebook with ID: {created_item.id}")
            return ImportNotebookResult(
                status="success",
                artifact_id=created_item.id,
                message=f"Notebook '{notebook_name}' imported successfully"
            )
            
        except (
            FabricItemNotFoundError,
            FabricValidationError,
            FabricAPIError,
            FileNotFoundError,
            ValueError
        ) as exc:
            logger.error(f"Import failed: {exc}")
            return ImportNotebookResult(
                status="error",
                message=str(exc)
            )
        except Exception as exc:
            logger.error(f"Unexpected error during import: {exc}")
            return ImportNotebookResult(
                status="error",
                message=f"Unexpected error: {exc}"
            )
    
    def get_notebook_content(
        self,
        workspace_name: str,
        notebook_name: str
    ) -> Dict[str, Any]:
        """Get notebook definition and content.
        
        Retrieves the full notebook definition in ipynb format, including cells,
        metadata, and dependencies. Handles Fabric's long-running operation pattern
        for fetching notebook definitions.
        
        Args:
            workspace_name: Name of the workspace
            notebook_name: Name of the notebook
            
        Returns:
            Dictionary containing notebook definition with cells and metadata
            
        Raises:
            FabricItemNotFoundError: If notebook not found
            FabricAPIError: If API request fails
            
        Example:
            ```python
            content = notebook_service.get_notebook_content(
                workspace_name="Analytics",
                notebook_name="ETL_Pipeline"
            )
            
            print(f"Cells: {len(content.get('cells', []))}")
            print(f"Lakehouse: {content.get('metadata', {}).get('dependencies', {}).get('lakehouse')}")
            ```
        """
        import json
        import time
        
        logger.info(
            f"Fetching content for notebook '{notebook_name}' "
            f"in workspace '{workspace_name}'"
        )
        
        # Resolve workspace ID
        workspace_id = self.workspace_service.resolve_workspace_id(workspace_name)
        
        # Find the notebook
        notebook = self.item_service.get_item_by_name(
            workspace_id, notebook_name, "Notebook"
        )
        
        # Get the notebook definition in ipynb format
        response = self.client.make_api_request(
            "POST",
            f"workspaces/{workspace_id}/items/{notebook.id}/getDefinition?format=ipynb"
        )
        
        # Handle 202 Accepted (long-running operation)
        if response.status_code == 202:
            location = response.headers.get("Location")
            retry_after = self._parse_retry_after(response.headers, 5)
            
            if location:
                max_retries = 30
                for _ in range(max_retries):
                    time.sleep(retry_after)
                    poll_response = self.client.make_api_request("GET", location)
                    if poll_response.status_code == 200:
                        op_result = poll_response.json()
                        if op_result.get("status") == "Succeeded":
                            # Get the actual result from the /result endpoint
                            result_response = self.client.make_api_request(
                                "GET", 
                                f"{location}/result"
                            )
                            if result_response.status_code == 200:
                                definition_response = result_response.json()
                                break
                            else:
                                raise FabricAPIError(
                                    result_response.status_code,
                                    f"Failed to get definition result: {result_response.text}"
                                )
                        elif op_result.get("status") == "Failed":
                            error_msg = op_result.get("error", {}).get("message", "Unknown error")
                            raise FabricError(f"Operation failed: {error_msg}")
                        else:
                            retry_after = self._parse_retry_after(poll_response.headers, 5)
                            continue
                    elif poll_response.status_code == 202:
                        retry_after = self._parse_retry_after(poll_response.headers, 5)
                        continue
                    else:
                        raise FabricAPIError(
                            poll_response.status_code,
                            f"Failed to poll operation: {poll_response.text}"
                        )
                else:
                    raise FabricError("Timeout waiting for notebook definition")
            else:
                raise FabricError("No Location header in 202 response")
        else:
            definition_response = response.json()
        
        # Extract the notebook content from the definition
        parts = definition_response.get("definition", {}).get("parts", [])
        
        for part in parts:
            if part.get("path", "").endswith(".ipynb"):
                payload = part.get("payload", "")
                notebook_content = json.loads(base64.b64decode(payload).decode('utf-8'))
                logger.info(f"Successfully fetched notebook content for {notebook_name}")
                return notebook_content
        
        # If no .ipynb found, return the raw definition
        logger.warning(f"No .ipynb content found for notebook {notebook_name}")
        return definition_response
    
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
            if job_result.status == "success" and job_result.job:
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
            else:
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
    
    def attach_lakehouse_to_notebook(
        self,
        workspace_name: str,
        notebook_name: str,
        lakehouse_name: str,
        lakehouse_workspace_name: Optional[str] = None
    ) -> AttachLakehouseResult:
        """Attach a default lakehouse to a notebook.
        
        Updates the notebook definition to set a default lakehouse. This lakehouse
        will be automatically mounted when the notebook runs, providing seamless
        access to the lakehouse tables and files.
        
        The implementation follows the Fabric REST API approach:
        1. Get the notebook definition in ipynb format
        2. Modify the notebook metadata to include lakehouse dependencies
        3. Update the notebook definition with the modified content
        
        Args:
            workspace_name: Name of the workspace containing the notebook
            notebook_name: Name of the notebook to update
            lakehouse_name: Name of the lakehouse to attach as default
            lakehouse_workspace_name: Optional workspace name for the lakehouse 
                                     (defaults to same workspace as notebook)
            
        Returns:
            AttachLakehouseResult with operation status and details
            
        Raises:
            FabricItemNotFoundError: If notebook or lakehouse not found
            FabricAPIError: If API request fails
            
        Example:
            ```python
            result = notebook_service.attach_lakehouse_to_notebook(
                workspace_name="Analytics",
                notebook_name="ETL_Pipeline",
                lakehouse_name="Bronze_Lakehouse"
            )
            
            if result.status == "success":
                print(f"Lakehouse '{result.lakehouse_name}' attached to notebook")
            ```
        """
        logger.info(
            f"Attaching lakehouse '{lakehouse_name}' to notebook '{notebook_name}' "
            f"in workspace '{workspace_name}'"
        )
        
        try:
            import json
            import time
            
            # Resolve notebook workspace ID
            notebook_workspace_id = self.workspace_service.resolve_workspace_id(workspace_name)
            
            # Find the notebook
            notebook = self.item_service.get_item_by_name(
                notebook_workspace_id, notebook_name, "Notebook"
            )
            
            # Resolve lakehouse workspace (same as notebook if not specified)
            lakehouse_workspace_name = lakehouse_workspace_name or workspace_name
            lakehouse_workspace_id = self.workspace_service.resolve_workspace_id(lakehouse_workspace_name)
            
            # Find the lakehouse
            lakehouse = self.item_service.get_item_by_name(
                lakehouse_workspace_id, lakehouse_name, "Lakehouse"
            )
            
            # Step 1: Get the current notebook definition in ipynb format
            logger.debug(f"Getting notebook definition for {notebook.id}")
            response = self.client.make_api_request(
                "POST",
                f"workspaces/{notebook_workspace_id}/items/{notebook.id}/getDefinition?format=ipynb"
            )
            
            # Handle 202 Accepted (long-running operation)
            if response.status_code == 202:
                # Poll for completion using the Location header
                location = response.headers.get("Location")
                retry_after = self._parse_retry_after(response.headers, 5)
                
                if location:
                    max_retries = 30
                    for _ in range(max_retries):
                        time.sleep(retry_after)
                        poll_response = self.client.make_api_request("GET", location)
                        if poll_response.status_code == 200:
                            # Check if operation succeeded
                            op_result = poll_response.json()
                            if op_result.get("status") == "Succeeded":
                                # Get the actual result from the /result endpoint
                                result_response = self.client.make_api_request(
                                    "GET", 
                                    f"{location}/result"
                                )
                                if result_response.status_code == 200:
                                    definition_response = result_response.json()
                                    break
                                else:
                                    raise FabricAPIError(
                                        result_response.status_code,
                                        f"Failed to get definition result: {result_response.text}"
                                    )
                            elif op_result.get("status") == "Failed":
                                error_msg = op_result.get("error", {}).get("message", "Unknown error")
                                raise FabricError(f"Operation failed: {error_msg}")
                            else:
                                # Still in progress
                                retry_after = self._parse_retry_after(poll_response.headers, 5)
                                continue
                        elif poll_response.status_code == 202:
                            retry_after = self._parse_retry_after(poll_response.headers, 5)
                            continue
                        else:
                            raise FabricAPIError(
                                poll_response.status_code,
                                f"Failed to poll operation: {poll_response.text}"
                            )
                    else:
                        raise FabricError("Timeout waiting for notebook definition")
                else:
                    raise FabricError("No Location header in 202 response")
            else:
                definition_response = response.json()
            
            if definition_response is None:
                raise FabricError("Empty response when getting notebook definition")
            
            # Extract the notebook content from the definition
            parts = definition_response.get("definition", {}).get("parts", [])
            notebook_content = None
            notebook_path = None
            
            logger.debug(f"Found {len(parts)} parts in notebook definition")
            
            for part in parts:
                path = part.get("path", "")
                logger.debug(f"Checking part: {path}")
                if path.endswith(".ipynb"):
                    notebook_path = path
                    payload = part.get("payload", "")
                    # Decode the base64 content
                    notebook_content = json.loads(base64.b64decode(payload).decode('utf-8'))
                    break
            
            if notebook_content is None:
                raise FabricError("Could not find notebook content in definition")
            
            # Step 2: Modify the notebook metadata to include lakehouse dependencies
            if "metadata" not in notebook_content:
                notebook_content["metadata"] = {}
            
            if "dependencies" not in notebook_content["metadata"]:
                notebook_content["metadata"]["dependencies"] = {}
            
            # Set the lakehouse configuration
            notebook_content["metadata"]["dependencies"]["lakehouse"] = {
                "default_lakehouse": lakehouse.id,
                "default_lakehouse_name": lakehouse_name,
                "default_lakehouse_workspace_id": lakehouse_workspace_id,
                "known_lakehouses": [
                    {"id": lakehouse.id}
                ]
            }
            
            # Step 3: Encode the updated notebook content and update the definition
            updated_content = json.dumps(notebook_content)
            encoded_content = base64.b64encode(updated_content.encode('utf-8')).decode('utf-8')
            
            update_payload = {
                "definition": {
                    "format": "ipynb",
                    "parts": [
                        {
                            "path": notebook_path or "notebook-content.ipynb",
                            "payload": encoded_content,
                            "payloadType": "InlineBase64"
                        }
                    ]
                }
            }
            
            # Call the updateDefinition endpoint
            self.client.make_api_request(
                "POST",
                f"workspaces/{notebook_workspace_id}/items/{notebook.id}/updateDefinition",
                payload=update_payload
            )
            
            logger.info(
                f"Successfully attached lakehouse '{lakehouse_name}' to notebook '{notebook_name}'"
            )
            
            return AttachLakehouseResult(
                status="success",
                message=f"Successfully attached lakehouse '{lakehouse_name}' to notebook '{notebook_name}'",
                notebook_id=notebook.id,
                notebook_name=notebook_name,
                lakehouse_id=lakehouse.id,
                lakehouse_name=lakehouse_name,
                workspace_id=notebook_workspace_id
            )
            
        except FabricItemNotFoundError as exc:
            logger.error(f"Item not found: {exc}")
            return AttachLakehouseResult(
                status="error",
                message=str(exc)
            )
        except FabricAPIError as exc:
            logger.error(f"API error attaching lakehouse: {exc}")
            return AttachLakehouseResult(
                status="error",
                message=str(exc)
            )
        except Exception as exc:
            logger.error(f"Unexpected error attaching lakehouse to notebook: {exc}")
            return AttachLakehouseResult(
                status="error",
                message=f"Unexpected error: {exc}"
            )
    
    def get_notebook_execution_details(
        self,
        workspace_name: str,
        notebook_name: str,
        job_instance_id: str
    ) -> Dict[str, Any]:
        """Get detailed execution information for a notebook run by job instance ID.
        
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
            details = notebook_service.get_notebook_execution_details(
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
            f"Getting execution details for notebook '{notebook_name}' "
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
                f"Successfully retrieved execution details for job instance '{job_instance_id}'"
            )
            
            return {
                "status": "success",
                "message": f"Execution details retrieved for job instance {job_instance_id}",
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
            logger.error(f"API error getting execution details: {exc}")
            return {
                "status": "error",
                "message": str(exc)
            }
        except Exception as exc:
            logger.error(f"Unexpected error getting execution details: {exc}")
            return {
                "status": "error",
                "message": f"Unexpected error: {exc}"
            }
    
    def list_notebook_executions(
        self,
        workspace_name: str,
        notebook_name: str,
        limit: Optional[int] = None
    ) -> Dict[str, Any]:
        """List all Livy sessions (execution history) for a notebook.
        
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
            history = notebook_service.list_notebook_executions(
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
            f"Listing executions for notebook '{notebook_name}' "
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
                f"Found {len(session_summaries)} executions for notebook '{notebook_name}'"
            )
            
            return {
                "status": "success",
                "message": f"Found {len(session_summaries)} executions",
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
            logger.error(f"API error listing executions: {exc}")
            return {
                "status": "error",
                "message": str(exc)
            }
        except Exception as exc:
            logger.error(f"Unexpected error listing executions: {exc}")
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
            
            # First, get the execution details to find the Livy session and Spark app ID
            exec_details = self.get_notebook_execution_details(
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
