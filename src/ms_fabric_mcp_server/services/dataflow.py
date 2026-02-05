# ABOUTME: Service for Fabric Dataflow Gen2 operations.
# ABOUTME: Handles dataflow creation, definition retrieval, and execution.
"""Service for Microsoft Fabric Dataflow Gen2 operations."""

import base64
import json
import logging
import re
import uuid
from typing import Any, Dict, Optional, Tuple

from ..client.exceptions import (
    FabricAPIError,
    FabricError,
    FabricItemNotFoundError,
    FabricValidationError,
)
from ..client.http_client import FabricClient
from ..models.results import RunJobResult
from .item import FabricItemService
from .workspace import FabricWorkspaceService

logger = logging.getLogger(__name__)


class FabricDataflowService:
    """Service for Microsoft Fabric Dataflow Gen2 operations.

    This service provides high-level operations for creating, inspecting, and
    running Dataflow Gen2 items in Fabric workspaces.

    Example:
        ```python
        from ms_fabric_mcp_server import FabricConfig, FabricClient
        from ms_fabric_mcp_server.services import (
            FabricDataflowService,
            FabricWorkspaceService,
            FabricItemService
        )

        config = FabricConfig.from_environment()
        client = FabricClient(config)
        workspace_service = FabricWorkspaceService(client)
        item_service = FabricItemService(client)
        dataflow_service = FabricDataflowService(client, workspace_service, item_service)

        # Create a dataflow
        dataflow_id = dataflow_service.create_dataflow(
            workspace_id="12345678-1234-1234-1234-123456789abc",
            dataflow_name="CustomerETL",
            mashup_content=\"\"\"
section Section1;
shared Customers = let
    Source = ...
in
    Source;
\"\"\"
        )
        ```
    """

    def __init__(
        self,
        client: FabricClient,
        workspace_service: FabricWorkspaceService,
        item_service: FabricItemService,
    ):
        """Initialize the dataflow service.

        Args:
            client: FabricClient instance for API calls
            workspace_service: Service for workspace operations
            item_service: Service for item operations
        """
        self.client = client
        self.workspace_service = workspace_service
        self.item_service = item_service
        logger.debug("FabricDataflowService initialized")

    def _parse_query_names_from_mashup(self, mashup_content: str) -> list[str]:
        """Extract query names from Power Query M code.

        Parses the mashup content to find shared query declarations.

        Args:
            mashup_content: Power Query M code in section format

        Returns:
            List of query names found in the mashup
        """
        # Match "shared QueryName =" patterns
        pattern = r"shared\s+(\w+)\s*="
        matches = re.findall(pattern, mashup_content)
        return matches

    def _generate_query_metadata(
        self, dataflow_name: str, mashup_content: str
    ) -> Dict[str, Any]:
        """Generate queryMetadata.json from mashup content.

        Creates a basic query metadata structure based on the queries
        found in the Power Query M code.

        Args:
            dataflow_name: Name of the dataflow
            mashup_content: Power Query M code

        Returns:
            Dictionary representing queryMetadata.json
        """
        query_names = self._parse_query_names_from_mashup(mashup_content)

        queries_metadata = {}
        for name in query_names:
            queries_metadata[name] = {
                "queryId": str(uuid.uuid4()),
                "queryName": name,
                "queryGroupId": None,
                "isHidden": False,
                "loadEnabled": True,
            }

        return {
            "formatVersion": "202502",
            "name": dataflow_name,
            "computeEngineSettings": {},
            "queryGroups": [],
            "documentLocale": "en-US",
            "queriesMetadata": queries_metadata,
            "connections": [],
            "fastCombine": False,
            "allowNativeQueries": True,
            "skipAutomaticTypeAndHeaderDetection": False,
        }

    def _validate_dataflow_inputs(
        self,
        dataflow_name: str,
        mashup_content: str,
    ) -> None:
        """Validate dataflow creation inputs.

        Args:
            dataflow_name: Display name for the dataflow
            mashup_content: Power Query M code

        Raises:
            FabricValidationError: If validation fails
        """
        if not dataflow_name or not dataflow_name.strip():
            raise FabricValidationError(
                field="dataflow_name",
                value=dataflow_name or "",
                message="dataflow_name cannot be empty",
            )

        if not mashup_content or not mashup_content.strip():
            raise FabricValidationError(
                field="mashup_content",
                value="",
                message="mashup_content cannot be empty",
            )

        # Check for section declaration
        if "section" not in mashup_content.lower():
            raise FabricValidationError(
                field="mashup_content",
                value=mashup_content[:50] + "..." if len(mashup_content) > 50 else mashup_content,
                message="mashup_content must contain a section declaration (e.g., 'section Section1;')",
            )

    @staticmethod
    def _extract_dataflow_error(payload: Dict[str, Any]) -> str:
        """Extract useful error message from Fabric dataflow error payloads."""
        if not isinstance(payload, dict):
            return "Unknown dataflow error"

        message = (
            payload.get("message")
            or payload.get("error", {}).get("message")
            or payload.get("failureReason", {}).get("message")
        )
        if message:
            return message

        for detail in payload.get("moreDetails", []) or []:
            if not isinstance(detail, dict):
                continue
            msg = detail.get("message")
            if not msg:
                detail_value = detail.get("detail")
                if isinstance(detail_value, dict):
                    msg = detail_value.get("value")
            if msg:
                return msg

        pbi_error = payload.get("pbi.error") or payload.get("error", {}).get("pbi.error")
        if isinstance(pbi_error, dict):
            for detail in pbi_error.get("details", []) or []:
                if not isinstance(detail, dict):
                    continue
                if detail.get("code") == "DetailsMessage":
                    detail_value = detail.get("detail")
                    if isinstance(detail_value, dict) and detail_value.get("value"):
                        return detail_value.get("value")
                    return "Unknown dataflow error"

        return "Unknown dataflow error"

    @staticmethod
    def _load_error_payload(response_body: Optional[str]) -> Dict[str, Any]:
        """Parse response body to JSON payload for error extraction."""
        if not response_body:
            return {}
        try:
            parsed = json.loads(response_body)
        except (TypeError, ValueError):
            return {"message": response_body}
        if isinstance(parsed, dict):
            return parsed
        return {"message": str(parsed)}

    def create_dataflow(
        self,
        workspace_id: str,
        dataflow_name: str,
        mashup_content: str,
        query_metadata: Optional[Dict[str, Any]] = None,
        description: Optional[str] = None,
        folder_id: Optional[str] = None,
    ) -> str:
        """Create a Dataflow Gen2 item with Power Query M code.

        Args:
            workspace_id: ID of the target workspace
            dataflow_name: Display name for the dataflow
            mashup_content: Power Query M code (mashup.pq content)
            query_metadata: Optional queryMetadata.json dict; auto-generated if omitted
            description: Optional description for the dataflow
            folder_id: Optional folder ID to place the dataflow in

        Returns:
            ID of the created dataflow

        Raises:
            FabricValidationError: If inputs are invalid
            FabricAPIError: If API request fails
        """
        self._validate_dataflow_inputs(dataflow_name, mashup_content)

        # Generate query metadata if not provided
        if query_metadata is None:
            query_metadata = self._generate_query_metadata(dataflow_name, mashup_content)

        logger.info(f"Creating dataflow '{dataflow_name}' in workspace '{workspace_id}'")

        # Encode definition parts
        mashup_encoded = base64.b64encode(mashup_content.encode("utf-8")).decode("utf-8")
        metadata_encoded = base64.b64encode(
            json.dumps(query_metadata).encode("utf-8")
        ).decode("utf-8")

        # Build request payload
        payload: Dict[str, Any] = {
            "displayName": dataflow_name,
            "type": "Dataflow",
            "definition": {
                "parts": [
                    {
                        "path": "mashup.pq",
                        "payload": mashup_encoded,
                        "payloadType": "InlineBase64",
                    },
                    {
                        "path": "queryMetadata.json",
                        "payload": metadata_encoded,
                        "payloadType": "InlineBase64",
                    },
                ]
            },
        }

        if description:
            payload["description"] = description

        if folder_id:
            payload["folderId"] = folder_id

        # Create the dataflow
        try:
            response = self.client.make_api_request(
                "POST",
                f"workspaces/{workspace_id}/dataflows",
                payload=payload,
                wait_for_lro=True,
            )
            response_data = response.json()
            dataflow_id = response_data.get("id")

            if not dataflow_id:
                raise FabricError("Failed to create dataflow: no ID in response")

            logger.info(f"Dataflow '{dataflow_name}' created with ID: {dataflow_id}")
            return dataflow_id

        except FabricAPIError:
            raise
        except Exception as exc:
            logger.error(f"Failed to create dataflow: {exc}")
            raise FabricError(f"Failed to create dataflow: {exc}")

    def get_dataflow_definition(
        self,
        workspace_id: str,
        dataflow_id: str,
    ) -> Tuple[Dict[str, Any], str]:
        """Retrieve the full definition of a dataflow.

        Args:
            workspace_id: ID of the workspace containing the dataflow
            dataflow_id: ID of the dataflow

        Returns:
            Tuple of (query_metadata dict, mashup_content string)

        Raises:
            FabricItemNotFoundError: If dataflow not found
            FabricAPIError: If API request fails
        """
        logger.info(
            f"Getting definition for dataflow '{dataflow_id}' in workspace '{workspace_id}'"
        )

        try:
            response = self.client.make_api_request(
                "POST",
                f"workspaces/{workspace_id}/dataflows/{dataflow_id}/getDefinition",
                wait_for_lro=True,
            )
            definition_data = response.json()
        except FabricAPIError as e:
            if e.status_code == 404:
                raise FabricItemNotFoundError(
                    item_type="Dataflow",
                    item_name=dataflow_id,
                    workspace_name=workspace_id,
                )
            raise

        # Parse the definition parts
        definition = definition_data.get("definition", {})
        parts = definition.get("parts", [])

        query_metadata = {}
        mashup_content = ""

        for part in parts:
            path = part.get("path", "")
            payload = part.get("payload", "")

            if path == "queryMetadata.json":
                decoded = base64.b64decode(payload).decode("utf-8")
                query_metadata = json.loads(decoded)
            elif path == "mashup.pq":
                mashup_content = base64.b64decode(payload).decode("utf-8")

        logger.info(f"Successfully retrieved definition for dataflow '{dataflow_id}'")
        return query_metadata, mashup_content

    def run_dataflow(
        self,
        workspace_id: str,
        dataflow_id: str,
        execution_data: Optional[Dict[str, Any]] = None,
    ) -> RunJobResult:
        """Trigger an on-demand dataflow refresh.

        Uses the dedicated Dataflow Execute API endpoint which differs from
        the generic job scheduler used by other item types.

        Args:
            workspace_id: ID of the workspace containing the dataflow
            dataflow_id: ID of the dataflow to run
            execution_data: Optional parameters for parameterized dataflows

        Returns:
            RunJobResult with job instance ID and location URL for polling

        Raises:
            FabricItemNotFoundError: If dataflow not found
            FabricAPIError: If API request fails
        """
        logger.info(
            f"Starting dataflow refresh for '{dataflow_id}' in workspace '{workspace_id}'"
        )

        # Dataflow-specific Execute endpoint
        endpoint = f"workspaces/{workspace_id}/dataflows/{dataflow_id}/jobs/Execute/instances"

        # Build request payload if execution data provided
        payload = None
        if execution_data:
            payload = {"executionData": execution_data}

        try:
            response = self.client.make_api_request(
                "POST",
                endpoint,
                payload=payload,
                timeout=60,
            )
        except FabricAPIError as e:
            if e.status_code == 404:
                raise FabricItemNotFoundError(
                    item_type="Dataflow",
                    item_name=dataflow_id,
                    workspace_name=workspace_id,
                )
            payload = self._load_error_payload(e.response_body)
            if payload:
                error_message = self._extract_dataflow_error(payload)
                raise FabricAPIError(
                    status_code=e.status_code,
                    message=error_message,
                    response_body=e.response_body,
                ) from e
            raise

        # Extract job info from response headers
        location_url = response.headers.get("Location", "").strip()
        retry_after = 30
        if "Retry-After" in response.headers:
            try:
                retry_after = int(response.headers["Retry-After"])
            except (ValueError, TypeError):
                pass

        # Extract job instance ID from location URL
        job_instance_id = None
        if location_url:
            # Location URL format: .../jobs/instances/{job_instance_id}
            parts = location_url.rstrip("/").split("/")
            if parts:
                job_instance_id = parts[-1]

        logger.info(f"Dataflow refresh started with job instance: {job_instance_id}")

        return RunJobResult(
            status="success",
            message="Dataflow refresh started",
            job_instance_id=job_instance_id,
            location_url=location_url,
            retry_after=retry_after,
        )
