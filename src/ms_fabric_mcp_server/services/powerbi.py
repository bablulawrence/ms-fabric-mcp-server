# ABOUTME: Service for Power BI REST API operations against Fabric semantic models.
# ABOUTME: Provides refresh and DAX query execution helpers.
"""Power BI REST API service for semantic model operations."""

import logging
import time
from typing import Any, Dict, Optional

from ms_fabric_mcp_server.client.exceptions import (
    FabricAPIError,
    FabricError,
    FabricValidationError,
)
from ms_fabric_mcp_server.client.http_client import FabricClient
from ms_fabric_mcp_server.services.item import FabricItemService
from ms_fabric_mcp_server.services.workspace import FabricWorkspaceService

logger = logging.getLogger(__name__)


class FabricPowerBIService:
    """Service for Power BI REST API operations on semantic models."""

    def __init__(
        self,
        powerbi_client: FabricClient,
        workspace_service: FabricWorkspaceService,
        item_service: FabricItemService,
        refresh_poll_interval: float = 5.0,
        refresh_wait_timeout: int = 1800,
    ):
        self.powerbi_client = powerbi_client
        self.workspace_service = workspace_service
        self.item_service = item_service
        self.refresh_poll_interval = refresh_poll_interval
        self.refresh_wait_timeout = refresh_wait_timeout

    def refresh_semantic_model(
        self,
        workspace_name: str,
        semantic_model_name: Optional[str] = None,
        semantic_model_id: Optional[str] = None,
        refresh_type: Optional[str] = None,
        objects: Optional[list[dict]] = None,
    ) -> Dict[str, Any]:
        """Trigger and wait for a semantic model refresh via Power BI REST API."""
        workspace_id = self.workspace_service.resolve_workspace_id(workspace_name)
        semantic_model = self._resolve_semantic_model(
            workspace_id, semantic_model_name, semantic_model_id
        )

        payload: Dict[str, Any] = {}
        if refresh_type:
            payload["type"] = refresh_type
        if objects:
            payload["objects"] = objects
        if not payload:
            payload = None

        try:
            response = self.powerbi_client.make_powerbi_request(
                "POST",
                f"groups/{workspace_id}/datasets/{semantic_model.id}/refreshes",
                payload=payload,
            )
            refresh_id = None
            try:
                data = response.json()
                if isinstance(data, dict):
                    refresh_id = data.get("id") or data.get("requestId")
            except Exception:
                data = None

            return self._wait_for_refresh_completion(
                workspace_id,
                semantic_model.id,
                refresh_id=refresh_id,
            )
        except FabricAPIError:
            raise
        except Exception as exc:
            raise FabricError(f"Failed to refresh semantic model: {exc}")

    def execute_dax_query(
        self,
        workspace_name: str,
        query: str,
        semantic_model_name: Optional[str] = None,
        semantic_model_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Execute a DAX query via Power BI REST API and return raw response."""
        if not query or not query.strip():
            raise FabricValidationError("query", query or "", "Query cannot be empty")

        workspace_id = self.workspace_service.resolve_workspace_id(workspace_name)
        semantic_model = self._resolve_semantic_model(
            workspace_id, semantic_model_name, semantic_model_id
        )

        payload = {"queries": [{"query": query}]}
        try:
            response = self.powerbi_client.make_powerbi_request(
                "POST",
                f"groups/{workspace_id}/datasets/{semantic_model.id}/executeQueries",
                payload=payload,
            )
            return response.json()
        except FabricAPIError:
            raise
        except Exception as exc:
            raise FabricError(f"Failed to execute DAX query: {exc}")

    def _wait_for_refresh_completion(
        self,
        workspace_id: str,
        dataset_id: str,
        refresh_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        start_time = time.time()
        last_seen: Optional[Dict[str, Any]] = None

        while True:
            if time.time() - start_time > self.refresh_wait_timeout:
                raise FabricError(
                    f"Refresh did not complete within {self.refresh_wait_timeout} seconds"
                )

            response = self.powerbi_client.make_powerbi_request(
                "GET",
                f"groups/{workspace_id}/datasets/{dataset_id}/refreshes?$top=5",
            )
            data = response.json() if response is not None else {}
            entries = data.get("value", []) if isinstance(data, dict) else []
            if entries:
                if refresh_id:
                    match = next(
                        (
                            entry
                            for entry in entries
                            if entry.get("id") == refresh_id
                            or entry.get("requestId") == refresh_id
                        ),
                        None,
                    )
                else:
                    match = entries[0]

                if match:
                    last_seen = match
                    status = match.get("status") or match.get("refreshStatus")
                    if status in {"Completed", "Failed", "Cancelled"}:
                        return {
                            "refresh_id": match.get("id") or match.get("requestId"),
                            "status": "success" if status == "Completed" else "error",
                            "refresh_status": status,
                            "start_time": match.get("startTime"),
                            "end_time": match.get("endTime"),
                            "service_exception": match.get("serviceException"),
                            "details": match,
                        }

            time.sleep(self.refresh_poll_interval)

        # fallback
        return {
            "refresh_id": refresh_id,
            "status": "error",
            "refresh_status": "Unknown",
            "details": last_seen,
        }

    def _resolve_semantic_model(
        self,
        workspace_id: str,
        semantic_model_name: Optional[str],
        semantic_model_id: Optional[str],
    ):
        if semantic_model_id:
            semantic_model = self.item_service.get_item_by_id(
                workspace_id, semantic_model_id
            )
            if semantic_model.type != "SemanticModel":
                raise FabricValidationError(
                    "semantic_model_id",
                    semantic_model_id,
                    f"Item type '{semantic_model.type}' is not SemanticModel",
                )
            return semantic_model

        if not semantic_model_name or not semantic_model_name.strip():
            raise FabricValidationError(
                "semantic_model_name",
                semantic_model_name or "",
                "Semantic model name cannot be empty",
            )

        return self.item_service.get_item_by_name(
            workspace_id, semantic_model_name, "SemanticModel"
        )

