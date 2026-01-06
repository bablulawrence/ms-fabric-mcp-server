"""Unit tests for FabricPowerBIService."""

from unittest.mock import Mock

import pytest

from ms_fabric_mcp_server.client.exceptions import FabricValidationError
from ms_fabric_mcp_server.models.item import FabricItem
from ms_fabric_mcp_server.services.powerbi import FabricPowerBIService


def _make_response(payload):
    response = Mock()
    response.json.return_value = payload
    return response


@pytest.mark.unit
class TestFabricPowerBIService:
    def test_refresh_semantic_model_success(self):
        powerbi_client = Mock()
        workspace_service = Mock()
        item_service = Mock()

        workspace_service.resolve_workspace_id.return_value = "ws-1"
        item_service.get_item_by_name.return_value = FabricItem(
            id="sm-1",
            display_name="Model",
            type="SemanticModel",
            workspace_id="ws-1",
        )

        powerbi_client.make_powerbi_request.side_effect = [
            _make_response({}),
            _make_response(
                {
                    "value": [
                        {
                            "id": "r-1",
                            "status": "Completed",
                            "startTime": "2024-01-01T00:00:00Z",
                            "endTime": "2024-01-01T00:01:00Z",
                        }
                    ]
                }
            ),
        ]

        service = FabricPowerBIService(
            powerbi_client,
            workspace_service,
            item_service,
            refresh_poll_interval=0,
            refresh_wait_timeout=10,
        )

        result = service.refresh_semantic_model(
            workspace_name="Workspace",
            semantic_model_name="Model",
            refresh_type="full",
            objects=None,
        )

        assert result["status"] == "success"
        assert result["refresh_status"] == "Completed"

    def test_execute_dax_query_success(self):
        powerbi_client = Mock()
        workspace_service = Mock()
        item_service = Mock()

        workspace_service.resolve_workspace_id.return_value = "ws-1"
        item_service.get_item_by_name.return_value = FabricItem(
            id="sm-1",
            display_name="Model",
            type="SemanticModel",
            workspace_id="ws-1",
        )
        powerbi_client.make_powerbi_request.return_value = _make_response(
            {"results": []}
        )

        service = FabricPowerBIService(
            powerbi_client,
            workspace_service,
            item_service,
            refresh_poll_interval=0,
            refresh_wait_timeout=10,
        )

        result = service.execute_dax_query(
            workspace_name="Workspace",
            semantic_model_name="Model",
            query="EVALUATE Sales",
        )

        assert result == {"results": []}

    def test_execute_dax_query_empty_raises(self):
        powerbi_client = Mock()
        workspace_service = Mock()
        item_service = Mock()

        service = FabricPowerBIService(
            powerbi_client,
            workspace_service,
            item_service,
        )

        with pytest.raises(FabricValidationError):
            service.execute_dax_query(
                workspace_name="Workspace",
                semantic_model_name="Model",
                query="",
            )
