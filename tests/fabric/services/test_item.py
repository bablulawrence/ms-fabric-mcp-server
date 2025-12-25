"""Unit tests for FabricItemService."""

from unittest.mock import Mock, patch

import pytest

from ms_fabric_mcp_server.client.exceptions import (
    FabricAPIError,
    FabricError,
    FabricItemNotFoundError,
    FabricValidationError,
)
from ms_fabric_mcp_server.models.item import FabricItem
from ms_fabric_mcp_server.models.lakehouse import FabricLakehouse
from tests.fixtures.mocks import FabricDataFactory, MockResponseFactory


@pytest.fixture
def item_service(mock_fabric_client):
    from ms_fabric_mcp_server.services.item import FabricItemService

    return FabricItemService(mock_fabric_client)


@pytest.mark.unit
class TestFabricItemService:
    """Test suite for FabricItemService."""

    def test_validate_item_type_success(self, item_service):
        """Known item types are accepted."""
        item_service._validate_item_type("Notebook")

    def test_validate_item_type_invalid(self, item_service):
        """Unknown item types raise validation error."""
        with pytest.raises(FabricValidationError):
            item_service._validate_item_type("NotAType")

    def test_list_items_success(self, item_service, mock_fabric_client):
        """List items returns FabricItem objects."""
        items_data = FabricDataFactory.item_list(2, "Notebook")
        mock_fabric_client.make_api_request.return_value = MockResponseFactory.success(items_data)

        items = item_service.list_items("ws-1", "Notebook")

        assert len(items) == 2
        assert all(isinstance(item, FabricItem) for item in items)
        mock_fabric_client.make_api_request.assert_called_once_with(
            "GET", "workspaces/ws-1/items?type=Notebook"
        )

    def test_list_items_without_type(self, item_service, mock_fabric_client):
        """List items without type omits query filter."""
        items_data = FabricDataFactory.item_list(1, "Notebook")
        mock_fabric_client.make_api_request.return_value = MockResponseFactory.success(items_data)

        items = item_service.list_items("ws-1")

        assert len(items) == 1
        mock_fabric_client.make_api_request.assert_called_once_with(
            "GET", "workspaces/ws-1/items"
        )

    def test_list_items_invalid_type(self, item_service):
        """Invalid type filter raises validation error."""
        with pytest.raises(FabricValidationError):
            item_service.list_items("ws-1", "NotAType")

    def test_get_item_by_name_success(self, item_service, mock_fabric_client):
        """Get item by name returns matching item."""
        items_data = FabricDataFactory.item_list(2, "Notebook")
        items_data["value"][1]["displayName"] = "Target"
        mock_fabric_client.make_api_request.return_value = MockResponseFactory.success(items_data)

        item = item_service.get_item_by_name("ws-1", "Target", "Notebook")

        assert item.display_name == "Target"

    def test_get_item_by_name_not_found(self, item_service, mock_fabric_client):
        """Missing item raises FabricItemNotFoundError."""
        items_data = FabricDataFactory.item_list(2, "Notebook")
        mock_fabric_client.make_api_request.return_value = MockResponseFactory.success(items_data)

        with pytest.raises(FabricItemNotFoundError):
            item_service.get_item_by_name("ws-1", "Missing", "Notebook")

    def test_get_item_by_id_success(self, item_service, mock_fabric_client):
        """Get item by ID maps response to FabricItem."""
        item_data = FabricDataFactory.item(item_id="item-1", item_type="Notebook")
        mock_fabric_client.make_api_request.return_value = MockResponseFactory.success(item_data)

        item = item_service.get_item_by_id("ws-1", "item-1")

        assert item.id == "item-1"
        assert item.type == "Notebook"

    def test_get_item_by_id_not_found(self, item_service, mock_fabric_client):
        """404 response raises FabricItemNotFoundError."""
        mock_fabric_client.make_api_request.side_effect = FabricAPIError(404, "missing")

        with pytest.raises(FabricItemNotFoundError):
            item_service.get_item_by_id("ws-1", "missing")

    def test_get_item_by_id_unexpected_error(self, item_service, mock_fabric_client):
        """Unexpected errors raise FabricError."""
        mock_fabric_client.make_api_request.side_effect = RuntimeError("boom")

        with pytest.raises(FabricError):
            item_service.get_item_by_id("ws-1", "item-1")

    def test_get_item_definition_success(self, item_service, mock_fabric_client):
        """Definition response is returned."""
        definition = {"definition": {"parts": []}}
        mock_fabric_client.make_api_request.return_value = MockResponseFactory.success(definition)

        result = item_service.get_item_definition("ws-1", "item-1")

        assert result == definition

    def test_get_item_definition_unexpected_error(self, item_service, mock_fabric_client):
        """Unexpected errors raise FabricError."""
        mock_fabric_client.make_api_request.side_effect = RuntimeError("boom")

        with pytest.raises(FabricError):
            item_service.get_item_definition("ws-1", "item-1")

    def test_get_item_definition_api_error_propagates(self, item_service, mock_fabric_client):
        """API errors are re-raised."""
        mock_fabric_client.make_api_request.side_effect = FabricAPIError(500, "boom")

        with pytest.raises(FabricAPIError):
            item_service.get_item_definition("ws-1", "item-1")

    def test_create_item_validation_missing_field(self, item_service):
        """Missing required fields raise validation error."""
        with pytest.raises(FabricValidationError):
            item_service.create_item("ws-1", {"type": "Notebook"})

    def test_create_item_validation_missing_type(self, item_service):
        """Missing type raises validation error."""
        with pytest.raises(FabricValidationError):
            item_service.create_item("ws-1", {"displayName": "Item"})

    def test_create_item_success_201(self, item_service, mock_fabric_client):
        """Create item maps 201 response."""
        item_data = FabricDataFactory.item(item_id="item-1", item_type="Notebook")
        response = MockResponseFactory.success(item_data, status_code=201)
        mock_fabric_client.make_api_request.return_value = response

        item = item_service.create_item("ws-1", {"displayName": "Item", "type": "Notebook"})

        assert item.id == "item-1"
        assert item.type == "Notebook"

    def test_create_item_accepted_202_fetches_item(self, item_service, mock_fabric_client):
        """Async create with id fetches item by ID."""
        response = MockResponseFactory.success({"id": "item-1"}, status_code=202)
        mock_fabric_client.make_api_request.return_value = response
        item_service.get_item_by_id = Mock(
            return_value=FabricItem(
                id="item-1",
                display_name="Item",
                type="Notebook",
                workspace_id="ws-1",
            )
        )

        with patch("time.sleep", return_value=None):
            item = item_service.create_item("ws-1", {"displayName": "Item", "type": "Notebook"})

        assert item.id == "item-1"
        item_service.get_item_by_id.assert_called_once_with("ws-1", "item-1")

    def test_create_item_accepted_202_without_id_returns_unknown(self, item_service, mock_fabric_client):
        """Async create without id returns placeholder item."""
        response = MockResponseFactory.success({}, status_code=202)
        mock_fabric_client.make_api_request.return_value = response
        item_service.get_item_by_id = Mock()

        with patch("time.sleep", return_value=None):
            item = item_service.create_item(
                "ws-1",
                {"displayName": "Item", "type": "Notebook", "description": "desc"},
            )

        assert item.id == "unknown"
        assert item.display_name == "Item"
        assert item.type == "Notebook"
        assert item.workspace_id == "ws-1"
        assert item.description == "desc"
        item_service.get_item_by_id.assert_not_called()

    def test_create_item_unexpected_status_raises(self, item_service, mock_fabric_client):
        """Unexpected status codes raise FabricAPIError."""
        response = MockResponseFactory.success({}, status_code=500)
        mock_fabric_client.make_api_request.return_value = response

        with pytest.raises(FabricAPIError):
            item_service.create_item("ws-1", {"displayName": "Item", "type": "Notebook"})

    def test_update_item_success(self, item_service, mock_fabric_client):
        """Update item maps response."""
        item_data = FabricDataFactory.item(item_id="item-1", item_type="Notebook")
        mock_fabric_client.make_api_request.return_value = MockResponseFactory.success(item_data)

        item = item_service.update_item("ws-1", "item-1", {"displayName": "Updated"})

        assert item.id == "item-1"
        assert item.display_name == item_data["displayName"]
        mock_fabric_client.make_api_request.assert_called_once_with(
            "PATCH",
            "workspaces/ws-1/items/item-1",
            payload={"displayName": "Updated"},
        )

    def test_update_item_api_error(self, item_service, mock_fabric_client):
        """API errors propagate."""
        mock_fabric_client.make_api_request.side_effect = FabricAPIError(500, "boom")

        with pytest.raises(FabricAPIError):
            item_service.update_item("ws-1", "item-1", {"displayName": "Updated"})

    def test_update_item_unexpected_error(self, item_service, mock_fabric_client):
        """Unexpected errors raise FabricError."""
        mock_fabric_client.make_api_request.side_effect = RuntimeError("boom")

        with pytest.raises(FabricError):
            item_service.update_item("ws-1", "item-1", {"displayName": "Updated"})

    def test_delete_item_success(self, item_service, mock_fabric_client):
        """Delete item calls endpoint."""
        mock_fabric_client.make_api_request.return_value = MockResponseFactory.success({})

        item_service.delete_item("ws-1", "item-1")

        mock_fabric_client.make_api_request.assert_called_once_with(
            "DELETE", "workspaces/ws-1/items/item-1"
        )

    def test_delete_item_api_error(self, item_service, mock_fabric_client):
        """API errors propagate."""
        mock_fabric_client.make_api_request.side_effect = FabricAPIError(500, "boom")

        with pytest.raises(FabricAPIError):
            item_service.delete_item("ws-1", "item-1")

    def test_delete_item_unexpected_error(self, item_service, mock_fabric_client):
        """Unexpected errors raise FabricError."""
        mock_fabric_client.make_api_request.side_effect = RuntimeError("boom")

        with pytest.raises(FabricError):
            item_service.delete_item("ws-1", "item-1")

    def test_create_lakehouse_success(self, item_service, mock_fabric_client):
        """create_lakehouse maps response to FabricLakehouse."""
        response_data = {
            "id": "lh-1",
            "displayName": "Lakehouse",
            "description": "desc",
        }
        mock_fabric_client.make_api_request.return_value = MockResponseFactory.success(response_data)

        lakehouse = item_service.create_lakehouse(
            workspace_id="ws-1",
            display_name="Lakehouse",
            description="desc",
            enable_schemas=False,
        )

        assert isinstance(lakehouse, FabricLakehouse)
        assert lakehouse.id == "lh-1"
        call_kwargs = mock_fabric_client.make_api_request.call_args.kwargs
        payload = call_kwargs["payload"]
        assert payload["creationPayload"]["enableSchemas"] is False
