"""Unit tests for FabricDataflowService."""

import base64
import json
from unittest.mock import Mock

import pytest

from ms_fabric_mcp_server.client.exceptions import (
    FabricAPIError,
    FabricItemNotFoundError,
    FabricValidationError,
)
from ms_fabric_mcp_server.services.dataflow import FabricDataflowService


def _encode_payload(content: str) -> str:
    """Base64 encode content for API payloads."""
    return base64.b64encode(content.encode("utf-8")).decode("utf-8")


def _decode_payload(payload: str) -> str:
    """Decode base64 payload."""
    return base64.b64decode(payload).decode("utf-8")


def _mock_response(data: dict, headers: dict = None) -> Mock:
    """Create a mock response object."""
    response = Mock()
    response.json.return_value = data
    response.headers = headers or {}
    response.status_code = 200
    return response


@pytest.mark.unit
class TestFabricDataflowService:
    """Test suite for FabricDataflowService."""

    @pytest.fixture
    def mock_client(self):
        """Create a mock FabricClient."""
        return Mock()

    @pytest.fixture
    def mock_workspace_service(self):
        """Create a mock FabricWorkspaceService."""
        return Mock()

    @pytest.fixture
    def mock_item_service(self):
        """Create a mock FabricItemService."""
        return Mock()

    @pytest.fixture
    def dataflow_service(self, mock_client, mock_workspace_service, mock_item_service):
        """Create a FabricDataflowService instance with mocked dependencies."""
        return FabricDataflowService(
            mock_client, mock_workspace_service, mock_item_service
        )

    # --- Query name parsing tests ---

    def test_parse_query_names_single_query(self, dataflow_service):
        """Test parsing a single shared query."""
        mashup = """
section Section1;
shared MyQuery = let
    Source = Table.FromRecords({})
in
    Source;
"""
        names = dataflow_service._parse_query_names_from_mashup(mashup)
        assert names == ["MyQuery"]

    def test_parse_query_names_multiple_queries(self, dataflow_service):
        """Test parsing multiple shared queries."""
        mashup = """
section Section1;
shared Query1 = let
    Source = 1
in
    Source;
shared Query2 = let
    Source = 2
in
    Source;
shared Query3 = 42;
"""
        names = dataflow_service._parse_query_names_from_mashup(mashup)
        assert names == ["Query1", "Query2", "Query3"]

    def test_parse_query_names_empty(self, dataflow_service):
        """Test parsing mashup with no shared queries."""
        mashup = "section Section1;"
        names = dataflow_service._parse_query_names_from_mashup(mashup)
        assert names == []

    # --- Query metadata generation tests ---

    def test_generate_query_metadata_basic(self, dataflow_service):
        """Test generating query metadata from mashup content."""
        mashup = """
section Section1;
shared Customers = let Source = 1 in Source;
"""
        metadata = dataflow_service._generate_query_metadata("TestDataflow", mashup)

        assert metadata["name"] == "TestDataflow"
        assert metadata["formatVersion"] == "202502"
        assert "Customers" in metadata["queriesMetadata"]
        assert metadata["queriesMetadata"]["Customers"]["queryName"] == "Customers"
        assert metadata["queriesMetadata"]["Customers"]["loadEnabled"] is True
        assert metadata["queriesMetadata"]["Customers"]["isHidden"] is False

    # --- Validation tests ---

    def test_validate_empty_name(self, dataflow_service):
        """Test validation fails for empty dataflow name."""
        with pytest.raises(FabricValidationError) as exc_info:
            dataflow_service._validate_dataflow_inputs(
                dataflow_name="",
                mashup_content="section Section1;",
            )
        assert "dataflow_name" in str(exc_info.value)

    def test_validate_empty_mashup(self, dataflow_service):
        """Test validation fails for empty mashup content."""
        with pytest.raises(FabricValidationError) as exc_info:
            dataflow_service._validate_dataflow_inputs(
                dataflow_name="TestDataflow",
                mashup_content="",
            )
        assert "mashup_content" in str(exc_info.value)

    def test_validate_missing_section(self, dataflow_service):
        """Test validation fails when mashup lacks section declaration."""
        with pytest.raises(FabricValidationError) as exc_info:
            dataflow_service._validate_dataflow_inputs(
                dataflow_name="TestDataflow",
                mashup_content="shared Query = 1;",
            )
        assert "section" in str(exc_info.value).lower()

    def test_validate_success(self, dataflow_service):
        """Test validation passes for valid inputs."""
        # Should not raise
        dataflow_service._validate_dataflow_inputs(
            dataflow_name="TestDataflow",
            mashup_content="section Section1; shared Query = 1;",
        )

    @pytest.mark.parametrize(
        "payload, expected",
        [
            ({"message": "top-level"}, "top-level"),
            ({"error": {"message": "nested"}}, "nested"),
            ({"failureReason": {"message": "failed"}}, "failed"),
            ({"moreDetails": [{"message": "detail"}]}, "detail"),
            ({"moreDetails": [{"detail": {"value": "detail-value"}}]}, "detail-value"),
            (
                {"pbi.error": {"details": [{"code": "DetailsMessage", "detail": {"value": "pbi"}}]}},
                "pbi",
            ),
            (
                {
                    "error": {
                        "pbi.error": {
                            "details": [{"code": "DetailsMessage", "detail": {"value": "pbi-nested"}}]
                        }
                    }
                },
                "pbi-nested",
            ),
        ],
    )
    def test_extract_dataflow_error_shapes(self, dataflow_service, payload, expected):
        """Extracts error message from multiple payload shapes."""
        assert dataflow_service._extract_dataflow_error(payload) == expected

    # --- Create dataflow tests ---

    def test_create_dataflow_success(self, dataflow_service, mock_client):
        """Test successful dataflow creation."""
        mock_client.make_api_request.return_value = _mock_response({"id": "df-123"})

        mashup = """
section Section1;
shared Customers = let Source = 1 in Source;
"""
        result = dataflow_service.create_dataflow(
            workspace_id="ws-123",
            dataflow_name="CustomerETL",
            mashup_content=mashup,
        )

        assert result == "df-123"
        mock_client.make_api_request.assert_called_once()

        # Verify payload structure
        call_args = mock_client.make_api_request.call_args
        assert call_args[0][0] == "POST"
        assert call_args[0][1] == "workspaces/ws-123/dataflows"
        payload = call_args[1]["payload"]
        assert payload["displayName"] == "CustomerETL"
        assert payload["type"] == "Dataflow"
        assert len(payload["definition"]["parts"]) == 2

    def test_create_dataflow_with_custom_metadata(self, dataflow_service, mock_client):
        """Test dataflow creation with custom query metadata."""
        mock_client.make_api_request.return_value = _mock_response({"id": "df-456"})

        custom_metadata = {
            "formatVersion": "202502",
            "name": "CustomDataflow",
            "queriesMetadata": {
                "MyQuery": {
                    "queryId": "custom-uuid",
                    "queryName": "MyQuery",
                    "loadEnabled": True,
                    "isHidden": False,
                }
            },
        }

        result = dataflow_service.create_dataflow(
            workspace_id="ws-123",
            dataflow_name="CustomDataflow",
            mashup_content="section Section1; shared MyQuery = 1;",
            query_metadata=custom_metadata,
        )

        assert result == "df-456"

        # Verify custom metadata was used
        call_args = mock_client.make_api_request.call_args
        payload = call_args[1]["payload"]
        parts = payload["definition"]["parts"]
        metadata_part = next(p for p in parts if p["path"] == "queryMetadata.json")
        decoded = json.loads(_decode_payload(metadata_part["payload"]))
        assert decoded["name"] == "CustomDataflow"
        assert decoded["queriesMetadata"]["MyQuery"]["queryId"] == "custom-uuid"

    def test_create_dataflow_with_folder(self, dataflow_service, mock_client):
        """Test dataflow creation in a folder."""
        mock_client.make_api_request.return_value = _mock_response({"id": "df-789"})

        dataflow_service.create_dataflow(
            workspace_id="ws-123",
            dataflow_name="TestDataflow",
            mashup_content="section Section1; shared Q = 1;",
            folder_id="folder-abc",
        )

        call_args = mock_client.make_api_request.call_args
        payload = call_args[1]["payload"]
        assert payload["folderId"] == "folder-abc"

    def test_create_dataflow_with_description(self, dataflow_service, mock_client):
        """Test dataflow creation with description."""
        mock_client.make_api_request.return_value = _mock_response({"id": "df-desc"})

        dataflow_service.create_dataflow(
            workspace_id="ws-123",
            dataflow_name="TestDataflow",
            mashup_content="section Section1; shared Q = 1;",
            description="My dataflow description",
        )

        call_args = mock_client.make_api_request.call_args
        payload = call_args[1]["payload"]
        assert payload["description"] == "My dataflow description"

    # --- Get definition tests ---

    def test_get_dataflow_definition_success(self, dataflow_service, mock_client):
        """Test successful dataflow definition retrieval."""
        mashup = "section Section1;\nshared Query = 1;"
        metadata = {"name": "TestDF", "queriesMetadata": {}}

        mock_client.make_api_request.return_value = _mock_response({
            "definition": {
                "parts": [
                    {
                        "path": "mashup.pq",
                        "payload": _encode_payload(mashup),
                    },
                    {
                        "path": "queryMetadata.json",
                        "payload": _encode_payload(json.dumps(metadata)),
                    },
                ]
            }
        })

        result_metadata, result_mashup = dataflow_service.get_dataflow_definition(
            workspace_id="ws-123",
            dataflow_id="df-123",
        )

        assert result_mashup == mashup
        assert result_metadata == metadata
        mock_client.make_api_request.assert_called_once_with(
            "POST",
            "workspaces/ws-123/dataflows/df-123/getDefinition",
            wait_for_lro=True,
        )

    def test_get_dataflow_definition_not_found(self, dataflow_service, mock_client):
        """Test definition retrieval when dataflow not found."""
        mock_client.make_api_request.side_effect = FabricAPIError(
            status_code=404, message="Not found"
        )

        with pytest.raises(FabricItemNotFoundError):
            dataflow_service.get_dataflow_definition(
                workspace_id="ws-123",
                dataflow_id="df-missing",
            )

    # --- Run dataflow tests ---

    def test_run_dataflow_success(self, dataflow_service, mock_client):
        """Test successful dataflow refresh trigger."""
        mock_client.make_api_request.return_value = _mock_response(
            {},
            headers={
                "Location": "https://api.fabric.microsoft.com/v1/workspaces/ws-123/items/df-123/jobs/instances/job-abc",
                "Retry-After": "60",
            },
        )

        result = dataflow_service.run_dataflow(
            workspace_id="ws-123",
            dataflow_id="df-123",
        )

        assert result.status == "success"
        assert result.job_instance_id == "job-abc"
        assert "job-abc" in result.location_url
        assert result.retry_after == 60

    def test_run_dataflow_with_params(self, dataflow_service, mock_client):
        """Test dataflow refresh with execution data."""
        mock_client.make_api_request.return_value = _mock_response(
            {},
            headers={
                "Location": "https://example.com/jobs/instances/job-xyz",
                "Retry-After": "30",
            },
        )

        execution_data = {"param1": "value1"}
        result = dataflow_service.run_dataflow(
            workspace_id="ws-123",
            dataflow_id="df-123",
            execution_data=execution_data,
        )

        assert result.status == "success"
        call_args = mock_client.make_api_request.call_args
        assert call_args[1]["payload"]["executionData"] == execution_data

    def test_run_dataflow_not_found(self, dataflow_service, mock_client):
        """Test run when dataflow not found."""
        mock_client.make_api_request.side_effect = FabricAPIError(
            status_code=404, message="Not found"
        )

        with pytest.raises(FabricItemNotFoundError):
            dataflow_service.run_dataflow(
                workspace_id="ws-123",
                dataflow_id="df-missing",
            )

    def test_run_dataflow_extracts_error_message(self, dataflow_service, mock_client):
        """Uses response payload to surface detailed errors."""
        error_payload = {"moreDetails": [{"message": "Something went wrong"}]}
        mock_client.make_api_request.side_effect = FabricAPIError(
            status_code=400,
            message="Bad Request",
            response_body=json.dumps(error_payload),
        )

        with pytest.raises(FabricAPIError) as exc_info:
            dataflow_service.run_dataflow(
                workspace_id="ws-123",
                dataflow_id="df-123",
            )

        assert "Something went wrong" in str(exc_info.value)
