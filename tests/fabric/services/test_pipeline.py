"""Unit tests for FabricPipelineService."""

import base64
import json
from unittest.mock import Mock

import pytest

from ms_fabric_mcp_server.client.exceptions import (
    FabricAPIError,
    FabricError,
    FabricItemNotFoundError,
    FabricValidationError,
)
from ms_fabric_mcp_server.models.item import FabricItem
from ms_fabric_mcp_server.services.pipeline import FabricPipelineService


def _decode_payload(payload: str) -> dict:
    return json.loads(base64.b64decode(payload).decode("utf-8"))


@pytest.mark.unit
class TestFabricPipelineService:
    """Test suite for FabricPipelineService."""
    
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
    def pipeline_service(self, mock_client, mock_workspace_service, mock_item_service):
        """Create a FabricPipelineService instance with mocked dependencies."""
        return FabricPipelineService(
            mock_client,
            mock_workspace_service,
            mock_item_service
        )
    
    def test_validate_pipeline_inputs_success(self, pipeline_service):
        """Test successful validation of pipeline inputs."""
        # Should not raise any exception
        pipeline_service._validate_pipeline_inputs(
            pipeline_name="Test_Pipeline",
            source_type="AzurePostgreSqlSource",
            source_connection_id="conn-123",
            source_schema="public",
            source_table="movie",
            destination_lakehouse_id="lakehouse-456",
            destination_connection_id="dest-conn-789",
            destination_table="movie"
        )
    
    def test_validate_pipeline_inputs_empty_name(self, pipeline_service):
        """Test validation fails for empty pipeline name."""
        with pytest.raises(FabricValidationError) as exc_info:
            pipeline_service._validate_pipeline_inputs(
                pipeline_name="",
                source_type="AzurePostgreSqlSource",
                source_connection_id="conn-123",
                source_schema="public",
                source_table="movie",
                destination_lakehouse_id="lakehouse-456",
                destination_connection_id="dest-conn-789",
                destination_table="movie"
            )
        assert "pipeline_name" in str(exc_info.value)
    
    def test_validate_pipeline_inputs_empty_source_type(self, pipeline_service):
        """Test validation fails for empty source type."""
        with pytest.raises(FabricValidationError) as exc_info:
            pipeline_service._validate_pipeline_inputs(
                pipeline_name="Test_Pipeline",
                source_type="",
                source_connection_id="conn-123",
                source_schema="public",
                source_table="movie",
                destination_lakehouse_id="lakehouse-456",
                destination_connection_id="dest-conn-789",
                destination_table="movie"
            )
        assert "source_type" in str(exc_info.value)

    def test_validate_pipeline_inputs_empty_connection(self, pipeline_service):
        """Test validation fails for empty connection ID."""
        with pytest.raises(FabricValidationError) as exc_info:
            pipeline_service._validate_pipeline_inputs(
                pipeline_name="Test_Pipeline",
                source_type="AzurePostgreSqlSource",
                source_connection_id="",
                source_schema="public",
                source_table="movie",
                destination_lakehouse_id="lakehouse-456",
                destination_connection_id="dest-conn-789",
                destination_table="movie"
            )
        assert "source_connection_id" in str(exc_info.value)

    def test_validate_pipeline_inputs_empty_schema(self, pipeline_service):
        """Test validation fails for empty schema."""
        with pytest.raises(FabricValidationError) as exc_info:
            pipeline_service._validate_pipeline_inputs(
                pipeline_name="Test_Pipeline",
                source_type="AzurePostgreSqlSource",
                source_connection_id="conn-123",
                source_schema="",
                source_table="movie",
                destination_lakehouse_id="lakehouse-456",
                destination_connection_id="dest-conn-789",
                destination_table="movie"
            )
        assert "source_schema" in str(exc_info.value)

    def test_validate_pipeline_inputs_empty_table(self, pipeline_service):
        """Test validation fails for empty source table."""
        with pytest.raises(FabricValidationError) as exc_info:
            pipeline_service._validate_pipeline_inputs(
                pipeline_name="Test_Pipeline",
                source_type="AzurePostgreSqlSource",
                source_connection_id="conn-123",
                source_schema="public",
                source_table="",
                destination_lakehouse_id="lakehouse-456",
                destination_connection_id="dest-conn-789",
                destination_table="movie"
            )
        assert "source_table" in str(exc_info.value)

    def test_validate_pipeline_inputs_empty_destination_lakehouse(self, pipeline_service):
        """Test validation fails for empty destination lakehouse ID."""
        with pytest.raises(FabricValidationError) as exc_info:
            pipeline_service._validate_pipeline_inputs(
                pipeline_name="Test_Pipeline",
                source_type="AzurePostgreSqlSource",
                source_connection_id="conn-123",
                source_schema="public",
                source_table="movie",
                destination_lakehouse_id="",
                destination_connection_id="dest-conn-789",
                destination_table="movie"
            )
        assert "destination_lakehouse_id" in str(exc_info.value)

    def test_validate_pipeline_inputs_empty_destination_connection(self, pipeline_service):
        """Test validation fails for empty destination connection ID."""
        with pytest.raises(FabricValidationError) as exc_info:
            pipeline_service._validate_pipeline_inputs(
                pipeline_name="Test_Pipeline",
                source_type="AzurePostgreSqlSource",
                source_connection_id="conn-123",
                source_schema="public",
                source_table="movie",
                destination_lakehouse_id="lakehouse-456",
                destination_connection_id="",
                destination_table="movie"
            )
        assert "destination_connection_id" in str(exc_info.value)

    def test_validate_pipeline_inputs_empty_destination_table(self, pipeline_service):
        """Test validation fails for empty destination table."""
        with pytest.raises(FabricValidationError) as exc_info:
            pipeline_service._validate_pipeline_inputs(
                pipeline_name="Test_Pipeline",
                source_type="AzurePostgreSqlSource",
                source_connection_id="conn-123",
                source_schema="public",
                source_table="movie",
                destination_lakehouse_id="lakehouse-456",
                destination_connection_id="dest-conn-789",
                destination_table=""
            )
        assert "destination_table" in str(exc_info.value)
    
    def test_build_copy_activity_definition(self, pipeline_service):
        """Test building Copy Activity definition."""
        definition = pipeline_service._build_copy_activity_definition(
            workspace_id="workspace-123",
            source_type="AzurePostgreSqlSource",
            source_connection_id="conn-123",
            source_schema="public",
            source_table="movie",
            destination_lakehouse_id="lakehouse-456",
            destination_connection_id="dest-conn-789",
            destination_table="movie",
            table_action_option="Append",
            apply_v_order=True,
            timeout="01:00:00",
            retry=0,
            retry_interval_seconds=30
        )
        
        # Verify structure
        assert "properties" in definition
        assert "activities" in definition["properties"]
        assert len(definition["properties"]["activities"]) == 1
        
        # Verify activity details
        activity = definition["properties"]["activities"][0]
        assert activity["type"] == "Copy"
        
        # Verify source with datasetSettings
        assert activity["typeProperties"]["source"]["type"] == "AzurePostgreSqlSource"
        source_dataset = activity["typeProperties"]["source"]["datasetSettings"]
        assert source_dataset["typeProperties"]["schema"] == "public"
        assert source_dataset["typeProperties"]["table"] == "movie"
        assert source_dataset["externalReferences"]["connection"] == "conn-123"
        
        # Verify sink with datasetSettings
        assert activity["typeProperties"]["sink"]["type"] == "LakehouseTableSink"
        assert activity["typeProperties"]["sink"]["tableActionOption"] == "Append"
        sink_dataset = activity["typeProperties"]["sink"]["datasetSettings"]
        assert sink_dataset["type"] == "LakehouseTable"
        assert sink_dataset["typeProperties"]["table"] == "movie"

    def test_get_source_dataset_type_mapping(self, pipeline_service):
        """Known source types map to dataset types."""
        assert pipeline_service._get_source_dataset_type("AzurePostgreSqlSource") == "AzurePostgreSqlTable"
        assert pipeline_service._get_source_dataset_type("AzureSqlSource") == "AzureSqlTable"

    def test_get_source_dataset_type_derivation(self, pipeline_service):
        """Source types ending with Source derive Table suffix."""
        assert pipeline_service._get_source_dataset_type("CustomSource") == "CustomTable"

    def test_get_source_dataset_type_invalid(self, pipeline_service):
        """Unsupported source types fall back to input value."""
        assert pipeline_service._get_source_dataset_type("UnsupportedType") == "UnsupportedType"

    def test_encode_definition(self, pipeline_service):
        """Test encoding pipeline definition to Base64."""
        test_definition = {
            "properties": {
                "activities": [],
                "parameters": {}
            }
        }
        
        encoded = pipeline_service._encode_definition(test_definition)
        
        # Verify it's a valid base64 string
        assert isinstance(encoded, str)
        
        # Verify we can decode it back
        decoded_bytes = base64.b64decode(encoded)
        decoded_str = decoded_bytes.decode('utf-8')
        decoded_obj = json.loads(decoded_str)
        
        assert decoded_obj == test_definition

    def test_decode_definition_round_trip(self, pipeline_service):
        """Encode/decode round trip returns original definition."""
        test_definition = {"properties": {"activities": [{"name": "A1"}]}}

        encoded = pipeline_service._encode_definition(test_definition)
        decoded = pipeline_service._decode_definition(encoded)

        assert decoded == test_definition

    def test_decode_definition_invalid_payload(self, pipeline_service):
        """Invalid payload raises FabricError."""
        with pytest.raises(FabricError):
            pipeline_service._decode_definition("not-base64")

    def test_create_blank_pipeline_success(self, pipeline_service, mock_item_service):
        """Create blank pipeline builds definition and returns ID."""
        mock_item_service.create_item.return_value = FabricItem(
            id="pipe-1",
            display_name="BlankPipe",
            type="DataPipeline",
            workspace_id="ws-1",
        )

        pipeline_id = pipeline_service.create_blank_pipeline(
            workspace_id="ws-1",
            pipeline_name="BlankPipe",
            description="desc",
        )

        assert pipeline_id == "pipe-1"
        args, kwargs = mock_item_service.create_item.call_args
        item_definition = args[1]
        assert item_definition["displayName"] == "BlankPipe"
        assert item_definition["type"] == "DataPipeline"
        assert item_definition["description"] == "desc"
        payload = item_definition["definition"]["parts"][0]["payload"]
        decoded = _decode_payload(payload)
        assert decoded["properties"]["activities"] == []

    def test_create_blank_pipeline_validation_error(self, pipeline_service):
        """Empty pipeline name raises validation error."""
        with pytest.raises(FabricValidationError):
            pipeline_service.create_blank_pipeline("ws-1", " ")

    def test_create_blank_pipeline_api_error(self, pipeline_service, mock_item_service):
        """API errors propagate."""
        mock_item_service.create_item.side_effect = FabricAPIError(500, "boom")

        with pytest.raises(FabricAPIError):
            pipeline_service.create_blank_pipeline("ws-1", "Pipe")

    def test_create_blank_pipeline_unexpected_error(self, pipeline_service, mock_item_service):
        """Unexpected errors raise FabricError."""
        mock_item_service.create_item.side_effect = RuntimeError("boom")

        with pytest.raises(FabricError):
            pipeline_service.create_blank_pipeline("ws-1", "Pipe")

    def test_add_copy_activity_to_pipeline_success(self, pipeline_service, mock_item_service, mock_client):
        """Adds copy activity and updates definition."""
        mock_item_service.get_item_by_name.return_value = FabricItem(
            id="pipe-1",
            display_name="Pipe",
            type="DataPipeline",
            workspace_id="ws-1",
        )
        base_definition = {"properties": {"activities": []}}
        encoded = pipeline_service._encode_definition(base_definition)
        mock_item_service.get_item_definition.return_value = {
            "definition": {"parts": [{"path": "pipeline-content.json", "payload": encoded}]}
        }

        pipeline_id = pipeline_service.add_copy_activity_to_pipeline(
            workspace_id="ws-1",
            pipeline_name="Pipe",
            source_type="AzurePostgreSqlSource",
            source_connection_id="conn-123",
            source_schema="public",
            source_table="movie",
            destination_lakehouse_id="lh-1",
            destination_connection_id="dest-conn",
            destination_table="movie",
            activity_name="CopyMovieData",
        )

        assert pipeline_id == "pipe-1"
        _, kwargs = mock_client.make_api_request.call_args
        update_payload = kwargs["payload"]
        payload = update_payload["definition"]["parts"][0]["payload"]
        updated = _decode_payload(payload)
        assert updated["properties"]["activities"][-1]["name"] == "CopyMovieData"

    def test_add_copy_activity_to_pipeline_missing_part(self, pipeline_service, mock_item_service):
        """Missing pipeline-content.json raises FabricError."""
        mock_item_service.get_item_by_name.return_value = FabricItem(
            id="pipe-1",
            display_name="Pipe",
            type="DataPipeline",
            workspace_id="ws-1",
        )
        mock_item_service.get_item_definition.return_value = {"definition": {"parts": []}}

        with pytest.raises(FabricError):
            pipeline_service.add_copy_activity_to_pipeline(
                workspace_id="ws-1",
                pipeline_name="Pipe",
                source_type="AzurePostgreSqlSource",
                source_connection_id="conn-123",
                source_schema="public",
                source_table="movie",
                destination_lakehouse_id="lh-1",
                destination_connection_id="dest-conn",
                destination_table="movie",
            )

    def test_add_copy_activity_to_pipeline_api_error(self, pipeline_service, mock_item_service, mock_client):
        """API errors propagate."""
        mock_item_service.get_item_by_name.return_value = FabricItem(
            id="pipe-1",
            display_name="Pipe",
            type="DataPipeline",
            workspace_id="ws-1",
        )
        base_definition = {"properties": {"activities": []}}
        encoded = pipeline_service._encode_definition(base_definition)
        mock_item_service.get_item_definition.return_value = {
            "definition": {"parts": [{"path": "pipeline-content.json", "payload": encoded}]}
        }
        mock_client.make_api_request.side_effect = FabricAPIError(500, "boom")

        with pytest.raises(FabricAPIError):
            pipeline_service.add_copy_activity_to_pipeline(
                workspace_id="ws-1",
                pipeline_name="Pipe",
                source_type="AzurePostgreSqlSource",
                source_connection_id="conn-123",
                source_schema="public",
                source_table="movie",
                destination_lakehouse_id="lh-1",
                destination_connection_id="dest-conn",
                destination_table="movie",
            )

    def test_add_activity_from_json_success(self, pipeline_service, mock_item_service, mock_client):
        """Adds generic activity and updates definition."""
        mock_item_service.get_item_by_name.return_value = FabricItem(
            id="pipe-1",
            display_name="Pipe",
            type="DataPipeline",
            workspace_id="ws-1",
        )
        base_definition = {"properties": {"activities": []}}
        encoded = pipeline_service._encode_definition(base_definition)
        mock_item_service.get_item_definition.return_value = {
            "definition": {"parts": [{"path": "pipeline-content.json", "payload": encoded}]}
        }
        activity = {"name": "MyActivity", "type": "Custom", "dependsOn": []}

        pipeline_id = pipeline_service.add_activity_from_json("ws-1", "Pipe", activity)

        assert pipeline_id == "pipe-1"
        _, kwargs = mock_client.make_api_request.call_args
        update_payload = kwargs["payload"]
        payload = update_payload["definition"]["parts"][0]["payload"]
        updated = _decode_payload(payload)
        assert updated["properties"]["activities"][-1]["name"] == "MyActivity"

    def test_add_activity_from_json_validation(self, pipeline_service):
        """Missing name/type in activity_json raises validation error."""
        with pytest.raises(FabricValidationError):
            pipeline_service.add_activity_from_json("ws-1", "Pipe", "not-a-dict")
        with pytest.raises(FabricValidationError):
            pipeline_service.add_activity_from_json("ws-1", "Pipe", {"type": "Copy"})
        with pytest.raises(FabricValidationError):
            pipeline_service.add_activity_from_json("ws-1", "Pipe", {"name": "A1"})

    def test_add_activity_from_json_item_not_found(self, pipeline_service, mock_item_service):
        """Item not found errors propagate."""
        mock_item_service.get_item_by_name.side_effect = FabricItemNotFoundError(
            "DataPipeline",
            "Pipe",
            "ws-1",
        )

        with pytest.raises(FabricItemNotFoundError):
            pipeline_service.add_activity_from_json("ws-1", "Pipe", {"name": "A1", "type": "Copy"})
