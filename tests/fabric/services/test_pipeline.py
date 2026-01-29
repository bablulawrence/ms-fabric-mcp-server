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

    def test_build_copy_activity_definition_lakehouse_omits_schema(self, pipeline_service):
        """LakehouseTableSource should omit schema in source dataset settings."""
        definition = pipeline_service._build_copy_activity_definition(
            workspace_id="workspace-123",
            source_type="LakehouseTableSource",
            source_connection_id="conn-123",
            source_schema="dbo",
            source_table="fact_sale",
            destination_lakehouse_id="lakehouse-456",
            destination_connection_id="dest-conn-789",
            destination_table="fact_sale_copy",
            table_action_option="Append",
            apply_v_order=True,
            timeout="01:00:00",
            retry=0,
            retry_interval_seconds=30,
        )

        activity = definition["properties"]["activities"][0]
        source_type_properties = activity["typeProperties"]["source"]["datasetSettings"]["typeProperties"]
        assert "schema" not in source_type_properties
        assert source_type_properties["table"] == "fact_sale"

    def test_build_copy_activity_definition_sql_mode(self, pipeline_service):
        """SQL mode should emit AzureSqlSource and AzureSqlTable with optional query."""
        definition = pipeline_service._build_copy_activity_definition(
            workspace_id="workspace-123",
            source_type="LakehouseTableSource",
            source_connection_id="conn-123",
            source_schema="dbo",
            source_table="fact_sale",
            destination_lakehouse_id="lakehouse-456",
            destination_connection_id="dest-conn-789",
            destination_table="fact_sale_copy",
            table_action_option="Append",
            apply_v_order=True,
            timeout="01:00:00",
            retry=0,
            retry_interval_seconds=30,
            source_access_mode="sql",
            source_sql_query="SELECT 1",
        )

        activity = definition["properties"]["activities"][0]
        source = activity["typeProperties"]["source"]
        assert source["type"] == "AzureSqlSource"
        assert source["datasetSettings"]["type"] == "AzureSqlTable"
        assert source["sqlReaderQuery"] == "SELECT 1"

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

    def test_validate_source_access_mode_invalid(self, pipeline_service):
        """Invalid source access mode raises validation error."""
        with pytest.raises(FabricValidationError):
            pipeline_service._validate_source_access_mode("bad", None)

    def test_validate_source_access_mode_query_requires_sql(self, pipeline_service):
        """SQL query requires sql access mode."""
        with pytest.raises(FabricValidationError):
            pipeline_service._validate_source_access_mode("direct", "SELECT 1")

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

    def test_create_blank_pipeline_with_folder_path(self, pipeline_service, mock_item_service):
        """Folder path resolves to folderId on create."""
        mock_item_service.create_item.return_value = FabricItem(
            id="pipe-2",
            display_name="BlankPipe",
            type="DataPipeline",
            workspace_id="ws-1",
        )
        mock_item_service.resolve_folder_id_from_path.return_value = "folder-1"

        pipeline_service.create_blank_pipeline(
            workspace_id="ws-1",
            pipeline_name="BlankPipe",
            folder_path="Pipelines/Daily",
        )

        mock_item_service.resolve_folder_id_from_path.assert_called_once_with(
            "ws-1", "Pipelines/Daily", create_missing=True
        )
        item_definition = mock_item_service.create_item.call_args.args[1]
        assert item_definition["folderId"] == "folder-1"

    def test_create_blank_pipeline_validation_error(self, pipeline_service):
        """Empty pipeline name raises validation error."""
        with pytest.raises(FabricValidationError):
            pipeline_service.create_blank_pipeline("ws-1", " ")

    def test_create_blank_pipeline_invalid_name(self, pipeline_service):
        """Names with separators raise validation error."""
        with pytest.raises(FabricValidationError):
            pipeline_service.create_blank_pipeline("ws-1", "Bad/Name")

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

    def test_create_pipeline_with_definition_success(self, pipeline_service, mock_client):
        """Create pipeline with definition posts encoded parts and returns ID."""
        response = Mock()
        response.status_code = 201
        response.json.return_value = {"id": "pipe-123", "displayName": "Pipe"}
        mock_client.make_api_request.return_value = response

        pipeline_id = pipeline_service.create_pipeline_with_definition(
            workspace_id="ws-1",
            display_name="Pipe",
            pipeline_content_json={"properties": {"activities": []}},
            platform={"metadata": {"key": "value"}},
            description="desc",
            folder_id="folder-1",
        )

        assert pipeline_id == "pipe-123"
        args, kwargs = mock_client.make_api_request.call_args
        assert args[0] == "POST"
        assert args[1] == "workspaces/ws-1/dataPipelines"
        payload = kwargs["payload"]
        assert payload["displayName"] == "Pipe"
        assert payload["description"] == "desc"
        assert payload["folderId"] == "folder-1"
        parts = payload["definition"]["parts"]
        assert parts[0]["path"] == "pipeline-content.json"
        assert _decode_payload(parts[0]["payload"]) == {"properties": {"activities": []}}
        assert parts[1]["path"] == ".platform"
        assert _decode_payload(parts[1]["payload"]) == {"metadata": {"key": "value"}}

    def test_create_pipeline_with_definition_invalid_definition(self, pipeline_service):
        """Non-dict definition raises validation error."""
        with pytest.raises(FabricValidationError):
            pipeline_service.create_pipeline_with_definition(
                workspace_id="ws-1",
                display_name="Pipe",
                pipeline_content_json="not-a-dict",
            )

    def test_create_pipeline_with_definition_invalid_name(self, pipeline_service):
        """Names with separators raise validation error."""
        with pytest.raises(FabricValidationError):
            pipeline_service.create_pipeline_with_definition(
                workspace_id="ws-1",
                display_name="Bad/Name",
                pipeline_content_json={"properties": {"activities": []}},
            )

    def test_create_pipeline_with_definition_invalid_platform(self, pipeline_service):
        """Non-dict platform raises validation error."""
        with pytest.raises(FabricValidationError):
            pipeline_service.create_pipeline_with_definition(
                workspace_id="ws-1",
                display_name="Pipe",
                pipeline_content_json={"properties": {"activities": []}},
                platform="not-a-dict",
            )

    def test_get_pipeline_definition_success(self, pipeline_service, mock_client):
        """Fetches and decodes pipeline definition with optional platform."""
        definition_payload = {"properties": {"activities": []}}
        encoded_definition = pipeline_service._encode_definition(definition_payload)
        platform_payload = {"metadata": {"key": "value"}}
        encoded_platform = pipeline_service._encode_definition(platform_payload)

        response = Mock()
        response.json.return_value = {
            "definition": {
                "parts": [
                    {"path": "pipeline-content.json", "payload": encoded_definition},
                    {"path": ".platform", "payload": encoded_platform},
                ]
            }
        }
        mock_client.make_api_request.return_value = response

        result = pipeline_service.get_pipeline_definition(
            workspace_id="ws-1",
            pipeline_id="pipe-1",
            format="json",
        )

        assert result["pipeline_content_json"] == definition_payload
        assert result["platform"] == platform_payload

        args, kwargs = mock_client.make_api_request.call_args
        assert args[0] == "POST"
        assert "workspaces/ws-1/dataPipelines/pipe-1/getDefinition" in args[1]
        assert kwargs["wait_for_lro"] is True

    def test_get_pipeline_definition_missing_content(self, pipeline_service, mock_client):
        """Missing pipeline-content.json raises FabricError."""
        response = Mock()
        response.json.return_value = {"definition": {"parts": []}}
        mock_client.make_api_request.return_value = response

        with pytest.raises(FabricError):
            pipeline_service.get_pipeline_definition("ws-1", "pipe-1")

    def test_get_pipeline_definition_platform_fallback(self, pipeline_service, mock_client):
        """Invalid platform JSON returns raw string."""
        definition_payload = {"properties": {"activities": []}}
        encoded_definition = pipeline_service._encode_definition(definition_payload)
        encoded_platform = base64.b64encode(b"not-json").decode("utf-8")

        response = Mock()
        response.json.return_value = {
            "definition": {
                "parts": [
                    {"path": "pipeline-content.json", "payload": encoded_definition},
                    {"path": ".platform", "payload": encoded_platform},
                ]
            }
        }
        mock_client.make_api_request.return_value = response

        result = pipeline_service.get_pipeline_definition("ws-1", "pipe-1")
        assert result["platform"] == "not-json"

    def test_update_pipeline_definition_success(self, pipeline_service, mock_client):
        """Update pipeline definition builds correct payload."""
        pipeline_content = {"properties": {"activities": []}}
        platform = {"metadata": {"key": "value"}}

        pipeline_service.update_pipeline_definition(
            workspace_id="ws-1",
            pipeline_id="pipe-1",
            pipeline_content_json=pipeline_content,
            platform=platform,
            update_metadata=True,
        )

        args, kwargs = mock_client.make_api_request.call_args
        assert args[0] == "POST"
        assert "workspaces/ws-1/dataPipelines/pipe-1/updateDefinition" in args[1]
        assert "updateMetadata=true" in args[1]
        payload = kwargs["payload"]["definition"]["parts"]
        assert payload[0]["path"] == "pipeline-content.json"
        assert _decode_payload(payload[0]["payload"]) == pipeline_content
        assert payload[1]["path"] == ".platform"
        assert _decode_payload(payload[1]["payload"]) == platform

    def test_update_pipeline_definition_requires_platform_for_metadata(self, pipeline_service):
        """update_metadata requires platform payload."""
        with pytest.raises(FabricValidationError):
            pipeline_service.update_pipeline_definition(
                workspace_id="ws-1",
                pipeline_id="pipe-1",
                pipeline_content_json={"properties": {"activities": []}},
                update_metadata=True,
            )

    def test_set_activity_dependency_add(self, pipeline_service, mock_item_service):
        """Add dependency to activity."""
        pipeline_item = FabricItem(
            id="pipe-1",
            display_name="Pipe",
            type="DataPipeline",
            workspace_id="ws-1",
        )
        mock_item_service.get_item_by_name.return_value = pipeline_item

        definition = {
            "properties": {
                "activities": [
                    {"name": "A", "dependsOn": []},
                    {"name": "B", "dependsOn": []},
                ]
            }
        }
        pipeline_service.get_pipeline_definition = Mock(
            return_value={"pipeline_content_json": definition}
        )
        pipeline_service.update_pipeline_definition = Mock()

        pipeline_id, changed = pipeline_service.set_activity_dependency(
            workspace_id="ws-1",
            pipeline_name="Pipe",
            activity_name="B",
            depends_on=["A"],
            mode="add",
        )

        assert pipeline_id == "pipe-1"
        assert changed == 1
        update_kwargs = pipeline_service.update_pipeline_definition.call_args.kwargs
        updated = update_kwargs["pipeline_content_json"]
        activity_b = next(
            activity for activity in updated["properties"]["activities"] if activity["name"] == "B"
        )
        assert activity_b["dependsOn"][0]["activity"] == "A"

    def test_set_activity_dependency_remove(self, pipeline_service, mock_item_service):
        """Remove dependency from activity."""
        pipeline_item = FabricItem(
            id="pipe-1",
            display_name="Pipe",
            type="DataPipeline",
            workspace_id="ws-1",
        )
        mock_item_service.get_item_by_name.return_value = pipeline_item

        definition = {
            "properties": {
                "activities": [
                    {
                        "name": "A",
                        "dependsOn": [
                            {"activity": "B", "dependencyConditions": ["Succeeded"]}
                        ],
                    },
                    {"name": "B", "dependsOn": []},
                ]
            }
        }
        pipeline_service.get_pipeline_definition = Mock(
            return_value={"pipeline_content_json": definition}
        )
        pipeline_service.update_pipeline_definition = Mock()

        _, changed = pipeline_service.set_activity_dependency(
            workspace_id="ws-1",
            pipeline_name="Pipe",
            activity_name="A",
            depends_on=["B"],
            mode="remove",
        )

        assert changed == 1
        updated = pipeline_service.update_pipeline_definition.call_args.kwargs[
            "pipeline_content_json"
        ]
        activity_a = next(
            activity for activity in updated["properties"]["activities"] if activity["name"] == "A"
        )
        assert activity_a["dependsOn"] == []

    def test_set_activity_dependency_replace(self, pipeline_service, mock_item_service):
        """Replace dependencies for activity."""
        pipeline_item = FabricItem(
            id="pipe-1",
            display_name="Pipe",
            type="DataPipeline",
            workspace_id="ws-1",
        )
        mock_item_service.get_item_by_name.return_value = pipeline_item

        definition = {
            "properties": {
                "activities": [
                    {"name": "A", "dependsOn": [{"activity": "B"}]},
                    {"name": "B", "dependsOn": []},
                    {"name": "C", "dependsOn": []},
                ]
            }
        }
        pipeline_service.get_pipeline_definition = Mock(
            return_value={"pipeline_content_json": definition}
        )
        pipeline_service.update_pipeline_definition = Mock()

        _, changed = pipeline_service.set_activity_dependency(
            workspace_id="ws-1",
            pipeline_name="Pipe",
            activity_name="A",
            depends_on=["C"],
            mode="replace",
            dependency_conditions=["Succeeded", "Failed"],
        )

        assert changed == 1
        updated = pipeline_service.update_pipeline_definition.call_args.kwargs[
            "pipeline_content_json"
        ]
        activity_a = next(
            activity for activity in updated["properties"]["activities"] if activity["name"] == "A"
        )
        assert activity_a["dependsOn"][0]["activity"] == "C"
        assert activity_a["dependsOn"][0]["dependencyConditions"] == ["Succeeded", "Failed"]
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

    def test_add_notebook_activity_to_pipeline_success(self, pipeline_service, mock_item_service, mock_client):
        """Adds notebook activity and updates definition."""
        pipeline_item = FabricItem(
            id="pipe-1",
            display_name="Pipe",
            type="DataPipeline",
            workspace_id="ws-1",
        )
        notebook_item = FabricItem(
            id="nb-1",
            display_name="Note",
            type="Notebook",
            workspace_id="ws-1",
        )
        mock_item_service.get_item_by_name.side_effect = [pipeline_item, notebook_item]
        base_definition = {"properties": {"activities": []}}
        encoded = pipeline_service._encode_definition(base_definition)
        mock_item_service.get_item_definition.return_value = {
            "definition": {"parts": [{"path": "pipeline-content.json", "payload": encoded}]}
        }

        pipeline_id = pipeline_service.add_notebook_activity_to_pipeline(
            workspace_id="ws-1",
            pipeline_name="Pipe",
            notebook_name="Note",
            activity_name="RunNotebook_Note",
            session_tag="tag-1",
            parameters={"p1": {"value": "x", "type": "string"}},
        )

        assert pipeline_id == "pipe-1"
        _, kwargs = mock_client.make_api_request.call_args
        payload = kwargs["payload"]["definition"]["parts"][0]["payload"]
        updated = _decode_payload(payload)
        activity = updated["properties"]["activities"][-1]
        assert activity["type"] == "TridentNotebook"
        assert activity["typeProperties"]["notebookId"] == "nb-1"
        assert activity["typeProperties"]["workspaceId"] == "ws-1"
        assert activity["typeProperties"]["sessionTag"] == "tag-1"
        assert activity["typeProperties"]["parameters"]["p1"]["value"] == "x"

    def test_add_notebook_activity_to_pipeline_dependency_missing(self, pipeline_service, mock_item_service):
        """Missing dependency raises validation error."""
        pipeline_item = FabricItem(
            id="pipe-1",
            display_name="Pipe",
            type="DataPipeline",
            workspace_id="ws-1",
        )
        notebook_item = FabricItem(
            id="nb-1",
            display_name="Note",
            type="Notebook",
            workspace_id="ws-1",
        )
        mock_item_service.get_item_by_name.side_effect = [pipeline_item, notebook_item]
        base_definition = {"properties": {"activities": [{"name": "Existing"}]}}
        encoded = pipeline_service._encode_definition(base_definition)
        mock_item_service.get_item_definition.return_value = {
            "definition": {"parts": [{"path": "pipeline-content.json", "payload": encoded}]}
        }

        with pytest.raises(FabricValidationError):
            pipeline_service.add_notebook_activity_to_pipeline(
                workspace_id="ws-1",
                pipeline_name="Pipe",
                notebook_name="Note",
                activity_name="RunNotebook_Note",
                depends_on_activity_name="MissingActivity",
            )

    def test_add_dataflow_activity_to_pipeline_success(self, pipeline_service, mock_item_service, mock_client):
        """Adds dataflow activity and updates definition."""
        pipeline_item = FabricItem(
            id="pipe-1",
            display_name="Pipe",
            type="DataPipeline",
            workspace_id="ws-1",
        )
        dataflow_item = FabricItem(
            id="df-1",
            display_name="Flow",
            type="Dataflow",
            workspace_id="ws-1",
        )
        mock_item_service.get_item_by_name.side_effect = [pipeline_item, dataflow_item]
        base_definition = {"properties": {"activities": []}}
        encoded = pipeline_service._encode_definition(base_definition)
        mock_item_service.get_item_definition.return_value = {
            "definition": {"parts": [{"path": "pipeline-content.json", "payload": encoded}]}
        }

        pipeline_id = pipeline_service.add_dataflow_activity_to_pipeline(
            workspace_id="ws-1",
            pipeline_name="Pipe",
            dataflow_name="Flow",
            activity_name="RunDataflow_Flow",
        )

        assert pipeline_id == "pipe-1"
        _, kwargs = mock_client.make_api_request.call_args
        payload = kwargs["payload"]["definition"]["parts"][0]["payload"]
        updated = _decode_payload(payload)
        activity = updated["properties"]["activities"][-1]
        assert activity["type"] == "RefreshDataflow"
        assert activity["typeProperties"]["dataflowId"] == "df-1"
        assert activity["typeProperties"]["workspaceId"] == "ws-1"
        assert activity["typeProperties"]["dataflowType"] == "Dataflow-Gen2"

    def test_add_dataflow_activity_to_pipeline_duplicate_name(self, pipeline_service, mock_item_service):
        """Duplicate activity names raise validation error."""
        pipeline_item = FabricItem(
            id="pipe-1",
            display_name="Pipe",
            type="DataPipeline",
            workspace_id="ws-1",
        )
        dataflow_item = FabricItem(
            id="df-1",
            display_name="Flow",
            type="Dataflow",
            workspace_id="ws-1",
        )
        mock_item_service.get_item_by_name.side_effect = [pipeline_item, dataflow_item]
        base_definition = {"properties": {"activities": [{"name": "RunDataflow_Flow"}]}}
        encoded = pipeline_service._encode_definition(base_definition)
        mock_item_service.get_item_definition.return_value = {
            "definition": {"parts": [{"path": "pipeline-content.json", "payload": encoded}]}
        }

        with pytest.raises(FabricValidationError):
            pipeline_service.add_dataflow_activity_to_pipeline(
                workspace_id="ws-1",
                pipeline_name="Pipe",
                dataflow_name="Flow",
                activity_name="RunDataflow_Flow",
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

    def test_delete_activity_from_pipeline_success(
        self, pipeline_service, mock_item_service, mock_client
    ):
        """Deletes an activity and updates definition."""
        mock_item_service.get_item_by_name.return_value = FabricItem(
            id="pipe-1",
            display_name="Pipe",
            type="DataPipeline",
            workspace_id="ws-1",
        )
        base_definition = {
            "properties": {
                "activities": [
                    {"name": "A", "dependsOn": []},
                    {"name": "B", "dependsOn": []},
                ]
            }
        }
        encoded = pipeline_service._encode_definition(base_definition)
        mock_item_service.get_item_definition.return_value = {
            "definition": {"parts": [{"path": "pipeline-content.json", "payload": encoded}]}
        }

        pipeline_id = pipeline_service.delete_activity_from_pipeline("ws-1", "Pipe", "A")

        assert pipeline_id == "pipe-1"
        _, kwargs = mock_client.make_api_request.call_args
        payload = kwargs["payload"]["definition"]["parts"][0]["payload"]
        updated = _decode_payload(payload)
        names = [activity.get("name") for activity in updated["properties"]["activities"]]
        assert "A" not in names
        assert "B" in names

    def test_delete_activity_from_pipeline_missing_activity(
        self, pipeline_service, mock_item_service
    ):
        """Missing activity name raises validation error."""
        mock_item_service.get_item_by_name.return_value = FabricItem(
            id="pipe-1",
            display_name="Pipe",
            type="DataPipeline",
            workspace_id="ws-1",
        )
        base_definition = {"properties": {"activities": [{"name": "B", "dependsOn": []}]}}
        encoded = pipeline_service._encode_definition(base_definition)
        mock_item_service.get_item_definition.return_value = {
            "definition": {"parts": [{"path": "pipeline-content.json", "payload": encoded}]}
        }

        with pytest.raises(FabricValidationError):
            pipeline_service.delete_activity_from_pipeline("ws-1", "Pipe", "A")

    def test_delete_activity_from_pipeline_with_dependents(
        self, pipeline_service, mock_item_service
    ):
        """Dependent activities block deletion."""
        mock_item_service.get_item_by_name.return_value = FabricItem(
            id="pipe-1",
            display_name="Pipe",
            type="DataPipeline",
            workspace_id="ws-1",
        )
        base_definition = {
            "properties": {
                "activities": [
                    {"name": "A", "dependsOn": []},
                    {
                        "name": "B",
                        "dependsOn": [
                            {"activity": "A", "dependencyConditions": ["Succeeded"]}
                        ],
                    },
                ]
            }
        }
        encoded = pipeline_service._encode_definition(base_definition)
        mock_item_service.get_item_definition.return_value = {
            "definition": {"parts": [{"path": "pipeline-content.json", "payload": encoded}]}
        }

        with pytest.raises(FabricValidationError):
            pipeline_service.delete_activity_from_pipeline("ws-1", "Pipe", "A")

    def test_delete_activity_from_pipeline_duplicate_names(
        self, pipeline_service, mock_item_service, mock_client
    ):
        """Deletes all activities with a matching name."""
        mock_item_service.get_item_by_name.return_value = FabricItem(
            id="pipe-1",
            display_name="Pipe",
            type="DataPipeline",
            workspace_id="ws-1",
        )
        base_definition = {
            "properties": {
                "activities": [
                    {"name": "A", "dependsOn": []},
                    {"name": "A", "dependsOn": []},
                    {"name": "B", "dependsOn": []},
                ]
            }
        }
        encoded = pipeline_service._encode_definition(base_definition)
        mock_item_service.get_item_definition.return_value = {
            "definition": {"parts": [{"path": "pipeline-content.json", "payload": encoded}]}
        }

        pipeline_id = pipeline_service.delete_activity_from_pipeline("ws-1", "Pipe", "A")

        assert pipeline_id == "pipe-1"
        _, kwargs = mock_client.make_api_request.call_args
        payload = kwargs["payload"]["definition"]["parts"][0]["payload"]
        updated = _decode_payload(payload)
        names = [activity.get("name") for activity in updated["properties"]["activities"]]
        assert names == ["B"]

    def test_remove_activity_dependency_global_success(
        self, pipeline_service, mock_item_service, mock_client
    ):
        """Removes dependency edges across all activities."""
        mock_item_service.get_item_by_name.return_value = FabricItem(
            id="pipe-1",
            display_name="Pipe",
            type="DataPipeline",
            workspace_id="ws-1",
        )
        base_definition = {
            "properties": {
                "activities": [
                    {"name": "A", "dependsOn": []},
                    {
                        "name": "B",
                        "dependsOn": [
                            {"activity": "A", "dependencyConditions": ["Succeeded"]},
                            {"activity": "C", "dependencyConditions": ["Succeeded"]},
                        ],
                    },
                    {
                        "name": "C",
                        "dependsOn": [
                            {"activity": "A", "dependencyConditions": ["Succeeded"]}
                        ],
                    },
                ]
            }
        }
        encoded = pipeline_service._encode_definition(base_definition)
        mock_item_service.get_item_definition.return_value = {
            "definition": {"parts": [{"path": "pipeline-content.json", "payload": encoded}]}
        }

        pipeline_id, removed_count = pipeline_service.remove_activity_dependency(
            "ws-1", "Pipe", "A"
        )

        assert pipeline_id == "pipe-1"
        assert removed_count == 2
        _, kwargs = mock_client.make_api_request.call_args
        payload = kwargs["payload"]["definition"]["parts"][0]["payload"]
        updated = _decode_payload(payload)
        activities = {activity["name"]: activity for activity in updated["properties"]["activities"]}
        assert activities["B"]["dependsOn"] == [
            {"activity": "C", "dependencyConditions": ["Succeeded"]}
        ]
        assert activities["C"]["dependsOn"] == []

    def test_remove_activity_dependency_specific_success(
        self, pipeline_service, mock_item_service, mock_client
    ):
        """Removes dependency edges from a specific activity."""
        mock_item_service.get_item_by_name.return_value = FabricItem(
            id="pipe-1",
            display_name="Pipe",
            type="DataPipeline",
            workspace_id="ws-1",
        )
        base_definition = {
            "properties": {
                "activities": [
                    {"name": "A", "dependsOn": []},
                    {
                        "name": "B",
                        "dependsOn": [
                            {"activity": "A", "dependencyConditions": ["Succeeded"]}
                        ],
                    },
                    {
                        "name": "C",
                        "dependsOn": [
                            {"activity": "A", "dependencyConditions": ["Succeeded"]}
                        ],
                    },
                ]
            }
        }
        encoded = pipeline_service._encode_definition(base_definition)
        mock_item_service.get_item_definition.return_value = {
            "definition": {"parts": [{"path": "pipeline-content.json", "payload": encoded}]}
        }

        pipeline_id, removed_count = pipeline_service.remove_activity_dependency(
            "ws-1", "Pipe", "A", from_activity_name="B"
        )

        assert pipeline_id == "pipe-1"
        assert removed_count == 1
        _, kwargs = mock_client.make_api_request.call_args
        payload = kwargs["payload"]["definition"]["parts"][0]["payload"]
        updated = _decode_payload(payload)
        activities = {activity["name"]: activity for activity in updated["properties"]["activities"]}
        assert activities["B"]["dependsOn"] == []
        assert activities["C"]["dependsOn"] == [
            {"activity": "A", "dependencyConditions": ["Succeeded"]}
        ]

    def test_remove_activity_dependency_no_edges(
        self, pipeline_service, mock_item_service
    ):
        """No dependency edges removed raises validation error."""
        mock_item_service.get_item_by_name.return_value = FabricItem(
            id="pipe-1",
            display_name="Pipe",
            type="DataPipeline",
            workspace_id="ws-1",
        )
        base_definition = {
            "properties": {
                "activities": [
                    {"name": "A", "dependsOn": []},
                    {
                        "name": "B",
                        "dependsOn": [
                            {"activity": "C", "dependencyConditions": ["Succeeded"]}
                        ],
                    },
                ]
            }
        }
        encoded = pipeline_service._encode_definition(base_definition)
        mock_item_service.get_item_definition.return_value = {
            "definition": {"parts": [{"path": "pipeline-content.json", "payload": encoded}]}
        }

        with pytest.raises(FabricValidationError):
            pipeline_service.remove_activity_dependency("ws-1", "Pipe", "A")

    def test_remove_activity_dependency_missing_target_succeeds(
        self, pipeline_service, mock_item_service, mock_client
    ):
        """Removes edges even if target activity is missing from activities list."""
        mock_item_service.get_item_by_name.return_value = FabricItem(
            id="pipe-1",
            display_name="Pipe",
            type="DataPipeline",
            workspace_id="ws-1",
        )
        base_definition = {
            "properties": {
                "activities": [
                    {"name": "B", "dependsOn": [{"activity": "Missing"}]},
                    {"name": "C", "dependsOn": []},
                ]
            }
        }
        encoded = pipeline_service._encode_definition(base_definition)
        mock_item_service.get_item_definition.return_value = {
            "definition": {"parts": [{"path": "pipeline-content.json", "payload": encoded}]}
        }

        pipeline_id, removed_count = pipeline_service.remove_activity_dependency(
            "ws-1", "Pipe", "Missing"
        )

        assert pipeline_id == "pipe-1"
        assert removed_count == 1
        _, kwargs = mock_client.make_api_request.call_args
        payload = kwargs["payload"]["definition"]["parts"][0]["payload"]
        updated = _decode_payload(payload)
        activities = {activity["name"]: activity for activity in updated["properties"]["activities"]}
        assert activities["B"]["dependsOn"] == []

    def test_add_activity_dependency_success(
        self, pipeline_service, mock_item_service, mock_client
    ):
        """Adds dependsOn entries to an activity."""
        mock_item_service.get_item_by_name.return_value = FabricItem(
            id="pipe-1",
            display_name="Pipe",
            type="DataPipeline",
            workspace_id="ws-1",
        )
        base_definition = {
            "properties": {
                "activities": [
                    {"name": "A", "dependsOn": []},
                    {"name": "B", "dependsOn": []},
                    {"name": "C", "dependsOn": []},
                ]
            }
        }
        encoded = pipeline_service._encode_definition(base_definition)
        mock_item_service.get_item_definition.return_value = {
            "definition": {"parts": [{"path": "pipeline-content.json", "payload": encoded}]}
        }

        pipeline_id, added_count = pipeline_service.add_activity_dependency(
            workspace_id="ws-1",
            pipeline_name="Pipe",
            activity_name="C",
            depends_on=["A", "B"],
        )

        assert pipeline_id == "pipe-1"
        assert added_count == 2
        payload = mock_client.make_api_request.call_args.kwargs["payload"]
        updated = _decode_payload(payload["definition"]["parts"][0]["payload"])
        depends_on = updated["properties"]["activities"][2]["dependsOn"]
        assert {"activity": "A", "dependencyConditions": ["Succeeded"]} in depends_on
        assert {"activity": "B", "dependencyConditions": ["Succeeded"]} in depends_on

    def test_add_activity_dependency_missing_target(self, pipeline_service, mock_item_service):
        """Missing target activity raises validation error."""
        mock_item_service.get_item_by_name.return_value = FabricItem(
            id="pipe-1",
            display_name="Pipe",
            type="DataPipeline",
            workspace_id="ws-1",
        )
        base_definition = {"properties": {"activities": [{"name": "A"}]}}
        encoded = pipeline_service._encode_definition(base_definition)
        mock_item_service.get_item_definition.return_value = {
            "definition": {"parts": [{"path": "pipeline-content.json", "payload": encoded}]}
        }

        with pytest.raises(FabricValidationError):
            pipeline_service.add_activity_dependency(
                workspace_id="ws-1",
                pipeline_name="Pipe",
                activity_name="C",
                depends_on=["A"],
            )

    def test_add_activity_dependency_missing_dependency(self, pipeline_service, mock_item_service):
        """Missing dependency activity raises validation error."""
        mock_item_service.get_item_by_name.return_value = FabricItem(
            id="pipe-1",
            display_name="Pipe",
            type="DataPipeline",
            workspace_id="ws-1",
        )
        base_definition = {"properties": {"activities": [{"name": "A"}, {"name": "C"}]}}
        encoded = pipeline_service._encode_definition(base_definition)
        mock_item_service.get_item_definition.return_value = {
            "definition": {"parts": [{"path": "pipeline-content.json", "payload": encoded}]}
        }

        with pytest.raises(FabricValidationError):
            pipeline_service.add_activity_dependency(
                workspace_id="ws-1",
                pipeline_name="Pipe",
                activity_name="C",
                depends_on=["Missing"],
            )

    def test_add_activity_dependency_no_new(self, pipeline_service, mock_item_service):
        """No new dependencies raises validation error."""
        mock_item_service.get_item_by_name.return_value = FabricItem(
            id="pipe-1",
            display_name="Pipe",
            type="DataPipeline",
            workspace_id="ws-1",
        )
        base_definition = {
            "properties": {
                "activities": [
                    {
                        "name": "C",
                        "dependsOn": [
                            {"activity": "A", "dependencyConditions": ["Succeeded"]}
                        ],
                    },
                    {"name": "A"},
                ]
            }
        }
        encoded = pipeline_service._encode_definition(base_definition)
        mock_item_service.get_item_definition.return_value = {
            "definition": {"parts": [{"path": "pipeline-content.json", "payload": encoded}]}
        }

        with pytest.raises(FabricValidationError):
            pipeline_service.add_activity_dependency(
                workspace_id="ws-1",
                pipeline_name="Pipe",
                activity_name="C",
                depends_on=["A"],
            )
