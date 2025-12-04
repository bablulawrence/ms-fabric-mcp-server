"""Unit tests for FabricPipelineService."""

import json
import base64
import pytest
from unittest.mock import Mock, MagicMock

from ms_fabric_mcp_server.services.pipeline import FabricPipelineService
from ms_fabric_mcp_server.client.exceptions import FabricValidationError, FabricError
from ms_fabric_mcp_server.models.item import FabricItem


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

if __name__ == "__main__":
    pytest.main([__file__, "-v"])
