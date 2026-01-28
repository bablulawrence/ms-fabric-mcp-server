# ABOUTME: Service for Fabric pipeline operations.
# ABOUTME: Handles pipeline creation and Copy Activity configuration.
"""Service for Microsoft Fabric pipeline operations."""

import base64
import json
import logging
from typing import Dict, Any, Optional

from ..client.http_client import FabricClient
from ..client.exceptions import (
    FabricAPIError,
    FabricError,
    FabricValidationError,
    FabricItemNotFoundError,
)
from .workspace import FabricWorkspaceService
from .item import FabricItemService


logger = logging.getLogger(__name__)


class FabricPipelineService:
    """Service for Microsoft Fabric pipeline operations.
    
    This service provides high-level operations for creating and managing
    Fabric Data Factory pipelines, particularly pipelines with Copy Activities
    for data ingestion from various sources to various destinations.
    
    Example:
        ```python
        from ms_fabric_mcp_server import FabricConfig, FabricClient
        from ms_fabric_mcp_server.services import (
            FabricPipelineService,
            FabricWorkspaceService,
            FabricItemService
        )
        
        config = FabricConfig.from_environment()
        client = FabricClient(config)
        workspace_service = FabricWorkspaceService(client)
        item_service = FabricItemService(client)
        pipeline_service = FabricPipelineService(client, workspace_service, item_service)
        
        # Create pipeline with Copy Activity
        pipeline_id = pipeline_service.create_pipeline_with_copy_activity(
            workspace_id="12345678-1234-1234-1234-123456789abc",
            pipeline_name="Copy_Data_Pipeline",
            source_type="AzurePostgreSqlSource",
            source_connection_id="conn-123",
            source_schema="public",
            source_table="movie",
            destination_lakehouse_id="lakehouse-456",
            destination_connection_id="dest-conn-123",
            destination_table="movie"
        )
        ```
    """
    
    def __init__(
        self,
        client: FabricClient,
        workspace_service: FabricWorkspaceService,
        item_service: FabricItemService,
    ):
        """Initialize the pipeline service.
        
        Args:
            client: FabricClient instance for API calls
            workspace_service: Service for workspace operations
            item_service: Service for item operations
        """
        self.client = client
        self.workspace_service = workspace_service
        self.item_service = item_service
        logger.debug("FabricPipelineService initialized")
    
    def create_pipeline_with_copy_activity(
        self,
        workspace_id: str,
        pipeline_name: str,
        source_type: str,
        source_connection_id: str,
        source_schema: str,
        source_table: str,
        destination_lakehouse_id: str,
        destination_connection_id: str,
        destination_table: str,
        source_access_mode: str = "direct",
        source_sql_query: Optional[str] = None,
        description: Optional[str] = None,
        table_action_option: str = "Append",
        apply_v_order: bool = True,
        timeout: str = "0.12:00:00",
        retry: int = 0,
        retry_interval_seconds: int = 30
    ) -> str:
        """Create a Fabric pipeline with a Copy Activity.
        
        Creates a Data Pipeline in the specified workspace with a Copy Activity
        configured to copy data from a source database table to a Fabric
        Lakehouse table.
        
        Args:
            workspace_id: Workspace ID where pipeline will be created
            pipeline_name: Name for the new pipeline (must be unique in workspace)
            source_type: Type of source (e.g., "AzurePostgreSqlSource", "AzureSqlSource", etc.)
            source_connection_id: Fabric workspace connection ID for source
            source_schema: Schema name of the source table (e.g., "public")
            source_table: Name of the source table (e.g., "movie")
            destination_lakehouse_id: Workspace artifact ID of the destination Lakehouse
            destination_connection_id: Fabric workspace connection ID for destination
            destination_table: Name for the destination table in Lakehouse
            source_access_mode: Source access mode ("direct" or "sql"). Default is "direct".
            source_sql_query: Optional SQL query for sql access mode.
            description: Optional description for the pipeline
            table_action_option: Table action option (default: "Append", options: "Append", "Overwrite")
            apply_v_order: Apply V-Order optimization (default: True)
            timeout: Activity timeout (default: "0.12:00:00")
            retry: Number of retry attempts (default: 0)
            retry_interval_seconds: Retry interval in seconds (default: 30)
            
        Returns:
            Pipeline ID (GUID) of the created pipeline
            
        Raises:
            FabricValidationError: If parameters are invalid
            FabricAPIError: If pipeline creation fails
            FabricError: For other errors
            
        Example:
            ```python
            pipeline_id = pipeline_service.create_pipeline_with_copy_activity(
                workspace_id="12345678-1234-1234-1234-123456789abc",
                pipeline_name="Copy_Movie_Table",
                source_type="AzurePostgreSqlSource",
                source_connection_id="conn-123",
                source_schema="public",
                source_table="movie",
                destination_lakehouse_id="lakehouse-456",
                destination_connection_id="dest-conn-123",
                destination_table="movie",
                description="Copy movie data from PostgreSQL to Bronze Lakehouse"
            )
            ```
        """
        logger.info(
            f"Creating pipeline '{pipeline_name}' with Copy Activity in workspace {workspace_id}"
        )
        
        # Validate inputs
        self._validate_pipeline_inputs(
            pipeline_name,
            source_type,
            source_connection_id,
            source_schema,
            source_table,
            destination_lakehouse_id,
            destination_connection_id,
            destination_table
        )
        source_access_mode = source_access_mode.lower().strip() if source_access_mode else ""
        self._validate_source_access_mode(source_access_mode, source_sql_query)
        
        try:
            # Build pipeline definition
            pipeline_definition = self._build_copy_activity_definition(
                workspace_id,
                source_type,
                source_connection_id,
                source_schema,
                source_table,
                destination_lakehouse_id,
                destination_connection_id,
                destination_table,
                table_action_option,
                apply_v_order,
                timeout,
                retry,
                retry_interval_seconds,
                source_access_mode=source_access_mode,
                source_sql_query=source_sql_query,
            )
            
            # Encode definition to Base64
            encoded_definition = self._encode_definition(pipeline_definition)
            
            # Create item definition for API
            item_definition = {
                "displayName": pipeline_name,
                "type": "DataPipeline",
                "definition": {
                    "parts": [
                        {
                            "path": "pipeline-content.json",
                            "payload": encoded_definition,
                            "payloadType": "InlineBase64"
                        }
                    ]
                }
            }
            
            if description:
                item_definition["description"] = description
            
            # Create the pipeline item
            created_item = self.item_service.create_item(workspace_id, item_definition)
            
            logger.info(f"Successfully created pipeline with ID: {created_item.id}")
            return created_item.id
            
        except FabricValidationError:
            raise
        except FabricAPIError:
            raise
        except Exception as exc:
            logger.error(f"Failed to create pipeline: {exc}")
            raise FabricError(f"Failed to create pipeline: {exc}")
    
    def _validate_pipeline_inputs(
        self,
        pipeline_name: str,
        source_type: str,
        source_connection_id: str,
        source_schema: str,
        source_table: str,
        destination_lakehouse_id: str,
        destination_connection_id: str,
        destination_table: str
    ) -> None:
        """Validate pipeline creation inputs.
        
        Args:
            pipeline_name: Pipeline name
            source_type: Source type
            source_connection_id: Source connection ID
            source_schema: Source schema name
            source_table: Source table name
            destination_lakehouse_id: Destination lakehouse ID
            destination_connection_id: Destination connection ID
            destination_table: Destination table name
            
        Raises:
            FabricValidationError: If any input is invalid
        """
        if not pipeline_name or not pipeline_name.strip():
            raise FabricValidationError(
                "pipeline_name",
                "empty",
                "Pipeline name cannot be empty"
            )
        
        if not source_type or not source_type.strip():
            raise FabricValidationError(
                "source_type",
                "empty",
                "Source type cannot be empty"
            )
        
        if not source_connection_id or not source_connection_id.strip():
            raise FabricValidationError(
                "source_connection_id",
                "empty",
                "Source connection ID cannot be empty"
            )
        
        if not source_schema or not source_schema.strip():
            raise FabricValidationError(
                "source_schema",
                "empty",
                "Source schema cannot be empty"
            )
        
        if not source_table or not source_table.strip():
            raise FabricValidationError(
                "source_table",
                "empty",
                "Source table cannot be empty"
            )
        
        if not destination_lakehouse_id or not destination_lakehouse_id.strip():
            raise FabricValidationError(
                "destination_lakehouse_id",
                "empty",
                "Destination lakehouse ID cannot be empty"
            )
        
        if not destination_connection_id or not destination_connection_id.strip():
            raise FabricValidationError(
                "destination_connection_id",
                "empty",
                "Destination connection ID cannot be empty"
            )
        
        if not destination_table or not destination_table.strip():
            raise FabricValidationError(
                "destination_table",
                "empty",
                "Destination table cannot be empty"
            )
    
    def _build_copy_activity_definition(
        self,
        workspace_id: str,
        source_type: str,
        source_connection_id: str,
        source_schema: str,
        source_table: str,
        destination_lakehouse_id: str,
        destination_connection_id: str,
        destination_table: str,
        table_action_option: str,
        apply_v_order: bool,
        timeout: str,
        retry: int,
        retry_interval_seconds: int,
        source_access_mode: str = "direct",
        source_sql_query: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Build the pipeline JSON structure with Copy Activity.
        
        Creates a complete Fabric pipeline definition with a Copy Activity that
        copies data from a source database to Lakehouse.
        
        Args:
            workspace_id: Workspace ID where pipeline will be created
            source_type: Type of source (e.g., "AzurePostgreSqlSource")
            source_connection_id: Fabric workspace connection ID for source
            source_schema: Source table schema name
            source_table: Source table name
            destination_lakehouse_id: Destination lakehouse ID
            destination_connection_id: Destination connection ID
            destination_table: Destination table name
            table_action_option: Table action option (e.g., "Append", "Overwrite")
            apply_v_order: Apply V-Order optimization
            timeout: Activity timeout
            retry: Number of retry attempts
            retry_interval_seconds: Retry interval in seconds
            source_access_mode: Source access mode ("direct" or "sql")
            source_sql_query: Optional SQL query for sql access mode
            
        Returns:
            Pipeline definition dictionary ready for encoding
        """
        logger.debug("Building Copy Activity definition")

        copy_activity = self._build_copy_activity(
            workspace_id=workspace_id,
            source_type=source_type,
            source_connection_id=source_connection_id,
            source_schema=source_schema,
            source_table=source_table,
            destination_lakehouse_id=destination_lakehouse_id,
            destination_connection_id=destination_connection_id,
            destination_table=destination_table,
            table_action_option=table_action_option,
            apply_v_order=apply_v_order,
            timeout=timeout,
            retry=retry,
            retry_interval_seconds=retry_interval_seconds,
            source_access_mode=source_access_mode,
            source_sql_query=source_sql_query,
        )

        definition = {
            "properties": {
                "activities": [copy_activity],
                "annotations": []
            }
        }
        
        logger.debug("Copy Activity definition built successfully")
        return definition
    
    def _get_source_dataset_type(self, source_type: str) -> str:
        """Map source type to dataset type.
        
        Args:
            source_type: Source type (e.g., "AzurePostgreSqlSource")
            
        Returns:
            Dataset type (e.g., "AzurePostgreSqlTable")
        """
        # Map common source types to dataset types
        type_mapping = {
            "AzurePostgreSqlSource": "AzurePostgreSqlTable",
            "AzureSqlSource": "AzureSqlTable",
            "SqlServerSource": "SqlServerTable",
            "MySqlSource": "MySqlTable",
            "OracleSource": "OracleTable",
            "LakehouseTableSource": "LakehouseTable",
        }
        
        # Try to get the dataset type from mapping, or derive it
        if source_type in type_mapping:
            return type_mapping[source_type]
        
        # Default derivation: replace "Source" with "Table"
        if source_type.endswith("Source"):
            return source_type.replace("Source", "Table")
        
        # Fallback: return as-is to allow newer/unknown source types
        return source_type

    def _validate_source_access_mode(
        self,
        source_access_mode: str,
        source_sql_query: Optional[str],
    ) -> None:
        if not source_access_mode:
            raise FabricValidationError(
                "source_access_mode",
                source_access_mode,
                "Source access mode cannot be empty",
            )

        if source_access_mode not in {"direct", "sql"}:
            raise FabricValidationError(
                "source_access_mode",
                source_access_mode,
                "Source access mode must be 'direct' or 'sql'",
            )

        if source_sql_query and source_access_mode != "sql":
            raise FabricValidationError(
                "source_sql_query",
                source_sql_query,
                "source_sql_query requires source_access_mode='sql'",
            )

    def _resolve_source_type(self, source_type: str, source_access_mode: str) -> str:
        if source_access_mode == "sql":
            return "AzureSqlSource"
        return source_type

    def _build_copy_activity(
        self,
        workspace_id: str,
        source_type: str,
        source_connection_id: str,
        source_schema: str,
        source_table: str,
        destination_lakehouse_id: str,
        destination_connection_id: str,
        destination_table: str,
        table_action_option: str,
        apply_v_order: bool,
        timeout: str,
        retry: int,
        retry_interval_seconds: int,
        source_access_mode: str = "direct",
        source_sql_query: Optional[str] = None,
    ) -> Dict[str, Any]:
        effective_source_type = self._resolve_source_type(source_type, source_access_mode)
        source_dataset_type = self._get_source_dataset_type(effective_source_type)

        include_schema = not (
            source_access_mode == "direct" and source_type == "LakehouseTableSource"
        )
        source_type_properties: Dict[str, Any] = {"table": source_table}
        if include_schema:
            source_type_properties["schema"] = source_schema

        source_settings: Dict[str, Any] = {
            "type": effective_source_type,
            "partitionOption": "None",
            "queryTimeout": "02:00:00",
            "datasetSettings": {
                "annotations": [],
                "type": source_dataset_type,
                "schema": [],
                "typeProperties": source_type_properties,
                "version": "2.0",
                "externalReferences": {
                    "connection": source_connection_id
                },
            },
        }
        if source_access_mode == "sql" and source_sql_query:
            source_settings["sqlReaderQuery"] = source_sql_query

        copy_activity = {
            "name": "CopyDataToLakehouse",
            "type": "Copy",
            "dependsOn": [],
            "policy": {
                "timeout": timeout,
                "retry": retry,
                "retryIntervalInSeconds": retry_interval_seconds,
                "secureOutput": False,
                "secureInput": False
            },
            "typeProperties": {
                "source": source_settings,
                "sink": {
                    "type": "LakehouseTableSink",
                    "tableActionOption": table_action_option,
                    "applyVOrder": apply_v_order,
                    "datasetSettings": {
                        "annotations": [],
                        "connectionSettings": {
                            "name": "DestinationLakehouse",
                            "properties": {
                                "annotations": [],
                                "type": "Lakehouse",
                                "typeProperties": {
                                    "workspaceId": workspace_id,
                                    "artifactId": destination_lakehouse_id,
                                    "rootFolder": "Tables"
                                },
                                "externalReferences": {
                                    "connection": destination_connection_id
                                }
                            }
                        },
                        "type": "LakehouseTable",
                        "schema": [],
                        "typeProperties": {
                            "schema": "dbo",
                            "table": destination_table
                        }
                    }
                },
                "enableStaging": False,
                "translator": {
                    "type": "TabularTranslator",
                    "typeConversion": True,
                    "typeConversionSettings": {
                        "allowDataTruncation": True,
                        "treatBooleanAsNumber": False
                    }
                }
            }
        }

        return copy_activity
    
    def _encode_definition(self, definition: Dict[str, Any]) -> str:
        """Encode pipeline definition to Base64 for API submission.
        
        Args:
            definition: Pipeline definition dictionary
            
        Returns:
            Base64-encoded JSON string
            
        Raises:
            FabricError: If encoding fails
        """
        try:
            # Convert to JSON string
            json_str = json.dumps(definition, indent=2)
            
            # Encode to Base64
            encoded = base64.b64encode(json_str.encode('utf-8')).decode('utf-8')
            
            logger.debug(f"Pipeline definition encoded: {len(encoded)} base64 characters")
            return encoded
            
        except Exception as exc:
            logger.error(f"Failed to encode pipeline definition: {exc}")
            raise FabricError(f"Failed to encode pipeline definition: {exc}")
    
    def _decode_definition(self, encoded_definition: str) -> Dict[str, Any]:
        """Decode Base64-encoded pipeline definition.
        
        Args:
            encoded_definition: Base64-encoded JSON string
            
        Returns:
            Pipeline definition dictionary
            
        Raises:
            FabricError: If decoding fails
        """
        try:
            # Decode from Base64
            decoded_bytes = base64.b64decode(encoded_definition)
            
            # Convert to dictionary
            definition = json.loads(decoded_bytes.decode('utf-8'))
            
            logger.debug("Pipeline definition decoded successfully")
            return definition
            
        except Exception as exc:
            logger.error(f"Failed to decode pipeline definition: {exc}")
            raise FabricError(f"Failed to decode pipeline definition: {exc}")
    
    def create_blank_pipeline(
        self,
        workspace_id: str,
        pipeline_name: str,
        description: Optional[str] = None
    ) -> str:
        """Create a blank Fabric pipeline with no activities.
        
        Creates a Data Pipeline in the specified workspace with an empty activities
        array, ready to be populated with activities later.
        
        Args:
            workspace_id: Workspace ID where pipeline will be created
            pipeline_name: Name for the new pipeline (must be unique in workspace)
            description: Optional description for the pipeline
            
        Returns:
            Pipeline ID (GUID) of the created pipeline
            
        Raises:
            FabricValidationError: If parameters are invalid
            FabricAPIError: If pipeline creation fails
            FabricError: For other errors
            
        Example:
            ```python
            pipeline_id = pipeline_service.create_blank_pipeline(
                workspace_id="12345678-1234-1234-1234-123456789abc",
                pipeline_name="My_New_Pipeline",
                description="A blank pipeline to be configured later"
            )
            ```
        """
        logger.info(f"Creating blank pipeline '{pipeline_name}' in workspace {workspace_id}")
        
        # Validate inputs
        if not pipeline_name or not pipeline_name.strip():
            raise FabricValidationError(
                "pipeline_name",
                "empty",
                "Pipeline name cannot be empty"
            )
        
        try:
            # Build blank pipeline definition
            pipeline_definition = {
                "properties": {
                    "activities": [],
                    "annotations": []
                }
            }
            
            # Encode definition to Base64
            encoded_definition = self._encode_definition(pipeline_definition)
            
            # Create item definition for API
            item_definition = {
                "displayName": pipeline_name,
                "type": "DataPipeline",
                "definition": {
                    "parts": [
                        {
                            "path": "pipeline-content.json",
                            "payload": encoded_definition,
                            "payloadType": "InlineBase64"
                        }
                    ]
                }
            }
            
            if description:
                item_definition["description"] = description
            
            # Create the pipeline item
            created_item = self.item_service.create_item(workspace_id, item_definition)
            
            logger.info(f"Successfully created blank pipeline with ID: {created_item.id}")
            return created_item.id
            
        except FabricValidationError:
            raise
        except FabricAPIError:
            raise
        except Exception as exc:
            logger.error(f"Failed to create blank pipeline: {exc}")
            raise FabricError(f"Failed to create blank pipeline: {exc}")
    
    def add_copy_activity_to_pipeline(
        self,
        workspace_id: str,
        pipeline_name: str,
        source_type: str,
        source_connection_id: str,
        source_schema: str,
        source_table: str,
        destination_lakehouse_id: str,
        destination_connection_id: str,
        destination_table: str,
        activity_name: Optional[str] = None,
        source_access_mode: str = "direct",
        source_sql_query: Optional[str] = None,
        table_action_option: str = "Append",
        apply_v_order: bool = True,
        timeout: str = "0.12:00:00",
        retry: int = 0,
        retry_interval_seconds: int = 30
    ) -> str:
        """Add a Copy Activity to an existing pipeline.
        
        Retrieves an existing pipeline, adds a Copy Activity to it, and updates
        the pipeline definition. The Copy Activity will be appended to any existing
        activities.
        
        Args:
            workspace_id: Workspace ID containing the pipeline
            pipeline_name: Name of the existing pipeline
            source_type: Type of source (e.g., "AzurePostgreSqlSource", "AzureSqlSource", etc.)
            source_connection_id: Fabric workspace connection ID for source
            source_schema: Schema name of the source table (e.g., "public")
            source_table: Name of the source table (e.g., "movie")
            destination_lakehouse_id: Workspace artifact ID of the destination Lakehouse
            destination_connection_id: Fabric workspace connection ID for destination
            destination_table: Name for the destination table in Lakehouse
            activity_name: Optional custom name for the activity (default: "CopyDataToLakehouse_{table}")
            source_access_mode: Source access mode ("direct" or "sql"). Default is "direct".
            source_sql_query: Optional SQL query for sql access mode.
            table_action_option: Table action option (default: "Append", options: "Append", "Overwrite")
            apply_v_order: Apply V-Order optimization (default: True)
            timeout: Activity timeout (default: "0.12:00:00")
            retry: Number of retry attempts (default: 0)
            retry_interval_seconds: Retry interval in seconds (default: 30)
            
        Returns:
            Pipeline ID (GUID) of the updated pipeline
            
        Raises:
            FabricValidationError: If parameters are invalid
            FabricItemNotFoundError: If pipeline not found
            FabricAPIError: If pipeline update fails
            FabricError: For other errors
            
        Example:
            ```python
            pipeline_id = pipeline_service.add_copy_activity_to_pipeline(
                workspace_id="12345678-1234-1234-1234-123456789abc",
                pipeline_name="My_Existing_Pipeline",
                source_type="AzurePostgreSqlSource",
                source_connection_id="conn-123",
                source_schema="public",
                source_table="movie",
                destination_lakehouse_id="lakehouse-456",
                destination_connection_id="dest-conn-123",
                destination_table="movie",
                activity_name="CopyMovieData"
            )
            ```
        """
        logger.info(
            f"Adding Copy Activity to pipeline '{pipeline_name}' in workspace {workspace_id}"
        )
        
        # Validate inputs
        self._validate_pipeline_inputs(
            pipeline_name,
            source_type,
            source_connection_id,
            source_schema,
            source_table,
            destination_lakehouse_id,
            destination_connection_id,
            destination_table
        )
        source_access_mode = source_access_mode.lower().strip() if source_access_mode else ""
        self._validate_source_access_mode(source_access_mode, source_sql_query)
        
        try:
            # Get existing pipeline
            pipeline = self.item_service.get_item_by_name(
                workspace_id, 
                pipeline_name, 
                "DataPipeline"
            )
            
            # Get pipeline definition
            definition_response = self.item_service.get_item_definition(
                workspace_id,
                pipeline.id
            )
            
            # Extract and decode the pipeline content
            parts = definition_response.get("definition", {}).get("parts", [])
            pipeline_content_part = None
            for part in parts:
                if part.get("path") == "pipeline-content.json":
                    pipeline_content_part = part
                    break
            
            if not pipeline_content_part:
                raise FabricError("Pipeline definition missing pipeline-content.json part")
            
            # Decode the existing definition
            encoded_payload = pipeline_content_part.get("payload", "")
            existing_definition = self._decode_definition(encoded_payload)
            
            # Generate activity name if not provided
            if not activity_name:
                activity_name = f"CopyDataToLakehouse_{destination_table}"

            copy_activity = self._build_copy_activity(
                workspace_id=workspace_id,
                source_type=source_type,
                source_connection_id=source_connection_id,
                source_schema=source_schema,
                source_table=source_table,
                destination_lakehouse_id=destination_lakehouse_id,
                destination_connection_id=destination_connection_id,
                destination_table=destination_table,
                table_action_option=table_action_option,
                apply_v_order=apply_v_order,
                timeout=timeout,
                retry=retry,
                retry_interval_seconds=retry_interval_seconds,
                source_access_mode=source_access_mode,
                source_sql_query=source_sql_query,
            )
            copy_activity["name"] = activity_name
            
            # Add the Copy Activity to existing activities
            if "properties" not in existing_definition:
                existing_definition["properties"] = {}
            if "activities" not in existing_definition["properties"]:
                existing_definition["properties"]["activities"] = []
            
            existing_definition["properties"]["activities"].append(copy_activity)
            
            # Encode updated definition
            encoded_definition = self._encode_definition(existing_definition)
            
            # Update the pipeline using updateDefinition endpoint
            update_payload = {
                "definition": {
                    "parts": [
                        {
                            "path": "pipeline-content.json",
                            "payload": encoded_definition,
                            "payloadType": "InlineBase64"
                        }
                    ]
                }
            }
            
            self.client.make_api_request(
                "POST",
                f"workspaces/{workspace_id}/items/{pipeline.id}/updateDefinition",
                payload=update_payload
            )
            
            logger.info(
                f"Successfully added Copy Activity '{activity_name}' to pipeline {pipeline.id}"
            )
            return pipeline.id
            
        except FabricValidationError:
            raise
        except FabricItemNotFoundError:
            raise
        except FabricAPIError:
            raise
        except Exception as exc:
            logger.error(f"Failed to add Copy Activity to pipeline: {exc}")
            raise FabricError(f"Failed to add Copy Activity to pipeline: {exc}")

    def add_notebook_activity_to_pipeline(
        self,
        workspace_id: str,
        pipeline_name: str,
        notebook_name: str,
        activity_name: Optional[str] = None,
        notebook_workspace_id: Optional[str] = None,
        depends_on_activity_name: Optional[str] = None,
        session_tag: Optional[str] = None,
        parameters: Optional[Dict[str, Any]] = None,
        timeout: str = "0.12:00:00",
        retry: int = 0,
        retry_interval_seconds: int = 30,
    ) -> str:
        """Add a Notebook Activity to an existing pipeline.

        Retrieves an existing pipeline, adds a Notebook Activity to it, and updates
        the pipeline definition. The Notebook Activity will be appended to any existing
        activities.

        Args:
            workspace_id: Workspace ID containing the pipeline
            pipeline_name: Name of the existing pipeline
            notebook_name: Name of the notebook to add
            activity_name: Name for the notebook activity
            notebook_workspace_id: Workspace ID containing the notebook (optional). Defaults to pipeline workspace.
            depends_on_activity_name: Optional name of an existing activity this one depends on
            session_tag: Optional session tag for the notebook execution
            parameters: Optional parameters to pass to the notebook
            timeout: Activity timeout (default: "0.12:00:00")
            retry: Number of retry attempts (default: 0)
            retry_interval_seconds: Retry interval in seconds (default: 30)

        Returns:
            Pipeline ID (GUID) of the updated pipeline
        """
        logger.info(
            f"Adding Notebook Activity to pipeline '{pipeline_name}' in workspace {workspace_id}"
        )

        activity_name = activity_name or f"RunNotebook_{notebook_name}"
        notebook_workspace_id = notebook_workspace_id or workspace_id

        self._validate_notebook_activity_inputs(
            workspace_id, pipeline_name, notebook_name, activity_name
        )

        try:
            pipeline, definition = self._get_pipeline_definition(
                workspace_id, pipeline_name
            )
            notebook = self.item_service.get_item_by_name(
                notebook_workspace_id, notebook_name, "Notebook"
            )

            type_properties = {
                "notebookId": notebook.id,
                "workspaceId": notebook_workspace_id,
                "parameters": parameters or {},
            }
            if session_tag is not None:
                type_properties["sessionTag"] = session_tag

            activity = {
                "name": activity_name,
                "type": "TridentNotebook",
                "dependsOn": [],
                "policy": {
                    "timeout": timeout,
                    "retry": retry,
                    "retryIntervalInSeconds": retry_interval_seconds,
                    "secureOutput": False,
                    "secureInput": False,
                },
                "typeProperties": type_properties,
            }

            self._append_activity_to_definition(
                definition, activity, depends_on_activity_name
            )
            self._update_pipeline_definition(workspace_id, pipeline.id, definition)

            logger.info(
                f"Successfully added Notebook Activity '{activity_name}' to pipeline {pipeline.id}"
            )
            return pipeline.id

        except FabricValidationError:
            raise
        except FabricItemNotFoundError:
            raise
        except FabricAPIError:
            raise
        except Exception as exc:
            logger.error(f"Failed to add Notebook Activity to pipeline: {exc}")
            raise FabricError(f"Failed to add Notebook Activity to pipeline: {exc}")

    def add_dataflow_activity_to_pipeline(
        self,
        workspace_id: str,
        pipeline_name: str,
        dataflow_name: str,
        activity_name: Optional[str] = None,
        dataflow_workspace_id: Optional[str] = None,
        depends_on_activity_name: Optional[str] = None,
        timeout: str = "0.12:00:00",
        retry: int = 0,
        retry_interval_seconds: int = 30,
    ) -> str:
        """Add a Dataflow Activity to an existing pipeline.

        Retrieves an existing pipeline, adds a Dataflow Activity to it, and updates
        the pipeline definition. The Dataflow Activity will be appended to any existing
        activities.

        Args:
            workspace_id: Workspace ID containing the pipeline
            pipeline_name: Name of the existing pipeline
            dataflow_name: Name of the dataflow to add
            activity_name: Name for the dataflow activity
            dataflow_workspace_id: Workspace ID containing the dataflow (optional). Defaults to pipeline workspace.
            depends_on_activity_name: Optional name of an existing activity this one depends on
            timeout: Activity timeout (default: "0.12:00:00")
            retry: Number of retry attempts (default: 0)
            retry_interval_seconds: Retry interval in seconds (default: 30)

        Returns:
            Pipeline ID (GUID) of the updated pipeline
        """
        logger.info(
            f"Adding Dataflow Activity to pipeline '{pipeline_name}' in workspace {workspace_id}"
        )

        activity_name = activity_name or f"RunDataflow_{dataflow_name}"
        dataflow_workspace_id = dataflow_workspace_id or workspace_id

        self._validate_dataflow_activity_inputs(
            workspace_id, pipeline_name, dataflow_name, activity_name
        )

        try:
            pipeline, definition = self._get_pipeline_definition(
                workspace_id, pipeline_name
            )
            dataflow = self.item_service.get_item_by_name(
                dataflow_workspace_id, dataflow_name, "Dataflow"
            )

            activity = {
                "name": activity_name,
                "type": "RefreshDataflow",
                "dependsOn": [],
                "policy": {
                    "timeout": timeout,
                    "retry": retry,
                    "retryIntervalInSeconds": retry_interval_seconds,
                    "secureOutput": False,
                    "secureInput": False,
                },
                "typeProperties": {
                    "dataflowId": dataflow.id,
                    "workspaceId": dataflow_workspace_id,
                    "notifyOption": "NoNotification",
                    "dataflowType": "Dataflow-Gen2",
                },
            }

            self._append_activity_to_definition(
                definition, activity, depends_on_activity_name
            )
            self._update_pipeline_definition(workspace_id, pipeline.id, definition)

            logger.info(
                f"Successfully added Dataflow Activity '{activity_name}' to pipeline {pipeline.id}"
            )
            return pipeline.id

        except FabricValidationError:
            raise
        except FabricItemNotFoundError:
            raise
        except FabricAPIError:
            raise
        except Exception as exc:
            logger.error(f"Failed to add Dataflow Activity to pipeline: {exc}")
            raise FabricError(f"Failed to add Dataflow Activity to pipeline: {exc}")
    
    def add_activity_from_json(
        self,
        workspace_id: str,
        pipeline_name: str,
        activity_json: Dict[str, Any]
    ) -> str:
        """Add a generic activity to an existing pipeline from a JSON template.
        
        Retrieves an existing pipeline, adds an activity from the provided JSON template,
        and updates the pipeline definition. The activity will be appended to any existing
        activities. This is a more general-purpose method compared to add_copy_activity_to_pipeline,
        allowing you to add any type of Fabric pipeline activity by providing its JSON definition.
        
        Args:
            workspace_id: Workspace ID containing the pipeline
            pipeline_name: Name of the existing pipeline
            activity_json: JSON dictionary representing the complete activity definition.
                          Must include "name", "type", and all required properties for the activity type.
                          Example:
                          {
                              "name": "MyActivity",
                              "type": "Copy",
                              "dependsOn": [],
                              "policy": {...},
                              "typeProperties": {...}
                          }
            
        Returns:
            Pipeline ID (GUID) of the updated pipeline
            
        Raises:
            FabricValidationError: If activity_json is invalid or missing required fields
            FabricItemNotFoundError: If pipeline not found
            FabricAPIError: If pipeline update fails
            FabricError: For other errors
            
        Example:
            ```python
            # Define a Copy Activity as JSON
            copy_activity = {
                "name": "CopyCustomData",
                "type": "Copy",
                "dependsOn": [],
                "policy": {
                    "timeout": "0.12:00:00",
                    "retry": 0,
                    "retryIntervalInSeconds": 30,
                    "secureOutput": False,
                    "secureInput": False
                },
                "typeProperties": {
                    "source": {
                        "type": "AzurePostgreSqlSource",
                        "queryTimeout": "02:00:00",
                        "datasetSettings": {...}
                    },
                    "sink": {
                        "type": "LakehouseTableSink",
                        "tableActionOption": "Append",
                        "datasetSettings": {...}
                    }
                }
            }
            
            pipeline_id = pipeline_service.add_activity_from_json(
                workspace_id="12345678-1234-1234-1234-123456789abc",
                pipeline_name="My_Existing_Pipeline",
                activity_json=copy_activity
            )
            ```
        """
        logger.info(
            f"Adding activity from JSON to pipeline '{pipeline_name}' in workspace {workspace_id}"
        )
        
        # Validate activity JSON structure
        if not isinstance(activity_json, dict):
            raise FabricValidationError(
                "activity_json",
                "invalid_type",
                "activity_json must be a dictionary"
            )
        
        if "name" not in activity_json or not activity_json["name"]:
            raise FabricValidationError(
                "activity_json",
                "missing_name",
                "activity_json must include a 'name' field"
            )
        
        if "type" not in activity_json or not activity_json["type"]:
            raise FabricValidationError(
                "activity_json",
                "missing_type",
                "activity_json must include a 'type' field"
            )
        
        activity_name = activity_json.get("name", "UnnamedActivity")
        activity_type = activity_json.get("type", "Unknown")
        
        try:
            # Get existing pipeline
            pipeline = self.item_service.get_item_by_name(
                workspace_id, 
                pipeline_name, 
                "DataPipeline"
            )
            
            # Get pipeline definition
            definition_response = self.item_service.get_item_definition(
                workspace_id,
                pipeline.id
            )
            
            # Extract and decode the pipeline content
            parts = definition_response.get("definition", {}).get("parts", [])
            pipeline_content_part = None
            for part in parts:
                if part.get("path") == "pipeline-content.json":
                    pipeline_content_part = part
                    break
            
            if not pipeline_content_part:
                raise FabricError("Pipeline definition missing pipeline-content.json part")
            
            # Decode the existing definition
            encoded_payload = pipeline_content_part.get("payload", "")
            existing_definition = self._decode_definition(encoded_payload)
            
            # Add the activity to existing activities
            if "properties" not in existing_definition:
                existing_definition["properties"] = {}
            if "activities" not in existing_definition["properties"]:
                existing_definition["properties"]["activities"] = []
            
            # Append the activity from JSON
            existing_definition["properties"]["activities"].append(activity_json)
            
            # Encode updated definition
            encoded_definition = self._encode_definition(existing_definition)
            
            # Update the pipeline using updateDefinition endpoint
            update_payload = {
                "definition": {
                    "parts": [
                        {
                            "path": "pipeline-content.json",
                            "payload": encoded_definition,
                            "payloadType": "InlineBase64"
                        }
                    ]
                }
            }
            
            self.client.make_api_request(
                "POST",
                f"workspaces/{workspace_id}/items/{pipeline.id}/updateDefinition",
                payload=update_payload
            )
            
            logger.info(
                f"Successfully added {activity_type} activity '{activity_name}' to pipeline {pipeline.id}"
            )
            return pipeline.id
            
        except FabricValidationError:
            raise
        except FabricItemNotFoundError:
            raise
        except FabricAPIError:
            raise
        except Exception as exc:
            logger.error(f"Failed to add activity from JSON to pipeline: {exc}")
            raise FabricError(f"Failed to add activity from JSON to pipeline: {exc}")

    def delete_activity_from_pipeline(
        self,
        workspace_id: str,
        pipeline_name: str,
        activity_name: str,
    ) -> str:
        """Delete an activity from an existing pipeline.

        Args:
            workspace_id: Workspace ID containing the pipeline
            pipeline_name: Name of the existing pipeline
            activity_name: Name of the activity to delete

        Returns:
            Pipeline ID (GUID) of the updated pipeline

        Raises:
            FabricValidationError: If activity not found or has dependencies
            FabricItemNotFoundError: If pipeline not found
            FabricAPIError: If pipeline update fails
            FabricError: For other errors
        """
        logger.info(
            f"Deleting activity '{activity_name}' from pipeline '{pipeline_name}' "
            f"in workspace {workspace_id}"
        )

        self._validate_activity_operation_inputs(
            workspace_id, pipeline_name, activity_name
        )

        try:
            pipeline, definition = self._get_pipeline_definition(
                workspace_id, pipeline_name
            )
            activities = self._get_pipeline_activities(definition)

            matching_indices = [
                index
                for index, activity in enumerate(activities)
                if activity.get("name") == activity_name
            ]
            if not matching_indices:
                raise FabricValidationError(
                    "activity_name",
                    activity_name,
                    "Activity not found in pipeline",
                )

            dependents = self._find_dependent_activities(activities, activity_name)
            if dependents:
                dependent_list = ", ".join(dependents)
                raise FabricValidationError(
                    "activity_name",
                    activity_name,
                    "Activity is referenced by dependsOn in: "
                    f"{dependent_list}. Remove dependencies first.",
                )

            activities[:] = [
                activity
                for activity in activities
                if activity.get("name") != activity_name
            ]
            self._update_pipeline_definition(workspace_id, pipeline.id, definition)

            logger.info(
                f"Successfully deleted {len(matching_indices)} activities named "
                f"'{activity_name}' from pipeline {pipeline.id}"
            )
            return pipeline.id

        except FabricValidationError:
            raise
        except FabricItemNotFoundError:
            raise
        except FabricAPIError:
            raise
        except Exception as exc:
            logger.error(f"Failed to delete activity from pipeline: {exc}")
            raise FabricError(f"Failed to delete activity from pipeline: {exc}")

    def remove_activity_dependency(
        self,
        workspace_id: str,
        pipeline_name: str,
        activity_name: str,
        from_activity_name: Optional[str] = None,
    ) -> tuple[str, int]:
        """Remove dependsOn references to a target activity.

        Args:
            workspace_id: Workspace ID containing the pipeline
            pipeline_name: Name of the existing pipeline
            activity_name: Target activity name to remove dependencies to
            from_activity_name: Optional source activity name to limit removal

        Returns:
            Tuple of (pipeline ID, removed dependency count)

        Raises:
            FabricValidationError: If activity not found or no dependencies removed
            FabricItemNotFoundError: If pipeline not found
            FabricAPIError: If pipeline update fails
            FabricError: For other errors
        """
        logger.info(
            f"Removing dependencies on '{activity_name}' in pipeline '{pipeline_name}' "
            f"from activity '{from_activity_name or 'ALL'}'"
        )

        self._validate_activity_operation_inputs(
            workspace_id, pipeline_name, activity_name
        )
        if from_activity_name is not None and not str(from_activity_name).strip():
            raise FabricValidationError(
                "from_activity_name",
                str(from_activity_name),
                "From activity name cannot be empty",
            )

        try:
            pipeline, definition = self._get_pipeline_definition(
                workspace_id, pipeline_name
            )
            activities = self._get_pipeline_activities(definition)
            activity_names = {
                activity.get("name") for activity in activities if activity.get("name")
            }

            if from_activity_name:
                if from_activity_name not in activity_names:
                    raise FabricValidationError(
                        "from_activity_name",
                        from_activity_name,
                        "From activity not found in pipeline",
                    )
                source_activity = next(
                    activity
                    for activity in activities
                    if activity.get("name") == from_activity_name
                )
                removed_count = self._remove_dependency_from_activity(
                    source_activity, activity_name
                )
            else:
                removed_count = 0
                for activity in activities:
                    removed_count += self._remove_dependency_from_activity(
                        activity, activity_name
                    )

            if removed_count == 0:
                raise FabricValidationError(
                    "activity_name",
                    activity_name,
                    "No dependencies found to remove",
                )

            self._update_pipeline_definition(workspace_id, pipeline.id, definition)

            logger.info(
                f"Removed {removed_count} dependencies on '{activity_name}' "
                f"from pipeline {pipeline.id}"
            )
            return pipeline.id, removed_count

        except FabricValidationError:
            raise
        except FabricItemNotFoundError:
            raise
        except FabricAPIError:
            raise
        except Exception as exc:
            logger.error(f"Failed to remove activity dependencies: {exc}")
            raise FabricError(f"Failed to remove activity dependencies: {exc}")

    def _validate_notebook_activity_inputs(
        self,
        workspace_id: str,
        pipeline_name: str,
        notebook_name: str,
        activity_name: str,
    ) -> None:
        if not workspace_id or not str(workspace_id).strip():
            raise FabricValidationError(
                "workspace_id", str(workspace_id), "Workspace ID cannot be empty"
            )
        if not pipeline_name or not pipeline_name.strip():
            raise FabricValidationError(
                "pipeline_name", pipeline_name, "Pipeline name cannot be empty"
            )
        if not notebook_name or not notebook_name.strip():
            raise FabricValidationError(
                "notebook_name", notebook_name, "Notebook name cannot be empty"
            )
        if not activity_name or not activity_name.strip():
            raise FabricValidationError(
                "activity_name", activity_name, "Activity name cannot be empty"
            )

    def _validate_activity_operation_inputs(
        self,
        workspace_id: str,
        pipeline_name: str,
        activity_name: str,
    ) -> None:
        if not workspace_id or not str(workspace_id).strip():
            raise FabricValidationError(
                "workspace_id", str(workspace_id), "Workspace ID cannot be empty"
            )
        if not pipeline_name or not pipeline_name.strip():
            raise FabricValidationError(
                "pipeline_name", pipeline_name, "Pipeline name cannot be empty"
            )
        if not activity_name or not activity_name.strip():
            raise FabricValidationError(
                "activity_name", activity_name, "Activity name cannot be empty"
            )

    def _validate_dataflow_activity_inputs(
        self,
        workspace_id: str,
        pipeline_name: str,
        dataflow_name: str,
        activity_name: str,
    ) -> None:
        if not workspace_id or not str(workspace_id).strip():
            raise FabricValidationError(
                "workspace_id", str(workspace_id), "Workspace ID cannot be empty"
            )
        if not pipeline_name or not pipeline_name.strip():
            raise FabricValidationError(
                "pipeline_name", pipeline_name, "Pipeline name cannot be empty"
            )
        if not dataflow_name or not dataflow_name.strip():
            raise FabricValidationError(
                "dataflow_name", dataflow_name, "Dataflow name cannot be empty"
            )
        if not activity_name or not activity_name.strip():
            raise FabricValidationError(
                "activity_name", activity_name, "Activity name cannot be empty"
            )

    def _get_pipeline_definition(
        self, workspace_id: str, pipeline_name: str
    ) -> tuple[Any, Dict[str, Any]]:
        pipeline = self.item_service.get_item_by_name(
            workspace_id, pipeline_name, "DataPipeline"
        )
        definition_response = self.item_service.get_item_definition(
            workspace_id, pipeline.id
        )

        parts = definition_response.get("definition", {}).get("parts", [])
        pipeline_content_part = None
        for part in parts:
            if part.get("path") == "pipeline-content.json":
                pipeline_content_part = part
                break

        if not pipeline_content_part:
            raise FabricError("Pipeline definition missing pipeline-content.json part")

        encoded_payload = pipeline_content_part.get("payload", "")
        existing_definition = self._decode_definition(encoded_payload)

        return pipeline, existing_definition

    def _get_pipeline_activities(self, definition: Dict[str, Any]) -> list:
        if "properties" not in definition:
            definition["properties"] = {}
        if "activities" not in definition["properties"]:
            definition["properties"]["activities"] = []
        return definition["properties"]["activities"]

    def _find_activity_index(
        self, activities: list, activity_name: str
    ) -> Optional[int]:
        for index, activity in enumerate(activities):
            if activity.get("name") == activity_name:
                return index
        return None

    def _find_dependent_activities(
        self, activities: list, activity_name: str
    ) -> list:
        dependents = []
        for activity in activities:
            for dependency in activity.get("dependsOn", []) or []:
                if dependency.get("activity") == activity_name:
                    dependents.append(activity.get("name") or "UnnamedActivity")
                    break
        return dependents

    def _remove_dependency_from_activity(
        self,
        activity: Dict[str, Any],
        target_name: str,
    ) -> int:
        depends_on = activity.get("dependsOn")
        if not depends_on or not isinstance(depends_on, list):
            return 0
        original_len = len(depends_on)
        filtered = [
            dependency
            for dependency in depends_on
            if dependency.get("activity") != target_name
        ]
        if len(filtered) != original_len:
            activity["dependsOn"] = filtered
        return original_len - len(filtered)

    def _append_activity_to_definition(
        self,
        definition: Dict[str, Any],
        activity: Dict[str, Any],
        depends_on_activity_name: Optional[str] = None,
    ) -> None:
        if "properties" not in definition:
            definition["properties"] = {}
        if "activities" not in definition["properties"]:
            definition["properties"]["activities"] = []

        activities = definition["properties"]["activities"]
        existing_names = {item.get("name") for item in activities if item.get("name")}

        if activity.get("name") in existing_names:
            raise FabricValidationError(
                "activity_name",
                activity.get("name", ""),
                "Activity name already exists in pipeline",
            )

        if depends_on_activity_name:
            if depends_on_activity_name not in existing_names:
                raise FabricValidationError(
                    "depends_on_activity_name",
                    depends_on_activity_name,
                    "Dependent activity does not exist in pipeline",
                )
            activity["dependsOn"].append(
                {
                    "activity": depends_on_activity_name,
                    "dependencyConditions": ["Succeeded"],
                }
            )

        activities.append(activity)

    def _update_pipeline_definition(
        self, workspace_id: str, pipeline_id: str, definition: Dict[str, Any]
    ) -> None:
        encoded_definition = self._encode_definition(definition)
        update_payload = {
            "definition": {
                "parts": [
                    {
                        "path": "pipeline-content.json",
                        "payload": encoded_definition,
                        "payloadType": "InlineBase64",
                    }
                ]
            }
        }

        self.client.make_api_request(
            "POST",
            f"workspaces/{workspace_id}/items/{pipeline_id}/updateDefinition",
            payload=update_payload,
        )
