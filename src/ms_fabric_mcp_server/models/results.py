# ABOUTME: Result models for Fabric operations.
# ABOUTME: Provides structured result types for notebook, job, SQL, and other operations.
"""Result models for Fabric operations."""

from typing import Optional, Dict, Any, List
from pydantic import BaseModel, ConfigDict, Field

from .item import FabricItem
from .job import FabricJob


class FabricOperationResult(BaseModel):
    """Base result model for all Fabric operations.
    
    All Fabric operation results inherit from this model to provide
    consistent status and message fields.
    
    Attributes:
        status: Operation status - "success" or "error"
        message: Human-readable message about the operation result
    """
    
    status: str = Field(description="Operation status: 'success' or 'error'")
    message: Optional[str] = Field(default=None, description="Human-readable message")
    
    model_config = ConfigDict(from_attributes=True)


class ImportNotebookResult(FabricOperationResult):
    """Result of notebook import operation.
    
    Attributes:
        status: Operation status
        message: Result message
        artifact_id: Fabric artifact ID if notebook was created successfully
    
    Example:
        ```python
        result = ImportNotebookResult(
            status="success",
            message="Notebook imported successfully",
            artifact_id="abc-123-def"
        )
        ```
    """
    
    artifact_id: Optional[str] = Field(default=None, description="Fabric artifact ID if created")
    
    model_config = ConfigDict(from_attributes=True)


class AttachLakehouseResult(FabricOperationResult):
    """Result of attaching a default lakehouse to a notebook.
    
    Attributes:
        status: Operation status
        message: Result message
        notebook_id: Fabric notebook ID
        notebook_name: Display name of the notebook
        lakehouse_id: ID of the attached lakehouse
        lakehouse_name: Display name of the attached lakehouse
        workspace_id: Workspace ID containing the notebook
    
    Example:
        ```python
        result = AttachLakehouseResult(
            status="success",
            message="Lakehouse attached successfully",
            notebook_id="abc-123",
            notebook_name="My Notebook",
            lakehouse_id="def-456",
            lakehouse_name="My Lakehouse",
            workspace_id="ghi-789"
        )
        ```
    """
    
    notebook_id: Optional[str] = Field(default=None, description="Fabric notebook ID")
    notebook_name: Optional[str] = Field(default=None, description="Display name of the notebook")
    lakehouse_id: Optional[str] = Field(default=None, description="ID of the attached lakehouse")
    lakehouse_name: Optional[str] = Field(default=None, description="Display name of the attached lakehouse")
    workspace_id: Optional[str] = Field(default=None, description="Workspace ID containing the notebook")
    
    model_config = ConfigDict(from_attributes=True)


class ExecuteNotebookResult(FabricOperationResult):
    """Result of notebook execution operation.
    
    Attributes:
        status: Operation status
        message: Result message
        job_instance_id: UUID of the notebook-run instance
        item_id: UUID of the notebook item
        job_type: Type of job
        invoke_type: How the job was invoked
        job_status: Current job status
        root_activity_id: Root activity ID for tracing
        start_time_utc: Job start time in UTC
        end_time_utc: Job end time in UTC
        failure_reason: Error details when job failed
        final_state: Deprecated - use job_status instead
        started_utc: Deprecated - use start_time_utc instead
        finished_utc: Deprecated - use end_time_utc instead
    """
    
    job_instance_id: Optional[str] = Field(default=None, description="UUID of the notebook-run instance")
    item_id: Optional[str] = Field(default=None, description="UUID of the notebook item")
    job_type: Optional[str] = Field(default=None, description="Type of job")
    invoke_type: Optional[str] = Field(default=None, description="How the job was invoked")
    job_status: Optional[str] = Field(default=None, description="Current job status")
    root_activity_id: Optional[str] = Field(default=None, description="Root activity ID")
    start_time_utc: Optional[str] = Field(default=None, description="Job start time in UTC")
    end_time_utc: Optional[str] = Field(default=None, description="Job end time in UTC")
    failure_reason: Optional[Dict[str, Any]] = Field(default=None, description="Error details when job failed")
    
    # Legacy fields for backward compatibility
    final_state: Optional[str] = Field(default=None, description="Deprecated: use job_status instead")
    started_utc: Optional[str] = Field(default=None, description="Deprecated: use start_time_utc instead")
    finished_utc: Optional[str] = Field(default=None, description="Deprecated: use end_time_utc instead")
    
    model_config = ConfigDict(from_attributes=True)


class CreateItemResult(FabricOperationResult):
    """Result of item creation operation.
    
    Attributes:
        status: Operation status
        message: Result message
        item_id: ID of the created item
        item: Created item details
    """
    
    item_id: Optional[str] = Field(default=None, description="ID of the created item")
    item: Optional[FabricItem] = Field(default=None, description="Created item details")
    
    model_config = ConfigDict(from_attributes=True)


class QueryResult(FabricOperationResult):
    """Result of SQL query execution.
    
    Attributes:
        status: Operation status
        message: Result message
        data: Query result rows (list of dictionaries)
        columns: Column names
        row_count: Number of rows returned
    
    Example:
        ```python
        result = QueryResult(
            status="success",
            message="Query executed successfully",
            data=[{"name": "Alice", "age": 30}, {"name": "Bob", "age": 25}],
            columns=["name", "age"],
            row_count=2
        )
        ```
    """
    
    data: List[Dict[str, Any]] = Field(default_factory=list, description="Query result rows")
    columns: List[str] = Field(default_factory=list, description="Column names")
    row_count: int = Field(default=0, description="Number of rows returned")
    
    model_config = ConfigDict(from_attributes=True)


class RunJobRequest(BaseModel):
    """Request payload for running on-demand jobs.
    
    Attributes:
        execution_data: Payload for run on-demand job request
    """
    
    execution_data: Optional[Dict[str, Any]] = Field(
        default=None,
        description="Payload for run on-demand job request"
    )
    
    model_config = ConfigDict(from_attributes=True)


class RunJobResult(FabricOperationResult):
    """Result of running an on-demand job.
    
    Attributes:
        status: Operation status
        message: Result message
        job_instance_id: ID of the created job instance
        location_url: Location URL to poll for job status
        retry_after: Suggested retry interval in seconds
    """
    
    job_instance_id: Optional[str] = Field(default=None, description="ID of the created job instance")
    location_url: Optional[str] = Field(default=None, description="Location URL to poll for job status")
    retry_after: Optional[int] = Field(default=None, description="Suggested retry interval in seconds")
    
    model_config = ConfigDict(from_attributes=True)


class JobStatusResult(FabricOperationResult):
    """Result of job status query.
    
    Attributes:
        status: Operation status
        message: Result message
        job: Job details and current status
    """
    
    job: Optional[FabricJob] = Field(default=None, description="Job details and current status")
    
    model_config = ConfigDict(from_attributes=True)


class OperationResult(FabricOperationResult):
    """Result of long-running operation query.
    
    Attributes:
        status: Operation status
        message: Result message
        operation_id: Operation ID
        result: Operation result data
    """
    
    operation_id: str = Field(description="Operation ID")
    result: Optional[Dict[str, Any]] = Field(default=None, description="Operation result data")
    
    model_config = ConfigDict(from_attributes=True)
