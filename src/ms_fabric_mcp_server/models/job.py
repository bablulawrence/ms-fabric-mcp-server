# ABOUTME: Job-related data models for Microsoft Fabric.
# ABOUTME: Provides FabricJob model for job execution representation and status tracking.
"""Job-related data models for Microsoft Fabric."""

from typing import Optional, Dict, Any
from pydantic import BaseModel, ConfigDict, Field


class FabricJob(BaseModel):
    """Fabric job execution model.
    
    Represents a job instance for running notebooks, pipelines, or other
    executable items in Microsoft Fabric.
    
    Attributes:
        job_instance_id: Unique identifier for the job instance (GUID)
        item_id: ID of the item being executed
        job_type: Type of job (RunNotebook, Pipeline, etc.)
        status: Current job status (NotStarted, InProgress, Completed, Failed, etc.)
        invoke_type: How the job was invoked (Manual, Scheduled, etc.)
        root_activity_id: Root activity ID for tracing
        start_time_utc: Job start time in UTC (ISO 8601 format)
        end_time_utc: Job end time in UTC (ISO 8601 format)
        failure_reason: Error details if job failed
        parameters: Job execution parameters
    
    Example:
        ```python
        job = FabricJob(
            job_instance_id="job-123-abc",
            item_id="notebook-456",
            job_type="RunNotebook",
            status="Completed"
        )
        
        if job.is_successful():
            print("Job completed successfully!")
        ```
    """
    
    job_instance_id: str = Field(description="Unique identifier for the job instance")
    item_id: str = Field(description="ID of the item being executed")
    job_type: str = Field(description="Type of job (RunNotebook, etc.)")
    status: str = Field(description="Current job status")
    invoke_type: Optional[str] = Field(default=None, description="How the job was invoked")
    root_activity_id: Optional[str] = Field(default=None, description="Root activity ID for tracing")
    start_time_utc: Optional[str] = Field(default=None, description="Job start time in UTC")
    end_time_utc: Optional[str] = Field(default=None, description="Job end time in UTC")
    failure_reason: Optional[Dict[str, Any]] = Field(default=None, description="Error details if job failed")
    parameters: Optional[Dict[str, Any]] = Field(default=None, description="Job execution parameters")
    
    model_config = ConfigDict(from_attributes=True)
    
    def is_terminal_state(self) -> bool:
        """Check if job is in a terminal state.
        
        Returns:
            True if job has finished (completed, failed, cancelled, etc.)
        """
        terminal_states = {"Completed", "Failed", "Cancelled", "Deduped"}
        return self.status in terminal_states
    
    def is_successful(self) -> bool:
        """Check if job completed successfully.
        
        Returns:
            True if job status is "Completed"
        """
        return self.status == "Completed"
    
    def is_failed(self) -> bool:
        """Check if job failed.
        
        Returns:
            True if job status is "Failed"
        """
        return self.status == "Failed"
    
    def is_running(self) -> bool:
        """Check if job is currently running.
        
        Returns:
            True if job is not yet started or in progress
        """
        running_states = {"NotStarted", "InProgress"}
        return self.status in running_states
