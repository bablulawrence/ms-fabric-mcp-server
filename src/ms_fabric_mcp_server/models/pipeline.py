# ABOUTME: Pipeline-related data models for Microsoft Fabric.
# ABOUTME: Provides PipelineActivityRun model for activity execution details.
"""Pipeline-related data models for Microsoft Fabric."""

from typing import Optional

from pydantic import BaseModel, ConfigDict, Field


class PipelineActivityRun(BaseModel):
    """Pipeline activity run details.

    Represents execution details for a single activity within a pipeline run,
    including timing, status, and output metrics.

    Attributes:
        activity_name: Name of the activity
        activity_type: Type of activity (Copy, Notebook, etc.)
        status: Execution status (Succeeded, Failed, InProgress, Cancelled)
        duration_ms: Duration in milliseconds
        start_time: Activity start time in UTC (ISO 8601)
        end_time: Activity end time in UTC (ISO 8601)
        rows_read: Number of rows read (Copy activities only)
        rows_written: Number of rows written (Copy activities only)
        error_message: Error message if activity failed

    Example:
        ```python
        activity = PipelineActivityRun(
            activity_name="CopyProducts",
            activity_type="Copy",
            status="Succeeded",
            duration_ms=5230,
            start_time="2026-02-03T13:00:00Z",
            end_time="2026-02-03T13:00:05Z",
            rows_read=1234,
            rows_written=1234
        )
        ```
    """

    activity_name: str = Field(description="Name of the activity")
    activity_type: str = Field(description="Type of activity (Copy, Notebook, etc.)")
    status: str = Field(description="Execution status")
    duration_ms: Optional[int] = Field(
        default=None, description="Duration in milliseconds"
    )
    start_time: Optional[str] = Field(
        default=None, description="Activity start time (UTC)"
    )
    end_time: Optional[str] = Field(default=None, description="Activity end time (UTC)")
    rows_read: Optional[int] = Field(default=None, description="Rows read (Copy only)")
    rows_written: Optional[int] = Field(
        default=None, description="Rows written (Copy only)"
    )
    error_message: Optional[str] = Field(
        default=None, description="Error message if failed"
    )

    model_config = ConfigDict(from_attributes=True)

    def is_successful(self) -> bool:
        """Check if activity completed successfully."""
        return self.status == "Succeeded"

    def is_failed(self) -> bool:
        """Check if activity failed."""
        return self.status == "Failed"
