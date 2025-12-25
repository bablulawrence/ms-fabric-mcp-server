"""Unit tests for FabricJobService."""

from unittest.mock import Mock, patch

import pytest

from ms_fabric_mcp_server.client.exceptions import FabricAPIError, FabricItemNotFoundError
from ms_fabric_mcp_server.models.item import FabricItem
from ms_fabric_mcp_server.models.job import FabricJob
from ms_fabric_mcp_server.models.results import JobStatusResult


def _make_response(status_code=200, json_data=None, headers=None):
    response = Mock()
    response.status_code = status_code
    response.ok = 200 <= status_code < 300
    response.json.return_value = json_data or {}
    response.headers = headers or {}
    return response


@pytest.fixture
def mock_workspace_service():
    service = Mock()
    service.resolve_workspace_id.return_value = "ws-123"
    return service


@pytest.fixture
def mock_item_service():
    service = Mock()
    service.get_item_by_name.return_value = FabricItem(
        id="item-123",
        display_name="Notebook",
        type="Notebook",
        workspace_id="ws-123",
    )
    return service


@pytest.fixture
def job_service(mock_fabric_client, mock_workspace_service, mock_item_service):
    from ms_fabric_mcp_server.services.job import FabricJobService

    return FabricJobService(mock_fabric_client, mock_workspace_service, mock_item_service)


@pytest.mark.unit
class TestFabricJobService:
    """Test suite for FabricJobService."""

    def test_run_on_demand_job_success(self, job_service, mock_fabric_client):
        """Run on-demand job parses headers and job ID."""
        headers = {
            "Location": "https://api.fabric.microsoft.com/v1/workspaces/ws-123/items/item-123/jobs/instances/job-999",
            "Retry-After": "15",
        }
        response = _make_response(202, json_data={}, headers=headers)
        mock_fabric_client.make_api_request.return_value = response

        result = job_service.run_on_demand_job(
            workspace_name="Workspace",
            item_name="Notebook",
            item_type="Notebook",
            job_type="RunNotebook",
        )

        assert result.status == "success"
        assert result.job_instance_id == "job-999"
        assert result.retry_after == 15

    @pytest.mark.parametrize(
        "exception",
        [
            FabricItemNotFoundError("Notebook", "Notebook", "Workspace"),
            FabricAPIError(500, "boom"),
        ],
    )
    def test_run_on_demand_job_expected_errors(self, job_service, mock_workspace_service, mock_item_service, exception):
        """Known errors return error result."""
        if isinstance(exception, FabricItemNotFoundError):
            mock_item_service.get_item_by_name.side_effect = exception
        else:
            job_service.client.make_api_request.side_effect = exception

        result = job_service.run_on_demand_job(
            workspace_name="Workspace",
            item_name="Notebook",
            item_type="Notebook",
            job_type="RunNotebook",
        )

        assert result.status == "error"

    def test_run_on_demand_job_unexpected_error(self, job_service, mock_fabric_client):
        """Unexpected errors return error result with prefix."""
        mock_fabric_client.make_api_request.side_effect = RuntimeError("boom")

        result = job_service.run_on_demand_job(
            workspace_name="Workspace",
            item_name="Notebook",
            item_type="Notebook",
            job_type="RunNotebook",
        )

        assert result.status == "error"
        assert "Unexpected error" in result.message

    def test_get_job_status_success(self, job_service, mock_fabric_client):
        """get_job_status maps response into FabricJob."""
        response = _make_response(
            200,
            json_data={
                "id": "job-1",
                "itemId": "item-123",
                "jobType": "RunNotebook",
                "status": "Completed",
                "invokeType": "Manual",
                "rootActivityId": "root-1",
                "startTimeUtc": "2025-01-01T00:00:00Z",
                "endTimeUtc": "2025-01-01T00:05:00Z",
                "failureReason": {"message": "none"},
            },
        )
        mock_fabric_client.make_api_request.return_value = response

        result = job_service.get_job_status(
            workspace_name="Workspace",
            item_name="Notebook",
            item_type="Notebook",
            job_instance_id="job-1",
        )

        assert result.status == "success"
        assert result.job is not None
        assert result.job.job_instance_id == "job-1"
        assert result.job.status == "Completed"
        assert result.job.failure_reason == {"message": "none"}

    def test_get_job_status_api_error(self, job_service, mock_fabric_client):
        """API errors return error result."""
        mock_fabric_client.make_api_request.side_effect = FabricAPIError(500, "boom")

        result = job_service.get_job_status(
            workspace_name="Workspace",
            item_name="Notebook",
            item_type="Notebook",
            job_instance_id="job-1",
        )

        assert result.status == "error"

    def test_get_job_status_by_url_success(self, job_service, mock_fabric_client):
        """get_job_status_by_url strips version prefix."""
        response = _make_response(200, json_data={"id": "job-1", "status": "Completed"})
        mock_fabric_client.make_api_request.return_value = response

        result = job_service.get_job_status_by_url(
            "https://api.fabric.microsoft.com/v1/workspaces/ws/items/item/jobs/instances/job-1"
        )

        assert result.status == "success"
        mock_fabric_client.make_api_request.assert_called_once_with(
            "GET", "workspaces/ws/items/item/jobs/instances/job-1"
        )

    @pytest.mark.parametrize(
        "location_url",
        [
            "not-a-url",
            "https://api.fabric.microsoft.com/v1",
            "https://api.fabric.microsoft.com/",
            "https://api.fabric.microsoft.com/v1/workspaces/ws/items/item/jobs/instances",
            "https://api.fabric.microsoft.com/v1/workspaces/ws",
        ],
    )
    def test_get_job_status_by_url_invalid(self, job_service, mock_fabric_client, location_url):
        """Invalid URLs return error results without calling API."""
        result = job_service.get_job_status_by_url(location_url)

        assert result.status == "error"
        assert "Invalid job status URL" in result.message
        mock_fabric_client.make_api_request.assert_not_called()

    def test_run_on_demand_job_missing_location(self, job_service, mock_fabric_client):
        """Missing Location header returns error result."""
        response = _make_response(202, json_data={}, headers={})
        mock_fabric_client.make_api_request.return_value = response

        result = job_service.run_on_demand_job(
            workspace_name="Workspace",
            item_name="Notebook",
            item_type="Notebook",
            job_type="RunNotebook",
        )

        assert result.status == "error"
        assert "Location" in result.message

    def test_wait_for_job_completion_success(self, job_service):
        """Wait for completion stops on terminal status."""
        in_progress = JobStatusResult(
            status="success",
            job=FabricJob(
                job_instance_id="job-1",
                item_id="item-123",
                job_type="RunNotebook",
                status="InProgress",
            ),
        )
        completed = JobStatusResult(
            status="success",
            job=FabricJob(
                job_instance_id="job-1",
                item_id="item-123",
                job_type="RunNotebook",
                status="Completed",
                end_time_utc="2025-01-01T00:05:00Z",
            ),
        )
        job_service.get_job_status = Mock(side_effect=[in_progress, completed])

        with patch("time.sleep", return_value=None):
            result = job_service.wait_for_job_completion(
                workspace_name="Workspace",
                item_name="Notebook",
                item_type="Notebook",
                job_instance_id="job-1",
                poll_interval=0,
                timeout_minutes=1,
            )

        assert result.job is not None
        assert result.job.status == "Completed"

    def test_wait_for_job_completion_by_url_failed(self, job_service):
        """Failed job by URL returns terminal status with failure reason."""
        failed = JobStatusResult(
            status="success",
            job=FabricJob(
                job_instance_id="job-1",
                item_id="item-123",
                job_type="RunNotebook",
                status="Failed",
                end_time_utc="2025-01-01T00:05:00Z",
                failure_reason={"message": "boom"},
            ),
        )
        job_service.get_job_status_by_url = Mock(return_value=failed)

        result = job_service.wait_for_job_completion_by_url(
            location_url="https://api.fabric.microsoft.com/v1/...",
            poll_interval=0,
            timeout_minutes=1,
        )

        assert result.job is not None
        assert result.job.is_failed()
        assert result.job.failure_reason == {"message": "boom"}

    def test_wait_for_job_completion_failed(self, job_service):
        """Failed job returns terminal status with failure reason."""
        failed = JobStatusResult(
            status="success",
            job=FabricJob(
                job_instance_id="job-1",
                item_id="item-123",
                job_type="RunNotebook",
                status="Failed",
                end_time_utc="2025-01-01T00:05:00Z",
                failure_reason={"message": "boom"},
            ),
        )
        job_service.get_job_status = Mock(return_value=failed)

        result = job_service.wait_for_job_completion(
            workspace_name="Workspace",
            item_name="Notebook",
            item_type="Notebook",
            job_instance_id="job-1",
            poll_interval=0,
            timeout_minutes=1,
        )

        assert result.job is not None
        assert result.job.is_failed()
        assert result.job.failure_reason == {"message": "boom"}

    def test_wait_for_job_completion_returns_error_result(self, job_service):
        """Error result during polling returns immediately."""
        error_result = JobStatusResult(status="error", message="boom")
        job_service.get_job_status = Mock(return_value=error_result)

        result = job_service.wait_for_job_completion(
            workspace_name="Workspace",
            item_name="Notebook",
            item_type="Notebook",
            job_instance_id="job-1",
            poll_interval=0,
            timeout_minutes=1,
        )

        assert result.status == "error"
        assert result.message == "boom"

    def test_wait_for_job_completion_timeout(self, job_service):
        """Timeout path returns final status with timeout message."""
        final = JobStatusResult(
            status="success",
            job=FabricJob(
                job_instance_id="job-1",
                item_id="item-123",
                job_type="RunNotebook",
                status="InProgress",
            ),
        )
        job_service.get_job_status = Mock(return_value=final)

        result = job_service.wait_for_job_completion(
            workspace_name="Workspace",
            item_name="Notebook",
            item_type="Notebook",
            job_instance_id="job-1",
            poll_interval=0,
            timeout_minutes=0,
        )

        assert "Timed out" in result.message

    def test_wait_for_job_completion_by_url_success(self, job_service):
        """Wait for completion by URL stops on terminal status."""
        in_progress = JobStatusResult(
            status="success",
            job=FabricJob(
                job_instance_id="job-1",
                item_id="item-123",
                job_type="RunNotebook",
                status="InProgress",
            ),
        )
        completed = JobStatusResult(
            status="success",
            job=FabricJob(
                job_instance_id="job-1",
                item_id="item-123",
                job_type="RunNotebook",
                status="Completed",
                end_time_utc="2025-01-01T00:05:00Z",
            ),
        )
        job_service.get_job_status_by_url = Mock(side_effect=[in_progress, completed])

        with patch("time.sleep", return_value=None):
            result = job_service.wait_for_job_completion_by_url(
                location_url="https://api.fabric.microsoft.com/v1/...",
                poll_interval=0,
                timeout_minutes=1,
            )

        assert result.job is not None
        assert result.job.status == "Completed"

    def test_wait_for_job_completion_terminal_without_end_time(self, job_service):
        """Terminal status returns even without end_time_utc."""
        completed = JobStatusResult(
            status="success",
            job=FabricJob(
                job_instance_id="job-1",
                item_id="item-123",
                job_type="RunNotebook",
                status="Completed",
                end_time_utc=None,
            ),
        )
        job_service.get_job_status = Mock(return_value=completed)

        result = job_service.wait_for_job_completion(
            workspace_name="Workspace",
            item_name="Notebook",
            item_type="Notebook",
            job_instance_id="job-1",
            poll_interval=0,
            timeout_minutes=1,
        )

        assert result.job is not None
        assert result.job.status == "Completed"

    def test_wait_for_job_completion_by_url_terminal_without_end_time(self, job_service):
        """Terminal status by URL returns even without end_time_utc."""
        completed = JobStatusResult(
            status="success",
            job=FabricJob(
                job_instance_id="job-1",
                item_id="item-123",
                job_type="RunNotebook",
                status="Completed",
                end_time_utc=None,
            ),
        )
        job_service.get_job_status_by_url = Mock(return_value=completed)

        result = job_service.wait_for_job_completion_by_url(
            location_url="https://api.fabric.microsoft.com/v1/...",
            poll_interval=0,
            timeout_minutes=1,
        )

        assert result.job is not None
        assert result.job.status == "Completed"
    def test_wait_for_job_completion_by_url_timeout(self, job_service):
        """Timeout path returns final status with timeout message."""
        final = JobStatusResult(
            status="success",
            job=FabricJob(
                job_instance_id="job-1",
                item_id="item-123",
                job_type="RunNotebook",
                status="InProgress",
            ),
        )
        job_service.get_job_status_by_url = Mock(return_value=final)

        result = job_service.wait_for_job_completion_by_url(
            location_url="https://api.fabric.microsoft.com/v1/...",
            poll_interval=0,
            timeout_minutes=0,
        )

        assert "Timed out" in result.message
