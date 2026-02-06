"""Tests for job MCP tools."""

from unittest.mock import Mock

import pytest

from ms_fabric_mcp_server.models.job import FabricJob
from ms_fabric_mcp_server.models.results import JobStatusResult, OperationResult, RunJobResult
from ms_fabric_mcp_server.tools.job_tools import register_job_tools
from tests.fabric.tools.utils import capture_tools


@pytest.mark.unit
class TestJobTools:
    def test_run_on_demand_job(self):
        tools, mcp = capture_tools()
        job_service = Mock()
        job_service.run_on_demand_job.return_value = RunJobResult(
            status="success",
            message="started",
            job_instance_id="job-1",
            location_url="https://example/jobs/1",
            retry_after=5,
        )

        register_job_tools(mcp, job_service)

        result = tools["run_on_demand_job"](
            workspace_name="Workspace",
            item_name="Notebook",
            item_type="Notebook",
            job_type="RunNotebook",
        )

        assert result["status"] == "success"
        assert result["job_instance_id"] == "job-1"
        job_service.run_on_demand_job.assert_called_once()

    def test_get_job_status(self):
        tools, mcp = capture_tools()
        job_service = Mock()
        job_service.get_job_status.return_value = JobStatusResult(
            status="success",
            message="ok",
            job=FabricJob(
                job_instance_id="job-1",
                item_id="item-1",
                job_type="RunNotebook",
                status="Completed",
            ),
        )

        register_job_tools(mcp, job_service)

        result = tools["get_job_status"](
            workspace_name="Workspace",
            item_name="Notebook",
            item_type="Notebook",
            job_instance_id="job-1",
        )

        assert result["status"] == "success"
        assert result["job"]["job_status"] == "Completed"
        job_service.get_job_status.assert_called_once()

    def test_get_job_status_by_url(self):
        tools, mcp = capture_tools()
        job_service = Mock()
        job_service.get_job_status_by_url.return_value = JobStatusResult(
            status="success",
            message="ok",
            job=FabricJob(
                job_instance_id="job-2",
                item_id="item-2",
                job_type="RunNotebook",
                status="InProgress",
            ),
        )

        register_job_tools(mcp, job_service)

        result = tools["get_job_status_by_url"](
            location_url="https://example/jobs/2",
        )

        assert result["status"] == "success"
        assert result["job"]["job_instance_id"] == "job-2"
        job_service.get_job_status_by_url.assert_called_once()

    def test_get_operation_result(self):
        tools, mcp = capture_tools()
        job_service = Mock()
        job_service.get_operation_result.return_value = OperationResult(
            status="success",
            message="ok",
            operation_id="op-1",
            result={"value": 1},
        )

        register_job_tools(mcp, job_service)

        result = tools["get_operation_result"](operation_id="op-1")

        assert result["status"] == "success"
        assert result["operation_id"] == "op-1"
        assert result["result"] == {"value": 1}
        job_service.get_operation_result.assert_called_once_with("op-1")
