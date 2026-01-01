"""Integration tests for notebook and job tools."""

import pytest

from tests.conftest import unique_name


@pytest.mark.integration
@pytest.mark.asyncio
async def test_notebook_tool_flow(
    call_tool,
    delete_item_if_exists,
    notebook_fixture_path,
    poll_until,
    workspace_name,
    lakehouse_name,
):
    notebook_name = unique_name("e2e_notebook")
    job_instance_id = None
    location_url = None

    try:
        import_result = await call_tool(
            "import_notebook_to_fabric",
            workspace_name=workspace_name,
            notebook_display_name=notebook_name,
            local_notebook_path=str(notebook_fixture_path),
        )
        assert import_result["status"] == "success"

        content_result = await call_tool(
            "get_notebook_content",
            workspace_name=workspace_name,
            notebook_display_name=notebook_name,
        )
        assert content_result["status"] == "success"

        attach_result = await call_tool(
            "attach_lakehouse_to_notebook",
            workspace_name=workspace_name,
            notebook_name=notebook_name,
            lakehouse_name=lakehouse_name,
        )
        assert attach_result["status"] == "success"

        run_result = await call_tool(
            "run_on_demand_job",
            workspace_name=workspace_name,
            item_name=notebook_name,
            item_type="Notebook",
            job_type="RunNotebook",
        )
        assert run_result["status"] == "success"
        job_instance_id = run_result.get("job_instance_id")
        location_url = run_result.get("location_url")
        assert job_instance_id
        assert location_url

        async def _wait_for_job():
            status_result = await call_tool(
                "get_job_status",
                workspace_name=workspace_name,
                item_name=notebook_name,
                item_type="Notebook",
                job_instance_id=job_instance_id,
            )
            if status_result.get("status") != "success":
                return status_result
            job = status_result.get("job", {})
            if job.get("is_terminal"):
                return status_result
            return None

        status_result = await poll_until(_wait_for_job, timeout_seconds=1800, interval_seconds=15)
        assert status_result is not None
        assert status_result["status"] == "success"
        job = status_result.get("job", {})
        assert job.get("is_terminal")
        assert job.get("is_successful"), f"Job failed: {job.get('failure_reason')}"

        status_by_url = await call_tool("get_job_status_by_url", location_url=location_url)
        assert status_by_url["status"] == "success"
        assert status_by_url.get("job", {}).get("is_terminal")

        async def _get_executions():
            history = await call_tool(
                "list_notebook_executions",
                workspace_name=workspace_name,
                notebook_name=notebook_name,
                limit=5,
            )
            if history.get("status") == "success" and history.get("sessions"):
                return history
            return None

        history = await poll_until(_get_executions, timeout_seconds=300, interval_seconds=10)
        assert history is not None
        assert history["status"] == "success"

        async def _get_details():
            details = await call_tool(
                "get_notebook_execution_details",
                workspace_name=workspace_name,
                notebook_name=notebook_name,
                job_instance_id=job_instance_id,
            )
            if details.get("status") == "success":
                return details
            return None

        details = await poll_until(_get_details, timeout_seconds=300, interval_seconds=10)
        assert details is not None
        assert details["status"] == "success"

        async def _get_logs():
            logs = await call_tool(
                "get_notebook_driver_logs",
                workspace_name=workspace_name,
                notebook_name=notebook_name,
                job_instance_id=job_instance_id,
                log_type="stdout",
                max_lines=200,
            )
            if logs.get("status") == "success" and logs.get("log_content"):
                return logs
            return None

        logs = await poll_until(_get_logs, timeout_seconds=300, interval_seconds=10)
        assert logs is not None
        assert logs["status"] == "success"

    finally:
        await delete_item_if_exists(notebook_name, "Notebook")
