"""Integration tests for notebook execution tools."""

import pytest


@pytest.mark.integration
@pytest.mark.asyncio
async def test_notebook_execution_tools(
    call_tool,
    executed_notebook_context,
    poll_until,
    workspace_name,
):
    notebook_name = executed_notebook_context["notebook_name"]
    job_instance_id = executed_notebook_context["job_instance_id"]

    def _is_scp_claim_error(result: dict) -> bool:
        message = (result.get("message") or "").lower()
        return "scp" in message and ("claim" in message or "unauthorized" in message)

    async def _get_executions():
        history = await call_tool(
            "list_notebook_runs",
            workspace_name=workspace_name,
            notebook_name=notebook_name,
            limit=5,
        )
        if history.get("status") == "error" and _is_scp_claim_error(history):
            pytest.skip("Notebook execution history requires delegated token (scp claim)")
        if history.get("status") == "success" and history.get("sessions"):
            return history
        return None

    history = await poll_until(_get_executions, timeout_seconds=300, interval_seconds=10)
    assert history is not None
    assert history["status"] == "success"

    async def _get_details():
        details = await call_tool(
            "get_notebook_run_details",
            workspace_name=workspace_name,
            notebook_name=notebook_name,
            job_instance_id=job_instance_id,
        )
        if details.get("status") == "error" and _is_scp_claim_error(details):
            pytest.skip("Notebook execution details require delegated token (scp claim)")
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
        if logs.get("status") == "error" and _is_scp_claim_error(logs):
            pytest.skip("Notebook driver logs require delegated token (scp claim)")
        if logs.get("status") == "success" and logs.get("log_content"):
            return logs
        return None

    logs = await poll_until(_get_logs, timeout_seconds=300, interval_seconds=10)
    assert logs is not None
    assert logs["status"] == "success"
