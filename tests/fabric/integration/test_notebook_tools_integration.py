"""Integration tests for notebook and job tools."""

import pytest


@pytest.mark.integration
@pytest.mark.asyncio
async def test_notebook_tool_flow(
    call_tool,
    executed_notebook_context,
    workspace_name,
):
    notebook_name = executed_notebook_context["notebook_name"]
    job_instance_id = executed_notebook_context["job_instance_id"]
    location_url = executed_notebook_context["location_url"]

    status_result = await call_tool(
        "get_job_status",
        workspace_name=workspace_name,
        item_name=notebook_name,
        item_type="Notebook",
        job_instance_id=job_instance_id,
    )
    assert status_result["status"] == "success"
    assert status_result.get("job", {}).get("is_terminal")

    status_by_url = await call_tool("get_job_status_by_url", location_url=location_url)
    assert status_by_url["status"] == "success"
    assert status_by_url.get("job", {}).get("is_terminal")
