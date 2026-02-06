"""Integration tests for dataflow tools."""

import pytest

from tests.conftest import unique_name


@pytest.mark.integration
@pytest.mark.asyncio
async def test_dataflow_create_get_run(
    call_tool,
    delete_item_if_exists,
    workspace_name,
    poll_until,
):
    """Test full dataflow lifecycle: create, get definition, and run."""
    dataflow_name = unique_name("e2e_dataflow")

    # Simple Power Query M code that creates a literal table
    mashup_content = """section Section1;
shared TestQuery = let
    Source = #table(
        type table [Column1 = text, Column2 = number],
        {{"Row1", 1}, {"Row2", 2}}
    )
in
    Source;
"""

    async def _get_definition():
        result = await call_tool(
            "get_dataflow_definition",
            workspace_name=workspace_name,
            dataflow_name=dataflow_name,
        )
        if result.get("status") == "success":
            return result
        message = (result.get("message") or "").lower()
        if "not found" in message or "notfound" in message:
            return None
        return result

    try:
        # Create
        create_result = await call_tool(
            "create_dataflow",
            workspace_name=workspace_name,
            dataflow_name=dataflow_name,
            mashup_content=mashup_content,
            description="Integration test dataflow",
        )
        assert create_result["status"] == "success"
        assert create_result.get("dataflow_id") is not None

        # Get definition (with polling for availability)
        get_result = await poll_until(
            _get_definition, timeout_seconds=120, interval_seconds=10
        )
        assert get_result is not None
        assert get_result["status"] == "success"
        assert "TestQuery" in get_result.get("mashup_content", "")

        # Run
        run_result = await call_tool(
            "run_dataflow",
            workspace_name=workspace_name,
            dataflow_name=dataflow_name,
        )
        assert run_result["status"] == "success"
        assert run_result.get("job_instance_id") is not None
        assert run_result.get("location_url") is not None

    finally:
        await delete_item_if_exists(dataflow_name, "Dataflow")

