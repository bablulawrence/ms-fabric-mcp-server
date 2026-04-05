"""Integration tests for dataflow tools."""

import json

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


@pytest.mark.integration
@pytest.mark.asyncio
async def test_dataflow_definition_save_to_path(
    call_tool, delete_item_if_exists, workspace_name, poll_until, tmp_path
):
    """Test saving dataflow definition to a local file."""
    dataflow_name = unique_name("e2e_dataflow_save")
    mashup_content = (
        "section Section1;\n"
        "shared SaveTest = let\n"
        '    Source = #table(type table [Col1 = text], {{"A"}, {"B"}})\n'
        "in\n"
        "    Source;\n"
    )

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
        create_result = await call_tool(
            "create_dataflow",
            workspace_name=workspace_name,
            dataflow_name=dataflow_name,
            mashup_content=mashup_content,
        )
        assert create_result["status"] == "success"

        await poll_until(_get_definition, timeout_seconds=120, interval_seconds=10)

        out_file = str(tmp_path / "dataflow_def.json")
        get_result = await call_tool(
            "get_dataflow_definition",
            workspace_name=workspace_name,
            dataflow_name=dataflow_name,
            save_to_path=out_file,
        )
        assert get_result["status"] == "success"
        assert get_result["file_path"] == out_file
        assert get_result["size_bytes"] > 0
        assert "query_metadata" not in get_result

        with open(out_file) as f:
            saved = json.load(f)
        assert "mashup_content" in saved
        assert "SaveTest" in saved["mashup_content"]
    finally:
        await delete_item_if_exists(dataflow_name, "Dataflow")


@pytest.mark.integration
@pytest.mark.asyncio
async def test_dataflow_create_from_file(
    call_tool, delete_item_if_exists, workspace_name, poll_until, tmp_path
):
    """Test creating a dataflow from a local mashup file."""
    dataflow_name = unique_name("e2e_dataflow_file")
    mashup_content = (
        "section Section1;\n"
        "shared FileQuery = let\n"
        '    Source = #table(type table [Col1 = text], {{"X"}, {"Y"}})\n'
        "in\n"
        "    Source;\n"
    )
    file_path = tmp_path / "mashup.pq"
    file_path.write_text(mashup_content)

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
        create_result = await call_tool(
            "create_dataflow",
            workspace_name=workspace_name,
            dataflow_name=dataflow_name,
            dataflow_file_path=str(file_path),
        )
        assert create_result["status"] == "success"

        get_result = await poll_until(
            _get_definition, timeout_seconds=120, interval_seconds=10
        )
        assert get_result is not None
        assert "FileQuery" in get_result.get("mashup_content", "")
    finally:
        await delete_item_if_exists(dataflow_name, "Dataflow")
