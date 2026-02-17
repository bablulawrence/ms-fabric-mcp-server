"""Integration tests for notebook and job tools."""

import json
import tempfile
from pathlib import Path

import pytest

from tests.conftest import unique_name


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


@pytest.mark.integration
@pytest.mark.asyncio
async def test_get_notebook_definition(
    call_tool,
    executed_notebook_context,
    workspace_name,
):
    notebook_name = executed_notebook_context["notebook_name"]

    content_result = await call_tool(
        "get_notebook_definition",
        workspace_name=workspace_name,
        notebook_name=notebook_name,
    )
    assert content_result["status"] == "success"
    assert content_result.get("definition") is not None


@pytest.mark.integration
@pytest.mark.asyncio
async def test_update_notebook_definition(
    call_tool,
    delete_item_if_exists,
    poll_until,
    workspace_name,
    lakehouse_name,
):
    notebook_name = unique_name("e2e_notebook_update")
    notebook_path = Path(__file__).resolve().parents[2] / "fixtures" / "minimal_notebook.ipynb"
    notebook_content = json.loads(notebook_path.read_text())

    async def _get_definition():
        result = await call_tool(
            "get_notebook_definition",
            workspace_name=workspace_name,
            notebook_name=notebook_name,
        )
        if result.get("status") == "success":
            return result
        message = (result.get("message") or "").lower()
        if "not found" in message or "notfound" in message:
            return None
        return result

    try:
        create_result = await call_tool(
            "create_notebook",
            workspace_name=workspace_name,
            notebook_name=notebook_name,
            notebook_content=notebook_content,
        )
        assert create_result["status"] == "success"

        definition_result = await poll_until(
            _get_definition, timeout_seconds=300, interval_seconds=10
        )
        assert definition_result is not None

        attach_result = await call_tool(
            "update_notebook_definition",
            workspace_name=workspace_name,
            notebook_name=notebook_name,
            notebook_content=None,
            default_lakehouse_name=lakehouse_name,
        )
        assert attach_result["status"] == "success"

        updated_content = {
            **notebook_content,
            "cells": notebook_content.get("cells", [])
            + [
                {
                    "cell_type": "markdown",
                    "metadata": {},
                    "source": ["updated cell"],
                }
            ],
        }

        update_result = await call_tool(
            "update_notebook_definition",
            workspace_name=workspace_name,
            notebook_name=notebook_name,
            notebook_content=updated_content,
        )
        assert update_result["status"] == "success"

        # Poll until updated cell is visible (eventual consistency)
        async def _check_updated_cell():
            result = await call_tool(
                "get_notebook_definition",
                workspace_name=workspace_name,
                notebook_name=notebook_name,
            )
            if result.get("status") != "success":
                return None
            cells = result.get("definition", {}).get("cells", [])
            if any("updated cell" in "".join(cell.get("source", [])) for cell in cells):
                return result
            return None

        updated_definition = await poll_until(
            _check_updated_cell, timeout_seconds=120, interval_seconds=10
        )
        assert updated_definition is not None, "Updated cell not found after polling"
    finally:
        await delete_item_if_exists(notebook_name, "Notebook")


@pytest.mark.integration
@pytest.mark.asyncio
async def test_run_on_demand_job_smoke(
    call_tool,
    executed_notebook_context,
    workspace_name,
):
    notebook_name = executed_notebook_context["notebook_name"]

    run_result = await call_tool(
        "run_on_demand_job",
        workspace_name=workspace_name,
        item_name=notebook_name,
        item_type="Notebook",
        job_type="RunNotebook",
    )
    assert run_result["status"] == "success"
    assert run_result.get("job_instance_id")
    assert run_result.get("location_url")


@pytest.mark.integration
@pytest.mark.asyncio
async def test_get_notebook_definition_save_to_path(
    call_tool,
    executed_notebook_context,
    workspace_name,
    tmp_path,
):
    """get_notebook_definition with save_to_path writes to local file."""
    notebook_name = executed_notebook_context["notebook_name"]
    out_file = tmp_path / "downloaded.ipynb"

    result = await call_tool(
        "get_notebook_definition",
        workspace_name=workspace_name,
        notebook_name=notebook_name,
        save_to_path=str(out_file),
    )
    assert result["status"] == "success"
    assert result["file_path"] == str(out_file)
    assert result["size_bytes"] > 0
    assert "definition" not in result

    # Verify the file is valid notebook JSON
    content = json.loads(out_file.read_text())
    assert "cells" in content or "metadata" in content


@pytest.mark.integration
@pytest.mark.asyncio
async def test_create_notebook_with_file_path(
    call_tool,
    delete_item_if_exists,
    workspace_name,
):
    """create_notebook with notebook_file_path reads content from disk."""
    notebook_name = unique_name("e2e_nb_from_file")
    notebook_path = Path(__file__).resolve().parents[2] / "fixtures" / "minimal_notebook.ipynb"

    try:
        result = await call_tool(
            "create_notebook",
            workspace_name=workspace_name,
            notebook_name=notebook_name,
            notebook_file_path=str(notebook_path),
        )
        assert result["status"] == "success"
        assert result.get("notebook_id")
    finally:
        await delete_item_if_exists(notebook_name, "Notebook")


@pytest.mark.integration
@pytest.mark.asyncio
async def test_notebook_file_path_round_trip(
    call_tool,
    delete_item_if_exists,
    poll_until,
    workspace_name,
):
    """Full round-trip: create → download to file → edit → re-upload from file."""
    notebook_name = unique_name("e2e_nb_roundtrip")
    notebook_path = Path(__file__).resolve().parents[2] / "fixtures" / "minimal_notebook.ipynb"
    notebook_content = json.loads(notebook_path.read_text())

    async def _get_definition():
        result = await call_tool(
            "get_notebook_definition",
            workspace_name=workspace_name,
            notebook_name=notebook_name,
        )
        if result.get("status") == "success":
            return result
        message = (result.get("message") or "").lower()
        if "not found" in message or "notfound" in message:
            return None
        return result

    try:
        # Step 1: Create notebook inline
        create_result = await call_tool(
            "create_notebook",
            workspace_name=workspace_name,
            notebook_name=notebook_name,
            notebook_content=notebook_content,
        )
        assert create_result["status"] == "success"

        # Wait for notebook to be available
        await poll_until(_get_definition, timeout_seconds=300, interval_seconds=10)

        # Step 2: Download to a temp file using save_to_path
        with tempfile.TemporaryDirectory() as tmpdir:
            save_path = Path(tmpdir) / "roundtrip.ipynb"

            download_result = await call_tool(
                "get_notebook_definition",
                workspace_name=workspace_name,
                notebook_name=notebook_name,
                save_to_path=str(save_path),
            )
            assert download_result["status"] == "success"
            assert save_path.exists()

            # Step 3: Edit the file on disk — add a marker cell
            saved_content = json.loads(save_path.read_text())
            saved_content.setdefault("cells", []).append(
                {
                    "cell_type": "markdown",
                    "metadata": {},
                    "source": ["roundtrip marker"],
                }
            )
            save_path.write_text(json.dumps(saved_content))

            # Step 4: Re-upload from the edited file
            update_result = await call_tool(
                "update_notebook_definition",
                workspace_name=workspace_name,
                notebook_name=notebook_name,
                notebook_file_path=str(save_path),
            )
            assert update_result["status"] == "success"

        # Step 5: Verify the marker cell is present
        async def _check_marker():
            result = await call_tool(
                "get_notebook_definition",
                workspace_name=workspace_name,
                notebook_name=notebook_name,
            )
            if result.get("status") != "success":
                return None
            cells = result.get("definition", {}).get("cells", [])
            if any(
                "roundtrip marker" in "".join(c.get("source", []))
                for c in cells
            ):
                return result
            return None

        verified = await poll_until(
            _check_marker, timeout_seconds=120, interval_seconds=10
        )
        assert verified is not None, "Roundtrip marker cell not found after polling"
    finally:
        await delete_item_if_exists(notebook_name, "Notebook")
