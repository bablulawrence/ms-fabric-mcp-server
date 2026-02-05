"""Integration tests for notebook and job tools."""

import json
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
