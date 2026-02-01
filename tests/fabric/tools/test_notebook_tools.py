"""Tests for notebook MCP tools."""

from unittest.mock import Mock

import pytest

from ms_fabric_mcp_server.models.results import CreateNotebookResult, UpdateNotebookResult
from ms_fabric_mcp_server.tools.notebook_tools import register_notebook_tools
from tests.fabric.tools.utils import capture_tools


@pytest.mark.unit
class TestNotebookTools:
    def test_notebook_tools_smoke(self):
        tools, mcp = capture_tools()
        notebook_service = Mock()
        notebook_service.create_notebook.return_value = CreateNotebookResult(
            status="success",
            message="created",
            notebook_id="nb-1",
        )
        notebook_service.get_notebook_definition.return_value = {"cells": []}
        notebook_service.update_notebook_content.return_value = UpdateNotebookResult(
            status="success",
            message="updated",
            notebook_id="nb-1",
            notebook_name="Notebook",
            workspace_id="ws-1",
        )
        notebook_service.get_notebook_run_details.return_value = {
            "status": "success",
            "execution_summary": {"state": "Success"},
        }
        notebook_service.list_notebook_runs.return_value = {
            "status": "success",
            "sessions": [],
            "total_count": 0,
        }
        notebook_service.get_notebook_driver_logs.return_value = {
            "status": "success",
            "log_content": "ok",
        }

        register_notebook_tools(mcp, notebook_service)

        result = tools["create_notebook"](
            workspace_name="Workspace",
            notebook_name="Notebook",
            notebook_content={"cells": []},
        )
        assert result["status"] == "success"
        assert result["notebook_id"] == "nb-1"

        content = tools["get_notebook_definition"](
            workspace_name="Workspace",
            notebook_name="Notebook",
        )
        assert content["status"] == "success"

        updated = tools["update_notebook_content"](
            workspace_name="Workspace",
            notebook_name="Notebook",
            notebook_content={"cells": []},
        )
        assert updated["status"] == "success"
        assert updated["notebook_id"] == "nb-1"

        exec_details = tools["get_notebook_run_details"](
            workspace_name="Workspace",
            notebook_name="Notebook",
            job_instance_id="job-1",
        )
        assert exec_details["status"] == "success"

        history = tools["list_notebook_runs"](
            workspace_name="Workspace",
            notebook_name="Notebook",
            limit=5,
        )
        assert history["status"] == "success"

        logs = tools["get_notebook_driver_logs"](
            workspace_name="Workspace",
            notebook_name="Notebook",
            job_instance_id="job-1",
        )
        assert logs["status"] == "success"
