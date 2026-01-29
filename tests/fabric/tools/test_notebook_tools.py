"""Tests for notebook MCP tools."""

from unittest.mock import Mock

import pytest

from ms_fabric_mcp_server.models.results import AttachLakehouseResult, ImportNotebookResult
from ms_fabric_mcp_server.tools.notebook_tools import register_notebook_tools
from tests.fabric.tools.utils import capture_tools


@pytest.mark.unit
class TestNotebookTools:
    def test_notebook_tools_smoke(self):
        tools, mcp = capture_tools()
        notebook_service = Mock()
        notebook_service.import_notebook.return_value = ImportNotebookResult(
            status="success",
            message="imported",
            artifact_id="nb-1",
        )
        notebook_service.get_notebook_content.return_value = {"cells": []}
        notebook_service.attach_lakehouse_to_notebook.return_value = AttachLakehouseResult(
            status="success",
            message="attached",
            notebook_id="nb-1",
            notebook_name="Notebook",
            lakehouse_id="lh-1",
            lakehouse_name="Lakehouse",
            workspace_id="ws-1",
        )
        notebook_service.get_notebook_execution_details.return_value = {
            "status": "success",
            "execution_summary": {"state": "Success"},
        }
        notebook_service.list_notebook_executions.return_value = {
            "status": "success",
            "sessions": [],
            "total_count": 0,
        }
        notebook_service.get_notebook_driver_logs.return_value = {
            "status": "success",
            "log_content": "ok",
        }

        register_notebook_tools(mcp, notebook_service)

        result = tools["import_notebook_to_fabric"](
            workspace_name="Workspace",
            notebook_display_name="Notebook",
            local_notebook_path="notebooks/test.ipynb",
        )
        assert result["status"] == "success"
        assert result["artifact_id"] == "nb-1"

        content = tools["get_notebook_content"](
            workspace_name="Workspace",
            notebook_display_name="Notebook",
        )
        assert content["status"] == "success"

        attached = tools["attach_lakehouse_to_notebook"](
            workspace_name="Workspace",
            notebook_name="Notebook",
            lakehouse_name="Lakehouse",
        )
        assert attached["status"] == "success"
        assert attached["notebook_id"] == "nb-1"

        exec_details = tools["get_notebook_execution_details"](
            workspace_name="Workspace",
            notebook_name="Notebook",
            job_instance_id="job-1",
        )
        assert exec_details["status"] == "success"

        history = tools["list_notebook_executions"](
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
