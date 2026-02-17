"""Tests for notebook MCP tools."""

import json
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
        notebook_service.update_notebook_definition.return_value = UpdateNotebookResult(
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

        updated = tools["update_notebook_definition"](
            workspace_name="Workspace",
            notebook_name="Notebook",
            notebook_content={"cells": []},
        )
        assert updated["status"] == "success"
        assert updated["notebook_id"] == "nb-1"

        updated_metadata_only = tools["update_notebook_definition"](
            workspace_name="Workspace",
            notebook_name="Notebook",
            notebook_content=None,
            default_lakehouse_name="Lakehouse",
        )
        assert updated_metadata_only["status"] == "success"

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

    def test_create_notebook_with_file_path(self, tmp_path):
        """create_notebook tool accepts notebook_file_path."""
        tools, mcp = capture_tools()
        notebook_service = Mock()
        notebook_service.create_notebook.return_value = CreateNotebookResult(
            status="success", message="created", notebook_id="nb-fp"
        )
        register_notebook_tools(mcp, notebook_service)

        nb_file = tmp_path / "nb.ipynb"
        nb_file.write_text(json.dumps({"cells": []}))

        result = tools["create_notebook"](
            workspace_name="WS",
            notebook_name="NB",
            notebook_file_path=str(nb_file),
        )
        assert result["status"] == "success"
        notebook_service.create_notebook.assert_called_once()
        call_kwargs = notebook_service.create_notebook.call_args[1]
        assert call_kwargs["notebook_file_path"] == str(nb_file)
        assert call_kwargs["notebook_content"] is None

    def test_update_notebook_with_file_path(self, tmp_path):
        """update_notebook_definition tool accepts notebook_file_path."""
        tools, mcp = capture_tools()
        notebook_service = Mock()
        notebook_service.update_notebook_definition.return_value = UpdateNotebookResult(
            status="success", message="updated", notebook_id="nb-1",
            notebook_name="NB", workspace_id="ws-1"
        )
        register_notebook_tools(mcp, notebook_service)

        nb_file = tmp_path / "nb.ipynb"
        nb_file.write_text(json.dumps({"cells": []}))

        result = tools["update_notebook_definition"](
            workspace_name="WS",
            notebook_name="NB",
            notebook_file_path=str(nb_file),
        )
        assert result["status"] == "success"
        call_kwargs = notebook_service.update_notebook_definition.call_args[1]
        assert call_kwargs["notebook_file_path"] == str(nb_file)

    def test_get_notebook_definition_save_to_path(self):
        """get_notebook_definition tool passes save_to_path and merges result."""
        tools, mcp = capture_tools()
        notebook_service = Mock()
        notebook_service.get_notebook_definition.return_value = {
            "file_path": "/tmp/nb.ipynb",
            "size_bytes": 123,
        }
        register_notebook_tools(mcp, notebook_service)

        result = tools["get_notebook_definition"](
            workspace_name="WS",
            notebook_name="NB",
            save_to_path="/tmp/nb.ipynb",
        )
        assert result["status"] == "success"
        assert result["file_path"] == "/tmp/nb.ipynb"
        assert result["size_bytes"] == 123
        assert "definition" not in result
