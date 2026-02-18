"""Unit tests for lakehouse file tools."""

from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from ms_fabric_mcp_server.tools.lakehouse_file_tools import \
    register_lakehouse_file_tools


def _capture_tools():
    tools = {}

    def tool(**_kwargs):
        def decorator(func):
            tools[func.__name__] = func
            return func

        return decorator

    return tools, tool


@pytest.mark.unit
class TestLakehouseFileTools:
    def test_list_lakehouse_files(self):
        tools, tool_decorator = _capture_tools()
        mcp = SimpleNamespace(tool=tool_decorator)

        lakehouse_file_service = Mock()
        workspace_service = Mock()
        item_service = Mock()
        workspace_service.resolve_workspace_id.return_value = "ws-1"
        item_service.get_item_by_name.return_value = Mock(
            id="lh-1",
            display_name="Lakehouse",
        )
        lakehouse_file_service.list_files.return_value = [
            {
                "name": "Files/raw/file.csv",
                "isDirectory": False,
                "contentLength": "10",
                "lastModified": "2024-01-01T00:00:00Z",
                "etag": "etag",
            }
        ]

        register_lakehouse_file_tools(
            mcp, lakehouse_file_service, workspace_service, item_service
        )

        result = tools["list_lakehouse_files"](
            workspace_name="Workspace",
            lakehouse_name="Lakehouse",
            path="raw",
            recursive=True,
        )

        assert result["status"] == "success"
        assert result["file_count"] == 1
        assert result["files"][0]["name"] == "Files/raw/file.csv"
        lakehouse_file_service.list_files.assert_called_once_with(
            workspace_id="ws-1",
            lakehouse_id="lh-1",
            path="raw",
            recursive=True,
        )

    def test_upload_lakehouse_file_defaults_destination(self):
        tools, tool_decorator = _capture_tools()
        mcp = SimpleNamespace(tool=tool_decorator)

        lakehouse_file_service = Mock()
        workspace_service = Mock()
        item_service = Mock()
        workspace_service.resolve_workspace_id.return_value = "ws-1"
        item_service.get_item_by_name.return_value = Mock(
            id="lh-1",
            display_name="Lakehouse",
        )
        lakehouse_file_service.upload_file.return_value = {
            "path": "sample.csv",
            "size_bytes": 10,
        }

        register_lakehouse_file_tools(
            mcp, lakehouse_file_service, workspace_service, item_service
        )

        result = tools["upload_lakehouse_file"](
            workspace_name="Workspace",
            lakehouse_name="Lakehouse",
            local_file_path="/tmp/sample.csv",
        )

        assert result["status"] == "success"
        lakehouse_file_service.upload_file.assert_called_once_with(
            workspace_id="ws-1",
            lakehouse_id="lh-1",
            local_file_path="/tmp/sample.csv",
            destination_path="sample.csv",
            create_missing_directories=True,
        )

    def test_list_lakehouse_files_rejects_tables_path(self):
        tools, tool_decorator = _capture_tools()
        mcp = SimpleNamespace(tool=tool_decorator)

        lakehouse_file_service = Mock()
        workspace_service = Mock()
        item_service = Mock()

        register_lakehouse_file_tools(
            mcp, lakehouse_file_service, workspace_service, item_service
        )

        result = tools["list_lakehouse_files"](
            workspace_name="Workspace",
            lakehouse_name="Lakehouse",
            path="Tables",
            recursive=True,
        )

        assert result["status"] == "error"
        assert "Files area" in result["message"]
        lakehouse_file_service.list_files.assert_not_called()

    def test_delete_lakehouse_file(self):
        tools, tool_decorator = _capture_tools()
        mcp = SimpleNamespace(tool=tool_decorator)

        lakehouse_file_service = Mock()
        workspace_service = Mock()
        item_service = Mock()
        workspace_service.resolve_workspace_id.return_value = "ws-1"
        item_service.get_item_by_name.return_value = Mock(
            id="lh-1",
            display_name="Lakehouse",
        )

        register_lakehouse_file_tools(
            mcp, lakehouse_file_service, workspace_service, item_service
        )

        result = tools["delete_lakehouse_file"](
            workspace_name="Workspace",
            lakehouse_name="Lakehouse",
            path="raw/sample.csv",
        )

        assert result["status"] == "success"
        lakehouse_file_service.delete_file.assert_called_once_with(
            workspace_id="ws-1",
            lakehouse_id="lh-1",
            path="raw/sample.csv",
            recursive=False,
        )
