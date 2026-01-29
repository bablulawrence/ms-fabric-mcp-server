"""Tests for item MCP tools."""

from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from ms_fabric_mcp_server.models.item import FabricItem
from ms_fabric_mcp_server.tools.item_tools import register_item_tools


def _capture_tools():
    tools = {}

    def tool(**_kwargs):
        def decorator(func):
            tools[func.__name__] = func
            return func

        return decorator

    return tools, tool


@pytest.mark.unit
class TestItemTools:
    def test_list_items_root_folder_path(self):
        tools, tool_decorator = _capture_tools()
        mcp = SimpleNamespace(tool=tool_decorator)

        item_service = Mock()
        workspace_service = Mock()
        workspace_service.resolve_workspace_id.return_value = "ws-1"
        item_service.resolve_folder_id_from_path.return_value = "folder-1"
        item_service.list_items.return_value = [
            FabricItem(
                id="item-1",
                display_name="Item",
                type="Notebook",
                workspace_id="ws-1",
                folder_id="folder-1",
            )
        ]

        register_item_tools(mcp, item_service, workspace_service)

        result = tools["list_items"](
            workspace_name="Workspace",
            root_folder_path="Team/ETL",
        )

        assert result["status"] == "success"
        assert result["items"][0]["folder_id"] == "folder-1"
        item_service.resolve_folder_id_from_path.assert_called_once_with(
            "ws-1", "Team/ETL", create_missing=False
        )
        item_service.list_items.assert_called_once_with(
            "ws-1",
            None,
            root_folder_id="folder-1",
            recursive=True,
        )

    def test_rename_item_includes_folder_id(self):
        tools, tool_decorator = _capture_tools()
        mcp = SimpleNamespace(tool=tool_decorator)

        item_service = Mock()
        workspace_service = Mock()
        workspace_service.resolve_workspace_id.return_value = "ws-1"
        item_service.rename_item.return_value = FabricItem(
            id="item-1",
            display_name="Renamed",
            type="Notebook",
            workspace_id="ws-1",
            folder_id="folder-9",
        )

        register_item_tools(mcp, item_service, workspace_service)

        result = tools["rename_item"](
            workspace_name="Workspace",
            item_id="item-1",
            new_display_name="Renamed",
        )

        assert result["status"] == "success"
        assert result["item"]["folder_id"] == "folder-9"

    def test_move_item_to_folder_includes_folder_id(self):
        tools, tool_decorator = _capture_tools()
        mcp = SimpleNamespace(tool=tool_decorator)

        item_service = Mock()
        workspace_service = Mock()
        workspace_service.resolve_workspace_id.return_value = "ws-1"
        item_service.move_item_to_folder.return_value = FabricItem(
            id="item-1",
            display_name="Item",
            type="Notebook",
            workspace_id="ws-1",
            folder_id="folder-2",
        )

        register_item_tools(mcp, item_service, workspace_service)

        result = tools["move_item_to_folder"](
            workspace_name="Workspace",
            item_id="item-1",
            target_folder_id="folder-2",
        )

        assert result["status"] == "success"
        assert result["item"]["folder_id"] == "folder-2"

    def test_delete_item_success(self):
        tools, tool_decorator = _capture_tools()
        mcp = SimpleNamespace(tool=tool_decorator)

        item_service = Mock()
        workspace_service = Mock()
        workspace_service.resolve_workspace_id.return_value = "ws-1"
        item_service.get_item_by_name.return_value = FabricItem(
            id="item-1",
            display_name="Notebook",
            type="Notebook",
            workspace_id="ws-1",
        )

        register_item_tools(mcp, item_service, workspace_service)

        result = tools["delete_item"](
            workspace_name="Workspace",
            item_display_name="Notebook",
            item_type="Notebook",
        )

        assert result["status"] == "success"
        item_service.delete_item.assert_called_once_with("ws-1", "item-1")
