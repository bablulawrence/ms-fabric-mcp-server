"""Integration tests for item tools."""

import pytest


@pytest.mark.integration
@pytest.mark.asyncio
async def test_list_items_in_workspace(call_tool, workspace_name):
    result = await call_tool("list_items", workspace_name=workspace_name)

    assert result["status"] == "success"
    assert isinstance(result.get("item_count"), int)


@pytest.mark.integration
@pytest.mark.asyncio
async def test_list_items_with_type_filter(call_tool, workspace_name):
    result = await call_tool(
        "list_items",
        workspace_name=workspace_name,
        item_type="Notebook",
    )

    assert result["status"] == "success"
    for item in result.get("items", []):
        assert item.get("type") == "Notebook"
