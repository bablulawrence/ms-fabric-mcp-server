"""Integration tests for item tools."""

import pytest

from tests.conftest import unique_name


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


@pytest.mark.integration
@pytest.mark.asyncio
async def test_list_items_with_root_folder_path(
    call_tool,
    delete_item_if_exists,
    workspace_name,
):
    folder_path = f"{unique_name('e2e_folder')}/pipelines"
    pipeline_name = unique_name("e2e_pipeline")
    try:
        create_result = await call_tool(
            "create_blank_pipeline",
            workspace_name=workspace_name,
            pipeline_name=pipeline_name,
            folder_path=folder_path,
        )
        assert create_result["status"] == "success"

        list_result = await call_tool(
            "list_items",
            workspace_name=workspace_name,
            root_folder_path=folder_path,
        )
        assert list_result["status"] == "success"
        assert any(
            item.get("display_name") == pipeline_name
            for item in list_result.get("items", [])
        )
    finally:
        await delete_item_if_exists(pipeline_name, "DataPipeline")
