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


@pytest.mark.integration
@pytest.mark.asyncio
async def test_delete_item_tool(call_tool, workspace_name, delete_item_if_exists):
    pipeline_name = unique_name("e2e_delete_item")
    try:
        create_result = await call_tool(
            "create_blank_pipeline",
            workspace_name=workspace_name,
            pipeline_name=pipeline_name,
        )
        assert create_result["status"] == "success"

        delete_result = await call_tool(
            "delete_item",
            workspace_name=workspace_name,
            item_display_name=pipeline_name,
            item_type="DataPipeline",
        )
        assert delete_result["status"] == "success"
    finally:
        await delete_item_if_exists(pipeline_name, "DataPipeline")


@pytest.mark.integration
@pytest.mark.asyncio
async def test_rename_item_tool(call_tool, workspace_name, delete_item_if_exists):
    pipeline_name = unique_name("e2e_rename_item")
    new_name = unique_name("e2e_renamed_item")
    try:
        create_result = await call_tool(
            "create_blank_pipeline",
            workspace_name=workspace_name,
            pipeline_name=pipeline_name,
        )
        assert create_result["status"] == "success"
        pipeline_id = create_result.get("pipeline_id")
        assert pipeline_id

        rename_result = await call_tool(
            "rename_item",
            workspace_name=workspace_name,
            item_id=pipeline_id,
            new_display_name=new_name,
        )
        assert rename_result["status"] == "success"
    finally:
        await delete_item_if_exists(new_name, "DataPipeline")


@pytest.mark.integration
@pytest.mark.asyncio
async def test_move_item_to_folder_tool(call_tool, workspace_name, delete_item_if_exists):
    pipeline_name = unique_name("e2e_move_item")
    folder_path = f"{unique_name('e2e_folder')}/target"
    target_pipeline = unique_name("e2e_folder_marker")
    try:
        # Create a pipeline in the target folder to obtain folder_id
        target_result = await call_tool(
            "create_blank_pipeline",
            workspace_name=workspace_name,
            pipeline_name=target_pipeline,
            folder_path=folder_path,
        )
        assert target_result["status"] == "success"

        list_result = await call_tool(
            "list_items",
            workspace_name=workspace_name,
            root_folder_path=folder_path,
            item_type="DataPipeline",
        )
        assert list_result["status"] == "success"
        folder_id = None
        for item in list_result.get("items", []):
            if item.get("display_name") == target_pipeline:
                folder_id = item.get("folder_id")
                break
        assert folder_id

        create_result = await call_tool(
            "create_blank_pipeline",
            workspace_name=workspace_name,
            pipeline_name=pipeline_name,
        )
        assert create_result["status"] == "success"
        pipeline_id = create_result.get("pipeline_id")
        assert pipeline_id

        move_result = await call_tool(
            "move_item_to_folder",
            workspace_name=workspace_name,
            item_id=pipeline_id,
            target_folder_id=folder_id,
        )
        assert move_result["status"] == "success"
    finally:
        await delete_item_if_exists(pipeline_name, "DataPipeline")
        await delete_item_if_exists(target_pipeline, "DataPipeline")
