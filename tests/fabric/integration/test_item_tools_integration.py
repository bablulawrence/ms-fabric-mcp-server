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
async def test_create_and_list_folders(call_tool, workspace_name):
    parent_folder = unique_name("e2e_folder_root")
    child_folder = unique_name("e2e_folder_child")

    create_parent = await call_tool(
        "create_folder",
        workspace_name=workspace_name,
        folder_name=parent_folder,
    )
    assert create_parent["status"] == "success"

    create_child = await call_tool(
        "create_folder",
        workspace_name=workspace_name,
        folder_name=child_folder,
        parent_folder_path=parent_folder,
    )
    assert create_child["status"] == "success"

    list_result = await call_tool(
        "list_folders",
        workspace_name=workspace_name,
        root_folder_path=parent_folder,
        recursive=True,
    )
    assert list_result["status"] == "success"
    assert any(
        folder.get("display_name") == child_folder
        for folder in list_result.get("folders", [])
    )


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
async def test_list_items_recursive_root_folder_path(
    call_tool,
    delete_item_if_exists,
    workspace_name,
):
    parent_folder = unique_name("e2e_parent_folder")
    folder_path = f"{parent_folder}/child"
    pipeline_name = unique_name("e2e_pipeline_nested")
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
            root_folder_path=parent_folder,
            recursive=True,
            item_type="DataPipeline",
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
async def test_move_item_to_folder_tool(
    call_tool,
    workspace_name,
    delete_item_if_exists,
    poll_until,
):
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

        async def _find_moved_item():
            listed = await call_tool(
                "list_items",
                workspace_name=workspace_name,
                root_folder_path=folder_path,
                item_type="DataPipeline",
            )
            if listed.get("status") != "success":
                return listed
            for item in listed.get("items", []):
                if item.get("display_name") == pipeline_name:
                    return listed
            return None

        found = await poll_until(_find_moved_item, timeout_seconds=120, interval_seconds=10)
        assert found is not None
    finally:
        await delete_item_if_exists(pipeline_name, "DataPipeline")
        await delete_item_if_exists(target_pipeline, "DataPipeline")


@pytest.mark.integration
@pytest.mark.asyncio
async def test_move_folder_tool(call_tool, workspace_name):
    source_parent = unique_name("e2e_folder_source")
    target_parent = unique_name("e2e_folder_target")
    child_folder = unique_name("e2e_folder_child")

    create_source = await call_tool(
        "create_folder",
        workspace_name=workspace_name,
        folder_name=source_parent,
    )
    assert create_source["status"] == "success"

    create_target = await call_tool(
        "create_folder",
        workspace_name=workspace_name,
        folder_name=target_parent,
    )
    assert create_target["status"] == "success"

    create_child = await call_tool(
        "create_folder",
        workspace_name=workspace_name,
        folder_name=child_folder,
        parent_folder_path=source_parent,
    )
    assert create_child["status"] == "success"
    folder_id = create_child.get("folder_id")
    assert folder_id

    move_result = await call_tool(
        "move_folder",
        workspace_name=workspace_name,
        folder_id=folder_id,
        target_folder_path=target_parent,
    )
    assert move_result["status"] == "success"

    list_result = await call_tool(
        "list_folders",
        workspace_name=workspace_name,
        root_folder_path=target_parent,
        recursive=True,
    )
    assert list_result["status"] == "success"
    assert any(
        folder.get("display_name") == child_folder
        for folder in list_result.get("folders", [])
    )


@pytest.mark.integration
@pytest.mark.asyncio
async def test_get_item_tool(call_tool, workspace_name, delete_item_if_exists):
    pipeline_name = unique_name("e2e_get_item")
    try:
        create_result = await call_tool(
            "create_blank_pipeline",
            workspace_name=workspace_name,
            pipeline_name=pipeline_name,
        )
        assert create_result["status"] == "success"
        pipeline_id = create_result.get("pipeline_id")
        assert pipeline_id

        by_name = await call_tool(
            "get_item",
            workspace_name=workspace_name,
            item_display_name=pipeline_name,
            item_type="DataPipeline",
        )
        assert by_name["status"] == "success"
        assert by_name["item"]["id"] == pipeline_id

        by_id = await call_tool(
            "get_item",
            workspace_name=workspace_name,
            item_id=pipeline_id,
        )
        assert by_id["status"] == "success"
        assert by_id["item"]["display_name"] == pipeline_name
    finally:
        await delete_item_if_exists(pipeline_name, "DataPipeline")


@pytest.mark.integration
@pytest.mark.asyncio
async def test_delete_folder_tool(call_tool, workspace_name):
    folder_name = unique_name("e2e_delete_folder")

    create_result = await call_tool(
        "create_folder",
        workspace_name=workspace_name,
        folder_name=folder_name,
    )
    assert create_result["status"] == "success"

    delete_result = await call_tool(
        "delete_folder",
        workspace_name=workspace_name,
        folder_path=folder_name,
    )
    assert delete_result["status"] == "success"


@pytest.mark.integration
@pytest.mark.asyncio
async def test_create_lakehouse_tool(call_tool, workspace_name, delete_item_if_exists):
    lakehouse_name = unique_name("e2e_lakehouse")
    try:
        create_result = await call_tool(
            "create_lakehouse",
            workspace_name=workspace_name,
            lakehouse_name=lakehouse_name,
            description="Test lakehouse",
            enable_schemas=True,
        )
        assert create_result["status"] == "success"

        item_result = await call_tool(
            "get_item",
            workspace_name=workspace_name,
            item_display_name=lakehouse_name,
            item_type="Lakehouse",
        )
        assert item_result["status"] == "success"
        assert item_result["item"]["display_name"] == lakehouse_name
    finally:
        await delete_item_if_exists(lakehouse_name, "Lakehouse")
