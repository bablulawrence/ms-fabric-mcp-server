"""Integration tests for pipeline tools."""

import pytest

from tests.conftest import unique_name


@pytest.mark.integration
@pytest.mark.asyncio
async def test_create_pipeline_and_add_activity(call_tool, delete_item_if_exists, workspace_name):
    pipeline_name = unique_name("e2e_pipeline")
    try:
        create_result = await call_tool(
            "create_blank_pipeline",
            workspace_name=workspace_name,
            pipeline_name=pipeline_name,
            description="Integration test pipeline",
        )
        assert create_result["status"] == "success"

        activity = {
            "name": "WaitShort",
            "type": "Wait",
            "dependsOn": [],
            "typeProperties": {"waitTimeInSeconds": 1},
        }
        add_result = await call_tool(
            "add_activity_to_pipeline",
            workspace_name=workspace_name,
            pipeline_name=pipeline_name,
            activity_json=activity,
        )
        assert add_result["status"] == "success"
    finally:
        await delete_item_if_exists(pipeline_name, "DataPipeline")


@pytest.mark.integration
@pytest.mark.asyncio
async def test_add_copy_activity_to_pipeline(
    call_tool,
    delete_item_if_exists,
    lakehouse_id,
    pipeline_copy_inputs,
    workspace_name,
):
    if not pipeline_copy_inputs:
        pytest.skip("Missing pipeline copy inputs")

    pipeline_name = unique_name("e2e_pipeline_copy")
    try:
        create_result = await call_tool(
            "create_blank_pipeline",
            workspace_name=workspace_name,
            pipeline_name=pipeline_name,
            description="Integration test pipeline copy",
        )
        assert create_result["status"] == "success"

        add_result = await call_tool(
            "add_copy_activity_to_pipeline",
            workspace_name=workspace_name,
            pipeline_name=pipeline_name,
            source_type=pipeline_copy_inputs["source_type"],
            source_connection_id=pipeline_copy_inputs["source_connection_id"],
            source_table_schema=pipeline_copy_inputs["source_schema"],
            source_table_name=pipeline_copy_inputs["source_table"],
            destination_lakehouse_id=lakehouse_id,
            destination_connection_id=pipeline_copy_inputs["destination_connection_id"],
            destination_table_name=pipeline_copy_inputs["destination_table"],
        )
        assert add_result["status"] == "success"
    finally:
        await delete_item_if_exists(pipeline_name, "DataPipeline")
