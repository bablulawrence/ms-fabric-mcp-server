"""Integration tests for pipeline tools."""

import asyncio
import json

import pytest

from tests.conftest import unique_name


@pytest.mark.integration
@pytest.mark.asyncio
async def test_create_pipeline_and_add_activity(
    call_tool, delete_item_if_exists, workspace_name
):
    pipeline_name = unique_name("e2e_pipeline")
    try:
        create_result = await call_tool(
            "create_pipeline",
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
async def test_pipeline_definition_roundtrip(
    call_tool,
    delete_item_if_exists,
    workspace_name,
):
    pipeline_name = unique_name("e2e_pipeline_definition")
    try:
        create_result = await call_tool(
            "create_pipeline",
            workspace_name=workspace_name,
            pipeline_name=pipeline_name,
            pipeline_content_json={"properties": {"activities": []}},
        )
        assert create_result["status"] == "success"

        get_result = await call_tool(
            "get_pipeline_definition",
            workspace_name=workspace_name,
            pipeline_name=pipeline_name,
        )
        assert get_result["status"] == "success"
        definition = get_result.get("pipeline_content_json", {})
        activities = definition.get("properties", {}).get("activities", [])
        assert isinstance(activities, list)

        updated_definition = {
            **definition,
            "properties": {
                **definition.get("properties", {}),
                "activities": activities
                + [
                    {
                        "name": "WaitShort",
                        "type": "Wait",
                        "dependsOn": [],
                        "typeProperties": {"waitTimeInSeconds": 1},
                    }
                ],
            },
        }

        update_result = await call_tool(
            "update_pipeline_definition",
            workspace_name=workspace_name,
            pipeline_name=pipeline_name,
            pipeline_content_json=updated_definition,
        )
        assert update_result["status"] == "success"
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
            "create_pipeline",
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


@pytest.mark.integration
@pytest.mark.asyncio
async def test_add_copy_activity_to_pipeline_sql_mode(
    call_tool,
    delete_item_if_exists,
    lakehouse_id,
    pipeline_copy_sql_inputs,
    workspace_name,
):
    if not pipeline_copy_sql_inputs:
        pytest.skip("Missing pipeline SQL copy inputs")

    pipeline_name = unique_name("e2e_pipeline_copy_sql")
    try:
        create_result = await call_tool(
            "create_pipeline",
            workspace_name=workspace_name,
            pipeline_name=pipeline_name,
            description="Integration test pipeline copy (sql mode)",
        )
        assert create_result["status"] == "success"

        add_kwargs = {
            "workspace_name": workspace_name,
            "pipeline_name": pipeline_name,
            "source_type": "LakehouseTableSource",
            "source_connection_id": pipeline_copy_sql_inputs["source_connection_id"],
            "source_table_schema": pipeline_copy_sql_inputs["source_schema"],
            "source_table_name": pipeline_copy_sql_inputs["source_table"],
            "destination_lakehouse_id": lakehouse_id,
            "destination_connection_id": pipeline_copy_sql_inputs[
                "destination_connection_id"
            ],
            "destination_table_name": pipeline_copy_sql_inputs["destination_table"],
            "source_access_mode": "sql",
        }
        if pipeline_copy_sql_inputs.get("source_sql_query"):
            add_kwargs["source_sql_query"] = pipeline_copy_sql_inputs[
                "source_sql_query"
            ]

        add_result = await call_tool("add_copy_activity_to_pipeline", **add_kwargs)
        assert add_result["status"] == "success"
    finally:
        await delete_item_if_exists(pipeline_name, "DataPipeline")


@pytest.mark.integration
@pytest.mark.asyncio
async def test_add_notebook_activity_to_pipeline(
    call_tool,
    delete_item_if_exists,
    notebook_fixture_path,
    workspace_name,
):
    pipeline_name = unique_name("e2e_pipeline_notebook")
    notebook_name = unique_name("e2e_notebook")
    try:
        create_result = await call_tool(
            "create_pipeline",
            workspace_name=workspace_name,
            pipeline_name=pipeline_name,
            description="Integration test pipeline notebook activity",
        )
        assert create_result["status"] == "success"

        notebook_content = json.loads(notebook_fixture_path.read_text())
        import_result = await call_tool(
            "create_notebook",
            workspace_name=workspace_name,
            notebook_name=notebook_name,
            notebook_content=notebook_content,
        )
        assert import_result["status"] == "success"

        add_result = await call_tool(
            "add_notebook_activity_to_pipeline",
            workspace_name=workspace_name,
            pipeline_name=pipeline_name,
            notebook_name=notebook_name,
        )
        assert add_result["status"] == "success"
    finally:
        await delete_item_if_exists(pipeline_name, "DataPipeline")
        await delete_item_if_exists(notebook_name, "Notebook")


@pytest.mark.integration
@pytest.mark.asyncio
async def test_add_dataflow_activity_to_pipeline(
    call_tool,
    delete_item_if_exists,
    dataflow_name,
    workspace_name,
):
    pipeline_name = unique_name("e2e_pipeline_dataflow")
    try:
        create_result = await call_tool(
            "create_pipeline",
            workspace_name=workspace_name,
            pipeline_name=pipeline_name,
            description="Integration test pipeline dataflow activity",
        )
        assert create_result["status"] == "success"

        add_result = await call_tool(
            "add_dataflow_activity_to_pipeline",
            workspace_name=workspace_name,
            pipeline_name=pipeline_name,
            dataflow_name=dataflow_name,
        )
        assert add_result["status"] == "success"
    finally:
        await delete_item_if_exists(pipeline_name, "DataPipeline")


@pytest.mark.integration
@pytest.mark.asyncio
async def test_add_activity_dependency_tool(
    call_tool,
    delete_item_if_exists,
    workspace_name,
):
    pipeline_name = unique_name("e2e_pipeline_dependency")
    try:
        create_result = await call_tool(
            "create_pipeline",
            workspace_name=workspace_name,
            pipeline_name=pipeline_name,
            description="Integration test pipeline dependency",
        )
        assert create_result["status"] == "success"

        activity_a = {
            "name": "WaitA",
            "type": "Wait",
            "dependsOn": [],
            "typeProperties": {"waitTimeInSeconds": 1},
        }
        activity_b = {
            "name": "WaitB",
            "type": "Wait",
            "dependsOn": [],
            "typeProperties": {"waitTimeInSeconds": 1},
        }

        add_a = await call_tool(
            "add_activity_to_pipeline",
            workspace_name=workspace_name,
            pipeline_name=pipeline_name,
            activity_json=activity_a,
        )
        assert add_a["status"] == "success"

        add_b = await call_tool(
            "add_activity_to_pipeline",
            workspace_name=workspace_name,
            pipeline_name=pipeline_name,
            activity_json=activity_b,
        )
        assert add_b["status"] == "success"

        dependency_result = await call_tool(
            "add_activity_dependency",
            workspace_name=workspace_name,
            pipeline_name=pipeline_name,
            activity_name="WaitB",
            depends_on=["WaitA"],
        )
        assert dependency_result["status"] == "success"
        assert dependency_result.get("added_count", 0) >= 1

        definition = await call_tool(
            "get_pipeline_definition",
            workspace_name=workspace_name,
            pipeline_name=pipeline_name,
        )
        assert definition["status"] == "success"
        activities = (
            definition.get("pipeline_content_json", {})
            .get("properties", {})
            .get("activities", [])
        )
        target = next(
            (activity for activity in activities if activity.get("name") == "WaitB"),
            None,
        )
        assert target is not None
        depends_on = target.get("dependsOn", [])
        assert any(dep.get("activity") == "WaitA" for dep in depends_on)
    finally:
        await delete_item_if_exists(pipeline_name, "DataPipeline")


@pytest.mark.integration
@pytest.mark.asyncio
async def test_remove_dependencies_and_delete_activity(
    call_tool,
    delete_item_if_exists,
    workspace_name,
):
    pipeline_name = unique_name("e2e_pipeline_delete_dep")
    try:
        create_result = await call_tool(
            "create_pipeline",
            workspace_name=workspace_name,
            pipeline_name=pipeline_name,
            description="Integration test pipeline delete activity",
        )
        assert create_result["status"] == "success"

        activity_a = {
            "name": "WaitA",
            "type": "Wait",
            "dependsOn": [],
            "typeProperties": {"waitTimeInSeconds": 1},
        }
        activity_b = {
            "name": "WaitB",
            "type": "Wait",
            "dependsOn": [{"activity": "WaitA", "dependencyConditions": ["Succeeded"]}],
            "typeProperties": {"waitTimeInSeconds": 1},
        }
        activity_c = {
            "name": "WaitC",
            "type": "Wait",
            "dependsOn": [{"activity": "WaitA", "dependencyConditions": ["Succeeded"]}],
            "typeProperties": {"waitTimeInSeconds": 1},
        }

        add_a = await call_tool(
            "add_activity_to_pipeline",
            workspace_name=workspace_name,
            pipeline_name=pipeline_name,
            activity_json=activity_a,
        )
        assert add_a["status"] == "success"

        add_b = await call_tool(
            "add_activity_to_pipeline",
            workspace_name=workspace_name,
            pipeline_name=pipeline_name,
            activity_json=activity_b,
        )
        assert add_b["status"] == "success"

        add_c = await call_tool(
            "add_activity_to_pipeline",
            workspace_name=workspace_name,
            pipeline_name=pipeline_name,
            activity_json=activity_c,
        )
        assert add_c["status"] == "success"

        delete_blocked = await call_tool(
            "delete_activity_from_pipeline",
            workspace_name=workspace_name,
            pipeline_name=pipeline_name,
            activity_name="WaitA",
        )
        assert delete_blocked["status"] == "error"

        remove_result = await call_tool(
            "remove_activity_dependency",
            workspace_name=workspace_name,
            pipeline_name=pipeline_name,
            activity_name="WaitA",
        )
        assert remove_result["status"] == "success"
        assert remove_result["removed_count"] == 2

        delete_result = await call_tool(
            "delete_activity_from_pipeline",
            workspace_name=workspace_name,
            pipeline_name=pipeline_name,
            activity_name="WaitA",
        )
        assert delete_result["status"] == "success"
    finally:
        await delete_item_if_exists(pipeline_name, "DataPipeline")


@pytest.mark.integration
@pytest.mark.asyncio
async def test_remove_dependency_from_specific_activity(
    call_tool,
    delete_item_if_exists,
    workspace_name,
):
    pipeline_name = unique_name("e2e_pipeline_delete_dep_specific")
    try:
        create_result = await call_tool(
            "create_pipeline",
            workspace_name=workspace_name,
            pipeline_name=pipeline_name,
            description="Integration test pipeline delete activity (specific)",
        )
        assert create_result["status"] == "success"

        activity_a = {
            "name": "WaitA",
            "type": "Wait",
            "dependsOn": [],
            "typeProperties": {"waitTimeInSeconds": 1},
        }
        activity_b = {
            "name": "WaitB",
            "type": "Wait",
            "dependsOn": [{"activity": "WaitA", "dependencyConditions": ["Succeeded"]}],
            "typeProperties": {"waitTimeInSeconds": 1},
        }

        add_a = await call_tool(
            "add_activity_to_pipeline",
            workspace_name=workspace_name,
            pipeline_name=pipeline_name,
            activity_json=activity_a,
        )
        assert add_a["status"] == "success"

        add_b = await call_tool(
            "add_activity_to_pipeline",
            workspace_name=workspace_name,
            pipeline_name=pipeline_name,
            activity_json=activity_b,
        )
        assert add_b["status"] == "success"

        remove_result = await call_tool(
            "remove_activity_dependency",
            workspace_name=workspace_name,
            pipeline_name=pipeline_name,
            activity_name="WaitA",
            from_activity_name="WaitB",
        )
        assert remove_result["status"] == "success"
        assert remove_result["removed_count"] == 1

        delete_result = await call_tool(
            "delete_activity_from_pipeline",
            workspace_name=workspace_name,
            pipeline_name=pipeline_name,
            activity_name="WaitA",
        )
        assert delete_result["status"] == "success"
    finally:
        await delete_item_if_exists(pipeline_name, "DataPipeline")


@pytest.mark.integration
@pytest.mark.asyncio
async def test_get_pipeline_activity_runs(
    call_tool,
    delete_item_if_exists,
    workspace_name,
):
    """Test get_pipeline_activity_runs after running a pipeline."""
    pipeline_name = unique_name("e2e_pipeline_activity_runs")
    try:
        # Create pipeline with a simple Wait activity
        create_result = await call_tool(
            "create_pipeline",
            workspace_name=workspace_name,
            pipeline_name=pipeline_name,
            description="Integration test pipeline for activity runs",
        )
        assert create_result["status"] == "success"

        activity = {
            "name": "WaitActivity",
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

        # Run the pipeline
        run_result = await call_tool(
            "run_on_demand_job",
            workspace_name=workspace_name,
            item_name=pipeline_name,
            item_type="DataPipeline",
            job_type="Pipeline",
        )
        assert run_result["status"] == "success"
        job_instance_id = run_result["job_instance_id"]

        # Wait for pipeline to complete (poll status)
        max_wait = 120  # 2 minutes max
        poll_interval = 5
        elapsed = 0
        while elapsed < max_wait:
            status_result = await call_tool(
                "get_job_status",
                workspace_name=workspace_name,
                item_name=pipeline_name,
                item_type="DataPipeline",
                job_instance_id=job_instance_id,
            )
            assert status_result["status"] == "success"
            job = status_result.get("job", {})
            if job.get("is_terminal"):
                break
            await asyncio.sleep(poll_interval)
            elapsed += poll_interval

        # Wait a bit more for activity runs to be available
        await asyncio.sleep(5)

        # Now get activity runs
        activity_runs_result = await call_tool(
            "get_pipeline_activity_runs",
            workspace_name=workspace_name,
            job_instance_id=job_instance_id,
        )
        assert activity_runs_result["status"] == "success"

        # Activity runs may not be immediately available; check gracefully
        activity_count = activity_runs_result["activity_count"]
        if activity_count == 0:
            # Retry once after additional delay
            await asyncio.sleep(10)
            activity_runs_result = await call_tool(
                "get_pipeline_activity_runs",
                workspace_name=workspace_name,
                job_instance_id=job_instance_id,
            )
            activity_count = activity_runs_result["activity_count"]

        assert (
            activity_count >= 1
        ), f"Expected at least 1 activity run, got {activity_count}"
        # pipeline_name may be None if not in the activity runs response
        # This is acceptable as long as we get activities back

        # Check that WaitActivity is in the results
        activities = activity_runs_result.get("activities", [])
        activity_names = [a["activity_name"] for a in activities]
        assert "WaitActivity" in activity_names

        # Verify activity details
        wait_activity = next(
            a for a in activities if a["activity_name"] == "WaitActivity"
        )
        assert wait_activity["activity_type"] == "Wait"
        assert wait_activity["status"] in [
            "Succeeded",
            "Failed",
            "InProgress",
            "Cancelled",
        ]
        assert (
            wait_activity["duration_ms"] is not None
            or wait_activity["status"] == "InProgress"
        )

    finally:
        await delete_item_if_exists(pipeline_name, "DataPipeline")
