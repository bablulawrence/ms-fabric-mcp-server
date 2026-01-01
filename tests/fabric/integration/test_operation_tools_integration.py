"""Integration tests for operation result tool."""

import pytest

from ms_fabric_mcp_server.client import FabricConfig, FabricClient
from ms_fabric_mcp_server.services import FabricItemService, FabricWorkspaceService
from tests.conftest import unique_name


@pytest.mark.integration
@pytest.mark.asyncio
async def test_get_operation_result_from_async_call(
    call_tool,
    delete_item_if_exists,
    poll_until,
    workspace_name,
):
    pipeline_name = unique_name("e2e_pipeline_op")
    pipeline_id = None

    try:
        create_result = await call_tool(
            "create_blank_pipeline",
            workspace_name=workspace_name,
            pipeline_name=pipeline_name,
            description="Integration test pipeline operation",
        )
        assert create_result["status"] == "success"
        pipeline_id = create_result.get("pipeline_id")

        if not pipeline_id or pipeline_id == "unknown":
            items_result = await call_tool(
                "list_items",
                workspace_name=workspace_name,
                item_type="DataPipeline",
            )
            for item in items_result.get("items", []):
                if item.get("display_name") == pipeline_name:
                    pipeline_id = item.get("id")
                    break

        if not pipeline_id or pipeline_id == "unknown":
            pytest.skip("Could not resolve pipeline ID for operation test")

        config = FabricConfig.from_environment()
        client = FabricClient(config)
        workspace_service = FabricWorkspaceService(client)
        item_service = FabricItemService(client)

        workspace_id = workspace_service.resolve_workspace_id(workspace_name)
        async def _get_definition():
            try:
                return item_service.get_item_definition(workspace_id, pipeline_id)
            except Exception:
                return None

        definition_response = await poll_until(_get_definition, timeout_seconds=120, interval_seconds=5)
        if not definition_response:
            pytest.skip("Pipeline definition not available for operation test")
        update_payload = {"definition": definition_response["definition"]}

        response = client.make_api_request(
            "POST",
            f"workspaces/{workspace_id}/items/{pipeline_id}/updateDefinition",
            payload=update_payload,
        )

        operation_id = response.headers.get("x-ms-operation-id")
        if not operation_id:
            pytest.skip("No x-ms-operation-id header returned (synchronous completion)")

        op_result = await call_tool("get_operation_result", operation_id=operation_id)
        assert op_result["status"] == "success"

    finally:
        if pipeline_name:
            await delete_item_if_exists(pipeline_name, "DataPipeline")
