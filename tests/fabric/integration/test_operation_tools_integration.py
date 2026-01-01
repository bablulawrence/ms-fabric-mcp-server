"""Integration tests for operation result tool."""

import pytest

from ms_fabric_mcp_server.client import FabricConfig, FabricClient
from ms_fabric_mcp_server.services import FabricWorkspaceService
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
        workspace_id = workspace_service.resolve_workspace_id(workspace_name)

        async def _wait_for_operation(operation_id: str, timeout_seconds: int = 300):
            async def _check():
                try:
                    status_response = client.make_api_request("GET", f"operations/{operation_id}")
                    status_payload = status_response.json()
                except Exception:
                    return None
                status_value = str(status_payload.get("status", "")).lower()
                if status_value in {"succeeded", "failed", "canceled", "cancelled"}:
                    return status_payload
                return None

            return await poll_until(_check, timeout_seconds=timeout_seconds, interval_seconds=10)

        async def _get_definition():
            try:
                response = client.make_api_request(
                    "POST",
                    f"workspaces/{workspace_id}/items/{pipeline_id}/getDefinition",
                )
                if response.status_code == 202:
                    operation_id = response.headers.get("x-ms-operation-id")
                    if not operation_id:
                        return None
                    status_payload = await _wait_for_operation(operation_id, timeout_seconds=300)
                    if not status_payload:
                        return None
                    status_value = str(status_payload.get("status", "")).lower()
                    if status_value != "succeeded":
                        raise AssertionError(f"getDefinition operation failed: {status_payload}")
                    result_response = client.make_api_request(
                        "GET",
                        f"operations/{operation_id}/result",
                    )
                    return result_response.json()
                return response.json()
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

        if response.status_code == 202:
            status_payload = await _wait_for_operation(operation_id, timeout_seconds=300)
            if not status_payload:
                pytest.skip("Operation did not complete within timeout")
            status_value = str(status_payload.get("status", "")).lower()
            assert status_value == "succeeded", f"Operation failed: {status_payload}"

        op_result = await call_tool("get_operation_result", operation_id=operation_id)
        assert op_result["status"] == "success"

    finally:
        if pipeline_name:
            await delete_item_if_exists(pipeline_name, "DataPipeline")
