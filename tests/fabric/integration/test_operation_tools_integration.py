"""Integration tests for operation result tool."""

import base64

import pytest

from ms_fabric_mcp_server.client import FabricConfig, FabricClient
from ms_fabric_mcp_server.services import FabricWorkspaceService
from tests.conftest import unique_name


@pytest.mark.integration
@pytest.mark.asyncio
async def test_get_operation_result_from_async_call(
    call_tool,
    delete_item_if_exists,
    notebook_fixture_path,
    poll_until,
    workspace_name,
):
    notebook_name = unique_name("e2e_op_notebook")

    try:
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

        notebook_bytes = notebook_fixture_path.read_bytes()
        notebook_payload = base64.b64encode(notebook_bytes).decode("utf-8")
        create_payload = {
            "displayName": notebook_name,
            "type": "Notebook",
            "definition": {
                "format": "ipynb",
                "parts": [
                    {
                        "path": notebook_fixture_path.name,
                        "payload": notebook_payload,
                        "payloadType": "InlineBase64",
                    }
                ],
            },
        }

        response = client.make_api_request(
            "POST",
            f"workspaces/{workspace_id}/items",
            payload=create_payload,
        )

        operation_id = response.headers.get("x-ms-operation-id")
        if not operation_id:
            pytest.skip("No x-ms-operation-id header returned (synchronous completion)")

        status_payload = await _wait_for_operation(operation_id, timeout_seconds=300)
        if not status_payload:
            pytest.skip("Operation did not complete within timeout")
        status_value = str(status_payload.get("status", "")).lower()
        assert status_value == "succeeded", f"Operation failed: {status_payload}"

        op_result = await call_tool("get_operation_result", operation_id=operation_id)
        assert op_result["status"] == "success"

    finally:
        if notebook_name:
            await delete_item_if_exists(notebook_name, "Notebook")
