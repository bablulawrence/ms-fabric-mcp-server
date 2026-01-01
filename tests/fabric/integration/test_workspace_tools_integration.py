"""Integration tests for workspace tools."""

import pytest


@pytest.mark.integration
@pytest.mark.asyncio
async def test_list_workspaces_includes_configured(call_tool, workspace_name):
    result = await call_tool("list_workspaces")

    assert result["status"] == "success"
    workspace_names = [ws.get("display_name") for ws in result.get("workspaces", [])]
    assert workspace_name in workspace_names
