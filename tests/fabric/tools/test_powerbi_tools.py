"""Tests for Power BI MCP tools."""

from unittest.mock import Mock

import pytest

from ms_fabric_mcp_server.tools.powerbi_tools import register_powerbi_tools
from tests.fabric.tools.utils import capture_tools


@pytest.mark.unit
class TestPowerBITools:
    def test_powerbi_tools_smoke(self):
        tools, mcp = capture_tools()
        powerbi_service = Mock()
        powerbi_service.refresh_semantic_model.return_value = {"status": "success"}
        powerbi_service.execute_dax_query.return_value = {"status": "success"}

        register_powerbi_tools(mcp, powerbi_service)

        refresh = tools["refresh_semantic_model"](
            workspace_name="Workspace",
            semantic_model_name="Model",
        )
        assert refresh["status"] == "success"

        dax = tools["execute_dax_query"](
            workspace_name="Workspace",
            semantic_model_name="Model",
            query="EVALUATE ROW(\"x\", 1)",
        )
        assert dax["status"] == "success"
