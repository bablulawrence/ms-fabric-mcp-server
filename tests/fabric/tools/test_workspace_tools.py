"""Tests for Fabric workspace tools."""

import pytest

from ms_fabric_mcp_server.models.workspace import FabricWorkspace
from tests.fabric.tools.utils import capture_tools
from tests.fixtures.mocks import ServiceMockFactory


@pytest.mark.unit
class TestWorkspaceTools:
    """Test suite for workspace MCP tools."""
    
    def test_list_workspaces_tool(self, mock_fastmcp):
        """Test list_workspaces tool registration and execution."""
        from ms_fabric_mcp_server.tools.workspace_tools import register_workspace_tools
        
        tools, mcp = capture_tools()
        workspace_service = ServiceMockFactory.workspace_service()
        workspace_service.list_workspaces.return_value = [
            FabricWorkspace(
                id="ws-1",
                display_name="Workspace 1",
                description="Workspace 1",
                type="Workspace",
            ),
            FabricWorkspace(
                id="ws-2",
                display_name="Workspace 2",
                description="Workspace 2",
                type="Workspace",
            ),
        ]

        register_workspace_tools(mcp, workspace_service)

        result = tools["list_workspaces"]()
        assert result["status"] == "success"
        assert result["workspace_count"] == 2


@pytest.mark.unit
class TestToolRegistration:
    """Test tool registration functions."""
    
    def test_register_fabric_tools(self, mock_fastmcp):
        """Test main register_fabric_tools function."""
        from ms_fabric_mcp_server import register_fabric_tools
        
        # Should not raise
        register_fabric_tools(mock_fastmcp)
    
    def test_individual_tool_sets_exported(self):
        """Test that individual registration functions are available from tools module."""
        from ms_fabric_mcp_server.tools import (
            register_workspace_tools,
            register_item_tools,
            register_notebook_tools,
            register_job_tools,
            register_sql_tools,
            register_livy_tools,
            register_pipeline_tools,
            register_semantic_model_tools,
            register_powerbi_tools,
        )
        
        assert all([
            callable(register_workspace_tools),
            callable(register_item_tools),
            callable(register_notebook_tools),
            callable(register_job_tools),
            callable(register_sql_tools),
            callable(register_livy_tools),
            callable(register_pipeline_tools),
            callable(register_semantic_model_tools),
            callable(register_powerbi_tools),
        ])
