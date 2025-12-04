"""Tests for Fabric workspace tools."""

import pytest
from unittest.mock import Mock
from tests.fixtures.mocks import FabricDataFactory, ServiceMockFactory


@pytest.mark.unit
class TestWorkspaceTools:
    """Test suite for workspace MCP tools."""
    
    def test_list_workspaces_tool(self, mock_fastmcp):
        """Test list_workspaces tool registration and execution."""
        from ms_fabric_mcp_server.tools.workspace_tools import register_workspace_tools
        
        # Create mock service
        workspace_service = ServiceMockFactory.workspace_service()
        workspace_service.list_workspaces.return_value = [
            FabricDataFactory.workspace(workspace_id="ws-1", name="Workspace 1"),
            FabricDataFactory.workspace(workspace_id="ws-2", name="Workspace 2"),
        ]
        
        # Register tools
        register_workspace_tools(mock_fastmcp, workspace_service)
        
        # Verify tool was registered
        assert mock_fastmcp.tool.called
    
    def test_create_workspace_tool(self, mock_fastmcp):
        """Test create_workspace tool."""
        from ms_fabric_mcp_server.tools.workspace_tools import register_workspace_tools
        
        workspace_service = ServiceMockFactory.workspace_service()
        workspace_service.create_workspace.return_value = FabricDataFactory.workspace(name="New Workspace")
        
        register_workspace_tools(mock_fastmcp, workspace_service)
        
        assert mock_fastmcp.tool.called


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
        )
        
        assert all([
            callable(register_workspace_tools),
            callable(register_item_tools),
            callable(register_notebook_tools),
            callable(register_job_tools),
            callable(register_sql_tools),
            callable(register_livy_tools),
        ])
