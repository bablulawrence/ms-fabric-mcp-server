"""Tests for Fabric workspace service."""

import pytest
from unittest.mock import Mock
from tests.fixtures.mocks import FabricDataFactory, MockResponseFactory


@pytest.mark.unit
class TestFabricWorkspaceService:
    """Test suite for FabricWorkspaceService."""
    
    def test_list_workspaces_success(self, mock_fabric_client):
        """Test listing workspaces successfully."""
        from ms_fabric_mcp_server.services.workspace import FabricWorkspaceService
        
        # Setup mock response
        workspace_data = FabricDataFactory.workspace_list(3)
        response = MockResponseFactory.success(workspace_data)
        mock_fabric_client.make_api_request.return_value = response
        
        service = FabricWorkspaceService(mock_fabric_client)
        workspaces = service.list_workspaces()
        
        assert len(workspaces) == 3
        mock_fabric_client.make_api_request.assert_called_once()
    
    def test_get_workspace_by_name(self, mock_fabric_client):
        """Test getting workspace by display name."""
        from ms_fabric_mcp_server.services.workspace import FabricWorkspaceService
        
        workspace_data = FabricDataFactory.workspace_list(3)
        response = MockResponseFactory.success(workspace_data)
        mock_fabric_client.make_api_request.return_value = response
        
        service = FabricWorkspaceService(mock_fabric_client)
        workspace = service.get_workspace_by_name("Workspace 1")
        
        assert workspace is not None
        assert workspace.display_name == "Workspace 1"
    
    def test_get_workspace_by_name_not_found(self, mock_fabric_client):
        """Test workspace not found returns None."""
        from ms_fabric_mcp_server.services.workspace import FabricWorkspaceService
        from ms_fabric_mcp_server.client.exceptions import FabricWorkspaceNotFoundError
        
        workspace_data = FabricDataFactory.workspace_list(3)
        response = MockResponseFactory.success(workspace_data)
        mock_fabric_client.make_api_request.return_value = response
        
        service = FabricWorkspaceService(mock_fabric_client)
        
        with pytest.raises(FabricWorkspaceNotFoundError):
            service.get_workspace_by_name("Nonexistent Workspace")
    
    def test_create_workspace(self, mock_fabric_client):
        """Test creating a new workspace."""
        from ms_fabric_mcp_server.services.workspace import FabricWorkspaceService
        
        new_workspace = FabricDataFactory.workspace(name="New Workspace")
        response = MockResponseFactory.success(new_workspace, 201)
        mock_fabric_client.make_api_request.return_value = response
        
        service = FabricWorkspaceService(mock_fabric_client)
        workspace = service.create_workspace("New Workspace", description="Test")
        
        assert workspace.display_name == "New Workspace"
        mock_fabric_client.make_api_request.assert_called_once()


@pytest.mark.unit
class TestServiceExports:
    """Test service exports."""
    
    def test_all_services_exported(self):
        """Test that all services are exported from fabric module."""
        from ms_fabric_mcp_server import (
            FabricWorkspaceService,
            FabricItemService,
            FabricNotebookService,
            FabricJobService,
            FabricSQLService,
            FabricLivyService,
        )
        
        assert all([
            FabricWorkspaceService,
            FabricItemService,
            FabricNotebookService,
            FabricJobService,
            FabricSQLService,
            FabricLivyService,
        ])
