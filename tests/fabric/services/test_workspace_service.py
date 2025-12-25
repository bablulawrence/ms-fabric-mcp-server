"""Tests for Fabric workspace service."""

from unittest.mock import Mock

import pytest

from ms_fabric_mcp_server.client.exceptions import (
    FabricAPIError,
    FabricWorkspaceNotFoundError,
)
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

    def test_list_workspaces_api_error(self, mock_fabric_client):
        """API errors propagate."""
        from ms_fabric_mcp_server.services.workspace import FabricWorkspaceService

        mock_fabric_client.make_api_request.side_effect = FabricAPIError(500, "boom")

        service = FabricWorkspaceService(mock_fabric_client)

        with pytest.raises(FabricAPIError):
            service.list_workspaces()
    
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
        
        workspace_data = FabricDataFactory.workspace_list(3)
        response = MockResponseFactory.success(workspace_data)
        mock_fabric_client.make_api_request.return_value = response
        
        service = FabricWorkspaceService(mock_fabric_client)
        
        with pytest.raises(FabricWorkspaceNotFoundError):
            service.get_workspace_by_name("Nonexistent Workspace")

    def test_get_workspace_by_id_success(self, mock_fabric_client):
        """Get workspace by ID maps response."""
        from ms_fabric_mcp_server.services.workspace import FabricWorkspaceService

        workspace = FabricDataFactory.workspace(workspace_id="ws-1", name="Workspace 1")
        response = MockResponseFactory.success(workspace)
        mock_fabric_client.make_api_request.return_value = response

        service = FabricWorkspaceService(mock_fabric_client)
        result = service.get_workspace_by_id("ws-1")

        assert result.id == "ws-1"
        assert result.display_name == "Workspace 1"

    def test_get_workspace_by_id_not_found(self, mock_fabric_client):
        """404 returns FabricWorkspaceNotFoundError."""
        from ms_fabric_mcp_server.services.workspace import FabricWorkspaceService

        mock_fabric_client.make_api_request.side_effect = FabricAPIError(404, "missing")

        service = FabricWorkspaceService(mock_fabric_client)

        with pytest.raises(FabricWorkspaceNotFoundError):
            service.get_workspace_by_id("missing")

    def test_resolve_workspace_id_with_id(self, mock_fabric_client):
        """Resolve workspace ID returns same ID when ID exists."""
        from ms_fabric_mcp_server.services.workspace import FabricWorkspaceService

        workspace_id = "12345678-1234-1234-1234-123456789abc"
        workspace = FabricDataFactory.workspace(workspace_id=workspace_id, name="Workspace 1")
        response = MockResponseFactory.success(workspace)
        mock_fabric_client.make_api_request.return_value = response

        service = FabricWorkspaceService(mock_fabric_client)
        result = service.resolve_workspace_id(workspace_id)

        assert result == workspace_id

    def test_resolve_workspace_id_with_name(self, mock_fabric_client):
        """Resolve workspace ID falls back to name when not ID."""
        from ms_fabric_mcp_server.services.workspace import FabricWorkspaceService

        workspace_data = FabricDataFactory.workspace_list(2)
        response = MockResponseFactory.success(workspace_data)
        mock_fabric_client.make_api_request.return_value = response

        service = FabricWorkspaceService(mock_fabric_client)
        result = service.resolve_workspace_id("Workspace 1")

        assert result == "ws-1"

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

    def test_create_workspace_with_capacity(self, mock_fabric_client):
        """Create workspace includes capacity ID."""
        from ms_fabric_mcp_server.services.workspace import FabricWorkspaceService

        new_workspace = FabricDataFactory.workspace(name="New Workspace", capacity_id="cap-1")
        response = MockResponseFactory.success(new_workspace, 201)
        mock_fabric_client.make_api_request.return_value = response

        service = FabricWorkspaceService(mock_fabric_client)
        workspace = service.create_workspace("New Workspace", capacity_id="cap-1")

        assert workspace.capacity_id == "cap-1"
        _, kwargs = mock_fabric_client.make_api_request.call_args
        assert kwargs["payload"]["capacityId"] == "cap-1"

    def test_create_workspace_api_error(self, mock_fabric_client):
        """API errors propagate."""
        from ms_fabric_mcp_server.services.workspace import FabricWorkspaceService

        mock_fabric_client.make_api_request.side_effect = FabricAPIError(500, "boom")

        service = FabricWorkspaceService(mock_fabric_client)

        with pytest.raises(FabricAPIError):
            service.create_workspace("New Workspace")

    def test_delete_workspace_success(self, mock_fabric_client):
        """Delete workspace resolves ID and calls endpoint."""
        from ms_fabric_mcp_server.services.workspace import FabricWorkspaceService

        workspace_id = "12345678-1234-1234-1234-123456789abc"
        workspace = FabricDataFactory.workspace(workspace_id=workspace_id, name="Workspace 1")
        response = MockResponseFactory.success(workspace)
        mock_fabric_client.make_api_request.side_effect = [response, Mock()]

        service = FabricWorkspaceService(mock_fabric_client)
        service.delete_workspace(workspace_id)

        mock_fabric_client.make_api_request.assert_any_call(
            "DELETE",
            f"workspaces/{workspace_id}",
            timeout=60,
        )

    def test_delete_workspace_api_error(self, mock_fabric_client):
        """API errors propagate."""
        from ms_fabric_mcp_server.services.workspace import FabricWorkspaceService

        workspace_id = "12345678-1234-1234-1234-123456789abc"
        workspace = FabricDataFactory.workspace(workspace_id=workspace_id, name="Workspace 1")
        response = MockResponseFactory.success(workspace)
        mock_fabric_client.make_api_request.side_effect = [response, FabricAPIError(500, "boom")]

        service = FabricWorkspaceService(mock_fabric_client)

        with pytest.raises(FabricAPIError):
            service.delete_workspace(workspace_id)


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
