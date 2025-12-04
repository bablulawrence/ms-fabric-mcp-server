"""Mock factories and test data generators for ms-fabric-mcp-server tests."""

from typing import Dict, Any, List
from unittest.mock import Mock
import base64


class MockResponseFactory:
    """Factory for creating mock HTTP responses."""
    
    @staticmethod
    def success(data: Dict[str, Any] = None, status_code: int = 200):
        """Create successful response."""
        response = Mock()
        response.status_code = status_code
        response.ok = True
        response.json.return_value = data or {}
        response.text = str(data) if data else ""
        response.headers = {}
        return response
    
    @staticmethod
    def error(status_code: int, message: str = "Error"):
        """Create error response."""
        response = Mock()
        response.status_code = status_code
        response.ok = False
        response.json.return_value = {
            "error": {
                "code": f"Error{status_code}",
                "message": message
            }
        }
        response.text = message
        response.headers = {}
        return response
    
    @staticmethod
    def rate_limit(retry_after: int = 60):
        """Create rate limit response."""
        response = Mock()
        response.status_code = 429
        response.ok = False
        response.headers = {"Retry-After": str(retry_after)}
        response.json.return_value = {
            "error": {
                "code": "TooManyRequests",
                "message": "Rate limit exceeded"
            }
        }
        response.text = "Rate limit exceeded"
        return response


class FabricDataFactory:
    """Factory for generating test data matching Fabric API schemas."""
    
    @staticmethod
    def workspace(
        workspace_id: str = "ws-123",
        name: str = "Test Workspace",
        capacity_id: str = None
    ) -> Dict[str, Any]:
        """Generate workspace data."""
        data = {
            "id": workspace_id,
            "displayName": name,
            "type": "Workspace",
            "description": f"Test workspace {name}"
        }
        if capacity_id:
            data["capacityId"] = capacity_id
        return data
    
    @staticmethod
    def workspace_list(count: int = 3) -> Dict[str, Any]:
        """Generate workspace list response."""
        return {
            "value": [
                FabricDataFactory.workspace(f"ws-{i}", f"Workspace {i}")
                for i in range(count)
            ]
        }
    
    @staticmethod
    def item(
        item_id: str = "item-123",
        name: str = "Test Item",
        item_type: str = "Notebook",
        workspace_id: str = "ws-123"
    ) -> Dict[str, Any]:
        """Generate item data."""
        return {
            "id": item_id,
            "displayName": name,
            "type": item_type,
            "workspaceId": workspace_id,
            "description": f"Test {item_type}"
        }
    
    @staticmethod
    def item_list(count: int = 5, item_type: str = None) -> Dict[str, Any]:
        """Generate item list response."""
        items = []
        types = [item_type] if item_type else ["Notebook", "Lakehouse", "Warehouse", "Report", "Dashboard"]
        for i in range(count):
            item_type_val = types[i % len(types)]
            items.append(FabricDataFactory.item(f"item-{i}", f"Item {i}", item_type_val))
        return {"value": items}
    
    @staticmethod
    def notebook_definition(content: str = '{"cells":[]}') -> Dict[str, Any]:
        """Generate notebook definition."""
        encoded_content = base64.b64encode(content.encode()).decode()
        return {
            "format": "ipynb",
            "definition": {
                "parts": [
                    {
                        "path": "notebook-content.py",
                        "payload": encoded_content,
                        "payloadType": "InlineBase64"
                    }
                ]
            }
        }
    
    @staticmethod
    def livy_session(
        session_id: int = 1,
        state: str = "idle",
        kind: str = "pyspark"
    ) -> Dict[str, Any]:
        """Generate Livy session data."""
        return {
            "id": session_id,
            "name": None,
            "appId": f"app-{session_id}" if state not in ["not_started", "starting"] else None,
            "owner": None,
            "proxyUser": None,
            "state": state,
            "kind": kind,
            "appInfo": {
                "driverLogUrl": None,
                "sparkUiUrl": None
            },
            "log": []
        }
    
    @staticmethod
    def livy_session_list(count: int = 3) -> Dict[str, Any]:
        """Generate Livy session list."""
        return {
            "from": 0,
            "total": count,
            "sessions": [
                FabricDataFactory.livy_session(i, "idle")
                for i in range(count)
            ]
        }
    
    @staticmethod
    def livy_statement(
        statement_id: int = 0,
        state: str = "available",
        output: Dict[str, Any] = None
    ) -> Dict[str, Any]:
        """Generate Livy statement data."""
        if output is None:
            output = {
                "status": "ok",
                "execution_count": statement_id,
                "data": {"text/plain": "result"}
            }
        return {
            "id": statement_id,
            "code": "print('test')",
            "state": state,
            "output": output,
            "progress": 1.0 if state == "available" else 0.0
        }
    
    @staticmethod
    def job_instance(
        instance_id: str = "job-123",
        status: str = "Completed",
        item_id: str = "item-123"
    ) -> Dict[str, Any]:
        """Generate job instance data."""
        return {
            "id": instance_id,
            "itemId": item_id,
            "jobType": "RunNotebook",
            "invokeType": "Manual",
            "status": status,
            "startTimeUtc": "2025-10-14T10:00:00Z",
            "endTimeUtc": "2025-10-14T10:05:00Z" if status in ["Completed", "Failed"] else None
        }
    
    @staticmethod
    def sql_query_result(columns: List[str] = None, rows: List[List[Any]] = None) -> tuple:
        """Generate SQL query result data."""
        if columns is None:
            columns = ["id", "name", "value"]
        if rows is None:
            rows = [
                [1, "item1", 100],
                [2, "item2", 200],
            ]
        return columns, rows


class ServiceMockFactory:
    """Factory for creating mock service instances."""
    
    @staticmethod
    def workspace_service(mock_client=None):
        """Create mock workspace service."""
        from unittest.mock import Mock
        service = Mock()
        service.client = mock_client
        service.list_workspaces = Mock()
        service.get_workspace_by_id = Mock()
        service.get_workspace_by_name = Mock()
        service.create_workspace = Mock()
        service.delete_workspace = Mock()
        return service
    
    @staticmethod
    def item_service(mock_client=None, mock_workspace_service=None):
        """Create mock item service."""
        from unittest.mock import Mock
        service = Mock()
        service.client = mock_client
        service.workspace_service = mock_workspace_service
        service.list_items = Mock()
        service.get_item_by_name = Mock()
        service.delete_item = Mock()
        return service
    
    @staticmethod
    def notebook_service(mock_client=None, mock_workspace_service=None, mock_item_service=None):
        """Create mock notebook service."""
        from unittest.mock import Mock
        service = Mock()
        service.client = mock_client
        service.workspace_service = mock_workspace_service
        service.item_service = mock_item_service
        service.import_notebook = Mock()
        service.get_notebook_definition = Mock()
        return service
    
    @staticmethod
    def job_service(mock_client=None, mock_workspace_service=None):
        """Create mock job service."""
        from unittest.mock import Mock
        service = Mock()
        service.client = mock_client
        service.workspace_service = mock_workspace_service
        service.run_on_demand_job = Mock()
        service.get_job_instance = Mock()
        service.get_operation_result = Mock()
        return service
    
    @staticmethod
    def sql_service(mock_client=None, mock_workspace_service=None):
        """Create mock SQL service."""
        from unittest.mock import Mock
        service = Mock()
        service.client = mock_client
        service.workspace_service = mock_workspace_service
        service.get_sql_endpoint = Mock()
        service.execute_query = Mock()
        service.execute_statement = Mock()
        return service
    
    @staticmethod
    def livy_service(mock_client=None):
        """Create mock Livy service."""
        from unittest.mock import Mock
        service = Mock()
        service.client = mock_client
        service.create_session = Mock()
        service.list_sessions = Mock()
        service.get_session = Mock()
        service.delete_session = Mock()
        service.run_statement = Mock()
        service.get_statement = Mock()
        service.cancel_statement = Mock()
        service.get_session_log = Mock()
        return service
