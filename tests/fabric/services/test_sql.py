"""Unit tests for FabricSQLService."""

import struct
from unittest.mock import Mock

import pytest

from ms_fabric_mcp_server.client.exceptions import FabricConnectionError, FabricError
from ms_fabric_mcp_server.models.item import FabricItem
from ms_fabric_mcp_server.models.results import QueryResult


@pytest.fixture
def sql_module():
    import ms_fabric_mcp_server.services.sql as sql_module

    return sql_module


@pytest.fixture
def sql_service(mock_fabric_client, sql_module, monkeypatch):
    """Create SQL service with patched pyodbc and AzureCliCredential."""
    monkeypatch.setattr(sql_module, "PYODBC_AVAILABLE", True)
    monkeypatch.setattr(sql_module, "OTEL_AVAILABLE", False)
    pyodbc_mock = Mock()
    monkeypatch.setattr(sql_module, "pyodbc", pyodbc_mock)
    azure_cred_mock = Mock()
    monkeypatch.setattr(sql_module, "AzureCliCredential", azure_cred_mock)

    workspace_service = Mock()
    item_service = Mock()
    service = sql_module.FabricSQLService(
        mock_fabric_client,
        workspace_service,
        item_service,
    )
    return service, workspace_service, item_service, pyodbc_mock, azure_cred_mock


@pytest.mark.unit
class TestFabricSQLService:
    """Test suite for FabricSQLService."""

    def test_init_missing_pyodbc_raises(self, sql_module, mock_fabric_client, monkeypatch):
        """Missing pyodbc raises ImportError."""
        monkeypatch.setattr(sql_module, "PYODBC_AVAILABLE", False)
        with pytest.raises(ImportError):
            sql_module.FabricSQLService(mock_fabric_client, Mock(), Mock())

    def test_get_sql_endpoint_warehouse(self, sql_service, mock_fabric_client):
        """Warehouse SQL endpoint uses properties.connectionString."""
        service, workspace_service, item_service, _, _ = sql_service
        workspace_service.resolve_workspace_id.return_value = "ws-1"
        item_service.get_item_by_name.return_value = FabricItem(
            id="wh-1",
            display_name="Warehouse",
            type="Warehouse",
            workspace_id="ws-1",
        )
        response = Mock()
        response.json.return_value = {"properties": {"connectionString": "wh-endpoint"}}
        mock_fabric_client.make_api_request.return_value = response

        endpoint = service.get_sql_endpoint("Workspace", "Warehouse", "Warehouse")

        assert endpoint == "wh-endpoint"
        mock_fabric_client.make_api_request.assert_called_once_with(
            "GET", "workspaces/ws-1/warehouses/wh-1"
        )

    def test_get_sql_endpoint_lakehouse(self, sql_service, mock_fabric_client):
        """Lakehouse SQL endpoint uses sqlEndpointProperties.connectionString."""
        service, workspace_service, item_service, _, _ = sql_service
        workspace_service.resolve_workspace_id.return_value = "ws-1"
        item_service.get_item_by_name.return_value = FabricItem(
            id="lh-1",
            display_name="Lakehouse",
            type="Lakehouse",
            workspace_id="ws-1",
        )
        response = Mock()
        response.json.return_value = {
            "properties": {"sqlEndpointProperties": {"connectionString": "lh-endpoint"}}
        }
        mock_fabric_client.make_api_request.return_value = response

        endpoint = service.get_sql_endpoint("Workspace", "Lakehouse", "Lakehouse")

        assert endpoint == "lh-endpoint"
        mock_fabric_client.make_api_request.assert_called_once_with(
            "GET", "workspaces/ws-1/lakehouses/lh-1"
        )

    def test_get_sql_endpoint_missing_connection_string(self, sql_service, mock_fabric_client):
        """Missing connection string raises FabricError."""
        service, workspace_service, item_service, _, _ = sql_service
        workspace_service.resolve_workspace_id.return_value = "ws-1"
        item_service.get_item_by_name.return_value = FabricItem(
            id="wh-1",
            display_name="Warehouse",
            type="Warehouse",
            workspace_id="ws-1",
        )
        response = Mock()
        response.json.return_value = {"properties": {}}
        mock_fabric_client.make_api_request.return_value = response

        with pytest.raises(FabricError):
            service.get_sql_endpoint("Workspace", "Warehouse", "Warehouse")

    def test_get_sql_endpoint_invalid_item_type(self, sql_service):
        """Invalid item type raises ValueError."""
        service, *_ = sql_service

        with pytest.raises(ValueError):
            service.get_sql_endpoint("Workspace", "Item", "Invalid")

    def test_get_token_bytes_success(self, sql_service):
        """Token bytes include length prefix."""
        service, _, _, _, azure_cred_mock = sql_service
        token = Mock()
        token.token = "abc"
        azure_cred_instance = Mock()
        azure_cred_instance.get_token.return_value = token
        azure_cred_mock.return_value = azure_cred_instance

        token_bytes = service._get_token_bytes()

        azure_cred_instance.get_token.assert_called_once_with("https://database.windows.net/.default")
        length = struct.unpack("<i", token_bytes[:4])[0]
        assert length == len(token_bytes[4:])

    def test_get_token_bytes_failure(self, sql_service):
        """Token failures raise FabricConnectionError."""
        service, _, _, _, azure_cred_mock = sql_service
        azure_cred_mock.side_effect = RuntimeError("boom")

        with pytest.raises(FabricConnectionError):
            service._get_token_bytes()

    def test_connect_adds_port_and_calls_pyodbc(self, sql_service):
        """Connect adds default port and uses token bytes."""
        service, _, _, pyodbc_mock, _ = sql_service
        service._get_token_bytes = Mock(return_value=b"token")
        pyodbc_mock.connect.return_value = Mock()

        service.connect("server-host")

        args, kwargs = pyodbc_mock.connect.call_args
        assert "Server=server-host,1433" in args[0]
        assert kwargs["attrs_before"][1256] == b"token"
        assert service._connection is not None

    def test_connect_failure(self, sql_service):
        """Connection failures raise FabricConnectionError."""
        service, _, _, pyodbc_mock, _ = sql_service
        service._get_token_bytes = Mock(return_value=b"token")
        pyodbc_mock.connect.side_effect = RuntimeError("boom")

        with pytest.raises(FabricConnectionError):
            service.connect("server-host")

    def test_execute_query_requires_connection(self, sql_service):
        """execute_query raises when not connected."""
        service, *_ = sql_service
        service._connection = None

        with pytest.raises(FabricConnectionError):
            service.execute_query("SELECT 1")

    def test_execute_query_success(self, sql_service):
        """execute_query returns QueryResult with rows."""
        service, *_ = sql_service
        cursor = Mock()
        cursor.description = [("id",), ("name",)]
        cursor.fetchall.return_value = [(1, "a"), (2, "b")]
        connection = Mock()
        connection.cursor.return_value = cursor
        service._connection = connection

        result = service.execute_query("SELECT * FROM test")

        assert result.status == "success"
        assert result.row_count == 2
        assert result.columns == ["id", "name"]
        assert result.data == [{"id": 1, "name": "a"}, {"id": 2, "name": "b"}]

    def test_execute_query_failure_returns_error(self, sql_service):
        """execute_query returns error result on failure."""
        service, *_ = sql_service
        cursor = Mock()
        cursor.execute.side_effect = RuntimeError("boom")
        connection = Mock()
        connection.cursor.return_value = cursor
        service._connection = connection

        result = service.execute_query("SELECT * FROM test")

        assert result.status == "error"
        assert "Query execution failed" in result.message

    def test_execute_statement_requires_connection(self, sql_service):
        """execute_statement raises when not connected."""
        service, *_ = sql_service
        service._connection = None

        with pytest.raises(FabricConnectionError):
            service.execute_statement("UPDATE test SET a=1")

    def test_execute_statement_success(self, sql_service):
        """execute_statement commits and returns success."""
        service, *_ = sql_service
        cursor = Mock()
        cursor.rowcount = 3
        connection = Mock()
        connection.cursor.return_value = cursor
        service._connection = connection

        result = service.execute_statement("UPDATE test SET a=1")

        assert result["status"] == "success"
        assert result["affected_rows"] == 3
        connection.commit.assert_called_once()

    def test_execute_statement_failure(self, sql_service):
        """execute_statement rolls back on error."""
        service, *_ = sql_service
        cursor = Mock()
        cursor.execute.side_effect = RuntimeError("boom")
        connection = Mock()
        connection.cursor.return_value = cursor
        service._connection = connection

        result = service.execute_statement("UPDATE test SET a=1")

        assert result["status"] == "error"
        connection.rollback.assert_called_once()

    def test_execute_sql_query_calls_close_on_error(self, sql_service):
        """execute_sql_query closes connection even on error."""
        service, *_ = sql_service
        service.connect = Mock()
        service.close = Mock()
        service.execute_query = Mock(side_effect=RuntimeError("boom"))

        with pytest.raises(RuntimeError):
            service.execute_sql_query("endpoint", "SELECT 1")

        service.close.assert_called_once()

    def test_execute_sql_query_closes_on_success(self, sql_service):
        """execute_sql_query closes connection on success."""
        service, *_ = sql_service
        service.connect = Mock()
        service.close = Mock()
        service.execute_query = Mock(
            return_value=QueryResult(
                status="success",
                data=[{"id": 1}],
                columns=["id"],
                row_count=1,
            )
        )

        result = service.execute_sql_query("endpoint", "SELECT 1")

        assert result.status == "success"
        service.connect.assert_called_once_with("endpoint", "Metadata")
        service.execute_query.assert_called_once_with("SELECT 1")
        service.close.assert_called_once()

    def test_execute_sql_statement_calls_close_on_error(self, sql_service):
        """execute_sql_statement closes connection even on error."""
        service, *_ = sql_service
        service.connect = Mock()
        service.close = Mock()
        service.execute_statement = Mock(side_effect=RuntimeError("boom"))

        with pytest.raises(RuntimeError):
            service.execute_sql_statement("endpoint", "UPDATE test SET a=1")

        service.close.assert_called_once()

    def test_execute_sql_statement_closes_on_success(self, sql_service):
        """execute_sql_statement closes connection on success."""
        service, *_ = sql_service
        service.connect = Mock()
        service.close = Mock()
        service.execute_statement = Mock(
            return_value={"status": "success", "affected_rows": 1}
        )

        result = service.execute_sql_statement("endpoint", "UPDATE test SET a=1")

        assert result["status"] == "success"
        service.connect.assert_called_once_with("endpoint", "Metadata")
        service.execute_statement.assert_called_once_with("UPDATE test SET a=1")
        service.close.assert_called_once()

    def test_get_tables_success(self, sql_service):
        """get_tables returns table list on success."""
        service, *_ = sql_service
        service.execute_query = Mock(
            return_value=QueryResult(
                status="success",
                data=[{"TABLE_NAME": "a"}, {"TABLE_NAME": "b"}],
                columns=["TABLE_NAME"],
                row_count=2,
            )
        )

        tables = service.get_tables("dbo")

        assert tables == ["a", "b"]

    def test_get_tables_error(self, sql_service):
        """get_tables returns empty list on error."""
        service, *_ = sql_service
        service.execute_query = Mock(return_value=QueryResult(status="error"))

        tables = service.get_tables("dbo")

        assert tables == []

    def test_get_table_schema_success(self, sql_service):
        """get_table_schema returns schema details on success."""
        service, *_ = sql_service
        schema_data = [{"COLUMN_NAME": "id", "DATA_TYPE": "int"}]
        service.execute_query = Mock(
            return_value=QueryResult(status="success", data=schema_data)
        )

        result = service.get_table_schema("table", "dbo")

        assert result == schema_data

    def test_get_table_schema_error(self, sql_service):
        """get_table_schema returns empty list on error."""
        service, *_ = sql_service
        service.execute_query = Mock(return_value=QueryResult(status="error"))

        result = service.get_table_schema("table", "dbo")

        assert result == []

    def test_is_connected_false_when_none(self, sql_service):
        """is_connected returns False when no connection."""
        service, *_ = sql_service
        service._connection = None

        assert service.is_connected() is False

    def test_is_connected_true(self, sql_service):
        """is_connected returns True on successful ping."""
        service, *_ = sql_service
        cursor = Mock()
        cursor.fetchone.return_value = (1,)
        connection = Mock()
        connection.cursor.return_value = cursor
        service._connection = connection

        assert service.is_connected() is True

    def test_is_connected_false_on_error(self, sql_service):
        """is_connected returns False on errors."""
        service, *_ = sql_service
        cursor = Mock()
        cursor.execute.side_effect = RuntimeError("boom")
        connection = Mock()
        connection.cursor.return_value = cursor
        service._connection = connection

        assert service.is_connected() is False

    def test_close_resets_connection(self, sql_service):
        """close resets connection even on close error."""
        service, *_ = sql_service
        connection = Mock()
        connection.close.side_effect = RuntimeError("boom")
        service._connection = connection

        service.close()

        assert service._connection is None
