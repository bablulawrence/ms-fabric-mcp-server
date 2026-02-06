"""Tests for SQL MCP tools."""

from unittest.mock import Mock

import pytest

from ms_fabric_mcp_server.models.results import QueryResult
from ms_fabric_mcp_server.tools.sql_tools import register_sql_tools
from tests.fabric.tools.utils import capture_tools


@pytest.mark.unit
class TestSQLTools:
    def test_sql_tools_smoke(self):
        tools, mcp = capture_tools()
        sql_service = Mock()
        sql_service.get_sql_endpoint.return_value = "endpoint"
        sql_service.execute_sql_query.return_value = QueryResult(
            status="success",
            message="ok",
            data=[{"id": 1}],
            columns=["id"],
            row_count=1,
        )
        sql_service.execute_sql_statement.return_value = {
            "status": "success",
            "affected_rows": 1,
            "message": "ok",
        }

        register_sql_tools(mcp, sql_service)

        endpoint = tools["get_sql_endpoint"](
            workspace_name="Workspace", item_name="Warehouse", item_type="Warehouse"
        )
        assert endpoint["status"] == "success"

        query = tools["execute_sql_query"](
            sql_endpoint="endpoint",
            query="SELECT 1",
            database="Metadata",
        )
        assert query["status"] == "success"

        stmt = tools["execute_sql_statement"](
            sql_endpoint="endpoint",
            statement="UPDATE t SET x=1",
            database="Metadata",
            allow_ddl=True,
        )
        assert stmt["status"] == "success"
        sql_service.execute_sql_statement.assert_called_once_with(
            sql_endpoint="endpoint",
            statement="UPDATE t SET x=1",
            database="Metadata",
            allow_ddl=True,
        )
