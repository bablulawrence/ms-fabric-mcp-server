"""Integration tests for SQL tools."""

import pytest


@pytest.mark.integration
@pytest.mark.asyncio
async def test_sql_tools(
    call_tool,
    lakehouse_name,
    sql_database,
    sql_dependencies_available,
    workspace_name,
):
    result = await call_tool(
        "get_sql_endpoint",
        workspace_name=workspace_name,
        item_name=lakehouse_name,
        item_type="Lakehouse",
    )
    assert result["status"] == "success"
    endpoint = result.get("connection_string")
    assert endpoint

    query_result = await call_tool(
        "execute_sql_query",
        sql_endpoint=endpoint,
        query="SELECT 1 AS value",
        database=sql_database,
    )
    assert query_result["status"] == "success"
    assert query_result.get("row_count", 0) >= 1

    statement_result = await call_tool(
        "execute_sql_statement",
        sql_endpoint=endpoint,
        statement="SELECT 1",
        database=sql_database,
    )
    assert statement_result["status"] == "error"
