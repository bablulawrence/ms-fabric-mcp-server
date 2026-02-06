"""Integration tests for Warehouse SQL tools (DDL/DML)."""

import uuid

import pytest


@pytest.mark.integration
@pytest.mark.asyncio
async def test_warehouse_sql_endpoint(
    call_tool,
    workspace_name,
    warehouse_name,
    sql_dependencies_available,
):
    """get_sql_endpoint returns a valid Warehouse connection string."""
    result = await call_tool(
        "get_sql_endpoint",
        workspace_name=workspace_name,
        item_name=warehouse_name,
        item_type="Warehouse",
    )
    assert result["status"] == "success"
    assert result.get("connection_string")
    assert "datawarehouse.fabric.microsoft.com" in result["connection_string"]


@pytest.mark.integration
@pytest.mark.asyncio
async def test_warehouse_ddl_and_dml(
    call_tool,
    workspace_name,
    warehouse_name,
    sql_dependencies_available,
):
    """Full DDL/DML lifecycle on Warehouse: CREATE, INSERT, UPDATE, DELETE, DROP."""
    # Get endpoint
    ep_result = await call_tool(
        "get_sql_endpoint",
        workspace_name=workspace_name,
        item_name=warehouse_name,
        item_type="Warehouse",
    )
    assert ep_result["status"] == "success"
    endpoint = ep_result["connection_string"]

    table_name = f"test_int_{uuid.uuid4().hex[:8]}"

    try:
        # CREATE TABLE (DDL)
        create_result = await call_tool(
            "execute_sql_statement",
            sql_endpoint=endpoint,
            statement=f"CREATE TABLE dbo.{table_name} (id INT, name VARCHAR(50))",
            database=warehouse_name,
            allow_ddl=True,
        )
        assert create_result["status"] == "success"

        # INSERT
        insert_result = await call_tool(
            "execute_sql_statement",
            sql_endpoint=endpoint,
            statement=f"INSERT INTO dbo.{table_name} (id, name) VALUES (1, 'alice')",
            database=warehouse_name,
        )
        assert insert_result["status"] == "success"
        assert insert_result.get("affected_rows", 0) >= 1

        # UPDATE
        update_result = await call_tool(
            "execute_sql_statement",
            sql_endpoint=endpoint,
            statement=f"UPDATE dbo.{table_name} SET name = 'bob' WHERE id = 1",
            database=warehouse_name,
        )
        assert update_result["status"] == "success"
        assert update_result.get("affected_rows", 0) >= 1

        # Verify UPDATE via SELECT
        select_result = await call_tool(
            "execute_sql_query",
            sql_endpoint=endpoint,
            query=f"SELECT name FROM dbo.{table_name} WHERE id = 1",
            database=warehouse_name,
        )
        assert select_result["status"] == "success"
        assert select_result.get("row_count", 0) == 1
        assert select_result["data"][0]["name"] == "bob"

        # DELETE
        delete_result = await call_tool(
            "execute_sql_statement",
            sql_endpoint=endpoint,
            statement=f"DELETE FROM dbo.{table_name} WHERE id = 1",
            database=warehouse_name,
        )
        assert delete_result["status"] == "success"
        assert delete_result.get("affected_rows", 0) >= 1

        # Verify DELETE via SELECT
        empty_result = await call_tool(
            "execute_sql_query",
            sql_endpoint=endpoint,
            query=f"SELECT COUNT(*) AS cnt FROM dbo.{table_name}",
            database=warehouse_name,
        )
        assert empty_result["status"] == "success"
        assert empty_result["data"][0]["cnt"] == 0

    finally:
        # DROP TABLE (cleanup)
        await call_tool(
            "execute_sql_statement",
            sql_endpoint=endpoint,
            statement=f"DROP TABLE IF EXISTS dbo.{table_name}",
            database=warehouse_name,
            allow_ddl=True,
        )


@pytest.mark.integration
@pytest.mark.asyncio
async def test_warehouse_sql_error_messages(
    call_tool,
    workspace_name,
    warehouse_name,
    sql_dependencies_available,
):
    """Verify differentiated error messages for DDL and SELECT."""
    ep_result = await call_tool(
        "get_sql_endpoint",
        workspace_name=workspace_name,
        item_name=warehouse_name,
        item_type="Warehouse",
    )
    assert ep_result["status"] == "success"
    endpoint = ep_result["connection_string"]

    # SELECT should tell user to use execute_sql_query
    select_err = await call_tool(
        "execute_sql_statement",
        sql_endpoint=endpoint,
        statement="SELECT 1",
        database=warehouse_name,
    )
    assert select_err["status"] == "error"
    assert "execute_sql_query" in select_err["message"]

    # DDL without allow_ddl should tell user to set the flag
    ddl_err = await call_tool(
        "execute_sql_statement",
        sql_endpoint=endpoint,
        statement="CREATE TABLE dbo.should_not_exist (id INT)",
        database=warehouse_name,
    )
    assert ddl_err["status"] == "error"
    assert "allow_ddl" in ddl_err["message"]
