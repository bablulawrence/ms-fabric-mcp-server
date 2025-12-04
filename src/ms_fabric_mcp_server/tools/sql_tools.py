# ABOUTME: SQL MCP tools for Microsoft Fabric.
# ABOUTME: Provides tools for SQL endpoint discovery and query execution.
"""SQL warehouse MCP tools.

This module provides MCP tools for executing SQL queries and DML statements
against Microsoft Fabric SQL warehouses.
"""

from typing import TYPE_CHECKING
import logging

if TYPE_CHECKING:
    from fastmcp import FastMCP

from ..services import FabricSQLService
from .base import handle_tool_errors, log_tool_invocation

logger = logging.getLogger(__name__)


def register_sql_tools(mcp: "FastMCP", sql_service: FabricSQLService):
    """Register SQL warehouse MCP tools.
    
    This function registers SQL-related tools:
    - get_sql_endpoint: Get SQL connection string for a Warehouse or Lakehouse (unified)
    - execute_sql_query: Execute SELECT/SHOW/DESCRIBE queries
    - execute_sql_statement: Execute DML statements (INSERT/UPDATE/DELETE)
    
    Args:
        mcp: FastMCP server instance to register tools on.
        sql_service: Initialized FabricSQLService instance.
        
    Example:
        ```python
        from ms_fabric_mcp_server import (
            FabricConfig, FabricClient,
            FabricWorkspaceService, FabricItemService, FabricSQLService
        )
        from ms_fabric_mcp_server.tools import register_sql_tools
        
        config = FabricConfig.from_environment()
        client = FabricClient(config)
        workspace_service = FabricWorkspaceService(client)
        item_service = FabricItemService(client)
        sql_service = FabricSQLService(client, workspace_service, item_service)
        
        register_sql_tools(mcp, sql_service)
        ```
    """
    
    @mcp.tool(title="Get SQL Endpoint")
    @handle_tool_errors
    def get_sql_endpoint(
        workspace_name: str,
        item_name: str,
        item_type: str = "Warehouse"
    ) -> dict:
        """Get the SQL endpoint/connection string for a Fabric Warehouse or Lakehouse.
        
        This unified tool supports both Warehouse and Lakehouse SQL endpoints, enabling
        direct T-SQL queries against either item type using the same connection pattern.
        
        Parameters:
            workspace_name: The display name of the workspace containing the item.
            item_name: The name of the Warehouse or Lakehouse to get the SQL endpoint for.
            item_type: Type of the item: "Warehouse" (default) or "Lakehouse".
                      Both types support T-SQL queries via ODBC connections.
            
        Returns:
            Dictionary with status, workspace_name, item_name, item_type, item_id,
            and connection_string (SQL endpoint URL).
            
        Example:
            ```python
            # Get Warehouse SQL endpoint
            result = get_sql_endpoint(
                workspace_name="My Workspace",
                item_name="Sales_Warehouse",
                item_type="Warehouse"
            )
            
            # Get Lakehouse SQL endpoint
            result = get_sql_endpoint(
                workspace_name="My Workspace",
                item_name="Data_Lakehouse",
                item_type="Lakehouse"
            )
            
            if result["status"] == "success":
                endpoint = result["connection_string"]
                # Use endpoint with execute_sql_query or external tools
            ```
        """
        log_tool_invocation("get_sql_endpoint",
                          workspace_name=workspace_name, item_name=item_name, item_type=item_type)
        logger.info(f"Getting SQL endpoint for {item_type} '{item_name}' in workspace '{workspace_name}'")
        
        connection_string = sql_service.get_sql_endpoint(workspace_name, item_name, item_type)
        
        result = {
            "status": "success",
            "workspace_name": workspace_name,
            "item_name": item_name,
            "item_type": item_type,
            "connection_string": connection_string,
            "message": f"Successfully retrieved connection string for {item_type} '{item_name}'"
        }
        
        logger.info(f"Successfully retrieved connection string for {item_type} '{item_name}'")
        return result

    @mcp.tool(title="Execute SQL Query")
    @handle_tool_errors
    def execute_sql_query(
        sql_endpoint: str,
        query: str,
        database: str = "Metadata"
    ) -> dict:
        """Execute a SQL query against a Fabric SQL Warehouse.
        
        Executes a SELECT, SHOW, or DESCRIBE query and returns the results as structured data.
        The query results include column names, data rows, and row count.
        
        **Note**: This tool is for queries that return data (SELECT/SHOW/DESCRIBE).
        For INSERT/UPDATE/DELETE statements, use execute_sql_statement instead.
        
        Parameters:
            sql_endpoint: The SQL endpoint URL (e.g., "abc-123.datawarehouse.fabric.microsoft.com").
                         Use get_sql_endpoint to retrieve this.
            query: SQL query to execute (SELECT, SHOW, DESCRIBE).
            database: Database/Warehouse name to connect to (default: "Metadata").
            
        Returns:
            Dictionary with status, message, data (rows), columns, row_count, and endpoint.
            
        Example:
            ```python
            # Get the endpoint first
            endpoint_result = get_sql_endpoint("My Workspace", "Sales_Warehouse", "Warehouse")
            endpoint = endpoint_result["connection_string"]
            
            # Execute a query
            result = execute_sql_query(
                sql_endpoint=endpoint,
                query="SELECT TOP 10 * FROM sales_data",
                database="sales_db"
            )
            
            if result["status"] == "success":
                for row in result["data"]:
                    print(row)
            ```
        """
        log_tool_invocation("execute_sql_query",
                          sql_endpoint=sql_endpoint, query=query[:100], database=database)
        logger.info(f"Executing SQL query on {sql_endpoint} (database: {database}): {query[:100]}...")
        
        result = sql_service.execute_sql_query(
            sql_endpoint=sql_endpoint,
            query=query,
            database=database
        )
        
        response = {
            "status": result.status,
            "message": result.message,
            "data": result.data,
            "columns": result.columns,
            "row_count": result.row_count,
            "endpoint": sql_endpoint
        }
        
        if result.status == "success":
            logger.info(f"Query executed successfully: {result.row_count} rows returned")
        else:
            logger.error(f"Query execution failed: {result.message}")
            
        return response

    @mcp.tool(title="Execute SQL DML Statement")
    @handle_tool_errors
    def execute_sql_statement(
        sql_endpoint: str,
        statement: str,
        database: str = "Metadata"
    ) -> dict:
        """Execute a DML SQL statement (INSERT, UPDATE, DELETE, MERGE) against a Fabric SQL Warehouse.
        
        Executes a data modification statement and returns the number of affected rows.
        
        **Note**: This tool is for DML statements that modify data (INSERT/UPDATE/DELETE/MERGE).
        For queries that return data (SELECT/SHOW/DESCRIBE), use execute_sql_query instead.
        
        Parameters:
            sql_endpoint: The SQL endpoint URL (e.g., "abc-123.datawarehouse.fabric.microsoft.com").
                         Use get_sql_endpoint to retrieve this.
            statement: DML SQL statement to execute (INSERT, UPDATE, DELETE, MERGE).
            database: Database/Warehouse name to connect to (default: "Metadata").
            
        Returns:
            Dictionary with status, message, affected_rows, and endpoint.
            
        Example:
            ```python
            # Get the endpoint first
            endpoint_result = get_sql_endpoint("My Workspace", "Sales_Warehouse", "Warehouse")
            endpoint = endpoint_result["connection_string"]
            
            # Execute an INSERT statement
            result = execute_sql_statement(
                sql_endpoint=endpoint,
                statement="INSERT INTO sales_data (id, amount) VALUES (1, 100.00)",
                database="sales_db"
            )
            
            if result["status"] == "success":
                print(f"Inserted {result['affected_rows']} row(s)")
            ```
        """
        log_tool_invocation("execute_sql_statement",
                          sql_endpoint=sql_endpoint, statement=statement[:100], database=database)
        logger.info(f"Executing DML statement on {sql_endpoint} (database: {database}): {statement[:100]}...")
        
        result = sql_service.execute_sql_statement(
            sql_endpoint=sql_endpoint,
            statement=statement,
            database=database
        )
        
        response = {
            "status": result["status"],
            "message": result["message"],
            "affected_rows": result["affected_rows"],
            "endpoint": sql_endpoint
        }
        
        if result["status"] == "success":
            logger.info(f"DML statement executed successfully: {result['affected_rows']} rows affected")
        else:
            logger.error(f"DML statement execution failed: {result['message']}")
            
        return response
    
    logger.info("SQL tools registered successfully (3 tools)")
