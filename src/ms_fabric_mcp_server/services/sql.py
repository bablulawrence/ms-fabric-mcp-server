# ABOUTME: Service for Fabric SQL operations.
# ABOUTME: Handles SQL endpoint discovery and query execution.
"""Fabric SQL Warehouse Service for connecting and executing queries."""

import logging
import struct
from itertools import chain, repeat
from typing import Any, Dict, List, Optional

try:
    from azure.identity import AzureCliCredential
    import pyodbc
    PYODBC_AVAILABLE = True
except ImportError:
    PYODBC_AVAILABLE = False
    AzureCliCredential = None
    pyodbc = None

from ..client.exceptions import FabricConnectionError, FabricError, FabricItemNotFoundError
from ..client.http_client import FabricClient
from ..models.results import QueryResult

# OpenTelemetry auto-instrumentation for database operations
try:
    from opentelemetry.instrumentation.dbapi import trace_integration
    from opentelemetry import trace
    OTEL_AVAILABLE = True
except ImportError:
    OTEL_AVAILABLE = False

logger = logging.getLogger(__name__)


class FabricSQLService:
    """Service for connecting to Fabric SQL Warehouse through SQL endpoint.
    
    This service provides SQL query execution capabilities for Fabric warehouses
    using pyodbc with Azure authentication and optional OpenTelemetry tracing.
    
    Note:
        This service requires optional dependencies. Install with:
        ```bash
        pip install ms-fabric-mcp-server[sql]
        ```
    
    Example:
        ```python
        from ms_fabric_mcp_server import FabricConfig, FabricClient
        from ms_fabric_mcp_server.services import (
            FabricWorkspaceService,
            FabricItemService,
            FabricSQLService
        )
        
        config = FabricConfig.from_environment()
        client = FabricClient(config)
        workspace_service = FabricWorkspaceService(client)
        item_service = FabricItemService(client, workspace_service)
        sql_service = FabricSQLService(client, workspace_service, item_service)
        
        # Get warehouse SQL endpoint
        endpoint = sql_service.get_warehouse_sql_endpoint(
            workspace_name="MyWorkspace",
            warehouse_name="MyWarehouse"
        )
        
        # Connect and query
        with sql_service:
            sql_service.connect(endpoint)
            result = sql_service.execute_query("SELECT TOP 10 * FROM sales")
            
            for row in result.data:
                print(row)
        ```
    """
    
    def __init__(
        self,
        client: FabricClient,
        workspace_service: "FabricWorkspaceService",
        item_service: "FabricItemService",
    ):
        """Initialize the SQL service.
        
        Args:
            client: FabricClient instance for API requests
            workspace_service: FabricWorkspaceService for workspace operations
            item_service: FabricItemService for item operations
            
        Raises:
            ImportError: If pyodbc is not installed
        """
        if not PYODBC_AVAILABLE:
            raise ImportError(
                "pyodbc is required for SQL operations. "
                "Install with: pip install ms-fabric-mcp-server[sql]"
            )
        
        self.client = client
        self.workspace_service = workspace_service
        self.item_service = item_service
        self._connection = None
        self._sql_endpoint = None
        
        # Enable OpenTelemetry auto-instrumentation if available
        if OTEL_AVAILABLE:
            try:
                # This creates CLIENT spans that appear in Dependencies table
                trace_integration(pyodbc, "connect", "sqlserver")
                logger.debug("OpenTelemetry auto-instrumentation enabled for SQL")
            except Exception as exc:
                logger.warning(f"Failed to enable OpenTelemetry for SQL: {exc}")
        
        logger.debug("FabricSQLService initialized")
    
    def get_sql_endpoint(
        self,
        workspace_name: str,
        item_name: str,
        item_type: str = "Warehouse"
    ) -> str:
        """Get the SQL endpoint for a Fabric Warehouse or Lakehouse.
        
        This unified method supports both Warehouse and Lakehouse SQL endpoints,
        enabling direct T-SQL queries against either item type using the same
        connection pattern.
        
        Args:
            workspace_name: Name of the workspace
            item_name: Name of the Warehouse or Lakehouse
            item_type: Type of the item - "Warehouse" (default) or "Lakehouse"
            
        Returns:
            SQL endpoint URL (e.g., "abc-123.datawarehouse.fabric.microsoft.com")
            
        Raises:
            FabricItemNotFoundError: If item not found
            FabricError: If endpoint retrieval fails
            ValueError: If item_type is not supported
            
        Example:
            ```python
            # Get Warehouse SQL endpoint
            endpoint = sql_service.get_sql_endpoint(
                workspace_name="Analytics",
                item_name="MainWarehouse",
                item_type="Warehouse"
            )
            
            # Get Lakehouse SQL endpoint
            endpoint = sql_service.get_sql_endpoint(
                workspace_name="Analytics",
                item_name="DataLakehouse",
                item_type="Lakehouse"
            )
            
            print(f"Endpoint: {endpoint}")
            ```
        """
        logger.info(
            f"Getting SQL endpoint for {item_type} '{item_name}' "
            f"in workspace '{workspace_name}'"
        )
        
        # Validate item_type
        if item_type not in ["Warehouse", "Lakehouse"]:
            raise ValueError(
                f"Unsupported item_type '{item_type}'. "
                "Supported types: Warehouse, Lakehouse"
            )
        
        try:
            # Resolve workspace ID
            workspace_id = self.workspace_service.resolve_workspace_id(workspace_name)
            
            # Get item
            item = self.item_service.get_item_by_name(
                workspace_id, item_name, item_type
            )
            
            # Build API endpoint based on item type
            if item_type == "Warehouse":
                endpoint = f"workspaces/{workspace_id}/warehouses/{item.id}"
            else:  # Lakehouse
                endpoint = f"workspaces/{workspace_id}/lakehouses/{item.id}"
            
            # Get item properties to retrieve SQL endpoint
            response = self.client.make_api_request("GET", endpoint)
            item_details = response.json()
            
            # Extract connection string based on item type
            if item_type == "Warehouse":
                connection_string = item_details.get("properties", {}).get("connectionString", "")
            else:  # Lakehouse
                connection_string = item_details.get("properties", {}).get(
                    "sqlEndpointProperties", {}
                ).get("connectionString", "")
            
            if not connection_string:
                raise FabricError(
                    f"No SQL endpoint found for {item_type} '{item_name}'"
                )
            
            logger.info(f"SQL endpoint retrieved: {connection_string}")
            return connection_string
            
        except FabricItemNotFoundError:
            raise
        except ValueError:
            raise
        except Exception as exc:
            logger.error(f"Failed to get SQL endpoint: {exc}")
            raise FabricError(f"Failed to get SQL endpoint: {exc}")
    
    def get_warehouse_sql_endpoint(
        self,
        workspace_name: str,
        warehouse_name: str
    ) -> str:
        """Get the SQL endpoint for a Fabric warehouse.
        
        This is a convenience method that calls get_sql_endpoint with item_type="Warehouse".
        For new code, prefer using get_sql_endpoint directly.
        
        Args:
            workspace_name: Name of the workspace
            warehouse_name: Name of the warehouse
            
        Returns:
            SQL endpoint URL (e.g., "abc-123.datawarehouse.fabric.microsoft.com")
            
        Raises:
            FabricItemNotFoundError: If warehouse not found
            FabricError: If endpoint retrieval fails
            
        Example:
            ```python
            endpoint = sql_service.get_warehouse_sql_endpoint(
                workspace_name="Analytics",
                warehouse_name="MainWarehouse"
            )
            ```
        """
        return self.get_sql_endpoint(workspace_name, warehouse_name, "Warehouse")
    
    def _get_token_bytes(self) -> bytes:
        """Get Azure authentication token formatted for ODBC.
        
        Returns:
            Token bytes formatted for SQL Server ODBC driver
            
        Raises:
            FabricConnectionError: If authentication fails
        """
        try:
            credential = AzureCliCredential()
            token = credential.get_token("https://database.windows.net/.default")
            
            # ODBC token must be a Windows-style byte string, padded with zero bytes
            token_bytes = bytes(token.token, "UTF-8")
            token_bytes = bytes(chain.from_iterable(zip(token_bytes, repeat(0))))
            token_bytes = struct.pack("<i", len(token_bytes)) + token_bytes  # length prefix
            
            return token_bytes
            
        except Exception as exc:
            logger.error(f"Failed to get authentication token: {exc}")
            raise FabricConnectionError(f"Authentication failed: {exc}")
    
    def connect(self, sql_endpoint: str, database: str = "Metadata") -> None:
        """Connect to Fabric SQL Warehouse.
        
        Args:
            sql_endpoint: The SQL endpoint URL 
                (e.g., "abc-123.datawarehouse.fabric.microsoft.com")
            database: Database name to connect to (default: "Metadata")
            
        Raises:
            FabricConnectionError: If connection fails
            
        Example:
            ```python
            sql_service.connect(
                sql_endpoint="abc-123.datawarehouse.fabric.microsoft.com",
                database="sales_dw"
            )
            ```
        """
        try:
            # Close existing connection if any
            self.close()
            
            token_bytes = self._get_token_bytes()
            attrs = {1256: token_bytes}  # 1256 = SQL_COPT_SS_ACCESS_TOKEN
            
            # Ensure endpoint has port if not specified
            if "," not in sql_endpoint and ":" not in sql_endpoint:
                sql_endpoint = f"{sql_endpoint},1433"
            
            cnx_str = (
                "Driver={ODBC Driver 18 for SQL Server};"
                f"Server={sql_endpoint};"
                f"Database={database};"
                "Encrypt=yes;TrustServerCertificate=no"
            )
            
            logger.info(f"Connecting to SQL endpoint: {sql_endpoint}, database: {database}")
            self._connection = pyodbc.connect(cnx_str, attrs_before=attrs)
            self._sql_endpoint = sql_endpoint
            logger.info("Successfully connected to Fabric SQL Warehouse")
            
        except Exception as exc:
            logger.error(f"Failed to connect to SQL endpoint {sql_endpoint}: {exc}")
            raise FabricConnectionError(f"Connection failed: {exc}")
    
    def execute_query(self, query: str) -> QueryResult:
        """Execute a SQL query and return results.
        
        Args:
            query: SQL query to execute (SELECT, SHOW, DESCRIBE, etc.)
            
        Returns:
            QueryResult containing the query results
            
        Raises:
            FabricConnectionError: If not connected
            
        Example:
            ```python
            result = sql_service.execute_query(
                "SELECT TOP 100 * FROM sales ORDER BY date DESC"
            )
            
            if result.status == "success":
                print(f"Returned {result.row_count} rows")
                for row in result.data:
                    print(row)
            ```
        """
        if not self._connection:
            raise FabricConnectionError(
                "Not connected to SQL endpoint. Call connect() first."
            )
        
        try:
            logger.info(f"Executing query: {query[:100]}...")
            cursor = self._connection.cursor()
            
            # Auto-instrumentation will create CLIENT spans for DB operations
            cursor.execute(query)
            
            # Get column names
            columns = (
                [column[0] for column in cursor.description]
                if cursor.description else []
            )
            
            # Fetch all rows
            rows = cursor.fetchall()
            
            # Convert rows to list of dictionaries
            data = []
            for row in rows:
                row_dict = {}
                for i, column in enumerate(columns):
                    row_dict[column] = row[i]
                data.append(row_dict)
            
            result = QueryResult(
                status="success",
                data=data,
                columns=columns,
                row_count=len(data),
                message=f"Query executed successfully. Returned {len(data)} rows."
            )
            
            logger.info(f"Query executed successfully. Returned {len(data)} rows.")
            return result
            
        except Exception as exc:
            logger.error(f"Query execution failed: {exc}")
            error_result = QueryResult(
                status="error",
                data=[],
                columns=[],
                row_count=0,
                message=f"Query execution failed: {exc}"
            )
            return error_result
    
    def execute_statement(self, statement: str) -> Dict[str, Any]:
        """Execute a DML SQL statement (INSERT, UPDATE, DELETE, MERGE).
        
        Args:
            statement: DML SQL statement to execute
            
        Returns:
            Dictionary with execution status and affected rows
            
        Raises:
            FabricConnectionError: If not connected
            
        Example:
            ```python
            result = sql_service.execute_statement(
                "UPDATE sales SET status = 'processed' WHERE date = '2025-01-01'"
            )
            
            if result["status"] == "success":
                print(f"Affected {result['affected_rows']} rows")
            ```
        """
        if not self._connection:
            raise FabricConnectionError(
                "Not connected to SQL endpoint. Call connect() first."
            )
        
        try:
            logger.info(f"Executing statement: {statement[:100]}...")
            cursor = self._connection.cursor()
            
            # Auto-instrumentation will create CLIENT spans for DB operations
            cursor.execute(statement)
            affected_rows = cursor.rowcount
            self._connection.commit()
            
            result = {
                "status": "success",
                "affected_rows": affected_rows,
                "message": f"Statement executed successfully. {affected_rows} rows affected."
            }
            
            logger.info(f"Statement executed successfully. {affected_rows} rows affected.")
            return result
            
        except Exception as exc:
            try:
                self._connection.rollback()
            except:
                pass  # Rollback may fail if connection is broken
            logger.error(f"Statement execution failed: {exc}")
            return {
                "status": "error",
                "affected_rows": 0,
                "message": f"Statement execution failed: {exc}"
            }
    
    def execute_sql_query(
        self,
        sql_endpoint: str,
        query: str,
        database: str = "Metadata"
    ) -> QueryResult:
        """Execute a SQL query against a Fabric SQL endpoint (Warehouse or Lakehouse).
        
        This is a convenience method that connects to the SQL endpoint and executes
        the query in one call.
        
        Args:
            sql_endpoint: The SQL endpoint URL 
                (e.g., "abc-123.datawarehouse.fabric.microsoft.com")
            query: SQL query to execute (SELECT, SHOW, DESCRIBE, etc.)
            database: Database name to connect to (default: "Metadata")
            
        Returns:
            QueryResult containing the query results
            
        Example:
            ```python
            result = sql_service.execute_sql_query(
                sql_endpoint="abc-123.datawarehouse.fabric.microsoft.com",
                query="SELECT TOP 10 * FROM sales",
                database="sales_db"
            )
            ```
        """
        self.connect(sql_endpoint, database)
        try:
            return self.execute_query(query)
        finally:
            self.close()
    
    def execute_sql_statement(
        self,
        sql_endpoint: str,
        statement: str,
        database: str = "Metadata"
    ) -> Dict[str, Any]:
        """Execute a DML SQL statement against a Fabric SQL endpoint (Warehouse or Lakehouse).
        
        This is a convenience method that connects to the SQL endpoint and executes
        the statement in one call.
        
        Args:
            sql_endpoint: The SQL endpoint URL 
                (e.g., "abc-123.datawarehouse.fabric.microsoft.com")
            statement: DML SQL statement to execute (INSERT, UPDATE, DELETE, MERGE)
            database: Database name to connect to (default: "Metadata")
            
        Returns:
            Dictionary with execution status and affected rows
            
        Example:
            ```python
            result = sql_service.execute_sql_statement(
                sql_endpoint="abc-123.datawarehouse.fabric.microsoft.com",
                statement="INSERT INTO sales VALUES (1, 100.00)",
                database="sales_db"
            )
            ```
        """
        self.connect(sql_endpoint, database)
        try:
            return self.execute_statement(statement)
        finally:
            self.close()
    
    def get_tables(self, schema: str = "dbo") -> List[str]:
        """Get list of tables in the specified schema.
        
        Args:
            schema: Schema name (default: "dbo")
            
        Returns:
            List of table names
            
        Example:
            ```python
            tables = sql_service.get_tables("sales")
            
            for table in tables:
                print(table)
            ```
        """
        query = f"""
        SELECT TABLE_NAME 
        FROM INFORMATION_SCHEMA.TABLES 
        WHERE TABLE_SCHEMA = '{schema}' 
        AND TABLE_TYPE = 'BASE TABLE'
        ORDER BY TABLE_NAME
        """
        
        result = self.execute_query(query)
        if result.status == "success":
            return [row["TABLE_NAME"] for row in result.data]
        return []
    
    def get_table_schema(
        self,
        table_name: str,
        schema: str = "dbo"
    ) -> List[Dict[str, Any]]:
        """Get schema information for a specific table.
        
        Args:
            table_name: Name of the table
            schema: Schema name (default: "dbo")
            
        Returns:
            List of column information dictionaries
            
        Example:
            ```python
            schema_info = sql_service.get_table_schema("customers", "sales")
            
            for column in schema_info:
                print(f"{column['COLUMN_NAME']}: {column['DATA_TYPE']}")
            ```
        """
        query = f"""
        SELECT 
            COLUMN_NAME,
            DATA_TYPE,
            IS_NULLABLE,
            COLUMN_DEFAULT,
            CHARACTER_MAXIMUM_LENGTH,
            NUMERIC_PRECISION,
            NUMERIC_SCALE
        FROM INFORMATION_SCHEMA.COLUMNS 
        WHERE TABLE_SCHEMA = '{schema}' 
        AND TABLE_NAME = '{table_name}'
        ORDER BY ORDINAL_POSITION
        """
        
        result = self.execute_query(query)
        if result.status == "success":
            return result.data
        return []
    
    def is_connected(self) -> bool:
        """Check if connected to the database.
        
        Returns:
            True if connected, False otherwise
            
        Example:
            ```python
            if sql_service.is_connected():
                print("Connected to warehouse")
            else:
                print("Not connected")
            ```
        """
        if not self._connection:
            return False
        
        try:
            # Test connection with a simple query
            cursor = self._connection.cursor()
            cursor.execute("SELECT 1")
            cursor.fetchone()
            return True
        except:
            return False
    
    def close(self) -> None:
        """Close the database connection.
        
        Example:
            ```python
            sql_service.close()
            ```
        """
        if self._connection:
            try:
                self._connection.close()
                logger.info("SQL connection closed")
            except Exception as exc:
                logger.warning(f"Error closing connection: {exc}")
            finally:
                self._connection = None
    
    def __enter__(self):
        """Context manager entry."""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()
