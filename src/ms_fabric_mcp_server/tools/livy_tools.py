# ABOUTME: Livy/Spark MCP tools for Microsoft Fabric.
# ABOUTME: Provides tools for Spark session and statement management.
"""Spark/Livy session management MCP tools.

This module provides MCP tools for creating and managing Apache Spark sessions via
the Livy API in Microsoft Fabric, including session creation, code execution, and monitoring.
"""

from typing import Optional, TYPE_CHECKING
import logging

if TYPE_CHECKING:
    from fastmcp import FastMCP

from ..services import FabricLivyService
from ..client.exceptions import FabricLivyError, FabricLivyTimeoutError
from .base import handle_tool_errors, log_tool_invocation

logger = logging.getLogger(__name__)


def register_livy_tools(mcp: "FastMCP", livy_service: FabricLivyService):
    """Register Spark/Livy session and statement MCP tools.
    
    This function registers eight Livy-related tools:
    - livy_create_session: Create new Spark session
    - livy_list_sessions: List all active sessions
    - livy_get_session_status: Get session status and details
    - livy_close_session: Terminate a session
    - livy_run_statement: Execute code in a session
    - livy_get_statement_status: Get statement status and output
    - livy_cancel_statement: Cancel a running statement
    - livy_get_session_log: Fetch session driver logs
    
    Args:
        mcp: FastMCP server instance to register tools on.
        livy_service: Initialized FabricLivyService instance.
        
    Example:
        ```python
        from ms_fabric_mcp_server import FabricConfig, FabricClient, FabricLivyService
        from ms_fabric_mcp_server.tools import register_livy_tools
        
        config = FabricConfig.from_environment()
        client = FabricClient(config)
        livy_service = FabricLivyService(client)
        
        register_livy_tools(mcp, livy_service)
        ```
    """
    
    @mcp.tool(title="Create Livy Session")
    @handle_tool_errors
    def livy_create_session(
        workspace_id: str,
        lakehouse_id: str,
        environment_id: Optional[str] = None,
        kind: str = "pyspark",
        conf: Optional[dict] = None,
        with_wait: bool = True,
        timeout_seconds: Optional[int] = None
    ) -> dict:
        """Create a new Livy session for Spark code execution.
        
        Creates a Spark session for executing PySpark, Scala, or SparkR code. Session
        creation can take 6+ minutes on first startup as Spark initializes. It's recommended
        to keep with_wait=True to ensure the session is ready before use.
        
        Parameters:
            workspace_id: Fabric workspace ID (use list_workspaces tool to find by name).
            lakehouse_id: Fabric lakehouse ID (use list_items tool with item_type="Lakehouse").
            environment_id: Optional Fabric environment ID for pre-installed libraries.
            kind: Session kind - 'pyspark' (default), 'scala', or 'sparkr'.
            conf: Optional Spark configuration as key-value pairs (e.g., {"spark.executor.memory": "4g"}).
            with_wait: If True (default), wait for session to become available before returning.
            timeout_seconds: Maximum time to wait for session availability (default: from config).
            
        Returns:
            Dictionary with session details including id, state, kind, appId, appInfo, and log.
            
        Example:
            ```python
            # Create a PySpark session
            result = livy_create_session(
                workspace_id="12345678-1234-1234-1234-123456789abc",
                lakehouse_id="87654321-4321-4321-4321-210987654321",
                kind="pyspark",
                with_wait=True
            )
            
            if result.get("state") == "idle":
                session_id = result["id"]
                # Session is ready to execute code
            ```
        """
        log_tool_invocation("livy_create_session",
                          workspace_id=workspace_id, lakehouse_id=lakehouse_id,
                          environment_id=environment_id, kind=kind, with_wait=with_wait)
        logger.info(f"Creating Livy session for workspace {workspace_id}, lakehouse {lakehouse_id}")
        
        try:
            result = livy_service.create_session(
                workspace_id=workspace_id,
                lakehouse_id=lakehouse_id,
                environment_id=environment_id,
                kind=kind,
                conf=conf,
                with_wait=with_wait,
                timeout_seconds=timeout_seconds
            )
            
            if result.get("state") == "idle":
                logger.info(f"Successfully created and started Livy session: {result.get('id')}")
            else:
                logger.info(f"Successfully created Livy session: {result.get('id')} (state: {result.get('state', 'unknown')})")
            return result
            
        except FabricLivyTimeoutError as exc:
            logger.error(f"Livy session creation timed out: {exc}")
            return {"status": "error", "message": f"Session creation timed out: {exc}"}
        except FabricLivyError as exc:
            logger.error(f"Livy error creating session: {exc}")
            return {"status": "error", "message": str(exc)}

    @mcp.tool(title="List Livy Sessions")
    @handle_tool_errors
    def livy_list_sessions(
        workspace_id: str,
        lakehouse_id: str
    ) -> dict:
        """List all Livy sessions in a workspace/lakehouse.
        
        Retrieves all active Livy sessions for the specified workspace and lakehouse,
        including session IDs, states, and configuration details.
        
        Parameters:
            workspace_id: Fabric workspace ID.
            lakehouse_id: Fabric lakehouse ID.
            
        Returns:
            Dictionary with sessions list containing id, state, kind, appId, and other details.
            
        Example:
            ```python
            result = livy_list_sessions(
                workspace_id="12345678-1234-1234-1234-123456789abc",
                lakehouse_id="87654321-4321-4321-4321-210987654321"
            )
            
            for session in result.get("sessions", []):
                print(f"Session {session['id']}: {session['state']}")
            ```
        """
        log_tool_invocation("livy_list_sessions",
                          workspace_id=workspace_id, lakehouse_id=lakehouse_id)
        logger.info(f"Listing Livy sessions for workspace {workspace_id}, lakehouse {lakehouse_id}")
        
        try:
            result = livy_service.list_sessions(
                workspace_id=workspace_id,
                lakehouse_id=lakehouse_id
            )
            
            logger.info(f"Successfully retrieved {len(result.get('sessions', []))} Livy sessions")
            return result
            
        except FabricLivyError as exc:
            logger.error(f"Livy error listing sessions: {exc}")
            return {"status": "error", "message": str(exc)}

    @mcp.tool(title="Get Livy Session Status")
    @handle_tool_errors
    def livy_get_session_status(
        workspace_id: str,
        lakehouse_id: str,
        session_id: str
    ) -> dict:
        """Get the current status and details of a Livy session.
        
        Retrieves detailed information about a session including its state, Spark application
        details, and configuration. Use this to check session health and readiness.
        
        Session States:
        - 'not_started': Session created but not yet started
        - 'starting': Session is initializing
        - 'idle': Session is ready to accept statements
        - 'busy': Session is currently executing a statement
        - 'shutting_down': Session is terminating
        - 'error': Session encountered an error
        - 'dead': Session has terminated
        - 'killed': Session was forcefully terminated
        - 'success': Session completed successfully
        
        Parameters:
            workspace_id: Fabric workspace ID.
            lakehouse_id: Fabric lakehouse ID.
            session_id: Livy session ID to check.
            
        Returns:
            Dictionary with session status including state, appId, appInfo, kind, and log.
            
        Example:
            ```python
            result = livy_get_session_status(
                workspace_id="12345678-1234-1234-1234-123456789abc",
                lakehouse_id="87654321-4321-4321-4321-210987654321",
                session_id="0"
            )
            
            if result.get("state") == "idle":
                # Session is ready to execute code
                pass
            elif result.get("state") == "busy":
                # Session is executing a statement
                pass
            ```
        """
        log_tool_invocation("livy_get_session_status",
                          workspace_id=workspace_id, lakehouse_id=lakehouse_id, session_id=session_id)
        logger.debug(f"Getting status for Livy session {session_id}")
        
        try:
            result = livy_service.get_session_status(
                workspace_id=workspace_id,
                lakehouse_id=lakehouse_id,
                session_id=session_id
            )
            
            state = result.get("state", "unknown")
            logger.debug(f"Session {session_id} state: {state}")
            
            return result
            
        except FabricLivyError as exc:
            logger.error(f"Livy error getting session status: {exc}")
            return {"status": "error", "message": str(exc)}

    @mcp.tool(title="Close Livy Session")
    @handle_tool_errors
    def livy_close_session(
        workspace_id: str,
        lakehouse_id: str,
        session_id: str
    ) -> dict:
        """Close (terminate) a Livy session.
        
        Terminates the specified Livy session and releases its resources. Any running
        statements will be cancelled.
        
        Parameters:
            workspace_id: Fabric workspace ID.
            lakehouse_id: Fabric lakehouse ID.
            session_id: Livy session ID to close.
            
        Returns:
            Dictionary with success/error status and message.
            
        Example:
            ```python
            result = livy_close_session(
                workspace_id="12345678-1234-1234-1234-123456789abc",
                lakehouse_id="87654321-4321-4321-4321-210987654321",
                session_id="0"
            )
            ```
        """
        log_tool_invocation("livy_close_session",
                          workspace_id=workspace_id, lakehouse_id=lakehouse_id, session_id=session_id)
        logger.info(f"Closing Livy session {session_id}")
        
        try:
            result = livy_service.close_session(
                workspace_id=workspace_id,
                lakehouse_id=lakehouse_id,
                session_id=session_id
            )
            
            logger.info(f"Successfully closed Livy session {session_id}")
            return result
            
        except FabricLivyError as exc:
            logger.error(f"Livy error closing session: {exc}")
            return {"status": "error", "message": str(exc)}

    @mcp.tool(title="Run Livy Statement")
    @handle_tool_errors
    def livy_run_statement(
        workspace_id: str,
        lakehouse_id: str,
        session_id: str,
        code: str,
        kind: str = "pyspark",
        with_wait: bool = True,
        timeout_seconds: Optional[int] = None
    ) -> dict:
        """Execute code in a Livy session.
        
        Executes PySpark, Scala, or SparkR code in an existing Livy session. The session
        must be in 'idle' state to accept new statements.
        
        **Important Notes**:
        - Use df.show() or df.printSchema() to inspect DataFrames before accessing columns
        - SHOW TABLES returns 'namespace' column, not 'database' in Fabric
        - Avoid direct Row attribute access without schema verification
        - When with_wait=False, returns immediately with statement ID - check status separately
        
        Parameters:
            workspace_id: Fabric workspace ID.
            lakehouse_id: Fabric lakehouse ID.
            session_id: Livy session ID (must be in 'idle' state).
            code: Code to execute (PySpark, Scala, or SparkR).
            kind: Statement kind - 'pyspark' (default), 'scala', or 'sparkr'.
            with_wait: If True (default), wait for statement completion before returning.
            timeout_seconds: Maximum time to wait for statement completion (default: from config).
            
        Returns:
            Dictionary with statement details including id, state, output, and execution details.
            
        Example:
            ```python
            # Execute PySpark code
            result = livy_run_statement(
                workspace_id="12345678-1234-1234-1234-123456789abc",
                lakehouse_id="87654321-4321-4321-4321-210987654321",
                session_id="0",
                code="df = spark.range(10)\\ndf.count()",
                kind="pyspark",
                with_wait=True
            )
            
            if result.get("state") == "available":
                output = result.get("output", {})
                if output.get("status") == "ok":
                    print(f"Result: {output.get('data', {}).get('text/plain')}")
            ```
        """
        log_tool_invocation("livy_run_statement",
                          workspace_id=workspace_id, lakehouse_id=lakehouse_id,
                          session_id=session_id, code=code[:100], kind=kind, with_wait=with_wait)
        logger.info(f"Running statement in Livy session {session_id}")
        
        try:
            result = livy_service.run_statement(
                workspace_id=workspace_id,
                lakehouse_id=lakehouse_id,
                session_id=session_id,
                code=code,
                kind=kind,
                with_wait=with_wait,
                timeout_seconds=timeout_seconds
            )
            
            logger.info(f"Successfully submitted statement: {result.get('id')}")
            return result
            
        except FabricLivyError as exc:
            logger.error(f"Livy error running statement: {exc}")
            return {"status": "error", "message": str(exc)}

    @mcp.tool(title="Get Livy Statement Status")
    @handle_tool_errors
    def livy_get_statement_status(
        workspace_id: str,
        lakehouse_id: str,
        session_id: str,
        statement_id: str
    ) -> dict:
        """Get the current status and output of a Livy statement.
        
        Retrieves the status, output, and execution details of a statement. Use this for
        manual status checking without auto-polling.
        
        Statement States:
        - 'waiting': Statement is queued for execution
        - 'running': Statement is currently executing
        - 'available': Statement completed successfully
        - 'error': Statement encountered an error
        - 'cancelling': Statement is being cancelled
        - 'cancelled': Statement was cancelled
        
        Parameters:
            workspace_id: Fabric workspace ID.
            lakehouse_id: Fabric lakehouse ID.
            session_id: Livy session ID.
            statement_id: Statement ID to check.
            
        Returns:
            Dictionary with statement status including id, state, output, and code.
            Output field contains execution results when state is 'available'.
            
        Example:
            ```python
            result = livy_get_statement_status(
                workspace_id="12345678-1234-1234-1234-123456789abc",
                lakehouse_id="87654321-4321-4321-4321-210987654321",
                session_id="0",
                statement_id="1"
            )
            
            if result.get("state") == "available":
                output = result.get("output", {})
                print(f"Status: {output.get('status')}")
                print(f"Result: {output.get('data', {}).get('text/plain')}")
            ```
        """
        log_tool_invocation("livy_get_statement_status",
                          workspace_id=workspace_id, lakehouse_id=lakehouse_id,
                          session_id=session_id, statement_id=statement_id)
        logger.debug(f"Getting status for statement {statement_id} in session {session_id}")
        
        try:
            result = livy_service.get_statement_status(
                workspace_id=workspace_id,
                lakehouse_id=lakehouse_id,
                session_id=session_id,
                statement_id=statement_id
            )
            
            state = result.get("state", "unknown")
            logger.debug(f"Statement {statement_id} state: {state}")
            
            return result
            
        except FabricLivyError as exc:
            logger.error(f"Livy error getting statement status: {exc}")
            return {"status": "error", "message": str(exc)}

    @mcp.tool(title="Cancel Livy Statement")
    @handle_tool_errors
    def livy_cancel_statement(
        workspace_id: str,
        lakehouse_id: str,
        session_id: str,
        statement_id: str
    ) -> dict:
        """Cancel a running Livy statement without killing the session.
        
        Cancels a statement that is currently 'waiting' or 'running'. The statement will
        transition to 'cancelling' then 'cancelled' state. The session remains available
        for new statements.
        
        **Note**: Only works on statements in 'waiting' or 'running' state.
        
        Parameters:
            workspace_id: Fabric workspace ID.
            lakehouse_id: Fabric lakehouse ID.
            session_id: Livy session ID.
            statement_id: Statement ID to cancel.
            
        Returns:
            Dictionary with cancellation result (typically {"msg": "canceled"}).
            
        Example:
            ```python
            result = livy_cancel_statement(
                workspace_id="12345678-1234-1234-1234-123456789abc",
                lakehouse_id="87654321-4321-4321-4321-210987654321",
                session_id="0",
                statement_id="1"
            )
            ```
        """
        log_tool_invocation("livy_cancel_statement",
                          workspace_id=workspace_id, lakehouse_id=lakehouse_id,
                          session_id=session_id, statement_id=statement_id)
        logger.info(f"Cancelling statement {statement_id} in session {session_id}")
        
        try:
            result = livy_service.cancel_statement(
                workspace_id=workspace_id,
                lakehouse_id=lakehouse_id,
                session_id=session_id,
                statement_id=statement_id
            )
            
            logger.info(f"Successfully cancelled statement {statement_id}")
            return result
            
        except FabricLivyError as exc:
            logger.error(f"Livy error cancelling statement: {exc}")
            return {"status": "error", "message": str(exc)}

    @mcp.tool(title="Get Livy Session Log")
    @handle_tool_errors
    def livy_get_session_log(
        workspace_id: str,
        lakehouse_id: str,
        session_id: str,
        start: int = 0,
        size: int = 500
    ) -> dict:
        """Fetch incremental Livy driver logs for a session.
        
        Retrieves Spark driver logs for debugging session startup issues or statement problems.
        Supports incremental reads with start/size parameters for paging through logs.
        
        **Use Cases**:
        - Debugging session startup issues
        - Troubleshooting failed statements
        - Investigating Spark driver problems
        - Monitoring session health
        
        **Note**: Returns driver-side logs only, not executor logs.
        
        Parameters:
            workspace_id: Fabric workspace ID.
            lakehouse_id: Fabric lakehouse ID.
            session_id: Livy session ID.
            start: Starting log line index (default: 0).
            size: Number of log lines to retrieve (default: 500).
            
        Returns:
            Dictionary with log data: {"id": <int>, "from": <int>, "size": <int>, "log": ["line1", ...]}.
            
        Example:
            ```python
            # Get first 100 log lines
            result = livy_get_session_log(
                workspace_id="12345678-1234-1234-1234-123456789abc",
                lakehouse_id="87654321-4321-4321-4321-210987654321",
                session_id="0",
                start=0,
                size=100
            )
            
            for log_line in result.get("log", []):
                print(log_line)
            
            # Get next 100 lines
            result = livy_get_session_log(..., start=100, size=100)
            ```
        """
        log_tool_invocation("livy_get_session_log",
                          workspace_id=workspace_id, lakehouse_id=lakehouse_id,
                          session_id=session_id, start=start, size=size)
        logger.debug(f"Getting logs for session {session_id} (start={start}, size={size})")
        
        try:
            result = livy_service.get_session_log(
                workspace_id=workspace_id,
                lakehouse_id=lakehouse_id,
                session_id=session_id,
                start=start,
                size=size
            )
            
            log_count = len(result.get("log", []))
            logger.debug(f"Retrieved {log_count} log lines for session {session_id}")
            return result
            
        except FabricLivyError as exc:
            logger.error(f"Livy error getting session log: {exc}")
            return {"status": "error", "message": str(exc)}
    
    logger.info("Livy tools registered successfully (8 tools)")
