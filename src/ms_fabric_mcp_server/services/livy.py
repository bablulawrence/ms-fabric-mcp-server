# ABOUTME: Service for Fabric Livy/Spark operations.
# ABOUTME: Handles Spark session and statement management.
"""Service for Microsoft Fabric Livy integration.

This service provides Spark session and statement management through
the Fabric Livy API for interactive PySpark, Scala, and SparkR execution.
"""

import json
import logging
import time
from typing import Any, Dict, Optional

from ..client.exceptions import (
    FabricAPIError,
    FabricError,
    FabricLivyError,
    FabricLivySessionError,
    FabricLivyStatementError,
    FabricLivyTimeoutError,
)
from ..client.http_client import FabricClient

logger = logging.getLogger(__name__)


class FabricLivyService:
    """Service for Microsoft Fabric Livy operations.
    
    This service manages Spark sessions and code execution through the Livy API.
    Sessions can take 6+ minutes to start on first creation as Spark initializes.
    
    Example:
        ```python
        from ms_fabric_mcp_server import FabricConfig, FabricClient
        from ms_fabric_mcp_server.services import FabricLivyService
        
        config = FabricConfig.from_environment()
        client = FabricClient(config)
        livy_service = FabricLivyService(client, config)
        
        # Create a session (waits for availability)
        session = livy_service.create_session(
            workspace_id="workspace-123",
            lakehouse_id="lakehouse-456",
            kind="pyspark",
            with_wait=True
        )
        
        # Run code
        result = livy_service.run_statement(
            workspace_id="workspace-123",
            lakehouse_id="lakehouse-456",
            session_id=session["id"],
            code="df = spark.read.parquet('Files/data.parquet'); df.count()",
            wait=True
        )
        
        # Clean up
        livy_service.close_session(
            workspace_id="workspace-123",
            lakehouse_id="lakehouse-456",
            session_id=session["id"]
        )
        ```
    """
    
    def __init__(
        self,
        client: FabricClient,
        session_wait_timeout: int = 600,
        statement_wait_timeout: int = 600,
        poll_interval: float = 5.0,
    ):
        """Initialize the Livy service.
        
        Args:
            client: FabricClient instance for API communication
            session_wait_timeout: Maximum seconds to wait for session availability (default: 600)
            statement_wait_timeout: Maximum seconds to wait for statement completion (default: 600)
            poll_interval: Seconds between status polls (default: 5.0)
        """
        self.client = client
        self.session_wait_timeout = session_wait_timeout
        self.statement_wait_timeout = statement_wait_timeout
        self.poll_interval = poll_interval
        
        logger.debug("FabricLivyService initialized")
    
    def create_session(
        self,
        workspace_id: str,
        lakehouse_id: str,
        environment_id: Optional[str] = None,
        kind: str = "pyspark",
        conf: Optional[Dict[str, Any]] = None,
        with_wait: bool = True,
        timeout_seconds: Optional[int] = None
    ) -> Dict[str, Any]:
        """Create a new Livy session.
        
        Args:
            workspace_id: Fabric workspace ID
            lakehouse_id: Fabric lakehouse ID
            environment_id: Optional Fabric environment ID for pre-installed libraries
            kind: Session kind - 'pyspark' (default), 'scala', or 'sparkr'
            conf: Additional Spark configuration as key-value pairs
            with_wait: If True, wait for session to become available (default: True)
            timeout_seconds: Maximum time to wait for session availability 
                (default: session_wait_timeout)
            
        Returns:
            Session creation response (with final state if with_wait=True)
            
        Raises:
            FabricLivySessionError: If session creation fails
            FabricLivyTimeoutError: If session doesn't become available within timeout
            FabricAPIError: If API request fails
            
        Note:
            Session creation can take 6+ minutes on first startup as Spark initializes.
            It's recommended to keep with_wait=True to ensure the session is ready for use.
            
        Example:
            ```python
            session = livy_service.create_session(
                workspace_id="abc-123",
                lakehouse_id="def-456",
                environment_id="env-789",  # Optional
                kind="pyspark",
                conf={"spark.executor.memory": "4g"},
                with_wait=True
            )
            
            print(f"Session ID: {session['id']}")
            print(f"State: {session['state']}")
            ```
        """
        logger.info(
            f"Creating Livy session in workspace {workspace_id}, lakehouse {lakehouse_id}"
        )
        
        # Prepare session payload
        payload = {
            "kind": kind,
            "conf": conf or {}
        }
        
        # Add environment configuration if provided
        if environment_id:
            payload["conf"]["spark.fabric.environmentDetails"] = json.dumps({
                "id": environment_id
            })
            logger.debug(f"Using environment ID: {environment_id}")
        
        # Build endpoint URL
        endpoint = (
            f"workspaces/{workspace_id}/lakehouses/{lakehouse_id}/"
            f"livyapi/versions/2023-12-01/sessions"
        )
        
        try:
            response = self.client.make_api_request(
                method="POST",
                endpoint=endpoint,
                payload=payload,
                timeout=60
            )
            
            session_data = response.json()
            session_id = session_data.get("id")
            
            logger.info(f"Successfully created Livy session: {session_id}")
            
            # If with_wait is True, poll until session becomes available
            if with_wait:
                logger.debug(f"Waiting for session {session_id} to become available...")
                final_session_data = self.wait_for_session(
                    workspace_id=workspace_id,
                    lakehouse_id=lakehouse_id,
                    session_id=session_id,
                    timeout_seconds=timeout_seconds
                )
                return final_session_data
            
            return session_data
            
        except FabricAPIError as exc:
            logger.error(f"API error creating Livy session: {exc}")
            raise FabricLivySessionError("new", f"Failed to create session: {exc}")
        except Exception as exc:
            logger.error(f"Unexpected error creating Livy session: {exc}")
            raise FabricLivyError(f"Unexpected error: {exc}")
    
    def list_sessions(
        self,
        workspace_id: str,
        lakehouse_id: str,
        top: Optional[int] = None,
        continuation_token: Optional[str] = None
    ) -> Dict[str, Any]:
        """List all Livy sessions in a workspace/lakehouse.
        
        Args:
            workspace_id: Fabric workspace ID
            lakehouse_id: Fabric lakehouse ID
            top: Maximum number of sessions to return (pagination)
            continuation_token: Token for pagination continuation
            
        Returns:
            Dictionary containing sessions list and pagination info
            
        Raises:
            FabricLivyError: If listing sessions fails
            FabricAPIError: If API request fails
            
        Example:
            ```python
            sessions = livy_service.list_sessions(
                workspace_id="abc-123",
                lakehouse_id="def-456"
            )
            
            for session in sessions.get("sessions", []):
                print(f"Session {session['id']}: {session['state']}")
            ```
        """
        logger.debug(
            f"Listing Livy sessions for workspace {workspace_id}, lakehouse {lakehouse_id}"
        )
        
        # Build endpoint URL
        endpoint = (
            f"workspaces/{workspace_id}/lakehouses/{lakehouse_id}/"
            f"livyapi/versions/2023-12-01/sessions"
        )
        
        # Build query parameters
        params = {}
        if top is not None:
            params["$top"] = top
        if continuation_token:
            params["continuationToken"] = continuation_token
        
        # Add query parameters to endpoint if any
        if params:
            param_str = "&".join([f"{k}={v}" for k, v in params.items()])
            endpoint = f"{endpoint}?{param_str}"
        
        try:
            response = self.client.make_api_request(
                method="GET",
                endpoint=endpoint,
                timeout=30
            )
            
            sessions_data = response.json()
            session_count = len(sessions_data.get("sessions", []))
            logger.debug(f"Retrieved {session_count} Livy sessions")
            
            return sessions_data
            
        except FabricAPIError as exc:
            logger.error(f"API error listing Livy sessions: {exc}")
            raise FabricLivyError(f"Failed to list sessions: {exc}")
        except Exception as exc:
            logger.error(f"Unexpected error listing Livy sessions: {exc}")
            raise FabricLivyError(f"Unexpected error: {exc}")
    
    def get_session_status(
        self,
        workspace_id: str,
        lakehouse_id: str,
        session_id: str
    ) -> Dict[str, Any]:
        """Get session details and status.
        
        Args:
            workspace_id: Fabric workspace ID
            lakehouse_id: Fabric lakehouse ID
            session_id: Livy session ID
            
        Returns:
            Session details including state, Spark info, and configuration
            
        Raises:
            FabricLivySessionError: If session retrieval fails
            FabricAPIError: If API request fails
            
        Note:
            Session states: 'not_started', 'starting', 'idle', 'busy', 
            'shutting_down', 'error', 'dead', 'killed', 'success'
            
        Example:
            ```python
            status = livy_service.get_session_status(
                workspace_id="abc-123",
                lakehouse_id="def-456",
                session_id="session-789"
            )
            
            print(f"State: {status['state']}")
            if status['state'] == 'idle':
                print("Session ready for statements")
            ```
        """
        logger.debug(f"Getting Livy session {session_id}")
        
        # Build endpoint URL
        endpoint = (
            f"workspaces/{workspace_id}/lakehouses/{lakehouse_id}/"
            f"livyapi/versions/2023-12-01/sessions/{session_id}"
        )
        
        try:
            response = self.client.make_api_request(
                method="GET",
                endpoint=endpoint,
                timeout=30
            )
            
            session_data = response.json()
            logger.debug(f"Session {session_id} state: {session_data.get('state', 'unknown')}")
            return session_data
            
        except FabricAPIError as exc:
            logger.error(f"API error getting Livy session: {exc}")
            raise FabricLivySessionError(session_id, f"Failed to get session: {exc}")
        except Exception as exc:
            logger.error(f"Unexpected error getting Livy session: {exc}")
            raise FabricLivyError(f"Unexpected error: {exc}")
    
    def close_session(
        self,
        workspace_id: str,
        lakehouse_id: str,
        session_id: str
    ) -> Dict[str, Any]:
        """Close (terminate) a Livy session.
        
        Args:
            workspace_id: Fabric workspace ID
            lakehouse_id: Fabric lakehouse ID
            session_id: Livy session ID
            
        Returns:
            Session closure response
            
        Raises:
            FabricLivySessionError: If session closure fails
            FabricAPIError: If API request fails
            
        Example:
            ```python
            livy_service.close_session(
                workspace_id="abc-123",
                lakehouse_id="def-456",
                session_id="session-789"
            )
            ```
        """
        logger.info(f"Closing Livy session {session_id}")
        
        # Build endpoint URL
        endpoint = (
            f"workspaces/{workspace_id}/lakehouses/{lakehouse_id}/"
            f"livyapi/versions/2023-12-01/sessions/{session_id}"
        )
        
        try:
            response = self.client.make_api_request(
                method="DELETE",
                endpoint=endpoint,
                timeout=30
            )
            
            # Handle successful deletion (may not return JSON)
            try:
                result = response.json()
            except Exception:
                result = {
                    "status": response.status_code,
                    "message": "Session closed successfully"
                }
            
            logger.info(f"Successfully closed Livy session {session_id}")
            return result
            
        except FabricAPIError as exc:
            logger.error(f"API error closing Livy session: {exc}")
            raise FabricLivySessionError(session_id, f"Failed to close session: {exc}")
        except Exception as exc:
            logger.error(f"Unexpected error closing Livy session: {exc}")
            raise FabricLivyError(f"Unexpected error: {exc}")
    
    def get_session_log(
        self,
        workspace_id: str,
        lakehouse_id: str,
        session_id: str,
        start: int = 0,
        size: int = 500
    ) -> Dict[str, Any]:
        """Fetch incremental Livy driver logs for a session.
        
        Args:
            workspace_id: Fabric workspace ID
            lakehouse_id: Fabric lakehouse ID
            session_id: Livy session ID
            start: Starting log line index (default: 0)
            size: Number of log lines to retrieve (default: 500)
            
        Returns:
            Log data with pagination info: 
            {"id": <int>, "from": <int>, "size": <int>, "log": ["line1", ...]}
            
        Raises:
            FabricLivySessionError: If log retrieval fails
            FabricAPIError: If API request fails
            
        Note:
            Use this for debugging session startup issues or Spark driver problems.
            Supports incremental reads with start/size parameters for paging.
            
        Example:
            ```python
            # Get first 100 log lines
            logs = livy_service.get_session_log(
                workspace_id="abc-123",
                lakehouse_id="def-456",
                session_id="session-789",
                start=0,
                size=100
            )
            
            for line in logs.get("log", []):
                print(line)
            ```
        """
        logger.debug(
            f"Getting logs for Livy session {session_id} (start={start}, size={size})"
        )
        
        # Build endpoint URL
        endpoint = (
            f"workspaces/{workspace_id}/lakehouses/{lakehouse_id}/"
            f"livyapi/versions/2023-12-01/sessions/{session_id}/log"
            f"?from={start}&size={size}"
        )
        
        try:
            response = self.client.make_api_request(
                method="GET",
                endpoint=endpoint,
                timeout=30
            )
            
            log_data = response.json()
            logger.debug(f"Retrieved {len(log_data.get('log', []))} log lines")
            return log_data
            
        except FabricAPIError as exc:
            logger.error(f"API error getting session logs: {exc}")
            raise FabricLivySessionError(session_id, f"Failed to get logs: {exc}")
        except Exception as exc:
            logger.error(f"Unexpected error getting session logs: {exc}")
            raise FabricLivyError(f"Unexpected error: {exc}")
    
    def run_statement(
        self,
        workspace_id: str,
        lakehouse_id: str,
        session_id: str,
        code: str,
        kind: str = "pyspark",
        with_wait: bool = True,
        timeout_seconds: Optional[int] = None
    ) -> Dict[str, Any]:
        """Execute code in a Livy session.
        
        Args:
            workspace_id: Fabric workspace ID
            lakehouse_id: Fabric lakehouse ID
            session_id: Livy session ID
            code: Code to execute (PySpark, Scala, or SparkR)
            kind: Statement kind - 'pyspark' (default), 'scala', or 'sparkr'
            with_wait: If True, wait for statement completion (default: True)
            timeout_seconds: Maximum time to wait for statement completion
                (default: statement_wait_timeout)
            
        Returns:
            Statement execution response (with final state if with_wait=True)
            
        Raises:
            FabricLivyStatementError: If statement execution fails
            FabricAPIError: If API request fails
            FabricLivyTimeoutError: If statement doesn't complete within timeout
            
        Note:
            - Use df.show() or df.printSchema() to inspect DataFrames before accessing columns
            - SHOW TABLES returns 'namespace' column, not 'database' in Fabric
            - Avoid direct Row attribute access without schema verification
            - When with_wait=False, returns immediately with statement ID
            
        Example:
            ```python
            result = livy_service.run_statement(
                workspace_id="abc-123",
                lakehouse_id="def-456",
                session_id="session-789",
                code=\"\"\"
                df = spark.read.parquet('Files/data.parquet')
                df.count()
                \"\"\",
                with_wait=True
            )
            
            if result.get("state") == "available":
                output = result.get("output", {})
                print(f"Result: {output.get('data', {}).get('text/plain')}")
            ```
        """
        logger.info(f"Running statement in Livy session {session_id}")
        logger.debug(f"Code to execute: {code[:200]}...")  # Log first 200 chars
        
        # Prepare statement payload
        payload = {
            "code": code,
            "kind": kind
        }
        
        # Build endpoint URL
        endpoint = (
            f"workspaces/{workspace_id}/lakehouses/{lakehouse_id}/"
            f"livyapi/versions/2023-12-01/sessions/{session_id}/statements"
        )
        
        try:
            response = self.client.make_api_request(
                method="POST",
                endpoint=endpoint,
                payload=payload,
                timeout=60
            )
            
            statement_data = response.json()
            statement_id = statement_data.get("id")
            
            logger.info(f"Successfully submitted statement: {statement_id}")
            
            # If with_wait is True, wait for completion
            if with_wait:
                final_statement_data = self.wait_for_statement(
                    workspace_id=workspace_id,
                    lakehouse_id=lakehouse_id,
                    session_id=session_id,
                    statement_id=statement_id,
                    timeout_seconds=timeout_seconds
                )
                return final_statement_data
            
            return statement_data
            
        except FabricLivyTimeoutError:
            raise
        except FabricLivyError:
            raise
        except FabricAPIError as exc:
            logger.error(f"API error running statement: {exc}")
            raise FabricLivyStatementError(
                session_id, "new", f"Failed to run statement: {exc}"
            )
        except Exception as exc:
            logger.error(f"Unexpected error running statement: {exc}")
            raise FabricLivyError(f"Unexpected error: {exc}")
    
    def get_statement_status(
        self,
        workspace_id: str,
        lakehouse_id: str,
        session_id: str,
        statement_id: str
    ) -> Dict[str, Any]:
        """Get the current status and output of a Livy statement.
        
        Args:
            workspace_id: Fabric workspace ID
            lakehouse_id: Fabric lakehouse ID
            session_id: Livy session ID
            statement_id: Statement ID to check
            
        Returns:
            Statement details including state, output, and execution details
            
        Raises:
            FabricLivyStatementError: If statement retrieval fails
            FabricAPIError: If API request fails
            
        Note:
            Statement states: 'waiting', 'running', 'available', 'error', 
            'cancelling', 'cancelled'
            
        Example:
            ```python
            status = livy_service.get_statement_status(
                workspace_id="abc-123",
                lakehouse_id="def-456",
                session_id="session-789",
                statement_id="stmt-123"
            )
            
            print(f"State: {status['state']}")
            if status['state'] == 'available':
                print(f"Output: {status.get('output')}")
            ```
        """
        logger.debug(f"Getting statement {statement_id} in session {session_id}")
        
        # Build endpoint URL
        endpoint = (
            f"workspaces/{workspace_id}/lakehouses/{lakehouse_id}/"
            f"livyapi/versions/2023-12-01/sessions/{session_id}/"
            f"statements/{statement_id}"
        )
        
        try:
            response = self.client.make_api_request(
                method="GET",
                endpoint=endpoint,
                timeout=30
            )
            
            statement_data = response.json()
            state = statement_data.get("state", "unknown")
            logger.debug(f"Statement {statement_id} state: {state}")
            return statement_data
            
        except FabricAPIError as exc:
            logger.error(f"API error getting statement: {exc}")
            raise FabricLivyStatementError(
                session_id, statement_id, f"Failed to get statement: {exc}"
            )
        except Exception as exc:
            logger.error(f"Unexpected error getting statement: {exc}")
            raise FabricLivyError(f"Unexpected error: {exc}")
    
    def cancel_statement(
        self,
        workspace_id: str,
        lakehouse_id: str,
        session_id: str,
        statement_id: str
    ) -> Dict[str, Any]:
        """Cancel a running Livy statement without killing the session.
        
        Args:
            workspace_id: Fabric workspace ID
            lakehouse_id: Fabric lakehouse ID
            session_id: Livy session ID
            statement_id: Statement ID to cancel
            
        Returns:
            Cancellation result (typically {"msg": "canceled"})
            
        Raises:
            FabricLivyStatementError: If statement cancellation fails
            FabricAPIError: If API request fails
            
        Note:
            - Use this to stop long-running statements without terminating the session
            - Only works on statements in 'waiting' or 'running' state
            - Statement will transition to 'cancelling' then 'cancelled' state
            - Session remains available for new statements
            
        Example:
            ```python
            result = livy_service.cancel_statement(
                workspace_id="abc-123",
                lakehouse_id="def-456",
                session_id="session-789",
                statement_id="stmt-123"
            )
            
            print(f"Cancellation: {result.get('msg')}")
            ```
        """
        logger.info(f"Cancelling statement {statement_id} in session {session_id}")
        
        # Build endpoint URL
        endpoint = (
            f"workspaces/{workspace_id}/lakehouses/{lakehouse_id}/"
            f"livyapi/versions/2023-12-01/sessions/{session_id}/"
            f"statements/{statement_id}/cancel"
        )
        
        try:
            response = self.client.make_api_request(
                method="POST",
                endpoint=endpoint,
                payload={},
                timeout=30
            )
            
            cancel_data = response.json()
            logger.info(f"Successfully cancelled statement {statement_id}")
            return cancel_data
            
        except FabricAPIError as exc:
            logger.error(f"API error cancelling statement: {exc}")
            raise FabricLivyStatementError(
                session_id, statement_id, f"Failed to cancel statement: {exc}"
            )
        except Exception as exc:
            logger.error(f"Unexpected error cancelling statement: {exc}")
            raise FabricLivyError(f"Unexpected error: {exc}")
    
    def wait_for_session(
        self,
        workspace_id: str,
        lakehouse_id: str,
        session_id: str,
        timeout_seconds: Optional[int] = None,
        poll_interval: Optional[float] = None
    ) -> Dict[str, Any]:
        """Poll session until it becomes available.
        
        Args:
            workspace_id: Fabric workspace ID
            lakehouse_id: Fabric lakehouse ID
            session_id: Livy session ID
            timeout_seconds: Maximum time to wait (default: session_wait_timeout)
            poll_interval: Polling interval in seconds (default: self.poll_interval)
            
        Returns:
            Final session data when available
            
        Raises:
            FabricLivyTimeoutError: If session doesn't become available within timeout
            FabricLivySessionError: If session fails or polling fails
            
        Example:
            ```python
            session = livy_service.wait_for_session(
                workspace_id="abc-123",
                lakehouse_id="def-456",
                session_id="session-789",
                timeout_seconds=900  # 15 minutes
            )
            
            print(f"Session ready: {session['state']}")
            ```
        """
        timeout = timeout_seconds or self.session_wait_timeout
        interval = poll_interval or self.poll_interval
        
        logger.info(
            f"Waiting for session {session_id} to become available (timeout={timeout}s)"
        )
        
        start_time = time.time()
        
        while True:
            elapsed = time.time() - start_time
            
            # Check timeout
            if elapsed > timeout:
                logger.error(f"Session {session_id} timed out after {timeout}s")
                raise FabricLivyTimeoutError("session creation", timeout)
            
            try:
                # Get current session status
                session_data = self.get_session_status(
                    workspace_id, lakehouse_id, session_id
                )
                
                state = session_data.get("state", "unknown")
                
                # Check if session is available
                if state == "idle":
                    logger.info(f"Session {session_id} is now available")
                    return session_data
                elif state in ["error", "dead", "killed"]:
                    error_msg = session_data.get("log", ["Session failed"])
                    if isinstance(error_msg, list) and error_msg:
                        error_msg = error_msg[-1]  # Get last log entry
                    logger.error(f"Session {session_id} failed: {error_msg}")
                    raise FabricLivySessionError(
                        session_id, f"Session {state}: {error_msg}"
                    )
                
                # Log progress for long-running session creation
                if elapsed % 30 < interval:  # Log every 30 seconds
                    logger.debug(
                        f"Session {session_id} still starting "
                        f"(state={state}, elapsed={elapsed:.1f}s)"
                    )
                
            except (FabricLivyTimeoutError, FabricLivySessionError):
                raise
            except Exception as exc:
                logger.error(f"Error polling session {session_id}: {exc}")
                raise FabricLivySessionError(session_id, f"Polling error: {exc}")
            
            # Wait before next poll
            time.sleep(interval)
    
    def wait_for_statement(
        self,
        workspace_id: str,
        lakehouse_id: str,
        session_id: str,
        statement_id: str,
        timeout_seconds: Optional[int] = None,
        poll_interval: Optional[float] = None
    ) -> Dict[str, Any]:
        """Poll statement until completion.
        
        Args:
            workspace_id: Fabric workspace ID
            lakehouse_id: Fabric lakehouse ID
            session_id: Livy session ID
            statement_id: Statement ID
            timeout_seconds: Maximum time to wait (default: statement_wait_timeout)
            poll_interval: Polling interval in seconds (default: self.poll_interval)
            
        Returns:
            Final statement data when completed
            
        Raises:
            FabricLivyTimeoutError: If statement doesn't complete within timeout
            FabricLivyStatementError: If statement fails or polling fails
            
        Example:
            ```python
            result = livy_service.wait_for_statement(
                workspace_id="abc-123",
                lakehouse_id="def-456",
                session_id="session-789",
                statement_id="stmt-123",
                timeout_seconds=300
            )
            
            if result['state'] == 'available':
                print("Statement completed successfully")
            ```
        """
        timeout = timeout_seconds or self.statement_wait_timeout
        interval = poll_interval or self.poll_interval
        
        logger.info(
            f"Waiting for statement {statement_id} to complete (timeout={timeout}s)"
        )
        
        start_time = time.time()
        
        while True:
            elapsed = time.time() - start_time
            
            # Check timeout
            if elapsed > timeout:
                logger.error(f"Statement {statement_id} timed out after {timeout}s")
                raise FabricLivyTimeoutError("statement execution", timeout)
            
            try:
                # Get current statement status
                statement_data = self.get_statement_status(
                    workspace_id, lakehouse_id, session_id, statement_id
                )
                
                state = statement_data.get("state", "unknown")
                
                # Check if statement is complete
                if state == "available":
                    logger.info(f"Statement {statement_id} completed successfully")
                    return statement_data
                elif state in ["error", "cancelled"]:
                    error_msg = statement_data.get("output", {}).get(
                        "evalue", "Statement failed"
                    )
                    logger.error(f"Statement {statement_id} failed: {error_msg}")
                    raise FabricLivyStatementError(
                        session_id, statement_id, f"Statement {state}: {error_msg}"
                    )
                
                # Log progress
                if elapsed % 30 < interval:  # Log every 30 seconds
                    logger.debug(
                        f"Statement {statement_id} still running "
                        f"(state={state}, elapsed={elapsed:.1f}s)"
                    )
                
            except (FabricLivyTimeoutError, FabricLivyStatementError):
                raise
            except Exception as exc:
                logger.error(f"Error polling statement {statement_id}: {exc}")
                raise FabricLivyStatementError(
                    session_id, statement_id, f"Polling error: {exc}"
                )
            
            # Wait before next poll
            time.sleep(interval)
