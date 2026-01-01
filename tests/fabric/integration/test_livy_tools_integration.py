"""Integration tests for Livy tools."""

import pytest


@pytest.mark.integration
@pytest.mark.asyncio
async def test_livy_session_lifecycle(call_tool, lakehouse_id, workspace_id):
    session_id = None

    try:
        create_result = await call_tool(
            "livy_create_session",
            workspace_id=workspace_id,
            lakehouse_id=lakehouse_id,
            with_wait=True,
        )
        assert create_result.get("status") != "error"
        session_id = str(create_result.get("id"))
        assert session_id is not None

        list_result = await call_tool(
            "livy_list_sessions",
            workspace_id=workspace_id,
            lakehouse_id=lakehouse_id,
        )
        assert list_result.get("status") != "error"
        sessions = list_result.get("sessions", list_result.get("items"))
        assert isinstance(sessions, list)

        status_result = await call_tool(
            "livy_get_session_status",
            workspace_id=workspace_id,
            lakehouse_id=lakehouse_id,
            session_id=session_id,
        )
        assert status_result.get("state")

        statement_result = await call_tool(
            "livy_run_statement",
            workspace_id=workspace_id,
            lakehouse_id=lakehouse_id,
            session_id=session_id,
            code="x = 1\nx + 1",
        )
        assert statement_result.get("state") == "available"
        assert statement_result.get("output", {}).get("status") == "ok"
        statement_id = str(statement_result.get("id"))

        statement_status = await call_tool(
            "livy_get_statement_status",
            workspace_id=workspace_id,
            lakehouse_id=lakehouse_id,
            session_id=session_id,
            statement_id=statement_id,
        )
        assert statement_status.get("state") == "available"

        log_result = await call_tool(
            "livy_get_session_log",
            workspace_id=workspace_id,
            lakehouse_id=lakehouse_id,
            session_id=session_id,
            start=0,
            size=10,
        )
        if log_result.get("status") == "error":
            message = (log_result.get("message") or "").lower()
            if "notfound" in message or "not found" in message:
                pytest.skip("Livy logs not available for this session")
        assert isinstance(log_result.get("log"), list)

    finally:
        if session_id:
            await call_tool(
                "livy_close_session",
                workspace_id=workspace_id,
                lakehouse_id=lakehouse_id,
                session_id=session_id,
            )
