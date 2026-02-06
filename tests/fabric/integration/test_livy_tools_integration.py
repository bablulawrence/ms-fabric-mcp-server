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

    finally:
        if session_id:
            await call_tool(
                "livy_close_session",
                workspace_id=workspace_id,
                lakehouse_id=lakehouse_id,
                session_id=session_id,
            )


@pytest.mark.integration
@pytest.mark.asyncio
async def test_livy_cancel_statement(call_tool, lakehouse_id, workspace_id, poll_until):
    session_id = None
    statement_id = None

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

        statement_result = await call_tool(
            "livy_run_statement",
            workspace_id=workspace_id,
            lakehouse_id=lakehouse_id,
            session_id=session_id,
            code="import time\ntime.sleep(120)",
            with_wait=False,
        )
        assert statement_result.get("status") != "error"
        statement_id = str(statement_result.get("id"))
        assert statement_id is not None

        async def _get_statement_state():
            status = await call_tool(
                "livy_get_statement_status",
                workspace_id=workspace_id,
                lakehouse_id=lakehouse_id,
                session_id=session_id,
                statement_id=statement_id,
            )
            if status.get("status") == "error":
                return status
            state = status.get("state")
            if state in ("waiting", "running", "available", "error", "cancelled", "cancelling"):
                return status
            return None

        status = await poll_until(_get_statement_state, timeout_seconds=120, interval_seconds=5)
        assert status is not None
        if status.get("state") == "available":
            pytest.skip("Statement completed before cancellation")
        if status.get("state") == "error":
            pytest.fail(f"Statement errored before cancellation: {status}")

        cancel_result = await call_tool(
            "livy_cancel_statement",
            workspace_id=workspace_id,
            lakehouse_id=lakehouse_id,
            session_id=session_id,
            statement_id=statement_id,
        )
        assert cancel_result.get("status") != "error"
    finally:
        if session_id:
            await call_tool(
                "livy_close_session",
                workspace_id=workspace_id,
                lakehouse_id=lakehouse_id,
                session_id=session_id,
            )
