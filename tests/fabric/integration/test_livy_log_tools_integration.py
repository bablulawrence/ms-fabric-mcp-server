"""Integration tests for Livy log tools."""

import pytest


@pytest.mark.integration
@pytest.mark.asyncio
async def test_livy_session_logs(call_tool, lakehouse_id, workspace_id, poll_until):
    session_id = None

    try:
        create_result = await call_tool(
            "livy_create_session",
            workspace_id=workspace_id,
            lakehouse_id=lakehouse_id,
        )
        assert create_result.get("status") != "error"
        session_id = create_result.get("id")
        assert session_id is not None
        session_id = str(session_id)

        # Poll until session is ready
        async def _session_ready():
            status = await call_tool(
                "livy_get_session_status",
                workspace_id=workspace_id,
                lakehouse_id=lakehouse_id,
                session_id=session_id,
            )
            state = status.get("state")
            if state == "idle":
                return status
            if state in ("error", "dead", "killed"):
                pytest.fail(f"Session entered terminal state: {state}")
            return None

        await poll_until(_session_ready, timeout_seconds=600, interval_seconds=10)

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
            if "scp" in message and ("claim" in message or "unauthorized" in message):
                pytest.skip("Livy logs require delegated token (scp claim)")
        assert isinstance(log_result.get("log_content"), str)

    finally:
        if session_id:
            await call_tool(
                "livy_close_session",
                workspace_id=workspace_id,
                lakehouse_id=lakehouse_id,
                session_id=session_id,
            )
