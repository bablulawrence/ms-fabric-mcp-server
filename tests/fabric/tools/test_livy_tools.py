"""Tests for Livy MCP tools."""

from unittest.mock import Mock

import pytest

from ms_fabric_mcp_server.tools.livy_tools import register_livy_tools
from tests.fabric.tools.utils import capture_tools


@pytest.mark.unit
class TestLivyTools:
    def test_livy_tools_smoke(self):
        tools, mcp = capture_tools()
        livy_service = Mock()
        livy_service.create_session.return_value = {"id": "1", "state": "idle"}
        livy_service.list_sessions.return_value = {"sessions": [{"id": "1"}]}
        livy_service.get_session_status.return_value = {"id": "1", "state": "idle"}
        livy_service.close_session.return_value = {"msg": "closed"}
        livy_service.run_statement.return_value = {"id": "1", "state": "available"}
        livy_service.get_statement_status.return_value = {"id": "1", "state": "available"}
        livy_service.cancel_statement.return_value = {"msg": "canceled"}
        livy_service.get_session_log.return_value = {"log": ["line"]}

        register_livy_tools(mcp, livy_service)

        assert tools["livy_create_session"](
            workspace_id="ws-1", lakehouse_id="lh-1"
        )["id"] == "1"
        assert tools["livy_list_sessions"](
            workspace_id="ws-1", lakehouse_id="lh-1"
        )["sessions"][0]["id"] == "1"
        assert tools["livy_get_session_status"](
            workspace_id="ws-1", lakehouse_id="lh-1", session_id="1"
        )["state"] == "idle"
        assert tools["livy_close_session"](
            workspace_id="ws-1", lakehouse_id="lh-1", session_id="1"
        )["msg"] == "closed"
        assert tools["livy_run_statement"](
            workspace_id="ws-1", lakehouse_id="lh-1", session_id="1", code="1+1"
        )["state"] == "available"
        assert tools["livy_get_statement_status"](
            workspace_id="ws-1", lakehouse_id="lh-1", session_id="1", statement_id="1"
        )["state"] == "available"
        assert tools["livy_cancel_statement"](
            workspace_id="ws-1", lakehouse_id="lh-1", session_id="1", statement_id="1"
        )["msg"] == "canceled"
        assert tools["livy_get_session_log"](
            workspace_id="ws-1", lakehouse_id="lh-1", session_id="1"
        )["log"] == ["line"]

    def test_livy_run_statement_passes_code_verbatim(self):
        tools, mcp = capture_tools()
        livy_service = Mock()
        livy_service.run_statement.return_value = {"id": "1", "state": "available"}
        register_livy_tools(mcp, livy_service)

        code = "x = 1\\nx + 1"
        tools["livy_run_statement"](
            workspace_id="ws-1",
            lakehouse_id="lh-1",
            session_id="1",
            code=code,
        )

        assert livy_service.run_statement.call_args.kwargs["code"] == code
