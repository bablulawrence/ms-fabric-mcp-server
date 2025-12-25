"""Unit tests for FabricLivyService."""

import itertools
import json
from unittest.mock import Mock, patch

import pytest

from ms_fabric_mcp_server.client.exceptions import (
    FabricAPIError,
    FabricLivyError,
    FabricLivySessionError,
    FabricLivyStatementError,
    FabricLivyTimeoutError,
)


def _make_response(status_code=200, json_data=None):
    response = Mock()
    response.status_code = status_code
    response.ok = 200 <= status_code < 300
    response.json.return_value = json_data or {}
    return response


@pytest.fixture
def livy_service(mock_fabric_client):
    from ms_fabric_mcp_server.services.livy import FabricLivyService

    return FabricLivyService(
        mock_fabric_client,
        session_wait_timeout=2,
        statement_wait_timeout=2,
        poll_interval=0.01,
    )


@pytest.mark.unit
class TestFabricLivyService:
    """Test suite for FabricLivyService."""

    def test_create_session_payload_includes_environment(self, livy_service, mock_fabric_client):
        """Create session payload includes environment details."""
        response = _make_response(200, {"id": 1, "state": "starting"})
        mock_fabric_client.make_api_request.return_value = response

        result = livy_service.create_session(
            workspace_id="ws-1",
            lakehouse_id="lh-1",
            environment_id="env-1",
            kind="pyspark",
            conf={"spark.executor.memory": "4g"},
            with_wait=False,
        )

        assert result["id"] == 1
        call_kwargs = mock_fabric_client.make_api_request.call_args.kwargs
        payload = call_kwargs["payload"]
        assert payload["kind"] == "pyspark"
        assert payload["conf"]["spark.executor.memory"] == "4g"
        env_details = json.loads(payload["conf"]["spark.fabric.environmentDetails"])
        assert env_details["id"] == "env-1"

    def test_create_session_with_wait_uses_wait_for_session(self, livy_service, mock_fabric_client):
        """with_wait True delegates to wait_for_session."""
        mock_fabric_client.make_api_request.return_value = _make_response(200, {"id": 7})
        livy_service.wait_for_session = Mock(return_value={"id": 7, "state": "idle"})

        result = livy_service.create_session(
            workspace_id="ws-1",
            lakehouse_id="lh-1",
            with_wait=True,
        )

        assert result["state"] == "idle"
        livy_service.wait_for_session.assert_called_once_with(
            workspace_id="ws-1",
            lakehouse_id="lh-1",
            session_id=7,
            timeout_seconds=None,
        )

    def test_create_session_api_error(self, livy_service, mock_fabric_client):
        """API errors are mapped to FabricLivySessionError."""
        mock_fabric_client.make_api_request.side_effect = FabricAPIError(500, "boom")

        with pytest.raises(FabricLivySessionError):
            livy_service.create_session("ws-1", "lh-1", with_wait=False)

    def test_list_sessions_success(self, livy_service, mock_fabric_client):
        """List sessions returns response payload."""
        response = _make_response(200, {"sessions": [{"id": 1}], "total": 1})
        mock_fabric_client.make_api_request.return_value = response

        result = livy_service.list_sessions("ws-1", "lh-1")

        assert result["total"] == 1
        assert result["sessions"][0]["id"] == 1

    def test_list_sessions_api_error(self, livy_service, mock_fabric_client):
        """API errors raise FabricLivyError."""
        mock_fabric_client.make_api_request.side_effect = FabricAPIError(500, "boom")

        with pytest.raises(FabricLivyError):
            livy_service.list_sessions("ws-1", "lh-1")

    def test_get_session_status_success(self, livy_service, mock_fabric_client):
        """Get session status returns session data."""
        response = _make_response(200, {"id": 1, "state": "idle"})
        mock_fabric_client.make_api_request.return_value = response

        result = livy_service.get_session_status("ws-1", "lh-1", "1")

        assert result["state"] == "idle"

    def test_get_session_status_api_error(self, livy_service, mock_fabric_client):
        """API errors raise FabricLivySessionError."""
        mock_fabric_client.make_api_request.side_effect = FabricAPIError(404, "missing")

        with pytest.raises(FabricLivySessionError):
            livy_service.get_session_status("ws-1", "lh-1", "1")

    def test_close_session_success(self, livy_service, mock_fabric_client):
        """Close session returns JSON body when available."""
        response = _make_response(200, {"status": "ok"})
        mock_fabric_client.make_api_request.return_value = response

        result = livy_service.close_session("ws-1", "lh-1", "1")

        assert result["status"] == "ok"

    def test_close_session_api_error(self, livy_service, mock_fabric_client):
        """API errors raise FabricLivySessionError."""
        mock_fabric_client.make_api_request.side_effect = FabricAPIError(500, "boom")

        with pytest.raises(FabricLivySessionError):
            livy_service.close_session("ws-1", "lh-1", "1")

    def test_get_session_log_success(self, livy_service, mock_fabric_client):
        """Get session log returns log payload."""
        response = _make_response(200, {"log": ["a", "b"], "size": 2})
        mock_fabric_client.make_api_request.return_value = response

        result = livy_service.get_session_log("ws-1", "lh-1", "1", start=0, size=2)

        assert result["log"] == ["a", "b"]

    def test_get_session_log_api_error(self, livy_service, mock_fabric_client):
        """API errors raise FabricLivySessionError."""
        mock_fabric_client.make_api_request.side_effect = FabricAPIError(500, "boom")

        with pytest.raises(FabricLivySessionError):
            livy_service.get_session_log("ws-1", "lh-1", "1")

    def test_run_statement_with_wait_false(self, livy_service, mock_fabric_client):
        """Run statement returns immediate response when with_wait False."""
        response = _make_response(200, {"id": 3, "state": "running"})
        mock_fabric_client.make_api_request.return_value = response

        result = livy_service.run_statement(
            workspace_id="ws-1",
            lakehouse_id="lh-1",
            session_id="1",
            code="print(1)",
            kind="pyspark",
            with_wait=False,
        )

        assert result["id"] == 3
        call_kwargs = mock_fabric_client.make_api_request.call_args.kwargs
        payload = call_kwargs["payload"]
        assert payload["code"] == "print(1)"
        assert payload["kind"] == "pyspark"

    def test_run_statement_with_wait_true(self, livy_service, mock_fabric_client):
        """Run statement waits for completion when with_wait True."""
        mock_fabric_client.make_api_request.return_value = _make_response(200, {"id": 4})
        livy_service.wait_for_statement = Mock(return_value={"id": 4, "state": "available"})

        result = livy_service.run_statement(
            workspace_id="ws-1",
            lakehouse_id="lh-1",
            session_id="1",
            code="print(1)",
            with_wait=True,
        )

        assert result["state"] == "available"
        livy_service.wait_for_statement.assert_called_once_with(
            workspace_id="ws-1",
            lakehouse_id="lh-1",
            session_id="1",
            statement_id=4,
            timeout_seconds=None,
        )

    def test_run_statement_api_error(self, livy_service, mock_fabric_client):
        """API errors raise FabricLivyStatementError."""
        mock_fabric_client.make_api_request.side_effect = FabricAPIError(500, "boom")

        with pytest.raises(FabricLivyStatementError):
            livy_service.run_statement("ws-1", "lh-1", "1", "print(1)", with_wait=False)

    def test_get_statement_status_success(self, livy_service, mock_fabric_client):
        """Get statement status returns payload."""
        response = _make_response(200, {"id": 1, "state": "available"})
        mock_fabric_client.make_api_request.return_value = response

        result = livy_service.get_statement_status("ws-1", "lh-1", "1", "1")

        assert result["state"] == "available"

    def test_get_statement_status_api_error(self, livy_service, mock_fabric_client):
        """API errors raise FabricLivyStatementError."""
        mock_fabric_client.make_api_request.side_effect = FabricAPIError(404, "missing")

        with pytest.raises(FabricLivyStatementError):
            livy_service.get_statement_status("ws-1", "lh-1", "1", "1")

    def test_cancel_statement_success(self, livy_service, mock_fabric_client):
        """Cancel statement returns response."""
        response = _make_response(200, {"msg": "canceled"})
        mock_fabric_client.make_api_request.return_value = response

        result = livy_service.cancel_statement("ws-1", "lh-1", "1", "1")

        assert result["msg"] == "canceled"
        call_kwargs = mock_fabric_client.make_api_request.call_args.kwargs
        assert call_kwargs["payload"] == {}

    def test_cancel_statement_api_error(self, livy_service, mock_fabric_client):
        """API errors raise FabricLivyStatementError."""
        mock_fabric_client.make_api_request.side_effect = FabricAPIError(500, "boom")

        with pytest.raises(FabricLivyStatementError):
            livy_service.cancel_statement("ws-1", "lh-1", "1", "1")

    def test_wait_for_session_idle_returns(self, livy_service):
        """Idle session returns immediately."""
        livy_service.get_session_status = Mock(return_value={"state": "idle"})

        with patch("time.sleep", return_value=None):
            result = livy_service.wait_for_session("ws-1", "lh-1", "1", timeout_seconds=2)

        assert result["state"] == "idle"

    @pytest.mark.parametrize("state", ["error", "dead", "killed"])
    def test_wait_for_session_error_state_with_logs(self, livy_service, state):
        """Error states raise FabricLivySessionError with log details."""
        livy_service.get_session_status = Mock(return_value={"state": state, "log": ["a", "b"]})

        with patch("time.sleep", return_value=None):
            with pytest.raises(FabricLivySessionError):
                livy_service.wait_for_session("ws-1", "lh-1", "1", timeout_seconds=2)

    def test_wait_for_session_error_state_no_logs(self, livy_service):
        """Error session raises FabricLivySessionError when logs empty."""
        livy_service.get_session_status = Mock(return_value={"state": "error", "log": []})

        with patch("time.sleep", return_value=None):
            with pytest.raises(FabricLivySessionError):
                livy_service.wait_for_session("ws-1", "lh-1", "1", timeout_seconds=2)

    def test_wait_for_session_timeout(self, livy_service):
        """Timeout raises FabricLivyTimeoutError."""
        livy_service.get_session_status = Mock()

        with patch("time.time", side_effect=itertools.chain([0], itertools.repeat(10))):
            with pytest.raises(FabricLivyTimeoutError):
                livy_service.wait_for_session("ws-1", "lh-1", "1", timeout_seconds=1)

    def test_wait_for_statement_available_returns(self, livy_service):
        """Available statement returns immediately."""
        livy_service.get_statement_status = Mock(return_value={"state": "available"})

        with patch("time.sleep", return_value=None):
            result = livy_service.wait_for_statement("ws-1", "lh-1", "1", "1", timeout_seconds=2)

        assert result["state"] == "available"

    def test_wait_for_statement_error_state(self, livy_service):
        """Error statement raises FabricLivyStatementError."""
        livy_service.get_statement_status = Mock(
            return_value={"state": "error", "output": {"evalue": "boom"}}
        )

        with patch("time.sleep", return_value=None):
            with pytest.raises(FabricLivyStatementError):
                livy_service.wait_for_statement("ws-1", "lh-1", "1", "1", timeout_seconds=2)

    def test_wait_for_statement_cancelled_state(self, livy_service):
        """Cancelled statement raises FabricLivyStatementError."""
        livy_service.get_statement_status = Mock(
            return_value={"state": "cancelled", "output": {"evalue": "cancelled"}}
        )

        with patch("time.sleep", return_value=None):
            with pytest.raises(FabricLivyStatementError):
                livy_service.wait_for_statement("ws-1", "lh-1", "1", "1", timeout_seconds=2)

    def test_wait_for_statement_timeout(self, livy_service):
        """Timeout raises FabricLivyTimeoutError."""
        livy_service.get_statement_status = Mock()

        with patch("time.time", side_effect=itertools.chain([0], itertools.repeat(10))):
            with pytest.raises(FabricLivyTimeoutError):
                livy_service.wait_for_statement("ws-1", "lh-1", "1", "1", timeout_seconds=1)
