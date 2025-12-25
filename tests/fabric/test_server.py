"""Unit tests for server factory."""

from unittest.mock import Mock, patch

import pytest


@pytest.mark.unit
class TestServerFactory:
    """Test suite for create_fabric_server."""

    def test_create_fabric_server_defaults(self):
        """Factory uses env defaults when no overrides provided."""
        from ms_fabric_mcp_server import server

        mcp = Mock()

        def getenv(key, default=None):
            values = {
                "MCP_LOG_LEVEL": "WARNING",
                "MCP_SERVER_NAME": "TestServer",
            }
            return values.get(key, default)

        with patch("ms_fabric_mcp_server.server.load_dotenv") as load_dotenv, \
            patch("ms_fabric_mcp_server.server.os.getenv", side_effect=getenv) as getenv_mock, \
            patch("ms_fabric_mcp_server.server.logging.basicConfig") as basic_config, \
            patch("ms_fabric_mcp_server.server.FastMCP", return_value=mcp) as fastmcp, \
            patch("ms_fabric_mcp_server.server.register_fabric_tools") as register_tools:
            result = server.create_fabric_server()

        load_dotenv.assert_called_once_with()
        getenv_mock.assert_any_call("MCP_LOG_LEVEL", "INFO")
        getenv_mock.assert_any_call("MCP_SERVER_NAME", "ms-fabric-mcp-server")
        basic_config.assert_called_once()
        fastmcp.assert_called_once_with("TestServer")
        register_tools.assert_called_once_with(mcp)
        assert result is mcp

    def test_create_fabric_server_overrides(self):
        """Factory honors explicit name and log level."""
        from ms_fabric_mcp_server import server

        mcp = Mock()

        with patch("ms_fabric_mcp_server.server.load_dotenv"), \
            patch("ms_fabric_mcp_server.server.os.getenv") as getenv_mock, \
            patch("ms_fabric_mcp_server.server.logging.basicConfig") as basic_config, \
            patch("ms_fabric_mcp_server.server.FastMCP", return_value=mcp) as fastmcp, \
            patch("ms_fabric_mcp_server.server.register_fabric_tools") as register_tools:
            result = server.create_fabric_server(name="Custom", log_level="DEBUG")

        getenv_mock.assert_not_called()
        basic_config.assert_called_once()
        fastmcp.assert_called_once_with("Custom")
        register_tools.assert_called_once_with(mcp)
        assert result is mcp
