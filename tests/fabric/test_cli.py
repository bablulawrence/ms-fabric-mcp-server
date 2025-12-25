"""Unit tests for CLI entry point."""

import runpy
from unittest.mock import Mock, patch

import pytest


@pytest.mark.unit
class TestCLI:
    """Test suite for CLI functions."""

    def test_parse_args_log_level(self):
        """Parse args returns provided log level."""
        from ms_fabric_mcp_server import cli

        parsed = cli.parse_args(["--log-level", "DEBUG"])

        assert parsed.log_level == "DEBUG"

    def test_main_runs_server(self):
        """Main creates server and runs it."""
        from ms_fabric_mcp_server import cli

        server = Mock()

        with patch("ms_fabric_mcp_server.cli.create_fabric_server", return_value=server) as create_server:
            cli.main(["--log-level", "WARNING"])

        create_server.assert_called_once_with(log_level="WARNING")
        server.run.assert_called_once_with()

    def test_main_handles_keyboard_interrupt(self):
        """KeyboardInterrupt exits cleanly."""
        from ms_fabric_mcp_server import cli

        server = Mock()
        server.run.side_effect = KeyboardInterrupt()

        with patch("ms_fabric_mcp_server.cli.create_fabric_server", return_value=server), \
            patch("ms_fabric_mcp_server.cli.sys.exit") as exit_mock:
            cli.main([])

        exit_mock.assert_called_once_with(0)

    def test_main_handles_exception(self):
        """Unhandled exceptions exit with error code."""
        from ms_fabric_mcp_server import cli

        server = Mock()
        server.run.side_effect = RuntimeError("boom")

        with patch("ms_fabric_mcp_server.cli.create_fabric_server", return_value=server), \
            patch("ms_fabric_mcp_server.cli.sys.exit") as exit_mock:
            cli.main([])

        exit_mock.assert_called_once_with(1)


@pytest.mark.unit
class TestModuleEntrypoint:
    """Test python -m entrypoint."""

    def test_module_calls_main(self, monkeypatch):
        """Module entrypoint invokes cli.main."""
        mock_main = Mock()
        monkeypatch.setattr("ms_fabric_mcp_server.cli.main", mock_main)

        runpy.run_module("ms_fabric_mcp_server.__main__", run_name="__main__")

        mock_main.assert_called_once_with()
