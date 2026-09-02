"""Packaged-wheel integration tests for the stdio MCP protocol."""

import asyncio
import json
import os
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Any

import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[3]


def _run_command(command: list[str], cwd: Path) -> None:
    result = subprocess.run(
        command,
        cwd=cwd,
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, (
        f"Command failed: {' '.join(command)}\n"
        f"stdout:\n{result.stdout}\n"
        f"stderr:\n{result.stderr}"
    )


async def _send_message(
    process: asyncio.subprocess.Process, message: dict[str, Any]
) -> None:
    assert process.stdin is not None
    payload = json.dumps(message, separators=(",", ":")) + "\n"
    process.stdin.write(payload.encode())
    await process.stdin.drain()


async def _request(
    process: asyncio.subprocess.Process,
    message: dict[str, Any],
    timeout_seconds: int = 30,
) -> dict[str, Any]:
    await _send_message(process, message)
    assert process.stdout is not None
    line = await asyncio.wait_for(
        process.stdout.readline(),
        timeout=timeout_seconds,
    )
    assert line, "Packaged server closed stdout before responding"
    return json.loads(line)


async def _stop_process(process: asyncio.subprocess.Process) -> None:
    if process.stdin is not None:
        process.stdin.close()
        await process.stdin.wait_closed()

    try:
        await asyncio.wait_for(process.wait(), timeout=5)
    except asyncio.TimeoutError:
        process.terminate()
        try:
            await asyncio.wait_for(process.wait(), timeout=5)
        except asyncio.TimeoutError:
            process.kill()
            await process.wait()


@pytest.mark.slow
@pytest.mark.asyncio
async def test_packaged_wheel_supports_copilot_stdio_sequence(
    tmp_path: Path,
) -> None:
    """Run the Copilot discovery and MCP handshake against an installed wheel."""
    uv = shutil.which("uv")
    assert uv is not None, "uv is required for packaged-wheel integration tests"

    venv_dir = tmp_path / "venv"
    packaged_wheel = os.getenv("FABRIC_PACKAGED_WHEEL")
    if packaged_wheel:
        wheel = Path(packaged_wheel).resolve()
        assert wheel.is_file(), f"Packaged wheel not found: {wheel}"
    else:
        dist_dir = tmp_path / "dist"
        _run_command(
            [uv, "build", "--wheel", "--out-dir", str(dist_dir)],
            cwd=PROJECT_ROOT,
        )
        wheel = next(dist_dir.glob("ms_fabric_mcp_server-*.whl"))

    _run_command(
        [uv, "venv", "--python", sys.executable, str(venv_dir)],
        cwd=tmp_path,
    )

    scripts_dir = "Scripts" if os.name == "nt" else "bin"
    executable_suffix = ".exe" if os.name == "nt" else ""
    venv_python = venv_dir / scripts_dir / f"python{executable_suffix}"
    server_executable = (
        venv_dir / scripts_dir / f"ms-fabric-mcp-server{executable_suffix}"
    )
    _run_command(
        [uv, "pip", "install", "--python", str(venv_python), str(wheel)],
        cwd=tmp_path,
    )

    environment = os.environ.copy()
    environment.pop("PYTHONPATH", None)
    environment["PYTHONUNBUFFERED"] = "1"

    process = await asyncio.create_subprocess_exec(
        str(server_executable),
        cwd=tmp_path,
        env=environment,
        stdin=asyncio.subprocess.PIPE,
        stdout=asyncio.subprocess.PIPE,
    )

    try:
        discover_response = await _request(
            process,
            {
                "jsonrpc": "2.0",
                "id": 1,
                "method": "server/discover",
                "params": {},
            },
        )
        assert discover_response["id"] == 1
        if "error" in discover_response:
            assert discover_response["error"]["code"] in {-32601, -32602}

        initialize_response = await _request(
            process,
            {
                "jsonrpc": "2.0",
                "id": 2,
                "method": "initialize",
                "params": {
                    "protocolVersion": "2025-11-25",
                    "capabilities": {},
                    "clientInfo": {
                        "name": "packaged-wheel-integration-test",
                        "version": "1.0.0",
                    },
                },
            },
        )
        initialize_result = initialize_response["result"]
        assert initialize_result["protocolVersion"] == "2025-11-25"
        assert initialize_result["serverInfo"]["name"] == "ms-fabric-mcp-server"
        assert "tools" in initialize_result["capabilities"]

        await _send_message(
            process,
            {
                "jsonrpc": "2.0",
                "method": "notifications/initialized",
                "params": {},
            },
        )

        tools_response = await _request(
            process,
            {
                "jsonrpc": "2.0",
                "id": 3,
                "method": "tools/list",
                "params": {},
            },
        )
        tool_names = {tool["name"] for tool in tools_response["result"]["tools"]}
        assert "list_workspaces" in tool_names

        call_response = await _request(
            process,
            {
                "jsonrpc": "2.0",
                "id": 4,
                "method": "tools/call",
                "params": {
                    "name": "list_workspaces",
                    "arguments": {},
                },
            },
            timeout_seconds=90,
        )
        structured = call_response["result"]["structuredContent"]
        assert structured["status"] in {"success", "error"}

        if os.getenv("FABRIC_PACKAGED_PROTOCOL_REQUIRE_LIVE") == "1":
            workspace_name = os.getenv("FABRIC_TEST_WORKSPACE_NAME")
            assert workspace_name, (
                "FABRIC_TEST_WORKSPACE_NAME is required when live packaged "
                "protocol validation is enabled"
            )
            assert structured["status"] == "success"
            workspace_names = {
                workspace["display_name"] for workspace in structured["workspaces"]
            }
            assert workspace_name in workspace_names
    finally:
        await _stop_process(process)
