# MCP Tool Testing (FastMCP CLI + Python Harness)

This document captures the local workflow used to validate MCP tool registration
and execute tool calls during development.

## Prereqs
- Set environment variables in `.env.integration` (Fabric auth + workspace inputs).
- Ensure local imports resolve to the working tree (use `PYTHONPATH=./src`).
- Run commands from the repo root (`.worktrees/semantic-model-tools`).

### Required env vars (minimum)
These are required for integration tests and live tool calls:
- `FABRIC_TEST_WORKSPACE_NAME`
- `FABRIC_TEST_LAKEHOUSE_NAME`
- `FABRIC_TEST_SQL_DATABASE`

### Required env vars (semantic model integration)
If you run semantic model tests or add tables programmatically:
- `FABRIC_TEST_SEMANTIC_MODEL_TABLE`
- `FABRIC_TEST_SEMANTIC_MODEL_COLUMNS` (JSON list, e.g. `[{"name":"Id","data_type":"int64"}]`)
- Optional second table for relationship tests:
  - `FABRIC_TEST_SEMANTIC_MODEL_TABLE_2`
  - `FABRIC_TEST_SEMANTIC_MODEL_COLUMNS_2`

### Auth requirements (avoid failures)
DefaultAzureCredential needs *one* valid auth path:
- Service principal (recommended for CI):  
  `AZURE_TENANT_ID`, `AZURE_CLIENT_ID`, `AZURE_CLIENT_SECRET`
- Or Azure CLI auth (`az login`) **with write access** to `~/.azure`  
  (permission errors on `~/.azure/az.sess` will break tests).

### SQL tool dependencies (if running SQL tests)
SQL tools require `pyodbc` + ODBC driver. See README for install steps.

## 1) Inspect tool registration with FastMCP CLI
`fastmcp inspect` is useful for confirming the server exposes the expected tools.
Because the server module uses relative imports, load it via a tiny shim module.

```bash
set -a
. ./.env.integration
set +a

TMP_FILE=$(mktemp /tmp/fabric_mcp_server_XXXX.py)
cat > "$TMP_FILE" <<'PY'
from ms_fabric_mcp_server.server import create_fabric_server

mcp = create_fabric_server()
PY

PYTHONPATH=./src fastmcp inspect "$TMP_FILE":mcp --format fastmcp | rg "semantic_model"
rm -f "$TMP_FILE"
```

Notes:
- `fastmcp inspect` is for *inspection*, not invocation.
- `PYTHONPATH=./src` ensures the local package is imported instead of any installed version.
 - If inspect fails with relative import errors, use the shim approach above.

## 2) Invoke tools via a Python harness
FastMCP doesn’t provide a CLI runner for tools; use a short async harness:

```bash
python - <<'PY'
import asyncio
import json
import os
import sys
from dotenv import load_dotenv
from fastmcp import FastMCP

sys.path.insert(0, os.path.abspath('src'))
from ms_fabric_mcp_server import register_fabric_tools

load_dotenv('.env.integration')

async def main():
    mcp = FastMCP("tool-runner")
    register_fabric_tools(mcp)
    tools = await mcp.get_tools()

    async def call_tool(name, **kwargs):
        tool = tools.get(name)
        if tool is None:
            raise RuntimeError(f"Tool not registered: {name}")
        result = await tool.run(kwargs)
        return result.structured_content

    # Example: list tools
    workspaces = await call_tool("list_workspaces")
    print(workspaces)

asyncio.run(main())
PY
```

Tips:
- Keep this harness local (don’t commit) or store it as a short snippet in your notes.
- Always validate with `get_*` tools after making semantic model changes.

## Common failure modes
- Auth failure: missing/invalid Azure credentials or Azure CLI session write errors.
- SQL tool failures: missing `pyodbc` or ODBC driver (SQL tools won’t register).
- JSON column specs invalid: `FABRIC_TEST_SEMANTIC_MODEL_COLUMNS*` must be valid JSON lists.
