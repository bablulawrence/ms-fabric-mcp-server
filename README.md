# ms-fabric-mcp-server

[![PyPI version](https://badge.fury.io/py/ms-fabric-mcp-server.svg)](https://pypi.org/project/ms-fabric-mcp-server/)
[![Python](https://img.shields.io/pypi/pyversions/ms-fabric-mcp-server.svg)](https://pypi.org/project/ms-fabric-mcp-server/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Tests](https://github.com/bablulawrence/ms-fabric-mcp-server/actions/workflows/tests.yml/badge.svg)](https://github.com/bablulawrence/ms-fabric-mcp-server/actions/workflows/tests.yml)

A Model Context Protocol (MCP) server for Microsoft Fabric. Exposes Fabric operations (workspaces, notebooks, SQL, Livy, pipelines, jobs) as MCP tools that AI agents can invoke.

> ⚠️ **Warning**: This package is intended for **development environments only** and should not be used in production. It includes tools that can perform destructive operations (e.g., `delete_notebook`, `delete_item`) and execute arbitrary code via Livy Spark sessions. Always review AI-generated tool calls before execution.

## Quick Start

The fastest way to use this MCP server is with `uvx`:

```bash
uvx ms-fabric-mcp-server
```

## Installation

```bash
# Using uv (recommended)
uv pip install ms-fabric-mcp-server

# Using pip
pip install ms-fabric-mcp-server

# With SQL support (requires pyodbc)
pip install ms-fabric-mcp-server[sql]

# With OpenTelemetry tracing
pip install ms-fabric-mcp-server[sql,telemetry]
```

## Authentication

Uses **DefaultAzureCredential** from `azure-identity` - no explicit credential configuration needed. This automatically tries multiple authentication methods:

1. Environment credentials (`AZURE_CLIENT_ID`, `AZURE_TENANT_ID`, `AZURE_CLIENT_SECRET`)
2. Managed Identity (when running on Azure)
3. Azure CLI credentials (`az login`)
4. VS Code credentials
5. Azure PowerShell credentials

**No Fabric-specific auth environment variables are needed** - it just works if you're authenticated via any of the above methods.

## Usage

### VS Code Integration

Add to your VS Code MCP settings (`.vscode/mcp.json` or User settings):

```json
{
  "servers": {
    "MS Fabric MCP Server": {
      "type": "stdio",
      "command": "uvx",
      "args": ["ms-fabric-mcp-server"]
    }
  }
}
```

### Claude Desktop Integration

Add to your `claude_desktop_config.json`:

```json
{
  "mcpServers": {
    "fabric": {
      "command": "uvx",
      "args": ["ms-fabric-mcp-server"]
    }
  }
}
```

### Codex Integration

Add to your Codex `config.toml`:

```toml
[mcp_servers.ms_fabric_mcp]
command = "uvx"
args = ["ms-fabric-mcp-server"]
```

### Running Standalone

```bash
# Using uvx (no installation needed)
uvx ms-fabric-mcp-server

# Direct execution (if installed)
ms-fabric-mcp-server

# Via Python module
python -m ms_fabric_mcp_server

# With MCP Inspector (development)
npx @modelcontextprotocol/inspector uvx ms-fabric-mcp-server
```

### Logging & Debugging (optional)

MCP stdio servers must keep protocol traffic on stdout, so redirect **stderr** to capture logs.
Giving the agent read access to the log file is a powerful way to debug failures.
You can also set `AZURE_LOG_LEVEL` (Azure SDK) and `MCP_LOG_LEVEL` (server) to control verbosity.

VS Code (Bash):

```json
{
  "servers": {
    "MS Fabric MCP Server": {
      "type": "stdio",
      "command": "bash",
      "args": [
        "-lc",
        "LOG_DIR=\"$HOME/mcp_logs\"; LOG_FILE=\"$LOG_DIR/ms-fabric-mcp-$(date +%Y%m%d_%H%M%S).log\"; uvx ms-fabric-mcp-server 2> \"$LOG_FILE\""
      ],
      "env": {
        "AZURE_LOG_LEVEL": "info",
        "MCP_LOG_LEVEL": "INFO"
      }
    }
  }
}
```

VS Code (PowerShell):

```json
{
  "servers": {
    "MS Fabric MCP Server": {
      "type": "stdio",
      "command": "powershell",
      "args": [
        "-NoProfile",
        "-Command",
        "$logDir=\"$env:USERPROFILE\\mcp_logs\"; New-Item -ItemType Directory -Force -Path $logDir | Out-Null; $ts=Get-Date -Format yyyyMMdd_HHmmss; $logFile=\"$logDir\\ms-fabric-mcp-$ts.log\"; uvx ms-fabric-mcp-server 2> $logFile"
      ],
      "env": {
        "AZURE_LOG_LEVEL": "info",
        "MCP_LOG_LEVEL": "INFO"
      }
    }
  }
}
```

### Programmatic Usage (Library Mode)

```python
from fastmcp import FastMCP
from ms_fabric_mcp_server import register_fabric_tools

# Create your own server
mcp = FastMCP("my-custom-server")

# Register all Fabric tools
register_fabric_tools(mcp)

# Add your own customizations...

mcp.run()
```

## Configuration

Environment variables (all optional with sensible defaults):

| Variable | Default | Description |
|----------|---------|-------------|
| `FABRIC_BASE_URL` | `https://api.fabric.microsoft.com/v1` | Fabric API base URL |
| `FABRIC_SCOPES` | `https://api.fabric.microsoft.com/.default` | OAuth scopes |
| `FABRIC_API_CALL_TIMEOUT` | `30` | API timeout (seconds) |
| `FABRIC_MAX_RETRIES` | `3` | Max retry attempts |
| `FABRIC_RETRY_BACKOFF` | `2.0` | Backoff factor |
| `LIVY_API_CALL_TIMEOUT` | `120` | Livy timeout (seconds) |
| `LIVY_POLL_INTERVAL` | `2.0` | Livy polling interval |
| `LIVY_STATEMENT_WAIT_TIMEOUT` | `10` | Livy statement wait timeout |
| `LIVY_SESSION_WAIT_TIMEOUT` | `240` | Livy session wait timeout |
| `MCP_SERVER_NAME` | `ms-fabric-mcp-server` | Server name for MCP |
| `MCP_LOG_LEVEL` | `INFO` | Logging level |
| `AZURE_LOG_LEVEL` | `info` | Azure SDK logging level |

Copy `.env.example` to `.env` and customize as needed.

## Available Tools

The server provides **29 core tools**, with **3 additional SQL tools** when installed with `[sql]` extras (32 total).

| Tool Group | Count | Tools |
|------------|-------|-------|
| **Workspace** | 1 | `list_workspaces` |
| **Item** | 2 | `list_items`, `delete_item` |
| **Notebook** | 6 | `import_notebook_to_fabric`, `get_notebook_content`, `attach_lakehouse_to_notebook`, `get_notebook_execution_details`, `list_notebook_executions`, `get_notebook_driver_logs` |
| **Job** | 4 | `run_on_demand_job`, `get_job_status`, `get_job_status_by_url`, `get_operation_result` |
| **Livy** | 8 | `livy_create_session`, `livy_list_sessions`, `livy_get_session_status`, `livy_close_session`, `livy_run_statement`, `livy_get_statement_status`, `livy_cancel_statement`, `livy_get_session_log` |
| **Pipeline** | 5 | `create_blank_pipeline`, `add_copy_activity_to_pipeline`, `add_notebook_activity_to_pipeline`, `add_dataflow_activity_to_pipeline`, `add_activity_to_pipeline` |
| **Semantic Model** | 3 | `create_semantic_model`, `add_table_to_semantic_model`, `add_relationship_to_semantic_model` |
| **SQL** *(optional)* | 3 | `get_sql_endpoint`, `execute_sql_query`, `execute_sql_statement` |

### SQL Tools (Optional)

SQL tools require `pyodbc` and the Microsoft ODBC Driver for SQL Server:

```bash
# Install with SQL support
pip install ms-fabric-mcp-server[sql]

# On Ubuntu/Debian, install the ODBC driver first:
curl https://packages.microsoft.com/keys/microsoft.asc | sudo apt-key add -
curl https://packages.microsoft.com/config/ubuntu/$(lsb_release -rs)/prod.list | sudo tee /etc/apt/sources.list.d/mssql-release.list
sudo apt-get update
sudo ACCEPT_EULA=Y apt-get install -y msodbcsql18
```

If `pyodbc` is not available, the server starts with 29 tools (SQL tools disabled).

## Development

```bash
# Clone and install with dev dependencies
git clone https://github.com/your-org/ms-fabric-mcp-server.git
cd ms-fabric-mcp-server
pip install -e ".[dev,sql,telemetry]"

# Run tests
pytest

# Run with coverage
pytest --cov

# Format code
black src tests
isort src tests

# Type checking
mypy src
```

### Integration tests

Integration tests run against live Fabric resources and are opt-in.

To get started locally, copy the example env file:
```bash
cp .env.integration.example .env.integration
```

Required environment variables:
- `FABRIC_INTEGRATION_TESTS=1`
- `FABRIC_TEST_WORKSPACE_NAME`
- `FABRIC_TEST_LAKEHOUSE_NAME`
- `FABRIC_TEST_SQL_DATABASE`

Optional pipeline copy inputs:
- `FABRIC_TEST_SOURCE_CONNECTION_ID`
- `FABRIC_TEST_SOURCE_TYPE`
- `FABRIC_TEST_SOURCE_SCHEMA`
- `FABRIC_TEST_SOURCE_TABLE`
- `FABRIC_TEST_DEST_CONNECTION_ID`
- `FABRIC_TEST_DEST_TABLE_NAME` (optional override; defaults to source table name)

Run integration tests:

```bash
FABRIC_INTEGRATION_TESTS=1 pytest
```

Notes:
- SQL tests require `pyodbc` and a SQL Server ODBC driver.
- Tests may skip when optional dependencies or environment variables are missing.
- These tests use live Fabric resources and may incur costs or side effects.

## License

MIT
