# ms-fabric-mcp-server

[![PyPI version](https://badge.fury.io/py/ms-fabric-mcp-server.svg)](https://pypi.org/project/ms-fabric-mcp-server/)
[![Python](https://img.shields.io/pypi/pyversions/ms-fabric-mcp-server.svg)](https://pypi.org/project/ms-fabric-mcp-server/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Tests](https://github.com/bablulawrence/ms-fabric-mcp-server/actions/workflows/tests.yml/badge.svg)](https://github.com/bablulawrence/ms-fabric-mcp-server/actions/workflows/tests.yml)

A Model Context Protocol (MCP) server for Microsoft Fabric. Exposes Fabric operations (workspaces, notebooks, SQL, Livy, pipelines, jobs) as MCP tools that AI agents can invoke.

> ⚠️ **Warning**: This package is intended for **development environments only** and should not be used in production. It includes tools that can perform destructive operations (e.g., `delete_item`, `delete_lakehouse_file`, `delete_activity_from_pipeline`) and execute arbitrary code via Livy Spark sessions. Always review AI-generated tool calls before execution.

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

The server provides **57 core tools**, with **3 additional SQL tools** when installed with `[sql]` extras (60 total).

| Tool Group | Count | Tools |
|------------|-------|-------|
| **Workspace** | 1 | `list_workspaces` |
| **Item** | 9 | `list_items`, `get_item`, `list_folders`, `create_folder`, `move_folder`, `delete_folder`, `delete_item`, `rename_item`, `move_item_to_folder` |
| **Lakehouse** | 4 | `create_lakehouse`, `list_lakehouse_files`, `upload_lakehouse_file`, `delete_lakehouse_file` |
| **Notebook** | 6 | `create_notebook`, `get_notebook_definition`, `update_notebook_definition`, `get_notebook_run_details`, `list_notebook_runs`, `get_notebook_driver_logs` |
| **Job** | 4 | `run_on_demand_job`, `get_job_status`, `get_job_status_by_url`, `get_operation_result` |
| **Livy** | 8 | `livy_create_session`, `livy_list_sessions`, `livy_get_session_status`, `livy_close_session`, `livy_run_statement`, `livy_get_statement_status`, `livy_cancel_statement`, `livy_get_session_log` |
| **Pipeline** | 11 | `create_pipeline`, `add_copy_activity_to_pipeline`, `add_notebook_activity_to_pipeline`, `add_dataflow_activity_to_pipeline`, `add_activity_to_pipeline`, `delete_activity_from_pipeline`, `remove_activity_dependency`, `add_activity_dependency`, `get_pipeline_definition`, `update_pipeline_definition`, `get_pipeline_activity_runs` |
| **Dataflow** | 3 | `create_dataflow`, `get_dataflow_definition`, `run_dataflow` |
| **Semantic Model** | 9 | `create_semantic_model`, `add_table_to_semantic_model`, `add_relationship_to_semantic_model`, `get_semantic_model_details`, `get_semantic_model_definition`, `add_measures_to_semantic_model`, `delete_measures_from_semantic_model`, `delete_table_from_semantic_model`, `delete_relationship_from_semantic_model` |
| **Power BI** | 2 | `refresh_semantic_model`, `execute_dax_query` |
| **SQL** *(optional)* | 3 | `get_sql_endpoint`, `execute_sql_query`, `execute_sql_statement` |

### SQL Tools (Optional)

SQL tools require `pyodbc` and the Microsoft ODBC Driver for SQL Server (Driver 18 or 17 — the service auto-detects which is installed and prefers Driver 18; set `FABRIC_ODBC_DRIVER` to override):

```bash
# Install with SQL support
pip install ms-fabric-mcp-server[sql]

# On Ubuntu/Debian, install the ODBC driver first:
curl https://packages.microsoft.com/keys/microsoft.asc | sudo apt-key add -
curl https://packages.microsoft.com/config/ubuntu/$(lsb_release -rs)/prod.list | sudo tee /etc/apt/sources.list.d/mssql-release.list
sudo apt-get update
sudo ACCEPT_EULA=Y apt-get install -y msodbcsql18  # or msodbcsql17
```

If `pyodbc` is not available, the server starts with 57 tools (SQL tools disabled).

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

Integration tests run against live Fabric resources and are opt-in. They require a pre-provisioned Fabric workspace, Lakehouse, Warehouse, and (for Copy Activity tests) at least one external-source connection. See **[Setup prerequisites](#setup-prerequisites-for-integration-tests)** below before your first run.

To get started locally, copy the example env file:
```bash
cp .env.integration.example .env.integration
```

Then fill in values matching your provisioned resources. The full set of variables (with inline comments grouping by purpose) lives in `.env.integration.example`; this list is for orientation:

**Required for all integration tests:**
- `FABRIC_INTEGRATION_TESTS=1`
- `FABRIC_TEST_WORKSPACE_NAME` — display name of the test workspace
- `FABRIC_TEST_LAKEHOUSE_NAME` — Lakehouse item in the workspace (used as the destination for Copy Activities)
- `FABRIC_TEST_LAKEHOUSE_SQL_DATABASE` — the Lakehouse's SQL endpoint database name (typically same as the Lakehouse name)
- `FABRIC_TEST_WAREHOUSE_NAME` — Warehouse item in the workspace (used by SQL DML tests)

**Required for Copy Activity tests (Pipeline Flow):**
- `FABRIC_TEST_DEST_CONNECTION_ID` — Fabric connection ID for the destination Lakehouse (used by all Copy Activity tests)

**Per-engine source inputs** — set all 5 vars of a block to enable that block's Copy Activity test; the test skips with a logged reason if any value is missing.

PostgreSQL source (e.g., VM-hosted Postgres reachable via a gateway):
- `FABRIC_TEST_POSTGRES_CONNECTION_ID`
- `FABRIC_TEST_POSTGRES_SOURCE_TYPE` (default `PostgreSqlSource`)
- `FABRIC_TEST_POSTGRES_SCHEMA`
- `FABRIC_TEST_POSTGRES_TABLE`
- `FABRIC_TEST_POSTGRES_DEST_TABLE_NAME`

SQL Server source (e.g., VM-hosted SQL Server, Azure SQL DB):
- `FABRIC_TEST_SQLSERVER_CONNECTION_ID`
- `FABRIC_TEST_SQLSERVER_SOURCE_TYPE` (default `SqlServerSource`)
- `FABRIC_TEST_SQLSERVER_SCHEMA`
- `FABRIC_TEST_SQLSERVER_TABLE`
- `FABRIC_TEST_SQLSERVER_DEST_TABLE_NAME`

**Optional inputs** (other flows skip cleanly when absent):
- Semantic Model: `FABRIC_TEST_SEMANTIC_MODEL_TABLE`, `FABRIC_TEST_SEMANTIC_MODEL_COLUMNS`, `FABRIC_TEST_SEMANTIC_MODEL_TABLE_2`, `FABRIC_TEST_SEMANTIC_MODEL_COLUMNS_2`, `FABRIC_TEST_SEMANTIC_MODEL_SCHEMA`
- Dataflow: `FABRIC_TEST_DATAFLOW_NAME`
- Azure SPN auth (when not using `az login`): `AZURE_TENANT_ID`, `AZURE_CLIENT_ID`, `AZURE_CLIENT_SECRET`
- Power BI tuning: `POWERBI_BASE_URL`, `POWERBI_SCOPES`, `POWERBI_API_CALL_TIMEOUT`, `POWERBI_REFRESH_POLL_INTERVAL`, `POWERBI_REFRESH_WAIT_TIMEOUT`

Run integration tests:

```bash
FABRIC_INTEGRATION_TESTS=1 pytest -m integration
```

Or filter to a specific test:
```bash
FABRIC_INTEGRATION_TESTS=1 pytest -m integration -k "copy_activity"
```

Notes:
- SQL tests require `pyodbc` and a SQL Server ODBC driver (Microsoft `msodbcsql18` recommended). The CI workflow installs it via `apt-get install msodbcsql18`.
- Tests may skip when optional dependencies or environment variables are missing — this is intentional, not a failure.
- These tests use live Fabric resources and may incur capacity-usage and storage costs. Run against a non-production workspace.

#### Setup prerequisites for integration tests

The tests assume the following Fabric / Azure infrastructure is already in place. This setup is one-time per environment and is not part of the test run itself:

1. **A dedicated Fabric workspace** on an active capacity. Do **not** use a production workspace — the tests create, modify, and delete items.

2. **A Lakehouse and a Warehouse** in that workspace, with display names matching `FABRIC_TEST_LAKEHOUSE_NAME` and `FABRIC_TEST_WAREHOUSE_NAME`. The Lakehouse's SQL endpoint database name (which usually matches the Lakehouse name) goes in `FABRIC_TEST_LAKEHOUSE_SQL_DATABASE`.

3. **Source databases reachable from Fabric** (for Copy Activity tests). Options:
   - **Azure-managed services** (Azure SQL DB, Azure Database for PostgreSQL): create directly; Fabric reaches them over the public Azure backbone.
   - **VM-hosted or on-premises databases**: require an [on-premises data gateway](https://learn.microsoft.com/data-integration/gateway/service-gateway-onprem) installed on a Windows host that can reach both the source database (on the source network) and Fabric (over the public internet).

4. **Fabric connections** to those source databases (one per source-engine you want to test). Create via Fabric portal → Settings → *Manage connections and gateways* → New connection. Use the [Fabric REST API](https://learn.microsoft.com/rest/api/fabric/core/connections/list-connections) to read back the GUID for `FABRIC_TEST_*_CONNECTION_ID`:
   ```bash
   TOKEN=$(az account get-access-token --resource https://api.fabric.microsoft.com --query accessToken -o tsv)
   curl -sS -H "Authorization: Bearer $TOKEN" https://api.fabric.microsoft.com/v1/connections \
     | python3 -c "import json,sys
   for c in json.load(sys.stdin).get('value',[]):
       print(f\"{c['displayName']:40s} {c['id']}\")"
   ```

5. **A Lakehouse-destination Fabric connection** (Cloud > Lakehouse). Used for `FABRIC_TEST_DEST_CONNECTION_ID`. Pipeline Copy Activities target this connection when writing to the destination Lakehouse.

6. **Auth**:
   - **Local development**: `az login` as a Fabric workspace member is sufficient (the server uses `DefaultAzureCredential`).
   - **CI / unattended**: create an Azure service principal, add it to the test workspace as **Member** (Fabric portal → workspace → Manage access), grant it **"Can use"** on each Fabric connection (Settings → Manage connections → select connection → Share → add SP), and provide its credentials via `AZURE_TENANT_ID` / `AZURE_CLIENT_ID` / `AZURE_CLIENT_SECRET`. Both ACL grants (workspace member + per-connection "Can use") are required — connection access does not inherit from workspace membership.

7. **GitHub Actions** (if running the bundled workflows): create an environment named `Integration` in your fork's repository settings and add every `FABRIC_TEST_*` and `AZURE_*` variable above as an environment secret with the same name. `.github/workflows/integration-tests.yml` lists the canonical secret-name set.

## License

MIT
