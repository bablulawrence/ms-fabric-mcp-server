# ms-fabric-mcp-server: Design and Implementation Plan

## Overview

**Package Name:** `ms-fabric-mcp-server`  
**Python Import Namespace:** `ms_fabric_mcp_server`  
**Version:** 0.6.0  
**License:** MIT

A standalone PyPI package that provides a Model Context Protocol (MCP) server for Microsoft Fabric. It exposes Fabric operations (workspaces, notebooks, SQL, Livy, pipelines, jobs) as MCP tools that AI agents can invoke.

---

## Package Structure

```
ms-fabric-mcp-server/
├── src/ms_fabric_mcp_server/
│   ├── __init__.py           # Package exports
│   ├── __main__.py           # python -m entry point
│   ├── cli.py                # CLI entry point
│   ├── server.py             # Main MCP server setup
│   ├── client/               # Fabric HTTP client
│   │   ├── __init__.py
│   │   ├── config.py         # FabricConfig
│   │   ├── http_client.py    # FabricClient
│   │   └── exceptions.py     # Custom exceptions
│   ├── models/               # Pydantic models
│   │   ├── __init__.py
│   │   ├── workspace.py
│   │   ├── item.py
│   │   ├── job.py
│   │   ├── lakehouse.py
│   │   └── results.py
│   ├── services/             # Business logic layer
│   │   ├── __init__.py
│   │   ├── workspace.py
│   │   ├── item.py
│   │   ├── notebook.py
│   │   ├── job.py
│   │   ├── sql.py
│   │   ├── livy.py
│   │   └── pipeline.py
│   └── tools/                # MCP tool registrations
│       ├── __init__.py
│       ├── base.py
│       ├── workspace_tools.py
│       ├── item_tools.py
│       ├── notebook_tools.py
│       ├── job_tools.py
│       ├── sql_tools.py
│       ├── livy_tools.py
│       └── pipeline_tools.py
├── tests/
│   ├── __init__.py
│   ├── conftest.py
│   └── ... (unit tests with mocks)
├── pyproject.toml
├── README.md
├── .env.example
└── .gitignore
```

---

## Entry Points

### CLI Command
Via `pyproject.toml` console scripts:
```bash
$ ms-fabric-mcp-server
$ ms-fabric-mcp-server --help
$ ms-fabric-mcp-server --version
$ ms-fabric-mcp-server --log-level DEBUG
```

**CLI Arguments:**
| Argument | Description |
|----------|-------------|
| `--help` | Show help message and exit |
| `--version` | Show version and exit |
| `--log-level` | Override `MCP_LOG_LEVEL` env var (DEBUG, INFO, WARNING, ERROR) |

### Python Module
```bash
$ python -m ms_fabric_mcp_server
$ uv run mcp dev src/ms_fabric_mcp_server   # For MCP Inspector testing
```

---

## Authentication

Uses **`DefaultAzureCredential`** from `azure-identity` - no explicit credential configuration needed. This automatically tries multiple authentication methods in order:

1. Environment credentials (`AZURE_CLIENT_ID`, `AZURE_TENANT_ID`, `AZURE_CLIENT_SECRET`)
2. Managed Identity (when running on Azure)
3. Azure CLI credentials (`az login`)
4. VS Code credentials
5. Azure PowerShell credentials

**No Fabric-specific auth environment variables are needed** - it just works if the user is authenticated via any of the above methods.

---

## Configuration

Environment variables + `.env` file support. Only operational settings (no auth):

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

---

## Dependencies

### Core (required)
```toml
dependencies = [
    "fastmcp>=2.10.0",               # FastMCP server framework
    "pydantic>=2.0.0",               # Data validation
    "python-dotenv>=1.0.0",          # .env file support
    "azure-identity>=1.23.0",        # Azure authentication
    "requests>=2.25.0",              # HTTP client for Fabric API
]
```

### Optional
```toml
[project.optional-dependencies]
sql = [
    "pyodbc>=5.0.0",                           # SQL connectivity
]
telemetry = [
    "opentelemetry-api>=1.20.0",               # OpenTelemetry tracing
    "opentelemetry-instrumentation-dbapi>=0.46b0",  # DB auto-instrumentation
]
dev = [
    "pytest>=7.0.0",
    "pytest-asyncio>=0.21.0",
    "pytest-mock>=3.10.0",
    "pytest-cov>=4.0.0",
    "responses>=0.23.0",             # Mock HTTP requests
    "black>=23.0.0",
    "isort>=5.13.0",
    "bandit[toml]>=1.7.5",
]
```

---

## Tools (28 total)

| Tool Group | Tools | Count |
|------------|-------|-------|
| **Workspace** | `list_workspaces`, `create_workspace` | 2 |
| **Item** | `list_items`, `delete_item` | 2 |
| **Notebook** | `import_notebook_to_fabric`, `get_notebook_content`, `attach_lakehouse_to_notebook`, `get_notebook_execution_details`, `list_notebook_executions`, `get_notebook_driver_logs` | 6 |
| **Job** | `run_on_demand_job`, `get_job_status`, `get_job_status_by_url`, `get_operation_result` | 4 |
| **SQL** | `get_sql_endpoint`, `execute_sql_query`, `execute_sql_statement` | 3 |
| **Livy** | Session and statement management for Spark | 8 |
| **Pipeline** | `create_blank_pipeline`, `add_copy_activity_to_pipeline`, `add_activity_to_pipeline` | 3 |

### Excluded Tools
- ~~`delete_workspace`~~ - Dangerous operation
- ~~`create_lakehouse`~~ - Convenience wrapper
- ~~`create_pipeline_with_copy_activity`~~ - Convenience wrapper

### Optional Tools (require `[sql]` extra)
- SQL tools (`get_sql_endpoint`, `execute_sql_query`, `execute_sql_statement`) require `pyodbc`
- If `pyodbc` is not available when `[sql]` extra is installed, the server will fail at startup with a clear error message

### Tool Design
- Tools take **workspace by name** (e.g., `workspace_name: str`), not ID
- Services internally resolve names to IDs via `workspace_service.resolve_workspace_id()`
- This makes tools more user-friendly for AI agents
- **Exception:** Livy tools require `workspace_id` and `lakehouse_id` directly (users must look up IDs first)

---

## Usage

### Installation
```bash
# Basic installation
pip install ms-fabric-mcp-server

# With SQL support
pip install ms-fabric-mcp-server[sql]

# With SQL and telemetry
pip install ms-fabric-mcp-server[sql,telemetry]

# Using uv
uv pip install ms-fabric-mcp-server
```

### Running the Server
```bash
# Direct execution
ms-fabric-mcp-server

# Via Python module
python -m ms_fabric_mcp_server

# With MCP Inspector (development)
uv run mcp dev ms-fabric-mcp-server
```

### Claude Desktop Integration
Add to `claude_desktop_config.json`:
```json
{
  "mcpServers": {
    "fabric": {
      "command": "ms-fabric-mcp-server"
    }
  }
}
```

Or with uv:
```json
{
  "mcpServers": {
    "fabric": {
      "command": "uv",
      "args": ["run", "ms-fabric-mcp-server"]
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

---

## Implementation Plan

### Phase 1: Project Scaffolding
1. Create package directory structure at workspace root (not inside `asa-lib`)
2. Create `pyproject.toml` with dependencies and entry points
3. Create `README.md` with installation and usage instructions
4. Create `.env.example` template
5. Create `.gitignore`

### Phase 2: Copy & Refactor Core Modules
Copy from `asa-lib/src/asa_lib/fabric/` and refactor imports from `asa_lib.fabric.*` → `ms_fabric_mcp_server.*`:

1. **client/** - `config.py`, `http_client.py`, `exceptions.py`
2. **models/** - All Pydantic models (workspace, item, job, lakehouse, results)
3. **services/** - All service classes (workspace, item, notebook, job, sql, livy, pipeline)
4. **tools/** - Tool registration modules with modifications:
   - Remove `delete_workspace` from workspace_tools.py
   - Remove `create_lakehouse` from lakehouse_tools.py (or remove file entirely)
   - Remove `create_pipeline_with_copy_activity` from pipeline_tools.py

### Phase 3: Entry Points
1. Create `server.py` - Main MCP server setup with `register_fabric_tools()`
2. Create `cli.py` - CLI entry point with argument parsing
3. Create `__main__.py` - Module entry point (`python -m`)
4. Create `__init__.py` - Package exports

### Phase 4: Testing
1. Copy relevant unit tests from `asa-lib/tests/fabric/`
2. Refactor test imports to use `ms_fabric_mcp_server`
3. Remove tests for excluded tools
4. Write new tests for missing coverage (notebook, job, sql, livy, item services and tools)
5. Ensure all tests pass with mocks

### Phase 5: Documentation
1. Finalize `README.md` with:
   - Installation instructions
   - Authentication setup (DefaultAzureCredential)
   - Configuration options
   - Claude Desktop integration
   - Tool reference (28 tools)
   - Examples

---

## Source Files to Copy

### From `asa-lib/src/asa_lib/fabric/`

| Source | Destination | Notes |
|--------|-------------|-------|
| `client/__init__.py` | `client/__init__.py` | Refactor imports |
| `client/config.py` | `client/config.py` | Refactor imports |
| `client/http_client.py` | `client/http_client.py` | Refactor imports |
| `client/exceptions.py` | `client/exceptions.py` | Refactor imports |
| `models/__init__.py` | `models/__init__.py` | Refactor imports |
| `models/workspace.py` | `models/workspace.py` | Refactor imports |
| `models/item.py` | `models/item.py` | Refactor imports |
| `models/job.py` | `models/job.py` | Refactor imports |
| `models/lakehouse.py` | `models/lakehouse.py` | Refactor imports |
| `models/results.py` | `models/results.py` | Refactor imports |
| `services/__init__.py` | `services/__init__.py` | Refactor imports |
| `services/workspace.py` | `services/workspace.py` | Refactor imports |
| `services/item.py` | `services/item.py` | Refactor imports |
| `services/notebook.py` | `services/notebook.py` | Refactor imports, remove `repo_root` parameter |
| `services/job.py` | `services/job.py` | Refactor imports |
| `services/sql.py` | `services/sql.py` | Refactor imports |
| `services/livy.py` | `services/livy.py` | Refactor imports |
| `services/pipeline.py` | `services/pipeline.py` | Refactor imports |
| `tools/__init__.py` | `tools/__init__.py` | Refactor imports, remove excluded tools |
| `tools/base.py` | `tools/base.py` | Refactor imports |
| `tools/workspace_tools.py` | `tools/workspace_tools.py` | Remove `delete_workspace` |
| `tools/item_tools.py` | `tools/item_tools.py` | Refactor imports |
| `tools/notebook_tools.py` | `tools/notebook_tools.py` | Refactor imports |
| `tools/job_tools.py` | `tools/job_tools.py` | Refactor imports |
| `tools/sql_tools.py` | `tools/sql_tools.py` | Refactor imports |
| `tools/livy_tools.py` | `tools/livy_tools.py` | Refactor imports |
| `tools/pipeline_tools.py` | `tools/pipeline_tools.py` | Remove `create_pipeline_with_copy_activity` wrapper |
| `tools/lakehouse_tools.py` | *(exclude)* | Do not copy - no lakehouse tools in this package |

---

## New Files to Create

| File | Purpose |
|------|---------|
| `src/ms_fabric_mcp_server/__init__.py` | Package exports |
| `src/ms_fabric_mcp_server/__main__.py` | `python -m` entry point |
| `src/ms_fabric_mcp_server/cli.py` | CLI entry point |
| `src/ms_fabric_mcp_server/server.py` | MCP server setup |
| `pyproject.toml` | Package metadata, dependencies, entry points |
| `README.md` | Documentation |
| `.env.example` | Configuration template |
| `.gitignore` | Git ignore patterns |
| `tests/conftest.py` | Pytest fixtures |
