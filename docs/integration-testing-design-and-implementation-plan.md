# Integration Testing Design and Implementation Plan

## Design

### Section 1: Goals, Scope, and Constraints
We are designing a live integration test suite for ms-fabric-mcp-server that exercises each MCP tool end-to-end against real Fabric resources. The tests are opt-in: they run only when the `integration` marker is selected and `FABRIC_INTEGRATION_TESTS=1` is set. We keep integration tests under `tests/` with `@pytest.mark.integration` (no separate test suite). Tests invoke tool functions directly via a FastMCP instance rather than running a stdio server process, which keeps coverage of the tool/service stack while minimizing flakiness.

Key resources are pre-provisioned and supplied via env vars: the target workspace (`FABRIC_TEST_WORKSPACE_NAME`), lakehouse (`FABRIC_TEST_LAKEHOUSE_NAME`), and optional pipeline copy inputs (`FABRIC_TEST_SOURCE_CONNECTION_ID`, `FABRIC_TEST_SOURCE_TYPE`, `FABRIC_TEST_SOURCE_SCHEMA`, `FABRIC_TEST_SOURCE_TABLE`, `FABRIC_TEST_DEST_CONNECTION_ID`). The destination table name defaults to the source table name, with an optional override via `FABRIC_TEST_DEST_TABLE_NAME`. Tests create and delete their own items (notebooks/pipelines) inside that workspace using MCP tools where possible, falling back to services only when a tool does not exist. There is no temp workspace. The `create_workspace` tool has been removed and is not part of the integration test coverage.

SQL tools use the lakehouse SQL endpoint. `execute_sql_query` runs a harmless query; `execute_sql_statement` is called with a non‑DML statement and should return an error response (per preference to avoid DML). SQL tests are included only if optional dependencies (pyodbc + driver) are present.

For `get_operation_result`, tests attempt to capture a real `x-ms-operation-id` from a known async API call. That header is only returned when Fabric responds with `202 Accepted` for an asynchronous operation. Many calls complete synchronously (200/201) and do not include the header at all. In that case, there is no valid operation ID to use, so the test is skipped rather than failing. This avoids a false failure caused by the API choosing a synchronous completion path, which is expected behavior.

### Section 2: Test Harness Structure and Tool Invocation
The integration harness creates a real FastMCP server in tests, registers tools with `register_fabric_tools`, and retrieves tool objects via `await mcp.get_tools()`. Each tool is a `FunctionTool` and is invoked using `tool.run(arguments)`; a helper unwraps the returned `ToolResult.structured_content` for assertions. This keeps tests focused on the tool contract rather than MCP transport concerns.

Tests assert only stable fields (e.g., `status == "success"`, presence of IDs) to reduce brittleness. The harness is intentionally minimal: it uses the real Fabric client and auth flow, and provides helpers for env var retrieval, unique name generation, and common tool calls.

### Section 3: Resource Lifecycle, SQL, and Operation ID Strategy
All test-created items live in the pre‑provisioned workspace. Notebooks and pipelines created by tests are deleted at the end using `delete_item`. We do not delete the workspace or lakehouse.

Livy tests resolve the lakehouse ID by listing items in the workspace and matching the configured lakehouse name; missing lakehouse results in a skip. Livy flow covers session creation, listing, status, running a simple statement, statement status, fetching logs, and session closure.

SQL tools use the lakehouse SQL endpoint and a database name from `FABRIC_TEST_SQL_DATABASE`. `execute_sql_query` runs `SELECT 1 AS value` and asserts success. `execute_sql_statement` runs a non‑DML statement (e.g., `SELECT 1`) and asserts an error. SQL tests skip if pyodbc or drivers are unavailable.

For `get_operation_result`, a helper performs a known async call and inspects response headers for `x-ms-operation-id`. If the header is present, it is passed to the tool. If the header is missing (because the API completed synchronously), the test skips to avoid false failures.

### Section 4: Configuration, Skip Logic, and Documentation
Configuration is driven by environment variables. Core variables:
- `FABRIC_INTEGRATION_TESTS=1`
- `FABRIC_TEST_WORKSPACE_NAME`
- `FABRIC_TEST_LAKEHOUSE_NAME`
- `FABRIC_TEST_SQL_DATABASE`

Optional pipeline copy variables:
- `FABRIC_TEST_SOURCE_CONNECTION_ID`
- `FABRIC_TEST_SOURCE_TYPE`
- `FABRIC_TEST_SOURCE_SCHEMA`
- `FABRIC_TEST_SOURCE_TABLE`
- `FABRIC_TEST_DEST_CONNECTION_ID`
- `FABRIC_TEST_DEST_TABLE_NAME` (optional override; defaults to source table name)

Tests skip per‑feature when required env vars or dependencies are missing, rather than failing the entire suite. README documentation will explain prerequisites, env vars, and how to run the suite safely (`FABRIC_INTEGRATION_TESTS=1 pytest`).

### Section 5: Implementation Plan Preview
1) Add integration scaffolding and env gating in `tests/conftest.py` plus helper functions/classes for gating, env var retrieval, tool invocation, and name generation.
2) Add `tests/fixtures/minimal_notebook.ipynb` as a deterministic notebook fixture.
3) Add integration tests per tool group under `tests/fabric/integration/`.
4) Update `README.md` with integration test instructions, env vars, and skip behavior.

### Section 6: Documentation and Test Running
README will include a concise “Integration tests” section with prerequisites (Fabric access + auth), required env vars, optional SQL dependencies, how to run (`FABRIC_INTEGRATION_TESTS=1 pytest`), and skip behavior. It will also highlight that tests use live Fabric resources and may incur costs or side effects.

## Specification

### Spec Section 1: Functional Requirements
- Integration tests must exercise MCP tools end‑to‑end against live Fabric, without mocks.
- Tests are opt‑in: run only when `FABRIC_INTEGRATION_TESTS=1`.
- No workspace creation/deletion; only test‑created items are deleted.
- Pre‑provisioned workspace and lakehouse are required via env vars.

Tool coverage:
- Workspace: `list_workspaces` returns success and includes configured workspace name.
- Item: `list_items` returns success; `delete_item` used for cleanup.
- Notebook: import fixture, get content, attach lakehouse.
- Job: run notebook job, wait to completion; check status by ID and URL.
- Livy: create/list/status/run/get statement/logs/close.
- Pipeline: create blank pipeline; add Wait activity; add copy activity if env vars provided.
- SQL: lakehouse SQL endpoint, query success; `execute_sql_statement` with non‑DML returns error.
- Operation result: capture real op ID where possible; otherwise skip.

### Spec Section 2: Non‑functional Requirements
Safety and isolation:
- Do not delete or alter shared resources beyond test items.
- Cleanup created notebooks/pipelines in `finally` blocks where possible.

Reliability:
- Assert minimal, stable fields.
- Skip when env vars or dependencies are missing.

Performance:
- Livy tests run only when explicitly enabled.
- Job polling uses reasonable defaults (poll interval ~15s, timeout up to 30 minutes).

Observability:
- Failures surface meaningful error messages (job failure reason, Livy error, etc.).
- Skips include missing env var names or dependency details.

Determinism:
- Unique names via timestamp/UUID for test-created items.
- Minimal notebook fixture committed to repo.

### Spec Section 3: Test Artifacts and Structure
- Integration tests live in `tests/fabric/integration/` with `@pytest.mark.integration`.
- `tests/fixtures/minimal_notebook.ipynb` provides a reusable notebook fixture.
- `tests/conftest.py` provides integration gate, env var helpers, and shared fixtures.

Key fixtures/helpers:
- `integration_enabled` autouse fixture (gated by env var).
- `get_env_or_skip(name)` for required vars.
- `mcp_server` fixture creating a real FastMCP server.
- `call_tool` helper to run a tool and return `structured_content`.
- `workspace_name`, `lakehouse_name`, `lakehouse_id` fixtures.
- `unique_name(prefix)` helper.

### Spec Section 4: Test Flows by Tool Group
Workspace:
- `list_workspaces` → assert success and configured workspace name is present.

Item:
- `list_items` → assert success, optional type filter.
- `delete_item` → cleanup created notebook/pipeline.

Notebook/Job:
- Import fixture notebook → get content → attach lakehouse.
- Run notebook job → wait for completion.
- Get job status by ID and URL.
- List notebook executions → get execution details → get driver logs.
- Cleanup: delete notebook.

Livy:
- Resolve lakehouse ID.
- Create session → list → status → run statement → get statement status → get session log → close session.

Pipeline:
- Create blank pipeline → add Wait activity.
- Add copy activity if connection env vars exist (destination table name defaults to source table name, with optional `FABRIC_TEST_DEST_TABLE_NAME` override).
- Cleanup: delete pipeline.

SQL:
- Get lakehouse SQL endpoint.
- Execute `SELECT 1 AS value`.
- Execute non‑DML statement with `execute_sql_statement` and assert error.

Operation result:
- Attempt async call to obtain op ID; call `get_operation_result` if present; otherwise skip.

## Implementation Plan

### Plan Section 1: Scaffolding + Fixtures (detailed)
1) Update `tests/conftest.py` with integration scaffolding:
   - Add an autouse fixture (e.g., `integration_enabled`) that:
     - checks the test has the `integration` marker, and
     - enforces `FABRIC_INTEGRATION_TESTS=1`; skip if missing or not `1`.
   - Add env var helpers:
     - `get_env_or_skip(name, *, allow_empty=False)` returns the env var or skips if missing.
     - `get_env_optional(name)` returns the env var or `None` if missing.
   - Add a unique name helper `unique_name(prefix)` that returns `f\"{prefix}_{timestamp}_{uuid4hex}\"`.
   - Add a `mcp_server` fixture (session-scoped) that constructs `FastMCP(\"integration-tests\")` and calls `register_fabric_tools(mcp)`.
   - Add a `tool_registry` fixture that awaits `mcp.get_tools()` once per session and caches the mapping.
   - Add a `call_tool` helper that:
     - looks up a tool by name,
     - invokes `await tool.run(arguments)`,
     - returns `result.structured_content`,
     - raises a clear error if the tool is missing or structured content is empty.
   - Add fixtures for:
     - `workspace_name` from `FABRIC_TEST_WORKSPACE_NAME`,
     - `lakehouse_name` from `FABRIC_TEST_LAKEHOUSE_NAME`,
     - `sql_database` from `FABRIC_TEST_SQL_DATABASE`,
     - `lakehouse_id` resolved by calling `list_items` with `item_type=\"Lakehouse\"` and matching `display_name` to `lakehouse_name` (skip if not found).
   - Add `pipeline_copy_inputs` fixture that reads the optional pipeline env vars and returns a dict when all are present; return `None` otherwise. Set `destination_table_name` to `FABRIC_TEST_DEST_TABLE_NAME` when provided; otherwise default to `FABRIC_TEST_SOURCE_TABLE`.
   - Add a `sql_dependencies_available` fixture that:
     - uses `pytest.importorskip(\"pyodbc\")`,
     - checks that an ODBC driver is installed (e.g., `\"ODBC Driver 18 for SQL Server\"` in `pyodbc.drivers()`),
     - verifies SQL tools are registered in the tool registry (skip if missing).
   - Add a small `poll_until` helper for job status and post-job consistency checks.

2) Add a minimal notebook fixture at `tests/fixtures/minimal_notebook.ipynb`:
   - Single code cell with a simple `print(\"ok\")`.
   - Minimal metadata (kernel spec + language info) to keep it deterministic.

### Plan Section 2: Workspace + Item + Notebook/Job Tests (detailed)
3) Add `tests/fabric/integration/test_workspace_tools_integration.py`:
   - Use `@pytest.mark.integration` and `@pytest.mark.asyncio`.
   - Call `list_workspaces` and assert `status == \"success\"`.
   - Verify `FABRIC_TEST_WORKSPACE_NAME` appears in the returned `workspaces` list.

4) Add `tests/fabric/integration/test_item_tools_integration.py`:
   - Call `list_items(workspace_name)` and assert success + `item_count` is an int.
   - Optional: call `list_items` with a type filter (e.g., `\"Notebook\"`) and verify all returned items match the filter.
   - Provide a `delete_item_if_exists` helper that calls the `delete_item` tool and treats “not found” as non-fatal.

5) Add `tests/fabric/integration/test_notebook_tools_integration.py`:
   - Create a unique notebook display name.
   - `import_notebook_to_fabric` using the fixture file; assert success.
   - `get_notebook_content`; assert success.
   - `attach_lakehouse_to_notebook` using configured lakehouse; assert success and IDs present.
   - `run_on_demand_job` with `job_type=\"RunNotebook\"`; assert job instance ID and location URL.
   - Poll `get_job_status` (every ~15s, up to 30 minutes) until terminal state.
   - `get_job_status_by_url`; assert terminal state is consistent.
   - Use a short retry/backoff loop to wait for notebook executions and logs to appear:
     - `list_notebook_executions`; assert at least one session.
     - `get_notebook_execution_details`; assert success.
     - `get_notebook_driver_logs` with `log_type=\"stdout\"`; assert success and non-empty content.
   - Cleanup in `finally`: delete the notebook via `delete_item` (ignore not found).

### Plan Section 3: Livy + Pipeline Tests (detailed)
6) Add `tests/fabric/integration/test_livy_tools_integration.py`:
   - Resolve `lakehouse_id` from configured name; skip if missing.
   - `livy_create_session` with `with_wait=True`; assert state not `error`.
   - `livy_list_sessions` and verify session ID appears.
   - `livy_get_session_status` and assert `state` present.
   - `livy_run_statement` with a simple PySpark snippet; assert `state == \"available\"` and `output.status == \"ok\"`.
   - `livy_get_statement_status` to verify the same statement result.
   - `livy_get_session_log` (small `size`) and assert log list present.
   - `livy_close_session` in `finally` to release resources.

7) Add `tests/fabric/integration/test_pipeline_tools_integration.py`:
   - Create a blank pipeline with a unique name; assert success.
   - Add a minimal Wait activity via `add_activity_to_pipeline`, e.g.:
     - `{\"name\": \"WaitShort\", \"type\": \"Wait\", \"dependsOn\": [], \"typeProperties\": {\"waitTimeInSeconds\": 1}}`
   - If `pipeline_copy_inputs` is present, call `add_copy_activity_to_pipeline` using resolved `lakehouse_id` and env var values; assert success. If missing, skip this step.
   - Cleanup: delete the pipeline via `delete_item` in `finally`.

### Plan Section 4: SQL + Operation Result + Docs (detailed)
8) Add `tests/fabric/integration/test_sql_tools_integration.py`:
   - Use the SQL dependency fixture to skip if pyodbc/driver are missing or SQL tools are not registered.
   - `get_sql_endpoint` with `item_type=\"Lakehouse\"` and configured lakehouse name; assert non-empty endpoint.
   - `execute_sql_query` with `SELECT 1 AS value` and `database=FABRIC_TEST_SQL_DATABASE`; assert success and row_count >= 1.
   - `execute_sql_statement` with non‑DML (e.g., `SELECT 1`); assert `status == \"error\"`.
   - If the current implementation accepts non‑DML, add a validation step in `FabricSQLService.execute_statement` (or in the SQL tool wrapper) to reject non‑DML statements and return an error response to match the tool contract.

9) Add `tests/fabric/integration/test_operation_tools_integration.py`:
   - Create a temporary pipeline via `create_blank_pipeline` to serve as a safe target.
   - Use `FabricConfig`, `FabricClient`, `FabricWorkspaceService`, and `FabricItemService` directly in the test to:
     - resolve `workspace_id`,
     - fetch the pipeline definition,
     - build `update_payload = {\"definition\": definition_response[\"definition\"]}`,
     - call `client.make_api_request(\"POST\", f\"workspaces/{workspace_id}/items/{pipeline_id}/updateDefinition\", payload=update_payload)`.
   - Inspect `response.headers` for `x-ms-operation-id`:
     - If present, call `get_operation_result` and assert success.
     - If missing (synchronous completion), `pytest.skip` with an explicit message.
   - Cleanup: delete the pipeline via `delete_item` in `finally`.

10) Update `README.md`:
   - Add “Integration tests” section with prerequisites, required env vars, optional pipeline/SQL vars, and run instructions (`FABRIC_INTEGRATION_TESTS=1 pytest`).
   - Note skip behavior and that tests use live Fabric resources.
