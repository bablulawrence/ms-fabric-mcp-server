# MCP Tool Test Plan (Direct Tool Invocation)

## Goal
Validate the Microsoft Fabric MCP tools by invoking them directly (as the integration tests do),
using real workspace/lakehouse resources and end-to-end flows that create, run, and clean up items.

## Scope
- Workspace, Item, Notebook, Job, Pipeline, SQL, Livy tools.
- Direct tool calls via MCP server (no unit-test harness).
- Uses the same environment values as integration tests.

## Quick Start (Suggested Order)
1. Load `.env.integration` values.
2. Discover workspace/lakehouse and capture their IDs.
3. Run Notebook + Job flow (creates the most artifacts).
4. Run Pipeline flow.
5. Run SQL flow.
6. Run Livy flow (use polling, not blocking waits).
7. Cleanup everything created.

## Preconditions
- Fabric capacity is active for the target workspace.
- Auth is available (DefaultAzureCredential).
- You can call MCP tools (through the Fabric MCP server).
- `.env.integration` values exist and are valid:
  - `FABRIC_TEST_WORKSPACE_NAME`
  - `FABRIC_TEST_LAKEHOUSE_NAME`
  - `FABRIC_TEST_SQL_DATABASE`
  - Pipeline copy inputs (optional)
- SQL drivers installed for SQL tool checks.

## Environment Inputs
Use the values from `.env.integration`:
- Workspace: `FABRIC_TEST_WORKSPACE_NAME`
- Lakehouse: `FABRIC_TEST_LAKEHOUSE_NAME`
- SQL database: `FABRIC_TEST_SQL_DATABASE`
- Optional pipeline copy inputs (source/destination connection, table, schema)

IDs you must capture during discovery:
- `workspace_id` from `list_workspaces` (needed for Livy).
- `lakehouse_id` from `list_items` (needed for Livy and copy activity).

## Tool Invocation Approach
Call tools directly through the MCP server, using the same sequences as integration tests,
and track created items by a timestamped prefix so they can be cleaned up.

Keep a simple run log for traceability:
- Timestamp
- Workspace/lakehouse names + IDs
- Created item names + IDs (notebook, pipeline)
- Job/session/statement IDs

Example naming convention:
- Notebook: `mcp_tools_smoke_<timestamp>`
- Pipeline: `mcp_tools_pipeline_<timestamp>`

## Test Flows

### 1) Workspace + Item Discovery
1. `list_workspaces`
2. `list_items` (Lakehouse)
3. `list_items` (Notebook)

Expected: Target workspace and lakehouse are present; item listing succeeds.

### 2) Notebook + Job Flow (Full)
1. `import_notebook_to_fabric` (local fixture: `tests/fixtures/minimal_notebook.ipynb`)
2. `get_notebook_content`
3. `attach_lakehouse_to_notebook`
4. `run_on_demand_job` (Notebook, RunNotebook)
5. Poll `get_job_status_by_url` until `is_terminal == true`
6. `get_notebook_execution_details`
7. `get_notebook_driver_logs` (stdout)

Notes:
- Job can remain `NotStarted` for several minutes; keep polling.
- `get_job_status_by_url` can lag and stay `NotStarted` while the run is active; use
  `list_notebook_executions` or `get_notebook_execution_details` as the source of truth
  until terminal.
- Driver logs require Spark app id; only available after execution starts.
- Driver logs are noisy; the notebook output may be buried under Spark warnings.
- `list_notebook_executions` can be used to confirm Livy session state.
- If the job does not start after 10+ minutes, check capacity state.

### 3) Pipeline Flow
1. `create_blank_pipeline`
2. `add_activity_to_pipeline` (Wait activity)
3. `add_copy_activity_to_pipeline` (optional, requires copy inputs)
4. (Optional) `run_on_demand_job` (Pipeline) and poll `get_job_status_by_url`

Expected: Activity appends successfully; pipeline id returned.

### 4) SQL Flow
1. `get_sql_endpoint` (Lakehouse)
2. `execute_sql_query` (`SELECT 1 AS value`)
3. `execute_sql_statement` (DML against a scratch table) OR `SELECT 1` -> expected error

Optional: use a pre-created scratch table for a fully successful DML run; otherwise keep
the expected error from `SELECT 1`.

### 5) Livy Flow (Session + Statement)
Observed in this environment: session creation often falls back to on-demand cluster
(`FallbackReasons: CustomLibraries, SystemSparkConfigMismatch`), which adds several
minutes of startup latency and can exceed MCP tool-call timeouts (~60s).

Recommended pattern:
1. `livy_create_session` with `with_wait=false` to get a session id quickly.
2. Poll `livy_get_session_status` until `state == "idle"`.
3. `livy_run_statement` with `with_wait=false`.
4. Poll `livy_get_statement_status` until `state == "available"`.
5. (Optional) `livy_get_session_log` for driver logs (may 404 until the session starts).
6. `livy_close_session` for cleanup.

If `livy_get_session_status` stays in `starting` for a long time, check `livy_list_sessions`
for fallback messages and continue polling with backoff.
Record any `FallbackReasons` from `livy_list_sessions` for the run log; they are expected
in this environment.

## Timeouts and Polling
- MCP tool calls may time out around 60 seconds in some environments.
- Prefer asynchronous or polling patterns for long-running operations.
- Use progressive backoff for long waits (e.g., 5s → 10s → 20s → 40s).
- Cap total wait to a sensible limit (10-20 minutes) before cancelling and reporting.
- When `livy_get_session_log` returns 404, keep polling status; logs appear after the
  session starts and the Spark app is running.
- When using shell sleeps, set `timeout_ms` explicitly to avoid short default timeouts.

## Cleanup
Always delete created items to avoid clutter/cost:
- `delete_item` for Notebook and Pipeline (DataPipeline).
- `livy_close_session` for Livy sessions.
- If the pipeline run executed a copy activity, remove any created destination table/data.

If delete fails, verify by `list_items` and retry.

## Expected Failure Modes
- `CapacityNotActive`: capacity paused or inactive.
- `scp` claim missing: some notebook history/log endpoints may require delegated tokens.
- Transient `NotStarted`/`Running` states: continue polling.
- Livy session logs 404 before the session is fully started.

## Reporting
Record:
- Tool name and parameters
- Status + key IDs (item_id, job_instance_id, session_id)
- Any skip or failure reasons
- How long each step took (especially job and Livy startup)

## Post-Run Checklist
- All created notebooks and pipelines removed.
- Livy sessions closed.
- Job run reached terminal state.
- SQL query succeeded; DML statement returned expected error.
- Any copy destination tables/data cleaned up (if pipeline execution was run).
