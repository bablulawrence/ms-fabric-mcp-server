# MCP Tool Test Plan (Direct Tool Invocation)

## Goal
Validate the Microsoft Fabric MCP tools by invoking them directly (as the integration tests do),
using real workspace/lakehouse resources and end-to-end flows that create, run, and clean up items.

## Scope
- Workspace, Item, Notebook, Job, Pipeline, SQL, Livy, Semantic Model, Power BI tools.
- Direct tool calls via MCP server (no unit-test harness).
- Uses the same environment values as integration tests.

## Quick Start (Suggested Order)
1. Load `.env.integration` values.
2. Discover workspace/lakehouse and capture their IDs.
3. Run Notebook + Job flow (creates the most artifacts).
4. Run Pipeline flow.
5. Run Semantic Model flow.
6. Run Power BI flow.
7. Run SQL flow.
8. Run Livy flow (use polling, not blocking waits).
9. Cleanup everything created.

## Preconditions
- Fabric capacity is active for the target workspace.
- Auth is available (DefaultAzureCredential).
- You can call MCP tools (through the Fabric MCP server).
- Power BI REST API access is available for the tenant and the test principal has dataset
  refresh + execute queries permissions.
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
  - Optional SQL endpoint connection for SQL fallback:
    - `FABRIC_TEST_SOURCE_SQL_CONNECTION_ID`
- Optional pipeline dataflow inputs:
  - `FABRIC_TEST_DATAFLOW_NAME`
  - `FABRIC_TEST_DATAFLOW_WORKSPACE_NAME` (if different from test workspace)
- Optional semantic model inputs:
  - `FABRIC_TEST_SEMANTIC_MODEL_NAME` (otherwise generate a timestamped name)
  - `FABRIC_TEST_SM_TABLE_1`, `FABRIC_TEST_SM_TABLE_2` (lakehouse tables to model)
  - `FABRIC_TEST_SM_COLUMNS_TABLE_1`, `FABRIC_TEST_SM_COLUMNS_TABLE_2`
    (column name + data type pairs; supported data types: `string`, `int64`, `decimal`,
    `double`, `boolean`, `dateTime`)
- Optional Power BI inputs:
  - `FABRIC_TEST_DAX_QUERY` (otherwise use a simple default query)

IDs you must capture during discovery:
- `workspace_id` from `list_workspaces` (needed for Livy).
- `lakehouse_id` from `list_items` (needed for Livy and copy activity).

## Tool Invocation Approach
Call tools directly through the MCP server, using the same sequences as integration tests.
Do not run scripts or call Fabric REST APIs directly for this plan; keep everything within
the MCP tool surface to validate end-to-end behavior.

Always load `.env.integration` into your shell before you start so tests can read
environment variables (pytest only reads `os.environ`):
```
set -a; source .env.integration; set +a
```

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
   - SQL fallback example (use SQL Analytics endpoint connection):
     ```
     add_copy_activity_to_pipeline(
       workspace_name=...,
       pipeline_name=...,
       source_type="LakehouseTableSource",
       source_connection_id=FABRIC_TEST_SOURCE_SQL_CONNECTION_ID,  # Fabric connection ID
       source_table_schema=FABRIC_TEST_SOURCE_SCHEMA,
       source_table_name=FABRIC_TEST_SOURCE_TABLE,
       destination_lakehouse_id=...,
       destination_connection_id=FABRIC_TEST_DEST_CONNECTION_ID,
       destination_table_name=FABRIC_TEST_DEST_TABLE_NAME,
       source_access_mode="sql",
       source_sql_query="SELECT * FROM dbo.fact_sale"  # optional
     )
     ```
4. `add_notebook_activity_to_pipeline` (use the notebook created earlier; set `depends_on_activity_name`)
5. `add_dataflow_activity_to_pipeline` (optional, requires dataflow inputs; set `depends_on_activity_name`)
4. (Optional) `run_on_demand_job` (Pipeline) and poll `get_job_status_by_url`

Expected: Activity appends successfully; pipeline id returned.

### 4) Semantic Model Flow
1. (Optional) Create two small lakehouse tables for testing (via `execute_sql_statement`)
2. `create_semantic_model`
3. `add_table_to_semantic_model` (table 1 with explicit columns)
4. `add_table_to_semantic_model` (table 2 with explicit columns)
5. `add_relationship_to_semantic_model` (e.g., Dim → Fact key)
6. `add_measures_to_semantic_model` (simple measure)
7. `get_semantic_model_details`
8. `get_semantic_model_definition` (`format="TMSL"`, optionally `decode_model_bim=true`)
9. `delete_measures_from_semantic_model` (validate delete path)

Expected: Semantic model updates are reflected in definition; measures add/delete succeeds.

### 5) Power BI Flow
1. `refresh_semantic_model` (use the semantic model created above)
2. `execute_dax_query` (e.g., `EVALUATE ROW("one", 1)` or use `FABRIC_TEST_DAX_QUERY`)

Expected: Refresh completes with status `success`; DAX query returns a Power BI response.

### 6) SQL Flow
1. `get_sql_endpoint` (Lakehouse)
2. `execute_sql_query` (`SELECT 1 AS value`)
3. `execute_sql_statement` (DML against a scratch table) OR `SELECT 1` -> expected error

Optional: use a pre-created scratch table for a fully successful DML run; otherwise keep
the expected error from `SELECT 1`.

### 7) Livy Flow (Session + Statement)
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
- If capacity is inactive, Notebook and Livy flows will fail with `CapacityNotActive`;
  record the failure and skip ahead rather than retrying indefinitely.

## Notebook + Livy Polling Tips
Notebook:
- `get_job_status_by_url` can remain `NotStarted` while the run is active; use
  `list_notebook_executions` to confirm `InProgress`/`Success`.
- Only call `get_notebook_driver_logs` after execution starts (Spark app ID present).
- Use backoff polling and cap total wait time (10-20 minutes).

Livy:
- Use `livy_create_session(with_wait=False)` and poll `livy_get_session_status` until `idle`.
- If startup is slow, call `livy_list_sessions` to capture `FallbackReasons`.
- Run statements with `with_wait=False`, then poll `livy_get_statement_status` until `available`.

## Cleanup
Always delete created items to avoid clutter/cost:
- `delete_item` for Notebook and Pipeline (DataPipeline).
- `delete_item` for Semantic Model.
- `livy_close_session` for Livy sessions.
- If the pipeline run executed a copy activity, remove any created destination table/data.
- If semantic model tests created scratch lakehouse tables, drop them.

If delete fails, verify by `list_items` and retry.

## Expected Failure Modes
- `CapacityNotActive`: capacity paused or inactive.
- `scp` claim missing: some notebook history/log endpoints may require delegated tokens.
- Transient `NotStarted`/`Running` states: continue polling.
- Livy session logs 404 before the session is fully started.
- Power BI REST calls denied due to tenant policy or insufficient dataset permissions.
- Semantic model definition not immediately available after creation; retry with backoff.

## Reporting
Record:
- Tool name and parameters
- Status + key IDs (item_id, job_instance_id, session_id)
- Any skip or failure reasons
- How long each step took (especially job and Livy startup)

## Post-Run Checklist
- All created notebooks and pipelines removed.
- Semantic model removed.
- Livy sessions closed.
- Job run reached terminal state.
- SQL query succeeded; DML statement returned expected error.
- Any copy destination tables/data cleaned up (if pipeline execution was run).
- Any semantic model scratch tables cleaned up.
