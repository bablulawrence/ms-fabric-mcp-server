# MCP Tool Test Plan - Minimal (Smoke Test)

## Goal
Validate the Microsoft Fabric MCP tools by invoking them directly (as the integration tests do),
using real workspace/lakehouse resources and end-to-end flows that create, run, and clean up items.

This is the **minimal version** focusing on happy paths with basic negative tests to verify
error handling. For comprehensive testing, see `mcp-tool-test-plan-comprehensive.md`.

## Scope
- Workspace, Item, Notebook, Job, Pipeline, SQL, Livy, Semantic Model, Power BI tools.
- Direct tool calls via MCP server (no unit-test harness).
- Uses the same environment values as integration tests.
- Basic negative test cases for error handling validation (1-2 per flow).

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
    - **Must have a valid foreign key relationship** (e.g., fact→dimension or dimension→dimension)
    - Example: `fact_sale` (table 1) and `dimension_city` (table 2) with `CityKey` relationship
  - `FABRIC_TEST_SM_RELATIONSHIP_FROM_COL`, `FABRIC_TEST_SM_RELATIONSHIP_TO_COL` (relationship key columns)
  - `FABRIC_TEST_SM_COLUMNS_TABLE_1`, `FABRIC_TEST_SM_COLUMNS_TABLE_2`
    (column name + data type pairs; supported data types: `string`, `int64`, `decimal`,
    `double`, `boolean`, `dateTime`)
    - **Must include the relationship key columns**
- Optional Power BI inputs:
  - `FABRIC_TEST_DAX_QUERY` (otherwise use a simple default query)

IDs you must capture during discovery:
- `workspace_id` from `list_workspaces` (needed for Livy).
- `lakehouse_id` from `list_items` (needed for Livy and copy activity).

## Tool Invocation Approach
Call tools directly through the MCP server, using the same sequences as integration tests.
Do not run scripts or call Fabric REST APIs directly for this plan; keep everything within
the MCP tool surface to validate end-to-end behavior.

**Recreate-After-Delete Rule:** Any item deleted as part of delete testing (e.g., measures,
relationships) must be **immediately recreated** after validating the delete succeeded. This
ensures artifacts remain in a complete state for subsequent tests and user review.

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

**Happy Path:**
1. `list_workspaces`
2. `list_items` (Lakehouse)
3. `list_items` (Notebook)

Expected: Target workspace and lakehouse are present; item listing succeeds.

**Negative Tests:**
1. `list_items` with non-existent workspace name
   - Expected: Error with `FabricWorkspaceNotFoundError` or similar message

### 2) Notebook + Job Flow (Full)

**Happy Path:**
1. `import_notebook_to_fabric` (local fixture: `tests/fixtures/minimal_notebook.ipynb`)
2. `get_notebook_content`
3. `attach_lakehouse_to_notebook`
4. `run_on_demand_job` (Notebook, RunNotebook)
5. Poll `get_job_status_by_url` until `is_terminal == true`
6. `get_notebook_execution_details`
7. `get_notebook_driver_logs` (stdout)

Expected: Notebook imports, runs successfully, and execution details are retrievable.

**Negative Tests:**
1. `get_notebook_content` with non-existent notebook name
   - Expected: Error indicating notebook not found
2. `attach_lakehouse_to_notebook` with non-existent lakehouse name
   - Expected: Error indicating lakehouse not found

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

**Happy Path:**
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
6. (Optional) `run_on_demand_job` (Pipeline) and poll `get_job_status_by_url`

Expected: Activity appends successfully; pipeline id returned.

**Negative Tests:**
1. `add_activity_to_pipeline` to non-existent pipeline
   - Expected: Error indicating pipeline not found
2. `add_notebook_activity_to_pipeline` with non-existent notebook
   - Expected: Error indicating notebook not found

### 4) Semantic Model Flow

**Happy Path:**
1. (Optional) Create two small lakehouse tables for testing (via `execute_sql_statement`)
2. **Query lakehouse schema to identify tables with valid relationships**
   - Use `execute_sql_query` to find tables with foreign key relationships, OR
   - Query `INFORMATION_SCHEMA` to identify fact/dimension tables, OR
   - Use known tables from the test lakehouse (e.g., `fact_sale` → `dimension_city`)
   - **Important:** Verify the relationship exists in the data schema before adding to semantic model
   - Document the chosen tables and the relationship key columns in the test log
   
   **Example queries to discover valid relationships:**
   ```sql
   -- Find tables with common column names (potential FK relationships)
   SELECT t1.TABLE_NAME as Table1, t1.COLUMN_NAME as Column1,
          t2.TABLE_NAME as Table2, t2.COLUMN_NAME as Column2
   FROM INFORMATION_SCHEMA.COLUMNS t1
   JOIN INFORMATION_SCHEMA.COLUMNS t2 
     ON t1.COLUMN_NAME = t2.COLUMN_NAME 
     AND t1.TABLE_NAME < t2.TABLE_NAME
   WHERE t1.COLUMN_NAME LIKE '%Key' OR t1.COLUMN_NAME LIKE '%ID'
   ORDER BY t1.COLUMN_NAME;
   
   -- Sample data from candidate tables to verify the relationship
   SELECT DISTINCT TableName.KeyColumn FROM TableName ORDER BY KeyColumn LIMIT 10;
   ```
   
3. `create_semantic_model`
4. `add_table_to_semantic_model` (table 1 with explicit columns including relationship key)
5. `add_table_to_semantic_model` (table 2 with explicit columns including relationship key)
6. `add_relationship_to_semantic_model` (use the validated relationship from step 2)
   - Example: `fact_sale.CityKey` → `dimension_city.CityKey` (many-to-one)
   - Verify cardinality makes semantic sense (many-to-one for fact→dimension)
7. `add_measures_to_semantic_model` (simple measure)
8. `get_semantic_model_details`
9. `get_semantic_model_definition` (`format="TMSL"`, optionally `decode_model_bim=true`)
10. `delete_measures_from_semantic_model` (validate delete path)
11. `add_measures_to_semantic_model` (**recreate deleted measure** - always restore after delete tests)

Expected: Semantic model updates are reflected in definition; measures add/delete succeeds; relationship is semantically valid.

**Important:** Any item deleted as part of testing (e.g., measures) must be **recreated immediately**
after the delete test, before proceeding to the next flow. This ensures the model remains in a
complete state for subsequent tests and final cleanup prompts.

**Negative Tests:**
1. `add_table_to_semantic_model` with invalid column data type (e.g., `"invalid_type"`)
   - Expected: Error indicating unsupported data type
2. `get_semantic_model_definition` for non-existent semantic model
   - Expected: Error indicating semantic model not found

### 5) Power BI Flow

**Happy Path:**
1. `refresh_semantic_model` (use the semantic model created above)
2. `execute_dax_query` (e.g., `EVALUATE ROW("one", 1)` or use `FABRIC_TEST_DAX_QUERY`)

Expected: Refresh completes with status `success`; DAX query returns a Power BI response.

**Negative Tests:**
1. `execute_dax_query` with malformed DAX syntax (e.g., `"EVALUATE INVALID_FUNCTION()"`)
   - Expected: Error with DAX parsing/execution failure message
2. `refresh_semantic_model` for non-existent semantic model
   - Expected: Error indicating semantic model not found

### 6) SQL Flow

**Happy Path:**
1. `get_sql_endpoint` (Lakehouse)
2. `execute_sql_query` (`SELECT 1 AS value`)
3. `execute_sql_statement` (DML against a scratch table) OR `SELECT 1` -> expected error

Optional: use a pre-created scratch table for a fully successful DML run; otherwise keep
the expected error from `SELECT 1`.

**Negative Tests:**
1. `execute_sql_query` with invalid SQL syntax (e.g., `"SELEC 1"`)
   - Expected: Error with SQL parsing failure message
2. `execute_sql_statement` with SELECT query (not DML)
   - Expected: Error indicating only DML statements are supported

### 7) Livy Flow (Session + Statement)
Observed in this environment: session creation often falls back to on-demand cluster
(`FallbackReasons: CustomLibraries, SystemSparkConfigMismatch`), which adds several
minutes of startup latency and can exceed MCP tool-call timeouts (~60s).

**Happy Path:**
1. `livy_create_session` with `with_wait=false` to get a session id quickly.
2. Poll `livy_get_session_status` until `state == "idle"`.
3. `livy_run_statement` with `with_wait=false`.
4. Poll `livy_get_statement_status` until `state == "available"`.
5. (Optional) `livy_get_session_log` for driver logs (may 404 until the session starts).
6. `livy_close_session` for cleanup.

Expected: Session starts, statement executes successfully, session closes cleanly.

**Negative Tests:**
1. `livy_run_statement` on non-existent session ID
   - Expected: Error indicating session not found (404)
2. `livy_get_statement_status` with invalid statement ID
   - Expected: Error indicating statement not found

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
At the end of the test run, **ask the user for permission** before deleting any items.

**Prompt the user:**
> "Test run complete. The following items were created:
> - Notebook: `<name>` (ID: `<id>`)
> - Pipeline: `<name>` (ID: `<id>`)
> - Semantic Model: `<name>` (ID: `<id>`)
>
> Would you like to clean up (delete) these items? (yes/no)"

**If user says YES:**
- `delete_item` for Notebook and Pipeline (DataPipeline).
- `delete_item` for Semantic Model.
- `livy_close_session` for Livy sessions (if still open).
- If the pipeline run executed a copy activity, remove any created destination table/data.
- If semantic model tests created scratch lakehouse tables, drop them.
- If delete fails, verify by `list_items` and retry.

**If user says NO:**
- Leave all created items in place.
- Report the list of items left behind for the user's reference.
- Livy sessions should still be closed to avoid resource waste (sessions are ephemeral).

## Negative Test Summary

| Flow | Test Case | Expected Error |
|------|-----------|----------------|
| Discovery | `list_items` with invalid workspace | Workspace not found |
| Notebook | `get_notebook_content` with invalid name | Notebook not found |
| Notebook | `attach_lakehouse_to_notebook` with invalid lakehouse | Lakehouse not found |
| Pipeline | `add_activity_to_pipeline` to invalid pipeline | Pipeline not found |
| Pipeline | `add_notebook_activity_to_pipeline` with invalid notebook | Notebook not found |
| Semantic Model | `add_table_to_semantic_model` with invalid data type | Unsupported data type |
| Semantic Model | `get_semantic_model_definition` for invalid model | Semantic model not found |
| Power BI | `execute_dax_query` with malformed DAX | DAX parsing error |
| Power BI | `refresh_semantic_model` for invalid model | Semantic model not found |
| SQL | `execute_sql_query` with invalid SQL | SQL parsing error |
| SQL | `execute_sql_statement` with SELECT | Only DML supported |
| Livy | `livy_run_statement` on invalid session | Session not found (404) |
| Livy | `livy_get_statement_status` with invalid ID | Statement not found |

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
- [ ] User prompted for cleanup permission
- [ ] If cleanup approved: all created notebooks and pipelines removed
- [ ] If cleanup approved: semantic model removed
- [ ] Livy sessions closed (always, regardless of cleanup choice)
- [ ] Job run reached terminal state
- [ ] SQL query succeeded; DML statement returned expected error
- [ ] If cleanup approved: any copy destination tables/data cleaned up
- [ ] If cleanup approved: any semantic model scratch tables cleaned up
- [ ] If cleanup declined: list of remaining items reported to user
