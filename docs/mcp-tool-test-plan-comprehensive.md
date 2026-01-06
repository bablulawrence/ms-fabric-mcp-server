# MCP Tool Test Plan - Comprehensive

## Goal
Validate the Microsoft Fabric MCP tools thoroughly with extensive coverage including:
- All happy path scenarios
- Comprehensive negative/error path testing
- Edge cases and boundary conditions
- Concurrent operations
- Update/modify operations
- All parameter variations

This is the **comprehensive version** for thorough validation. For quick smoke testing,
see `mcp-tool-test-plan-minimal.md`.

## Scope
- All Workspace, Item, Notebook, Job, Pipeline, SQL, Livy, Semantic Model, Power BI tools.
- Direct tool calls via MCP server (no unit-test harness).
- Uses the same environment values as integration tests.
- Comprehensive negative test cases (3+ per flow).
- Edge cases, boundary conditions, and parameter variations.
- Concurrent/parallel operation testing.
- Update and modify operations on existing items.

## Quick Start (Suggested Order)
1. Load `.env.integration` values.
2. Discover workspace/lakehouse and capture their IDs.
3. Run Workspace + Item Discovery flow (including negative tests).
4. Run Notebook + Job flow (full lifecycle with variations).
5. Run Pipeline flow (all activity types).
6. Run Semantic Model flow (all data types, relationships, measures).
7. Run Power BI flow (refresh and DAX variations).
8. Run SQL flow (queries and statements).
9. Run Livy flow (session lifecycle with statements).
10. Run Concurrent Operations tests.
11. Prompt user for cleanup permission.

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
  - Pipeline copy inputs (required for comprehensive copy activity tests)
  - Dataflow inputs (required for dataflow activity tests)
- SQL drivers installed for SQL tool checks.
- Secondary workspace available for cross-workspace tests (optional).

## Environment Inputs
Use the values from `.env.integration`:
- Workspace: `FABRIC_TEST_WORKSPACE_NAME`
- Lakehouse: `FABRIC_TEST_LAKEHOUSE_NAME`
- SQL database: `FABRIC_TEST_SQL_DATABASE`
- Pipeline copy inputs (source/destination connection, table, schema):
  - `FABRIC_TEST_SOURCE_TYPE`
  - `FABRIC_TEST_SOURCE_CONNECTION_ID`
  - `FABRIC_TEST_SOURCE_SQL_CONNECTION_ID` (for SQL fallback mode)
  - `FABRIC_TEST_SOURCE_SCHEMA`
  - `FABRIC_TEST_SOURCE_TABLE`
  - `FABRIC_TEST_DEST_CONNECTION_ID`
  - `FABRIC_TEST_DEST_TABLE_NAME`
- Pipeline dataflow inputs:
  - `FABRIC_TEST_DATAFLOW_NAME`
  - `FABRIC_TEST_DATAFLOW_WORKSPACE_NAME` (if different from test workspace)
- Semantic model inputs:
  - `FABRIC_TEST_SEMANTIC_MODEL_NAME` (otherwise generate a timestamped name)
  - `FABRIC_TEST_SEMANTIC_MODEL_TABLE` (table 1)
  - `FABRIC_TEST_SEMANTIC_MODEL_TABLE_2` (table 2)
    - **Tables must have a valid foreign key relationship** (e.g., fact→dimension)
    - Example: `fact_sale` (table 1) and `dimension_city` (table 2) with `CityKey` relationship
  - `FABRIC_TEST_SM_RELATIONSHIP_FROM_COL` (foreign key column in table 1)
  - `FABRIC_TEST_SM_RELATIONSHIP_TO_COL` (primary key column in table 2)
  - `FABRIC_TEST_SEMANTIC_MODEL_COLUMNS` (columns for table 1, JSON array)
  - `FABRIC_TEST_SEMANTIC_MODEL_COLUMNS_2` (columns for table 2, JSON array)
    - **Must include the relationship key columns**
- Power BI inputs:
  - `FABRIC_TEST_DAX_QUERY` (otherwise use default queries)
- Cross-workspace testing (optional):
  - `FABRIC_TEST_SECONDARY_WORKSPACE_NAME`

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

**Timing and Metrics:** Record execution time for each operation to identify performance
issues and establish baselines.

Always load `.env.integration` into your shell before you start so tests can read
environment variables (pytest only reads `os.environ`):
```
set -a; source .env.integration; set +a
```

Keep a detailed run log for traceability:
- Timestamp for each operation
- Workspace/lakehouse names + IDs
- Created item names + IDs (notebook, pipeline, semantic model)
- Job/session/statement IDs
- Execution times
- Error messages for negative tests

Example naming convention:
- Notebook: `mcp_comprehensive_nb_<timestamp>`
- Pipeline: `mcp_comprehensive_pl_<timestamp>`
- Semantic Model: `mcp_comprehensive_sm_<timestamp>`

---

## Test Flows

### 1) Workspace + Item Discovery

**Happy Path:**
1. `list_workspaces` - verify target workspace present
2. `list_items` (no filter) - list all items in workspace
3. `list_items` (Lakehouse) - filter by Lakehouse type
4. `list_items` (Notebook) - filter by Notebook type
5. `list_items` (SemanticModel) - filter by SemanticModel type
6. `list_items` (DataPipeline) - filter by DataPipeline type

Expected: All item types listed correctly with proper filtering.

**Negative Tests:**
1. `list_items` with non-existent workspace name
   - Expected: `FabricWorkspaceNotFoundError`
2. `list_items` with empty string workspace name
   - Expected: Error indicating invalid workspace name
3. `list_items` with invalid item_type filter
   - Expected: Error or empty result (depending on API behavior)

**Edge Cases:**
1. `list_workspaces` - verify pagination if >100 workspaces exist
2. `list_items` on empty workspace (if available)
   - Expected: Empty list, no error

---

### 2) Notebook + Job Flow (Full Lifecycle)

**Happy Path:**
1. `import_notebook_to_fabric` (local fixture: `tests/fixtures/minimal_notebook.ipynb`)
2. `get_notebook_content` - verify content matches imported notebook
3. `attach_lakehouse_to_notebook` - attach default lakehouse
4. `get_notebook_content` - verify lakehouse attachment in metadata
5. `run_on_demand_job` (Notebook, RunNotebook)
6. Poll `get_job_status_by_url` until `is_terminal == true`
7. `get_job_status` (using workspace/item/job_instance_id)
8. `list_notebook_executions` - verify execution appears in history
9. `get_notebook_execution_details` - get Livy session details
10. `get_notebook_driver_logs` (stdout) - attempt to get logs
11. `get_notebook_driver_logs` (stderr) - attempt to get stderr

Expected: Full notebook lifecycle completes successfully.

**Negative Tests:**
1. `import_notebook_to_fabric` with non-existent local file
   - Expected: File not found error
2. `import_notebook_to_fabric` with invalid notebook JSON
   - Expected: Parse/validation error
3. `import_notebook_to_fabric` with duplicate name (import same notebook twice)
   - Expected: Error indicating item already exists
4. `get_notebook_content` with non-existent notebook name
   - Expected: Notebook not found error
5. `attach_lakehouse_to_notebook` with non-existent notebook
   - Expected: Notebook not found error
6. `attach_lakehouse_to_notebook` with non-existent lakehouse
   - Expected: Lakehouse not found error
7. `run_on_demand_job` with non-existent notebook
   - Expected: Item not found error
8. `get_job_status` with invalid job_instance_id
   - Expected: Job not found error
9. `get_notebook_execution_details` with invalid job_instance_id
   - Expected: Execution not found error
10. `get_notebook_driver_logs` before job starts (no Spark app)
    - Expected: 404 or appropriate error

**Edge Cases:**
1. `import_notebook_to_fabric` with folder path (e.g., `"folder/notebook_name"`)
   - Expected: Notebook created in folder structure
2. `attach_lakehouse_to_notebook` - attach different lakehouse (if second available)
   - Expected: Lakehouse attachment updated

Notes:
- Job can remain `NotStarted` for several minutes; keep polling.
- Driver logs may 404 after job completion (logs cleaned up).
- Record timing for job startup and execution.

---

### 3) Pipeline Flow (All Activity Types)

**Happy Path:**
1. `create_blank_pipeline`
2. `add_activity_to_pipeline` (Wait activity - 5 seconds)
3. `add_activity_to_pipeline` (Second Wait activity - depends on first)
4. `add_copy_activity_to_pipeline` (SQL fallback mode)
   ```
   source_type="LakehouseTableSource"
   source_access_mode="sql"
   table_action_option="Overwrite"
   ```
5. `add_notebook_activity_to_pipeline` (depends on Wait activity)
6. `add_dataflow_activity_to_pipeline` (if dataflow available, depends on notebook)
7. Verify pipeline structure by running or inspecting

Expected: Pipeline created with all activity types and proper dependencies.

**Activity Dependency Chain Test:**
1. Create pipeline with chain: Wait → Notebook → Wait2
2. Verify `depends_on_activity_name` creates proper dependencies

**Copy Activity Variations:**
1. `add_copy_activity_to_pipeline` with `source_access_mode="direct"` (if supported)
2. `add_copy_activity_to_pipeline` with `table_action_option="Append"`
3. `add_copy_activity_to_pipeline` with custom `source_sql_query`

**Negative Tests:**
1. `create_blank_pipeline` with duplicate name
   - Expected: Error indicating pipeline already exists
2. `add_activity_to_pipeline` to non-existent pipeline
   - Expected: Pipeline not found error
3. `add_activity_to_pipeline` with invalid activity JSON (missing required fields)
   - Expected: Validation error
4. `add_activity_to_pipeline` with duplicate activity name
   - Expected: Error indicating activity name conflict
5. `add_notebook_activity_to_pipeline` with non-existent notebook
   - Expected: Notebook not found error
6. `add_notebook_activity_to_pipeline` with invalid `depends_on_activity_name`
   - Expected: Dependency activity not found error
7. `add_copy_activity_to_pipeline` with invalid connection ID
   - Expected: Connection not found or invalid error
8. `add_dataflow_activity_to_pipeline` with non-existent dataflow
   - Expected: Dataflow not found error

**Edge Cases:**
1. Create pipeline with many activities (10+) to test scaling
2. Create activity with very long name (boundary test)

---

### 4) Semantic Model Flow (Comprehensive)

**Prerequisite - Discover Valid Relationships:**
Before creating the semantic model, identify tables with valid foreign key relationships:

**Example queries to discover relationships:**
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

-- Verify relationship by sampling data from both tables
SELECT DISTINCT FK_Column FROM Table1 ORDER BY FK_Column LIMIT 10;
SELECT DISTINCT PK_Column FROM Table2 ORDER BY PK_Column LIMIT 10;

-- Check for orphaned foreign keys (values in FK not in PK)
SELECT COUNT(*) as OrphanedCount
FROM Table1 t1
LEFT JOIN Table2 t2 ON t1.FK_Column = t2.PK_Column
WHERE t2.PK_Column IS NULL;
```

**Document the chosen relationship** (e.g., `fact_sale.CityKey` → `dimension_city.CityKey`, cardinality: many-to-one)

**Happy Path - Full Lifecycle:**
1. `create_semantic_model`
2. `get_semantic_model_details` - verify creation
3. `add_table_to_semantic_model` (table 1 with all column types **including relationship key**)
4. `add_table_to_semantic_model` (table 2 **including relationship key**)
5. `add_relationship_to_semantic_model` (use validated relationship from prerequisite)
   - Example: `fact_sale.CityKey` → `dimension_city.CityKey` (manyToOne, oneDirection)
   - Verify cardinality matches data reality
6. `add_measures_to_semantic_model` (simple SUM measure)
7. `add_measures_to_semantic_model` (add second measure - COUNT)
8. `get_semantic_model_definition` (format="TMSL", decode_model_bim=false)
9. `get_semantic_model_definition` (format="TMSL", decode_model_bim=true) - verify relationship exists
10. `delete_measures_from_semantic_model` (delete one measure)
11. `add_measures_to_semantic_model` (**recreate deleted measure**)
12. Verify final state with `get_semantic_model_definition`

**Column Data Type Coverage:**
Test all supported data types in `add_table_to_semantic_model`:
- `string`
- `int64`
- `decimal`
- `double`
- `boolean`
- `dateTime`

Create a test table with one column of each type to verify all are handled correctly.

**Relationship Variations:**
**Note:** All relationship tests should use the same validated FK relationship from the prerequisite.
Only vary the parameters (cardinality, cross_filter_direction, is_active), not the tables/columns.

1. `add_relationship_to_semantic_model` with `cardinality="manyToOne"` (default) - validates tool
2. Test parameter variations on the **same** relationship:
   - `cardinality="oneToMany"` (reverse direction test)
   - `cardinality="oneToOne"` (1:1 constraint test)
   - `cardinality="manyToMany"` (M:M junction test)
   - `cross_filter_direction="bothDirections"` (bidirectional filter test)
   - `is_active=false` (inactive relationship test)

**Important:** If testing multiple relationships, ensure each one is based on a valid FK in the data schema.
6. `add_relationship_to_semantic_model` with `is_active=false`

**Measure Variations:**
1. Simple aggregation: `SUM(table[column])`
2. Count: `COUNT(table[column])`
3. Average: `AVERAGE(table[column])`
4. Calculated measure: `DIVIDE(SUM(table[col1]), SUM(table[col2]))`
5. Measure with format string (if supported)

**Negative Tests:**
1. ~~`create_semantic_model` with duplicate name~~
   - **SKIP:** Fabric API allows duplicate display names (creates new model with different ID)
2. `add_table_to_semantic_model` to non-existent model
   - Expected: Semantic model not found error
3. `add_table_to_semantic_model` with invalid column data type
   - Expected: Unsupported data type error
4. `add_table_to_semantic_model` with duplicate table name
   - Expected: Table already exists error
5. `add_table_to_semantic_model` with empty columns array
   - Expected: Validation error
6. `add_relationship_to_semantic_model` with non-existent table
   - Expected: Table not found error
7. `add_relationship_to_semantic_model` with non-existent column
   - Expected: Column not found error
8. `add_relationship_to_semantic_model` with invalid cardinality value
   - Expected: Invalid cardinality error
9. ~~`add_measures_to_semantic_model` with invalid DAX expression~~
   - **SKIP:** Fabric API accepts invalid DAX at creation time; validation occurs at refresh/query time
10. `add_measures_to_semantic_model` to non-existent table
    - Expected: Table not found error
11. `delete_measures_from_semantic_model` with non-existent measure name
    - Expected: Measure not found error (or silent success)
12. `get_semantic_model_definition` for non-existent model
    - Expected: Semantic model not found error
13. `get_semantic_model_definition` with invalid format
    - Expected: Invalid format error

**Important:** Any item deleted as part of testing must be **recreated immediately**
after the delete test, before proceeding to the next flow.

---

### 5) Power BI Flow (Refresh and DAX)

**Happy Path:**
1. `refresh_semantic_model` - full refresh
2. Verify refresh completed successfully (status="Completed")
3. `execute_dax_query` - simple literal: `EVALUATE ROW("one", 1)`
4. `execute_dax_query` - table query: `EVALUATE TOPN(5, table_name)`
5. `execute_dax_query` - aggregation: `EVALUATE ROW("total", [Total Sales])`

Expected: Refresh completes; DAX queries return expected results.

**DAX Query Variations:**
1. Simple literal query: `EVALUATE ROW("test", 123)`
2. Table scan: `EVALUATE table_name`
3. TOPN query: `EVALUATE TOPN(10, table_name)`
4. Aggregation with measure: `EVALUATE ROW("value", [MeasureName])`
5. SUMMARIZE query: `EVALUATE SUMMARIZE(table, column)`
6. FILTER query: `EVALUATE FILTER(table, condition)`

**Negative Tests:**
1. `refresh_semantic_model` for non-existent model
   - Expected: Semantic model not found error
2. `refresh_semantic_model` for model with invalid data source
   - Expected: Refresh fails with data source error
3. `execute_dax_query` with malformed DAX syntax
   - Expected: DAX parsing error
4. `execute_dax_query` referencing non-existent table
   - Expected: Table not found error
5. `execute_dax_query` referencing non-existent measure
   - Expected: Measure not found error
6. `execute_dax_query` with empty query string
   - Expected: Validation error
7. `execute_dax_query` for non-existent semantic model
   - Expected: Semantic model not found error

**Edge Cases:**
1. `execute_dax_query` with very large result set
   - Expected: Results returned (may be paginated or truncated)
2. `refresh_semantic_model` while another refresh is in progress
   - Expected: Error or queued behavior

---

### 6) SQL Flow (Queries and Statements)

**Happy Path:**
1. `get_sql_endpoint` (Lakehouse)
2. `get_sql_endpoint` (Warehouse) - if warehouse available
3. `execute_sql_query` - simple: `SELECT 1 AS value`
4. `execute_sql_query` - table query: `SELECT TOP 10 * FROM table_name`
5. `execute_sql_query` - aggregation: `SELECT COUNT(*) AS cnt FROM table_name`
6. `execute_sql_query` - with WHERE clause
7. `execute_sql_statement` - DML test (if scratch table available)

Expected: SQL endpoint retrieved; queries execute successfully.

**Query Variations:**
1. `SELECT 1 AS value` - literal
2. `SELECT TOP N * FROM table` - limit rows
3. `SELECT column1, column2 FROM table WHERE condition` - filtered
4. `SELECT COUNT(*), SUM(column) FROM table` - aggregations
5. `SELECT * FROM table1 JOIN table2 ON ...` - joins
6. `SHOW TABLES` - metadata query
7. `DESCRIBE table_name` - schema query

**Negative Tests:**
1. `get_sql_endpoint` for non-existent lakehouse
   - Expected: Lakehouse not found error
2. `get_sql_endpoint` with invalid item_type
   - Expected: Invalid item type error
3. `execute_sql_query` with invalid SQL syntax
   - Expected: SQL parsing error
4. `execute_sql_query` referencing non-existent table
   - Expected: Table not found error
5. `execute_sql_query` with invalid endpoint URL
   - Expected: Connection error
6. `execute_sql_query` with wrong database name
   - Expected: Database not found error
7. `execute_sql_statement` with SELECT (not DML)
   - Expected: "Only DML statements supported" error
8. `execute_sql_statement` with invalid DML syntax
   - Expected: SQL parsing error

**Edge Cases:**
1. `execute_sql_query` with empty result set
   - Expected: Empty data array, no error
2. `execute_sql_query` with NULL values in results
   - Expected: NULLs properly represented

---

### 7) Livy Flow (Session Lifecycle)

**Happy Path:**
1. `livy_create_session` (with_wait=false)
2. `livy_list_sessions` - verify session appears
3. Poll `livy_get_session_status` until `state == "idle"`
4. `livy_get_session_log` (start=0, size=100)
5. `livy_run_statement` - simple print: `print("Hello")`
6. Poll `livy_get_statement_status` until `state == "available"`
7. `livy_run_statement` - Spark operation: `spark.range(10).count()`
8. Poll `livy_get_statement_status` until `state == "available"`
9. `livy_run_statement` - DataFrame operation
10. `livy_cancel_statement` (on a long-running statement, if possible)
11. `livy_close_session`
12. Verify session no longer in `livy_list_sessions`

Expected: Full Livy session lifecycle works correctly.

**Statement Variations:**
1. Simple print: `print("test")`
2. Spark range: `spark.range(100).count()`
3. DataFrame creation: `df = spark.createDataFrame([(1, "a"), (2, "b")], ["id", "name"])`
4. Table read (if tables exist): `spark.sql("SELECT * FROM table LIMIT 5").show()`
5. Multi-line code block

**Negative Tests:**
1. `livy_get_session_status` with non-existent session ID
   - Expected: Session not found (404)
2. `livy_run_statement` on non-existent session
   - Expected: Session not found error
3. `livy_run_statement` on closed session
   - Expected: Session not available error
4. `livy_run_statement` with syntax error in code
   - Expected: Statement completes with error status in output
5. `livy_get_statement_status` with invalid statement ID
   - Expected: Statement not found error
6. `livy_cancel_statement` on completed statement
   - Expected: Error or no-op
7. `livy_close_session` on already closed session
   - Expected: Error or no-op
8. `livy_get_session_log` before session starts
   - Expected: 404 or empty log

**Edge Cases:**
1. Create session with custom Spark config
2. Run multiple statements in sequence (verify ordering)
3. `livy_get_session_log` with pagination (start/size parameters)

Notes:
- Session creation may take 3-6+ minutes due to cluster fallback.
- Record FallbackReasons from `livy_list_sessions` for diagnostics.
- Always close sessions to avoid resource waste.

---

### 8) Concurrent Operations Testing

**Parallel Item Creation:**
1. Create 3 notebooks simultaneously (parallel tool calls)
2. Verify all 3 created successfully
3. Delete all 3 (after user permission)

**Parallel Queries:**
1. Execute 3 different `execute_sql_query` calls simultaneously
2. Verify all return correct results

**Parallel DAX Queries:**
1. Execute 3 different `execute_dax_query` calls simultaneously
2. Verify all return correct results

**Notes:**
- Use parallel tool calls where supported
- Record timing to identify contention issues
- Some operations may not support true concurrency

---

### 9) Cross-Workspace Operations (Optional)

If `FABRIC_TEST_SECONDARY_WORKSPACE_NAME` is configured:

1. `attach_lakehouse_to_notebook` with lakehouse from different workspace
   - Use `lakehouse_workspace_name` parameter
2. `add_notebook_activity_to_pipeline` with notebook from different workspace
   - Use `notebook_workspace_name` parameter
3. Verify cross-workspace references work correctly

---

## Timeouts and Polling
- MCP tool calls may time out around 60 seconds in some environments.
- Prefer asynchronous or polling patterns for long-running operations.
- Use progressive backoff for long waits (e.g., 5s → 10s → 20s → 40s).
- Cap total wait to a sensible limit (10-20 minutes) before cancelling and reporting.
- When `livy_get_session_log` returns 404, keep polling status; logs appear after the
  session starts and the Spark app is running.
- If capacity is inactive, Notebook and Livy flows will fail with `CapacityNotActive`;
  record the failure and skip ahead rather than retrying indefinitely.

## Cleanup
At the end of the test run, **ask the user for permission** before deleting any items.

**Prompt the user:**
> "Comprehensive test run complete. The following items were created:
> - Notebooks: `<list with IDs>`
> - Pipelines: `<list with IDs>`
> - Semantic Models: `<list with IDs>`
>
> Would you like to clean up (delete) these items? (yes/no)"

**If user says YES:**
- `delete_item` for all Notebooks
- `delete_item` for all Pipelines (DataPipeline)
- `delete_item` for all Semantic Models
- `livy_close_session` for any open Livy sessions
- Remove any copy destination tables/data created
- Remove any scratch tables created

**If user says NO:**
- Leave all created items in place
- Report the complete list of items left behind
- Livy sessions should still be closed (ephemeral resources)

## Negative Test Summary

| Flow | Test Cases | Key Validations |
|------|------------|-----------------|
| Discovery | 3 | Invalid workspace, empty name, invalid filter |
| Notebook | 10 | Invalid file, duplicate name, missing dependencies |
| Pipeline | 8 | Duplicate name, invalid activity, missing dependencies |
| Semantic Model | 11 | Invalid types, missing tables/columns (2 skipped - see notes) |
| Power BI | 7 | Invalid model, bad DAX syntax, missing references |
| SQL | 8 | Invalid endpoint, bad SQL, wrong statement type |
| Livy | 8 | Invalid session/statement, closed session, syntax errors |
| **Total** | **55** | |

**Skipped Negative Tests (Fabric API Behavior):**
- `create_semantic_model` with duplicate name: Fabric allows duplicate display names
- `add_measures_to_semantic_model` with invalid DAX: Validation occurs at refresh/query time, not creation

## Expected Failure Modes
- `CapacityNotActive`: capacity paused or inactive.
- `scp` claim missing: some notebook history/log endpoints may require delegated tokens.
- Transient `NotStarted`/`Running` states: continue polling.
- Livy session logs 404 before the session is fully started.
- Power BI REST calls denied due to tenant policy or insufficient dataset permissions.
- Semantic model definition not immediately available after creation; retry with backoff.
- Duplicate item names rejected by Fabric API.
- Invalid DAX/SQL syntax rejected with parsing errors.

## Reporting
Record for each operation:
- Tool name and all parameters
- Status (success/error)
- Key IDs (item_id, job_instance_id, session_id, statement_id)
- Execution time in seconds
- Error message (for negative tests, verify expected error received)
- Any skip or failure reasons

Generate summary report:
- Total operations executed
- Success count / Failure count
- Negative tests: expected errors matched vs unexpected errors
- Performance metrics (min/max/avg execution times per operation type)
- Items created and cleanup status

## Post-Run Checklist
- [ ] All happy path tests executed
- [ ] All negative tests executed with expected errors verified
- [ ] All deleted items recreated before cleanup phase
- [ ] User prompted for cleanup permission
- [ ] If cleanup approved: all created items removed
- [ ] Livy sessions closed (always)
- [ ] If cleanup declined: complete item list reported to user
- [ ] Summary report generated with metrics
- [ ] No unexpected errors encountered
