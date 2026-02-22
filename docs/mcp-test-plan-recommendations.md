# MCP Comprehensive Test Plan & Tooling Recommendations

## Context
These recommendations are based on executing the comprehensive MCP test plan end-to-end against the Fabric workspace.
They focus on speeding up future runs and reducing avoidable failures.

## Recommendations: Test Plan Document
1. **Add explicit create/availability waits**
   - **Issue observed:** immediate `get_*`/dependent calls returned “item not found” after create/rename.
   - **Reason:** item creation/rename is asynchronous in Fabric; metadata indexing and list APIs can lag behind create responses, so subsequent reads hit a different service replica that hasn’t seen the update yet.
   - **Recommendation:** after `create_notebook`, `create_pipeline`, `create_semantic_model`, add a short wait + `get_item`/`get_*_details` check before dependent calls.
   - **Benefit:** prevents eventual-consistency failures and reduces retry loops.

2. **Clarify lakehouse file paths**
   - **Issue observed:** deletes failed with `PathNotFound` and listings showed nested `Files/Files/...` after using `Files/` in destination paths.
   - **Reason:** path handling previously allowed duplicated `Files` segments in list/upload flows, which could surface nested-looking results.
   - **Recommendation:** use paths relative to the Files root and verify list/upload expectations with exact-path checks.
   - **Benefit:** avoids misaligned paths and reduces cleanup retries.

3. **Note `move_item_to_folder` root limitation**
   - **Issue observed:** `target_folder_id` is required; root moves failed validation.
   - **Reason:** the API requires a concrete folder ID and the tool does not translate “root” to a null/empty value, so the request schema validation fails before the move is attempted.
   - **Recommendation:** document the limitation and use a root-level folder or skip “move back to root.”
   - **Benefit:** prevents validation errors and wasted steps.

4. **SQL guidance and syntax**
   - **Issue observed:** `SHOW TABLES` failed; `execute_sql_statement` rejected DDL.
   - **Reason:** Fabric SQL Warehouse does not support MySQL-style `SHOW TABLES`, and the tool enforces DML-only statements; DDL is blocked by the API contract.
   - **Recommendation:** use `INFORMATION_SCHEMA.TABLES` and keep `execute_sql_statement` to DML only.
   - **Benefit:** avoids predictable SQL failures.

5. **Semantic model refresh prerequisites**
   - **Issue observed:** refresh failed when extra `sm_types_*` tables lacked access/definitions; DAX failed on unrefreshed tables.
   - **Reason:** DirectLake refresh requires valid table metadata and permissions; adding tables that don’t exist or aren’t readable causes the refresh to fail and leaves the model in a partially loaded state, which then breaks DAX queries.
   - **Recommendation:** add a pre-check to only add accessible tables and refresh before DAX queries.
   - **Benefit:** makes refresh/DAX reliable.

6. **Pipeline validation**
   - **Issue observed:** initial copy target table missing after pipeline run.
   - **Reason:** copy activities complete asynchronously; table registration in the lakehouse can lag the pipeline completion status, so immediate validation queries can observe a missing table.
   - **Recommendation:** add a retry/poll step and document fallback for copy validation.
   - **Benefit:** reduces false negatives and repeat runs.

7. **Explicit negative test expectations**
   - **Issue observed:** multiple negative tests returned different error codes depending on state.
   - **Reason:** Fabric returns different error types for the same logical failure depending on resource state and replication (e.g., `NotFound` vs `BadRequest`), which makes strict matching brittle.
   - **Recommendation:** document expected error patterns to validate outcomes quickly.
   - **Benefit:** faster triage and clearer pass/fail.

## Recommendations: MCP Tools
1. **`move_item_to_folder` root support**
   - **Issue observed:** root move not supported; validation fails when `target_folder_id` is empty.
   - **Reason:** the tool requires a non-empty folder ID and passes it through to the API, so there is no way to express “move to root,” even though the backend can accept a null target.
   - **Recommendation:** allow `target_folder_id` to be `null`/omitted for root.
   - **Benefit:** removes workaround folders and speeds cleanup.

2. **Semantic model management APIs**
   - **Issue observed:** cannot remove or inspect tables/relationships directly; duplicate DirectLake errors required full model deletion.
   - **Reason:** the current tool surface only supports add operations; when a bad table was added, the API returned duplicate DirectLake errors and there was no tool to delete or inspect table state, forcing full model recreation.
   - **Recommendation:** add list/delete endpoints and idempotent add/upsert operations.
   - **Benefit:** enables targeted recovery without rebuilding models.

3. **Semantic model refresh status**
   - **Issue observed:** “model is refreshing” blocked updates with unclear timing.
   - **Reason:** refresh holds an internal lock and the API response does not expose progress or ETA, so subsequent update calls race the refresh and fail repeatedly.
   - **Recommendation:** expose refresh lock state and a `wait_until_refresh_complete` helper.
   - **Benefit:** avoids conflicting updates and reduces retries.

4. **SQL tool reliability**
   - **Issue observed:** intermittent “connection closed” and ambiguous auth/database errors.
   - **Reason:** transient SQL gateway disconnects and token-related auth failures surface with generic errors, making it unclear whether to retry, re-auth, or fix the query/database.
   - **Recommendation:** improve connection reuse and error diagnostics.
   - **Benefit:** fewer transient failures, quicker root-cause analysis.

5. **Pipeline execution visibility**
   - **Issue observed:** pipeline validation required SQL probing without activity-level details.
   - **Reason:** the current tool does not expose per-activity outputs, row counts, or copy diagnostics, so the only validation path is external SQL inspection.
   - **Recommendation:** provide activity run status/outputs.
   - **Benefit:** faster validation and clearer failures.

6. **Notebook lifecycle robustness**
   - **Issue observed:** immediate calls after import/rename returned “not found.”
   - **Reason:** notebook creation/rename completes before all metadata is queryable; the tool immediately calls content/attach APIs that depend on the notebook being indexed.
   - **Recommendation:** add optional wait-until-available behavior.
   - **Benefit:** reduces retry logic and flaky failures.

7. **Lakehouse file path clarity**
   - **Issue observed:** nested `Files/Files/...` from mixed path prefixes.
   - **Reason:** the tool accepts paths verbatim without normalizing or stripping a leading `Files/`, so callers can accidentally create duplicated roots and break later delete/list expectations.
   - **Recommendation:** normalize `destination_path` handling.
   - **Benefit:** prevents accidental directory nesting and failed deletes.

## Operational Tips for Faster Runs
- Batch independent tool calls in parallel where possible (list/discovery, DAX/SQL queries).
- After each create, immediately verify item availability before dependent steps (avoids “not found”).
- Maintain a small, known-good semantic model template to avoid refresh failures.
