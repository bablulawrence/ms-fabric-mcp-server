# Testing Design and Implementation Plan

## Purpose
This document is the **source of truth** for the testing effort. It captures all decisions, priorities, and detailed test cases needed so a future implementation session can proceed without this chat history.

## High-Level Intent
- Improve **unit test coverage**, starting with services, then tools.
- Initial coverage threshold: **75%** (with a path to raise later).
- Maintain current test style and use deterministic mocks.

## Confirmed Decisions
- **Priority order (services)**: Notebook → Livy → SQL → Job → Item → Pipeline → Workspace.
- **Coverage depth**: Default to happy path + 2–3 key error cases; go deeper for Notebook, Livy, SQL.
- **Mocking**: Mixed approach — use existing factories for common data/response shapes and explicit `Mock()` for edge cases.
- **Private helpers**: Test private helpers when they contain meaningful logic.
- **Test style**: Class-based tests with `@pytest.mark.unit` to match existing suite.
- **Coverage enforcement**: Keep `--cov` reporting in `pyproject.toml` addopts, but **do not** enforce `--cov-fail-under` on all local runs. Enforce the 75% threshold in CI or via an explicit test command.

## Current Repository Context (as of this doc)
- Existing tests:
  - HTTP client: `tests/fabric/client/test_http_client.py`
  - Pipeline service: `tests/fabric/services/test_pipeline.py`
  - Workspace tools registration: `tests/fabric/tools/test_workspace_tools.py`
- Shared factories: `tests/fixtures/mocks.py` (FabricDataFactory, ServiceMockFactory, MockResponseFactory).
- Services to cover: `src/ms_fabric_mcp_server/services/*.py`.

## Detailed Test Plan (Service-by-Service)

### 1) Notebook Service (`FabricNotebookService`)
**New file**: `tests/fabric/services/test_notebook.py`

**Helpers/fixtures**
- Use `tmp_path` for creating real `.ipynb` files (byte content).
- Patch `time.sleep` to avoid real delays.
- Use `FabricDataFactory.notebook_definition()` for response shapes where useful, but ensure the `parts[*].path` ends with `.ipynb` when testing `get_notebook_content` (either extend the factory to accept a `path` parameter or build inline response dicts).

**Tests for private helpers**
- `_resolve_notebook_path`
  - Absolute path → returned unchanged.
  - Relative path + `repo_root` set → joins `repo_root`.
  - Relative path + no `repo_root` → resolves to cwd absolute path.
- `_encode_notebook_file`
  - Success with non-empty file (ensure base64 output).
  - Missing file → raises `FileNotFoundError`.
  - Empty file → raises `ValueError`.
  - Unexpected error (simulate open/read failure) → raises `FabricError`.
- `_create_notebook_definition`
  - Includes `displayName`, `type`, `definition.parts[*]` with payload.
  - `description` only included when provided.

**Public method tests**
- `import_notebook`
  - Success: workspace resolve → item_service.create_item → returns `ImportNotebookResult(status="success")`.
  - Error mapping: `FabricItemNotFoundError`, `FabricValidationError`, `FileNotFoundError`, `ValueError` → `status="error"`.
  - Unexpected exception → `status="error"` with "Unexpected error" prefix.
- `get_notebook_content`
  - **200 response**: `response.json()` with `definition.parts` containing `.ipynb` payload → returns decoded notebook content dict.
  - **202 LRO response** (Location present):
    - Poll returns 200 with `status="Succeeded"` → GET `{location}/result` → returns decoded notebook content.
    - Poll returns 200 with `status="Failed"` → raises `FabricError`.
    - Poll returns 202 repeatedly → eventually timeout (after max retries) → raises `FabricError`.
    - 202 without Location → raises `FabricError`.
  - **No .ipynb part** → returns raw definition response.
- `list_notebooks`
  - Returns list from `item_service.list_items` filtered by "Notebook".
- `get_notebook_by_name` / `update_notebook_metadata` / `delete_notebook`
  - Verify correct delegation to item/workspace services (minimal coverage; no deep logic).
- `execute_notebook`
  - Patch `FabricJobService` in `ms_fabric_mcp_server.services.notebook` to intercept the internal instantiation.
  - Test success path (job created + wait returns) and error path (job creation fails / API error mapping).
- `attach_lakehouse_to_notebook`
  - Treat as primary test focus: LRO handling, metadata dependency mutation, lakehouse in same vs different workspace, missing lakehouse, update failure.
- `get_notebook_execution_details`, `list_notebook_executions`, `get_notebook_driver_logs`
  - Add tests for success path + 1–2 error cases (API error mapping), focusing on parameter shaping and response mapping.

### 2) Livy Service (`FabricLivyService`)
**New file**: `tests/fabric/services/test_livy.py`

**Key behaviors**
- Patch `time.sleep` and time progression to avoid delays.

**Tests**
- `create_session`
  - Payload includes `kind` and `conf`.
  - If `environment_id` provided, injects `spark.fabric.environmentDetails` JSON into conf.
  - `with_wait=True` delegates to `wait_for_session`.
  - `with_wait=False` returns immediate session response.
  - API error → `FabricLivySessionError`.
- `list_sessions`, `get_session_status`, `close_session`, `get_session_log`
  - Success path and API error mapping.
- `run_statement`
  - Payload includes `code` and `kind`.
  - `with_wait=True` uses `wait_for_statement`.
  - API error → `FabricLivyStatementError`.
- `get_statement_status`, `cancel_statement`
  - Success path and API error mapping.
- `wait_for_session`
  - `idle` → returns.
  - `error/dead/killed` → `FabricLivySessionError` (cover log extraction when logs present, and empty-log fallback).
  - timeout → `FabricLivyTimeoutError`.
- `wait_for_statement`
  - `available` → returns.
  - `error/cancelled` → `FabricLivyStatementError`.
  - timeout → `FabricLivyTimeoutError`.

### 3) SQL Service (`FabricSQLService`)
**New file**: `tests/fabric/services/test_sql.py`

**Important constraints**
- `pyodbc` is optional; tests must work even if it is not installed.
- Patch `pyodbc` and `AzureCliCredential` where needed.

**Tests**
- `__init__`
  - If `PYODBC_AVAILABLE` false → raises `ImportError`.
- `get_sql_endpoint`
  - Warehouse path: endpoint uses `workspaces/{id}/warehouses/{item.id}` and reads `properties.connectionString`.
  - Lakehouse path: endpoint uses `workspaces/{id}/lakehouses/{item.id}` and reads `properties.sqlEndpointProperties.connectionString`.
  - Invalid `item_type` → `ValueError`.
  - Missing connection string → `FabricError`.
- `_get_token_bytes`
  - Success: returns bytes with length prefix (mock AzureCliCredential token).
  - Failure: raises `FabricConnectionError`.
- `connect`
  - Appends `,1433` when port is absent.
  - Calls `pyodbc.connect` with `attrs_before` containing token bytes.
  - Failure: raises `FabricConnectionError`.
- `execute_query`
  - Requires an existing `_connection` (set a mock connection directly).
  - Not connected → `FabricConnectionError`.
  - Success: returns `QueryResult(status="success")` with columns/rows mapped into dicts.
  - Exception: returns `QueryResult(status="error")` with message.
- `execute_statement`
  - Not connected → `FabricConnectionError`.
  - Success: commits and returns `status="success"` with affected rows.
  - Failure: attempts rollback, returns `status="error"`.
- `execute_sql_query` / `execute_sql_statement`
  - Wrapper behavior: `connect()` called, `execute_*()` invoked, and `close()` called in `finally` (including on errors).
- `get_tables`, `get_table_schema`
  - Success uses `execute_query` data; error returns empty list.
- `is_connected`
  - True when connection and simple query succeed; False otherwise.
- `close`
  - Closes connection and resets `_connection` even on close exceptions.

### 4) Job Service (`FabricJobService`)
**New file**: `tests/fabric/services/test_job.py`

**Tests**
- `run_on_demand_job`
  - Success: extracts `Location` header, parses job instance ID from URL, parses `Retry-After`.
  - API error or not found → `RunJobResult(status="error")`.
  - Unexpected exception → `status="error"` with "Unexpected error" prefix.
- `get_job_status`
  - Success: maps JSON into `FabricJob` with correct fields.
  - API error → `JobStatusResult(status="error")`.
- `get_job_status_by_url`
  - Valid URL parsed to endpoint; invalid URL yields error result.
  - Invalid examples to cover: missing job ID segment, malformed URL (e.g., `not-a-url`), insufficient path depth.
- `wait_for_job_completion` / `wait_for_job_completion_by_url`
  - Simulate transitions: `InProgress` → `Completed` returns success.
  - `Failed` returns error (ensure failure reason surfaced).
  - Timeout path returns error result.

### 5) Item Service (`FabricItemService`)
**New file**: `tests/fabric/services/test_item.py`

**Tests**
- `_validate_item_type`
  - Accepts known types; rejects unknown types with `FabricValidationError`.
- `list_items`
  - Returns list of `FabricItem` objects, handles item_type filter.
- `get_item_by_name`
  - Returns match when present; raises `FabricItemNotFoundError` when not found.
- `get_item_by_id`, `get_item_definition`, `create_item`, `update_item`, `delete_item`
  - Verify correct endpoint usage and response mapping.
  - Error mapping to Fabric exceptions where used.
- `create_lakehouse`
  - Minimal coverage if still present (ensure endpoint/payload correctness).

### 6) Pipeline Service (`FabricPipelineService`)
**Existing file**: `tests/fabric/services/test_pipeline.py` (extend)

**Add tests**
- `_validate_pipeline_inputs` for each required field missing (pipeline_name, source_type, source_connection_id, source_schema, source_table, destination_lakehouse_id, destination_connection_id, destination_table).
- `_get_source_dataset_type` maps known types, derives `Source` → `Table`, and **falls back to passthrough** for unknown types.
- `_encode_definition` / `_decode_definition` round‑trip success; invalid decode raises appropriate error.
- `create_blank_pipeline`, `add_copy_activity_to_pipeline`, `add_activity_from_json`:
  - Success path payload and response mapping.
  - Error path mapping (validation or API errors).

### 7) Workspace Service (`FabricWorkspaceService`)
**Existing file**: `tests/fabric/services/test_workspace_service.py` (extend)

**Tests**
- `list_workspaces` maps JSON into `FabricWorkspace` objects.
- `get_workspace_by_name` returns match; raises not found.
- `resolve_workspace_id` accepts ID or name and returns ID.
- `create_workspace` success + error mapping.
- `delete_workspace` (if present in service) uses correct endpoint and error mapping.

## Coverage Enforcement Configuration
- `pyproject.toml` keeps coverage reporting in `addopts` (strict markers/config, verbose, `--cov=src/ms_fabric_mcp_server`, `--cov-report=term-missing`, `--cov-report=html`).
- Do **not** add `--cov-fail-under=75` to `addopts` to avoid failing focused local runs.
- Enforce 75% in CI or with an explicit command when desired.

## Implementation Sequence (Exactly)
1) Add `tests/fabric/services/test_notebook.py`.
2) Add `tests/fabric/services/test_livy.py`.
3) Add `tests/fabric/services/test_sql.py`.
4) Add `tests/fabric/services/test_job.py`.
5) Add `tests/fabric/services/test_item.py`.
6) Extend `tests/fabric/services/test_pipeline.py`.
7) Extend `tests/fabric/services/test_workspace_service.py`.
8) Keep coverage reporting in `pyproject.toml`; enforce 75% threshold in CI or optional local command.

## Status Tracker
| Task | Status |
| --- | --- |
| Phase 1: Notebook tests | Completed |
| Phase 2: Livy tests | Completed |
| Phase 3: SQL tests | Completed |
| Phase 4: Job tests | Completed |
| Phase 5: Item tests | Completed |
| Phase 6: Pipeline tests (extend) | Completed |
| Phase 7: Workspace tests | Completed |
| Coverage enforcement (75%) | Completed (CI/optional only) |
| Additional: CLI/server entrypoint tests | Completed |

## Implementation Updates (Phases 1–7 + Additions)
### Phase 1: Notebook Service
- Added LRO in-progress polling coverage (`Running`/202 paths) in `test_get_notebook_content_lro_in_progress`.
- Added coverage for LRO success/failure/timeout/missing-location, raw-definition fallback, and driver log edge cases.
- Added safe `Retry-After` parsing coverage for non-integer values in LRO polling.

### Phase 2: Livy Service
- Implemented full CRUD + wait behavior tests (session/statement) with payload verification and error mapping.

### Phase 3: SQL Service
- Added success-path close behavior tests for `execute_sql_query` and `execute_sql_statement`.
- Disabled OpenTelemetry instrumentation in the SQL test fixture to avoid mock connection attribute issues.
- Fixed token scope to `https://database.windows.net/.default` and asserted in tests.

### Phase 4: Job Service
- Added early-exit coverage when polling receives a `status="error"` result.
- Updated service implementation to use timezone-aware UTC timestamps in wait loops to remove `datetime.utcnow()` deprecation warnings.
- Added URL structure validation for `get_job_status_by_url` to avoid parsing unrelated responses.
- Added coverage for terminal job statuses without `end_time_utc`.
- Added error handling when `run_on_demand_job` returns no `Location` header.

### Phase 5: Item Service
- Added coverage for `get_item_definition` re-raising `FabricAPIError`.
- Added 202-accepted branch when response has no `id` (returns placeholder item).

### Phase 6: Pipeline Service
- Expanded validation tests to cover each required field and error mapping.
- Added `_encode_definition` / `_decode_definition` round-trip tests (including invalid decode).
- Covered `create_blank_pipeline`, `add_copy_activity_to_pipeline`, and `add_activity_from_json` success/error paths.
- **Behavior decision**: `_get_source_dataset_type` remains permissive for unknown types (passthrough), while still mapping known types and deriving `Source` → `Table`.

### Phase 7: Workspace Service
- Added coverage for API errors in `list_workspaces` and `create_workspace`.
- Added `get_workspace_by_id` success/not-found coverage.
- Verified `resolve_workspace_id` with ID vs name.
- Added `delete_workspace` success/error coverage.

### Coverage Enforcement
- Coverage **reporting** remains enabled in `pyproject.toml`.
- Global `--cov-fail-under=75` is **not** enforced in addopts to keep local focused runs usable.
- 75% threshold should be enforced in CI or via explicit command.

### CLI/Server Additions
- Added CLI tests: `tests/fabric/test_cli.py`.
- Added server factory tests: `tests/fabric/test_server.py`.

### Test Harness
- Updated `tests/conftest.py` to prepend local `src/` to `sys.path` so tests execute against the working tree instead of any installed package.

## Notes for the Next Session
- Use `tests/fixtures/mocks.py` for common shapes; add helpers only if repetition appears.
- Patch `time.sleep` and time progression in polling tests to keep runs fast.
- Keep tests deterministic and mock-only; do not hit real Fabric APIs.
- Align with existing `@pytest.mark.unit` usage and class-based test organization.
- For `get_notebook_content`, ensure `.ipynb` parts are used in mocked definitions.
- For `execute_notebook`, patch `FabricJobService` at the class import path inside the notebook service.
