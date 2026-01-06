# Semantic Model Refresh, DAX, and Measures Tools (Design)

This document captures the design for the remaining semantic model tools:
- Refresh semantic model
- Execute DAX query
- Add measures
- Delete measures

The design explicitly separates Fabric REST and Power BI REST usage.

## 1) API split, auth, and identification

**API split**
- **Fabric REST**: semantic model definition changes (measure add/delete).
- **Power BI REST**: semantic model refresh and DAX query execution.

**Rationale**
- Fabric SemanticModel REST supports get/update definition (TMSL/TMDL) but does not expose refresh or DAX query endpoints.
- Power BI REST exposes dataset refresh and executeQueries endpoints.

**Auth/config**
- Keep Fabric REST config as-is.
- Add Power BI REST config:
  - `POWERBI_BASE_URL` (default: `https://api.powerbi.com/v1.0/myorg`)
  - `POWERBI_SCOPES` (default: `https://analysis.windows.net/powerbi/api/.default`)
  - Optional `POWERBI_API_CALL_TIMEOUT`
- Use the same credential flow as Fabric, but request Power BI scopes for Power BI endpoints.

**Identification**
- All tools accept either `semantic_model_name` or `semantic_model_id`.
- Resolve `workspace_id` using workspace name (groupId in Power BI).
- Resolve `semantic_model_id` using name or id (datasetId in Power BI).

## 2) Refresh tool (Power BI REST)

**Tool name**
- `refresh_semantic_model`

**Inputs**
- `workspace_name`
- `semantic_model_name` or `semantic_model_id`
- `type` (optional, Power BI refresh type)
- `objects` (optional, list of table/partition objects for partial refresh)

**Behavior**
- Resolve `workspace_id` and `semantic_model_id`.
- POST refresh request.
- **Poll until completion** (per user requirement):
  - Poll refresh history on an interval (configurable, e.g., 5-10s).
  - Stop on terminal state: Completed / Failed / Cancelled.
  - Time out with a clear error if not complete by configured timeout.

**Notes**
- `objects` entries follow Power BI refresh schema: `{ "table": "TableName", "partition": "PartitionName" }`.
- The polling endpoint uses the dataset refresh history to find the submitted refresh.

**Output**
- `status`, `refresh_id`
- `start_time`, `end_time`
- final `refresh_status`
- error payload when failed

## 3) DAX query tool (Power BI REST)

**Tool name**
- `execute_dax_query`

**Inputs**
- `workspace_name`
- `semantic_model_name` or `semantic_model_id`
- `query` (single DAX query string)

**Behavior**
- Resolve `workspace_id` and `semantic_model_id`.
- POST to executeQueries endpoint with `queries: [{ query: "<DAX>" }]`.
- **Return raw response** (per user requirement).

**Output**
- Raw Power BI `executeQueries` response (no transformation).

**Notes**
- Requires tenant setting allowing execute queries and dataset Build permission.
- Errors are returned verbatim for clarity.
 - Optional `POWERBI_API_CALL_TIMEOUT` can cap request duration.

## 4) Measure tools (Fabric REST via definition update)

**Tool names**
- `add_measures_to_semantic_model`
- `delete_measures_from_semantic_model`

**Inputs for add**
- `workspace_name`
- `semantic_model_name` or `semantic_model_id`
- `table_name`
- `measures` list; each entry:
  - `name` (required)
  - `expression` (required)
  - `format_string` (optional)
  - `display_folder` (optional)
  - `description` (optional)

**Add behavior**
- Fetch TMSL definition and decode `model.bim`.
- Locate table by `table_name`.
- Validate no duplicate measure names.
- Append measures to `table["measures"]`.
- Update definition via `updateDefinition` (LRO-aware).

**Inputs for delete**
- `workspace_name`
- `semantic_model_name` or `semantic_model_id`
- `table_name`
- `measure_names` list

**Delete behavior**
- Fetch TMSL definition and decode `model.bim`.
- Locate table by `table_name`.
- Remove measures with matching names.
- Error if any requested measure does not exist (default behavior).
- Update definition via `updateDefinition`.

**Output**
- `status`, `semantic_model_id`, `semantic_model_name`
- `table_name`
- `added`/`deleted` list and counts

## 5) Integration points, tests, and docs

**Service and tools**
- Add Power BI API client support (separate base URL and scopes).
- Extend `FabricSemanticModelService` with `add_measures` and `delete_measures`.
- Add/extend tool registration for:
  - `refresh_semantic_model`
  - `execute_dax_query`
  - `add_measures_to_semantic_model`
  - `delete_measures_from_semantic_model`
- Update tool counts in README and tool registry logs.

**Tests**
- Unit tests for measure add/delete:
  - Ensure measures are appended/removed in the TMSL payload.
  - Validate duplicate/missing handling.
- Unit tests for Power BI calls:
  - Verify correct endpoints and payloads.
  - Mock responses for success/failure.
- Integration tests are optional depending on Power BI tenant permissions.

**Documentation**
- Update semantic model design doc with these tools.
- Document Power BI env vars and required permissions.
