# Pipeline Notebook/Dataflow Activities - Design & Implementation Plan

## Goals
- Add two convenience MCP tools for Microsoft Fabric pipelines:
  - `add_notebook_activity_to_pipeline`
  - `add_dataflow_activity_to_pipeline`
- Provide dependency chaining via `depends_on_activity_name` with validation.
- Keep the existing generic JSON activity tool for advanced/unknown activity types.
- Maintain compatibility with existing pipeline tools and patterns.

## Scope (What’s Included)
- New service methods to append Notebook and Dataflow activities to a pipeline definition.
- New MCP tools that expose the service methods with optional retry/timeout controls.
- Unit tests for activity insertion, dependency validation, and payload structure.

## Non-Goals
- Removing or changing existing tools (`add_activity_to_pipeline` remains).
- Enforcing stricter source-type validation in Copy activities.
- Adding semantic model tools in this phase.

## Design Summary

### New Tools (MCP)
Add to `src/ms_fabric_mcp_server/tools/pipeline_tools.py`:
- `add_notebook_activity_to_pipeline`
  - Required: `workspace_name`, `pipeline_name`, `notebook_name`
  - Optional: `notebook_workspace_name`, `activity_name`, `depends_on_activity_name`,
    `session_tag`, `parameters`, `timeout`, `retry`, `retry_interval_seconds`
  - Default activity name: `RunNotebook_<notebook_name>`
- `add_dataflow_activity_to_pipeline`
  - Required: `workspace_name`, `pipeline_name`, `dataflow_name`
  - Optional: `dataflow_workspace_name`, `activity_name`, `depends_on_activity_name`,
    `timeout`, `retry`, `retry_interval_seconds`
  - Default activity name: `RunDataflow_<dataflow_name>`

### Service Layer
Add to `src/ms_fabric_mcp_server/services/pipeline.py`:
- `add_notebook_activity_to_pipeline(...) -> str` (returns pipeline id)
- `add_dataflow_activity_to_pipeline(...) -> str`

Both will:
1) Accept resolved workspace IDs from the tool layer (pipeline workspace and optional notebook/dataflow workspace).
2) Resolve item IDs for pipeline + notebook/dataflow.
2) Fetch pipeline definition and decode `pipeline-content.json`.
3) Validate duplicate activity names and dependency existence.
4) Append activity to `properties.activities`.
5) Encode and update definition via `updateDefinition` endpoint.

### Activity Payloads
Notebook activity:
- `type`: `TridentNotebook`
- `typeProperties`: `notebookId`, `workspaceId`, `sessionTag` (optional), `parameters` (optional)

Dataflow activity:
- `type`: `RefreshDataflow`
- `typeProperties`: `dataflowId`, `workspaceId`, `notifyOption: "NoNotification"`,
  `dataflowType: "Dataflow-Gen2"`

Dependencies:
- When `depends_on_activity_name` is set, add:
  - `dependsOn: [{"activity": <name>, "dependencyConditions": ["Succeeded"]}]`
- Validation will ensure the dependency exists in the current activity list.

## Implementation Plan

1) Service Layer Updates
- File: `src/ms_fabric_mcp_server/services/pipeline.py`
- Add helpers to:
  - Load/Decode pipeline definition
  - Validate activity name uniqueness
  - Validate dependency existence
  - Update definition (encode + updateDefinition)
- Implement:
  - `add_notebook_activity_to_pipeline(...)` (accepts pipeline workspace id + optional notebook workspace id)
  - `add_dataflow_activity_to_pipeline(...)` (accepts pipeline workspace id + optional dataflow workspace id)

2) Tool Layer Updates
- File: `src/ms_fabric_mcp_server/tools/pipeline_tools.py`
- Add new MCP tool functions with `@mcp.tool` and `@handle_tool_errors`.
- Resolve workspace IDs (pipeline workspace + optional notebook/dataflow workspace) via `workspace_service`.
- Wire to the service methods and return structured responses consistent with existing tools.

3) Tests
- File: `tests/fabric/services/test_pipeline.py`
- Add tests for:
  - Notebook activity insertion
  - Dataflow activity insertion
  - Dependency validation failure (missing dependency)
  - Duplicate activity name handling
  - Payload shape assertions (`type`, `typeProperties`)

4) (Optional) Docs
- Update README/tool count if desired after implementation.

## Step-by-Step Checklist
1) Update service helpers in `src/ms_fabric_mcp_server/services/pipeline.py` to:\n   - Load/Decode pipeline definition once\n   - Validate duplicate activity names\n   - Validate `depends_on_activity_name` exists\n   - Re-encode and update definition\n2) Implement `add_notebook_activity_to_pipeline` in the service:\n   - Resolve pipeline + notebook item IDs\n   - Build TridentNotebook activity payload\n   - Apply dependency if provided\n   - Append and update definition\n3) Implement `add_dataflow_activity_to_pipeline` in the service:\n   - Resolve pipeline + dataflow item IDs\n   - Build RefreshDataflow activity payload\n   - Apply dependency if provided\n   - Append and update definition\n4) Add MCP tools in `src/ms_fabric_mcp_server/tools/pipeline_tools.py`:\n   - Resolve workspace IDs\n   - Call service methods\n   - Return structured response\n5) Add unit tests in `tests/fabric/services/test_pipeline.py` for:\n   - Notebook activity payload correctness\n   - Dataflow activity payload correctness\n   - Dependency validation errors\n   - Duplicate activity name errors\n6) (Optional) Update README/tool counts after confirming tests pass.

## Risks & Mitigations
- Dependency validation prevents forward references (intentional for now). Users can
  always use the generic JSON tool for advanced sequencing needs.
- If pipeline definition is missing `pipeline-content.json`, the service will raise
  a clear error (consistent with current behavior).
