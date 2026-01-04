# Semantic Model Tools - Design & Implementation Plan

## Goals
- Port semantic model tooling from asa-lib to this MCP server.
- Provide three MCP tools:
  - `create_semantic_model`
  - `add_table_to_semantic_model`
  - `add_relationship_to_semantic_model`
- Add retrieval tools for semantic model metadata and definitions.
- Support richer relationship metadata: `cardinality`, `cross_filter_direction`, `is_active`.
- Add unit + integration tests for semantic model functionality.

## Implementation Status (as of 2026-01-04)
- Implemented:
  - `create_semantic_model`
  - `add_table_to_semantic_model`
  - `add_relationship_to_semantic_model` (uses `fromCardinality`/`toCardinality` + `crossFilteringBehavior` + `isActive`)
  - `get_semantic_model_details` (metadata-only)
  - `get_semantic_model_definition` (definition parts, LRO-aware)
  - Semantic model models/service/tool registration
  - Unit + integration tests for semantic model tools
  - LRO handling updates for create/update definitions
- Planned (not implemented yet):
  - Optional: schema summary extraction (tables/columns/relationships/measures)

## Scope (What’s Included)
- New semantic model service and models.
- New semantic model MCP tool module.
- Tool registration updates and README updates.
- Unit tests and integration tests with env-driven inputs.
- Planned retrieval tools for metadata/definition.

## Non-Goals
- Auto-discovery of columns from SQL endpoints.
- Automatic creation of Dataflow or Lakehouse assets.

## Tool Contracts

### `create_semantic_model`
- Inputs: `workspace_name`, `semantic_model_name`
- Output: status + IDs
- Behavior: creates a SemanticModel item with minimal TMSL parts.

### `add_table_to_semantic_model`
- Inputs: `workspace_name`, `semantic_model_name`, `lakehouse_name`, `table_name`, `columns`
- `columns` is required and must include `{name, data_type}` entries.
- `data_type` values will follow a fixed enum (e.g., `string`, `int64`, `decimal`, `double`, `boolean`, `dateTime`).
- Behavior: inserts DirectLake expression (if missing), appends table + columns + partition.

### `add_relationship_to_semantic_model`
- Inputs: `workspace_name`, `semantic_model_name`, `from_table`, `from_column`, `to_table`, `to_column`,
  optional `cardinality`, `cross_filter_direction`, `is_active`.
- Defaults: `cardinality="manyToOne"`, `cross_filter_direction="oneDirection"`, `is_active=True`.
- Behavior: validates duplicates and appends relationship entry in BIM.
- `cross_filter_direction` maps to TMSL `crossFilteringBehavior`.
- `cardinality` maps to TMSL `fromCardinality`/`toCardinality`.

### `get_semantic_model_details` (planned)
- Inputs: `workspace_name`, `semantic_model_name` (or `semantic_model_id`)
- Output: status + metadata fields (no definition parts).
- Behavior: fetches semantic model metadata only (id, displayName, description, type, workspaceId).
- Rationale: fast, cheap, avoids LRO and large payloads.

### `get_semantic_model_definition` (planned)
- Inputs: `workspace_name`, `semantic_model_name` (or `semantic_model_id`), optional `format` (`TMSL` default, `TMDL`)
- Output: status + definition parts.
- Behavior: calls semantic model definition API with LRO polling and returns parts verbatim.
- Notes:
  - If sensitivity labels are encrypted, definition retrieval may fail; surface a clear error.
  - Consumers can decode and parse `model.bim` (TMSL) or TMDL parts.

## Service Design
- `FabricSemanticModelService` manages TMSL definitions:
  - `create_semantic_model`
  - `add_table_to_semantic_model`
  - `add_relationships_to_semantic_model`
- Uses `get_item_definition(..., format="TMSL")` and `update_item_definition`.
- Keeps `definition.pbism` unchanged; updates `model.bim` only.
- Planned additions:
  - `get_semantic_model_details`:
    - resolve workspace + semantic model id
    - fetch metadata using the semantic model REST endpoint (or item metadata if equivalent)
  - `get_semantic_model_definition`:
    - call semantic model definition endpoint with `wait_for_lro=True`
    - return parts with format metadata

## Tests

### Unit
- Validate TMSL parts creation.
- Validate DirectLake expression + table insertion.
- Validate relationship insertion with metadata.
- Validate duplicate checks.
- Planned: add tests for metadata + definition retrieval tools.

### Integration (env-driven)
- Create a semantic model in a real workspace.
- Add a table with explicit columns.
- Add a relationship when a second table is provided.
- Skip when required env vars are missing.
- Planned: add integration coverage for definition retrieval.

## Environment Variables (Integration)
Proposed additions to `.env.integration.example`:
- `FABRIC_TEST_SEMANTIC_MODEL_TABLE`
- `FABRIC_TEST_SEMANTIC_MODEL_COLUMNS` (JSON list of `{name, data_type}`)
- Optional second table + columns:
  - `FABRIC_TEST_SEMANTIC_MODEL_TABLE_2`
  - `FABRIC_TEST_SEMANTIC_MODEL_COLUMNS_2`

## Step-by-Step Implementation Plan
1) Add `semantic_model` models in `src/ms_fabric_mcp_server/models/semantic_model.py` and export in `models/__init__.py`.
2) Add `FabricSemanticModelService` in `src/ms_fabric_mcp_server/services/semantic_model.py`.
3) Add `semantic_model_tools.py` under `src/ms_fabric_mcp_server/tools/`.
4) Register tools in `src/ms_fabric_mcp_server/tools/__init__.py` and update tool counts.
5) Add unit tests: `tests/fabric/services/test_semantic_model.py`.
6) Add integration tests: `tests/fabric/integration/test_semantic_model_tools_integration.py`.
7) Add env var documentation to `.env.integration.example` and README.
8) Add metadata tool: `get_semantic_model_details`.
9) Add definition tool: `get_semantic_model_definition` (LRO-aware).
10) Add unit + integration tests for new retrieval tools.
