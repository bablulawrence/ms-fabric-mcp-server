# MCP Tools Interface Cleanup Plan

Date: 2026-02-01

> Note: This is a historical design plan; some tool names below reflect pre-rename interfaces.

## Goals
- Simplify and standardize tool names for notebook and pipeline operations.
- Replace single-purpose notebook tools with a clean definition-based interface.
- Reduce parameter naming ambiguity (e.g., display_name vs name).
- Keep functional coverage while allowing breaking changes (beta phase).

## Scope
### Notebook tools
- Rename tools for clarity and consistency.
- Add a generic notebook definition update tool.
- Integrate lakehouse attachment into create/update definition flows.

### Pipeline tools
- Unify pipeline creation into a single tool that accepts optional definitions.

### Parameter naming
- Standardize on `notebook_name` and `item_name` (remove `*_display_name`).
- Add folder path convenience for item moves.

## Proposed Changes (Breaking)

### Notebook Tools
1. **Rename** `import_notebook_to_fabric` → `create_notebook`
   - Accepts a notebook definition (ipynb JSON) as input.
   - Optional `folder_path` and `description` preserved.

2. **Remove** `attach_lakehouse_to_notebook`
   - Replace with optional `default_lakehouse_name` (+ optional `lakehouse_workspace_name`) on:
     - `create_notebook`
     - `update_notebook_definition`
   - Default behavior: if not provided, no changes to dependencies.

3. **Add** `update_notebook_definition`
   - Inputs: `workspace_name`, `notebook_name`, `notebook_content` (JSON dict).
   - Optional: `default_lakehouse_name`, `lakehouse_workspace_name`.
   - Behavior:
     - Fetch notebook definition in ipynb format.
     - Preserve existing `metadata.dependencies` unless lakehouse args provided.
     - Replace ipynb payload with supplied content.
     - Update definition via `updateDefinition`.

4. **Rename** `get_notebook_content` → `get_notebook_definition`
   - Return full definition as today.

5. **Rename** `get_notebook_execution_details` → `get_notebook_run_details`
6. **Rename** `list_notebook_executions` → `list_notebook_runs`

7. **Parameter normalization**
   - Use `notebook_name` consistently (no `notebook_display_name`).

### Pipeline Tools
8. **Replace** `create_blank_pipeline` + `create_pipeline_with_definition` with `create_pipeline`
   - Parameters:
     - `workspace_name`, `display_name`, `description`, `folder_path`
     - Optional `pipeline_content_json`
     - Optional `platform`
   - Behavior:
     - If `pipeline_content_json` is omitted, create a blank pipeline.
     - If provided, create with definition.

### Item Tool Parameter Cleanup
9. **Rename** `item_display_name` → `item_name` in `get_item` and `delete_item` tools.
10. **Add** `target_folder_path` to `move_item_to_folder` to avoid folder IDs.

## Implementation Outline

### Notebook
- Add new service method: `update_notebook_definition` in `services/notebook.py`.
  - Reuse `getDefinition?format=ipynb` logic already used in `attach_lakehouse_to_notebook`.
  - Apply lakehouse metadata patch only if `default_lakehouse_name` provided.
  - Encode updated ipynb payload and call `updateDefinition`.
- Update tools in `tools/notebook_tools.py`:
  - Replace `import_notebook_to_fabric` tool with `create_notebook`.
  - Remove `attach_lakehouse_to_notebook` tool.
  - Add `update_notebook_definition` tool.
  - Rename `get_notebook_content`, `get_notebook_execution_details`, `list_notebook_executions`.
  - Update parameter names to `notebook_name`.
- Update tool registry and counts in `tools/__init__.py` and `README.md`.

### Pipeline
- Add new service method `create_pipeline` or adapt existing `create_blank_pipeline` and `create_pipeline_with_definition` to a single entry point in the tool layer.
- Deprecate/remove old tool functions.
- Update registry and docs.

### Item Tools
- Rename params in `get_item`, `delete_item` to `item_name` (keep `item_id` as alt).
- Add optional `target_folder_path` to `move_item_to_folder` and resolve folder ID via existing folder path resolution.

## Tests
- Update unit tests for notebook tools/services:
  - `create_notebook` happy path.
  - `update_notebook_definition` happy path with/without lakehouse metadata.
  - Rename assertions for run tools.
- Update pipeline tool tests:
  - `create_pipeline` blank and with definition.
- Update item tool tests:
  - `get_item`, `delete_item` parameter rename.
  - `move_item_to_folder` with `target_folder_path`.

## Migration Notes (Doc)
- Provide a short migration guide in `docs/` explaining name changes.
- Update README tool list and counts.

## Open Questions
- Confirm default behavior for `update_notebook_definition`:
  - Preserve metadata unless lakehouse args provided (recommended).
- Confirm `create_notebook` input format:
  - JSON dict vs raw string (recommended: JSON dict).
