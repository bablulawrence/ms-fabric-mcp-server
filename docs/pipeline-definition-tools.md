# Pipeline definition tools: get/update

## Scope
Add two MCP tools for DataPipeline definitions:
- get_pipeline_definition
- update_pipeline_definition

These extend existing pipeline tooling and use DataPipeline-specific endpoints.

## Tool: get_pipeline_definition

### Inputs
- workspace_name (string, required)
- pipeline_name (string, required)
- format (string, optional; pass-through)

### Behavior
- Resolve workspace_id via FabricWorkspaceService.
- Resolve pipeline_id by name via FabricItemService (type DataPipeline).
- Call POST /workspaces/{workspaceId}/dataPipelines/{dataPipelineId}/getDefinition
  - Include format as query parameter if provided.
  - wait_for_lro=True (block until completion).
- Parse definition.parts:
  - Required: pipeline-content.json
    - Base64 decode, parse JSON, return as pipeline_content_json.
    - If decode or JSON parse fails, raise FabricError.
  - Optional: .platform
    - Base64 decode, attempt JSON parse; if parse fails, return raw string.

### Outputs
- status: "success"
- workspace_name, workspace_id
- pipeline_name, pipeline_id
- pipeline_content_json (dict)
- platform (dict or string, optional)

## Tool: update_pipeline_definition

### Inputs
- workspace_name (string, required)
- pipeline_name (string, required)
- pipeline_content_json (dict, required)
- platform (dict, optional)
- update_metadata (bool, optional, default False)

### Behavior
- Resolve workspace_id and pipeline_id (same as get).
- Base64-encode pipeline_content_json.
- If platform provided, base64-encode platform.
- Call POST /workspaces/{workspaceId}/dataPipelines/{dataPipelineId}/updateDefinition
  - Include updateMetadata=true as query parameter when update_metadata is True.
  - wait_for_lro=True (block until completion).

### Outputs
- status: "success"
- workspace_name, workspace_id
- pipeline_name, pipeline_id

## Error handling
- Missing pipeline-content.json: FabricError.
- Invalid base64 or JSON parsing (pipeline content): FabricError.
- Invalid input types: FabricValidationError.
- API failures: FabricAPIError.

## Implementation plan
- Add service methods to src/ms_fabric_mcp_server/services/pipeline.py:
  - get_pipeline_definition(workspace_id, pipeline_id, format=None)
  - update_pipeline_definition(workspace_id, pipeline_id, pipeline_content_json, platform=None, update_metadata=False)
- Add tool registrations in src/ms_fabric_mcp_server/tools/pipeline_tools.py:
  - get_pipeline_definition(workspace_name, pipeline_name, format=None)
  - update_pipeline_definition(workspace_name, pipeline_name, pipeline_content_json, platform=None, update_metadata=False)
- Reuse existing base64 helpers (_encode_definition/_decode_definition).
- Add tests covering:
  - LRO handling and endpoint construction
  - Missing pipeline-content.json error
  - .platform parse fallback
  - update payload shape with/without update_metadata

## References
- https://learn.microsoft.com/en-us/rest/api/fabric/datapipeline/items/get-data-pipeline-definition
- https://learn.microsoft.com/en-us/rest/api/fabric/datapipeline/items/update-data-pipeline-definition
- https://learn.microsoft.com/en-us/rest/api/fabric/articles/item-management/definitions/datapipeline-definition
