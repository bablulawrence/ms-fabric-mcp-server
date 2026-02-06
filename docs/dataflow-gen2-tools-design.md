# Dataflow Gen2 Tools Design

## Overview

This document describes the design for adding Dataflow Gen2 support to the MCP server, enabling AI agents to create, inspect, and run Power Query-based dataflows in Microsoft Fabric.

## Scope

### New Tools (3)

| Tool | Purpose |
|------|---------|
| `create_dataflow` | Create a Dataflow Gen2 item with Power Query M code |
| `get_dataflow_definition` | Retrieve the full definition of an existing dataflow |
| `run_dataflow` | Trigger a dataflow refresh using the dedicated Execute API |

### Existing Tools (no changes needed)

| Tool | Use with Dataflows |
|------|-------------------|
| `delete_item(item_type="Dataflow")` | Delete a dataflow |
| `list_items(item_type="Dataflow")` | List dataflows in a workspace |
| `get_job_status_by_url(location_url)` | Poll dataflow run status |
| `add_dataflow_activity_to_pipeline` | Add dataflow to a pipeline |

## API Background

### Dataflow Definition Structure

Fabric Dataflow Gen2 definitions consist of two parts:

1. **`queryMetadata.json`** — JSON metadata describing queries, connections, and settings
2. **`mashup.pq`** — Power Query M code in section format

Example `queryMetadata.json`:
```json
{
  "formatVersion": "202502",
  "name": "SampleDataflow",
  "computeEngineSettings": {},
  "queryGroups": [],
  "documentLocale": "en-US",
  "queriesMetadata": {
    "MyQuery": {
      "queryId": "a0a0a0a0-bbbb-cccc-dddd-e1e1e1e1e1e1",
      "queryName": "MyQuery",
      "queryGroupId": null,
      "isHidden": false,
      "loadEnabled": true
    }
  },
  "connections": [],
  "fastCombine": false,
  "allowNativeQueries": true,
  "skipAutomaticTypeAndHeaderDetection": false
}
```

Example `mashup.pq`:
```
section Section1;
shared MyQuery = let
    Source = ...
in
    Source;
```

### API Endpoints

| Operation | Endpoint |
|-----------|----------|
| Create dataflow | `POST /workspaces/{workspaceId}/dataflows` |
| Get definition | `GET /workspaces/{workspaceId}/dataflows/{dataflowId}` with `?format=...` |
| Run dataflow | `POST /workspaces/{workspaceId}/dataflows/{dataflowId}/jobs/Execute/instances` |

**Note:** The run endpoint is dataflow-specific and differs from the generic `/items/{itemId}/jobs/instances` endpoint used by other item types.

## Tool Specifications

### `create_dataflow`

Creates a Dataflow Gen2 item with the provided definition.

**Parameters:**

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `workspace_name` | string | Yes | Target workspace display name |
| `dataflow_name` | string | Yes | Display name for the dataflow |
| `mashup_content` | string | Yes | Power Query M code (`mashup.pq` content) |
| `query_metadata` | dict | No | `queryMetadata.json` as dict; auto-generated if omitted |
| `description` | string | No | Optional description |
| `folder_path` | string | No | Optional folder path (e.g., "etl/daily") |

**Returns:**
```python
{
    "status": "success",
    "message": "Dataflow 'CustomerETL' created successfully",
    "dataflow_id": "12345678-...",
    "dataflow_name": "CustomerETL",
    "workspace_name": "Analytics"
}
```

**Example usage:**
```python
create_dataflow(
    workspace_name="Analytics",
    dataflow_name="CustomerETL",
    mashup_content="""
section Section1;
shared Customers = let
    Source = Lakehouse.Contents([]),
    Nav1 = Source{[workspaceId = "..."]}[Data],
    Nav2 = Nav1{[lakehouseId = "..."]}[Data],
    Result = Nav2{[Id = "customers", ItemKind = "Table"]}[Data]
in
    Result;
""",
    query_metadata={
        "formatVersion": "202502",
        "name": "CustomerETL",
        "queriesMetadata": {
            "Customers": {
                "queryId": "a1b2c3d4-...",
                "queryName": "Customers",
                "loadEnabled": True,
                "isHidden": False
            }
        }
    }
)
```

---

### `get_dataflow_definition`

Retrieves the full definition of an existing dataflow.

**Parameters:**

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `workspace_name` | string | Yes | Workspace containing the dataflow |
| `dataflow_name` | string | Yes | Name of the dataflow |

**Returns:**
```python
{
    "status": "success",
    "dataflow_id": "12345678-...",
    "dataflow_name": "CustomerETL",
    "workspace_name": "Analytics",
    "query_metadata": { ... },      # Parsed queryMetadata.json
    "mashup_content": "section..."  # Raw mashup.pq content
}
```

---

### `run_dataflow`

Triggers an on-demand dataflow refresh.

**Parameters:**

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `workspace_name` | string | Yes | Workspace containing the dataflow |
| `dataflow_name` | string | Yes | Name of the dataflow to run |
| `execution_data` | dict | No | Optional parameters for parameterized dataflows |

**Returns:**
```python
{
    "status": "success",
    "message": "Dataflow refresh started",
    "job_instance_id": "abcd1234-...",
    "location_url": "https://api.fabric.microsoft.com/v1/workspaces/.../items/.../jobs/instances/...",
    "retry_after": 30
}
```

**Polling status:** Use `get_job_status_by_url(location_url)` to check completion.

## Architecture

### New Files

| File | Purpose |
|------|---------|
| `src/ms_fabric_mcp_server/services/dataflow.py` | `FabricDataflowService` business logic |
| `src/ms_fabric_mcp_server/tools/dataflow_tools.py` | MCP tool definitions |
| `tests/fabric/test_dataflow_service.py` | Unit tests for service |
| `tests/fabric/test_dataflow_tools.py` | Unit tests for tools |

### Service Class

```python
class FabricDataflowService:
    """Service for Fabric Dataflow Gen2 operations."""
    
    def __init__(
        self,
        client: FabricClient,
        workspace_service: FabricWorkspaceService,
        item_service: FabricItemService,
    ):
        self.client = client
        self.workspace_service = workspace_service
        self.item_service = item_service

    def create_dataflow(
        self,
        workspace_id: str,
        dataflow_name: str,
        mashup_content: str,
        query_metadata: Optional[dict] = None,
        description: Optional[str] = None,
        folder_id: Optional[str] = None,
    ) -> str:
        """Create a Dataflow Gen2 item. Returns dataflow_id."""
        ...

    def get_dataflow_definition(
        self,
        workspace_id: str,
        dataflow_id: str,
    ) -> tuple[dict, str]:
        """Get dataflow definition. Returns (query_metadata, mashup_content)."""
        ...

    def run_dataflow(
        self,
        workspace_id: str,
        dataflow_id: str,
        execution_data: Optional[dict] = None,
    ) -> RunJobResult:
        """Trigger dataflow refresh. Returns job info with location_url."""
        ...
```

### Registration

In `tools/__init__.py`:
```python
from .dataflow_tools import register_dataflow_tools

# In register_fabric_tools():
dataflow_service = FabricDataflowService(client, workspace_service, item_service)
register_dataflow_tools(mcp, dataflow_service)
```

## Design Decisions

### Why raw M code input?

The tool accepts raw Power Query M code (`mashup_content`) rather than a high-level abstraction because:
1. Provides full flexibility for any transformation
2. Matches how notebooks/pipelines work in this codebase
3. AI agents can generate valid M code
4. Avoids building/maintaining a translation layer

### Why a dedicated `run_dataflow` tool?

Dataflow Gen2 uses a different API endpoint (`/dataflows/{id}/jobs/Execute/instances`) than the generic job scheduler (`/items/{id}/jobs/instances`). The dedicated endpoint:
- Supports dataflow-specific parameters
- Is the officially documented approach for Dataflow Gen2
- Will be more stable as the API matures

### Why no destination configuration in create?

Destination configuration (Lakehouse table output) is not part of the dataflow definition in the Fabric API — it's managed separately via the UI or update APIs. Initial implementation creates "transformation only" dataflows; destination configuration can be added in a future iteration.

## Implementation Plan

- [x] Create `FabricDataflowService` in `services/dataflow.py`
  - [x] Implement `create_dataflow()`
  - [x] Implement `get_dataflow_definition()`
  - [x] Implement `run_dataflow()`
- [x] Create `register_dataflow_tools()` in `tools/dataflow_tools.py`
  - [x] Implement `create_dataflow` tool
  - [x] Implement `get_dataflow_definition` tool
  - [x] Implement `run_dataflow` tool
- [x] Register tools in `tools/__init__.py`
- [x] Add unit tests
  - [x] `tests/fabric/services/test_dataflow_service.py`
  - [x] `tests/fabric/tools/test_dataflow_tools.py`
- [x] Update README tool count and table
- [x] Add integration tests
  - [x] `tests/fabric/integration/test_dataflow_tools_integration.py`

## Future Enhancements

- `update_dataflow_definition` — modify existing dataflow
- `set_dataflow_destination` — configure Lakehouse/Warehouse output
- Helper functions to generate `query_metadata` from `mashup_content`
- Support for Warehouse and external destinations
