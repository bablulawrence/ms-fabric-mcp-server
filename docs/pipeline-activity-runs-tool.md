# Pipeline Activity Runs Tool - Design & Implementation Plan

## Problem Statement

When an AI agent runs a Fabric Data Pipeline via `run_on_demand_job` and checks status with `get_job_status`, it can only see whether the overall job succeeded or failed. There's no visibility into per-activity details (which activities ran, duration, row counts, errors). The only workaround is external SQL probing, which is indirect and doesn't help diagnose failures.

## Solution

Add a new tool `get_pipeline_activity_runs` that queries the Fabric `queryactivityruns` API to retrieve per-activity execution details for a given pipeline run.

---

## Tool Specification

**Tool Name:** `get_pipeline_activity_runs`  
**Title:** "Get Pipeline Activity Runs"  
**Location:** `src/ms_fabric_mcp_server/tools/pipeline_tools.py`

### Parameters

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `workspace_name` | str | Yes | Display name of the workspace |
| `job_instance_id` | str | Yes | The job instance ID from `run_on_demand_job` or `get_job_status` |
| `status_filter` | str | No | Filter by status: "Succeeded", "Failed", "InProgress", "Cancelled" |
| `activity_name` | str | No | Filter to a specific activity by name |

### Response Structure

```python
{
    "status": "success",
    "message": "Retrieved 3 activity runs",
    "workspace_name": "My Workspace",
    "job_instance_id": "abc-123-...",
    "pipeline_name": "Copy_Data_Pipeline",
    "activity_count": 3,
    "activities": [
        {
            "activity_name": "CopyProducts",
            "activity_type": "Copy",
            "status": "Succeeded",
            "duration_ms": 5230,
            "start_time": "2026-02-03T13:00:00Z",
            "end_time": "2026-02-03T13:00:05Z",
            "rows_read": 1234,
            "rows_written": 1234,
            "error_message": None
        },
        ...
    ]
}
```

### API Endpoint

```
POST https://api.fabric.microsoft.com/v1/workspaces/{workspaceId}/datapipelines/pipelineruns/{jobId}/queryactivityruns
```

Request body:
```json
{
  "lastUpdatedAfter": "<7 days ago>",
  "lastUpdatedBefore": "<now + 1 day>",
  "filters": [
    {"operand": "Status", "operator": "Equals", "values": ["Failed"]},
    {"operand": "ActivityName", "operator": "Equals", "values": ["CopyProducts"]}
  ]
}
```

### Data Extraction

| Curated Field | Source Path | Notes |
|---------------|-------------|-------|
| `activity_name` | `activityName` | Direct |
| `activity_type` | `activityType` | Direct |
| `status` | `status` | Direct |
| `duration_ms` | `durationInMs` | Direct |
| `start_time` | `activityRunStart` | Direct |
| `end_time` | `activityRunEnd` | Direct |
| `rows_read` | `output.rowsRead` | Only for Copy activities |
| `rows_written` | `output.rowsCopied` | Only for Copy activities |
| `error_message` | `error.message` | Only if activity failed |

---

## Implementation Tasks

### Phase 1: Models
- [x] Add `PipelineActivityRun` model to `src/ms_fabric_mcp_server/models/`
- [x] ~~Add `PipelineActivityRunsResult` model to `src/ms_fabric_mcp_server/models/`~~ (not needed - using dict response)
- [x] Export new models from `models/__init__.py`

### Phase 2: Service Layer
- [x] Add `get_pipeline_activity_runs()` method to `FabricPipelineService`
- [x] Implement time window auto-generation (7 days ago → now + 1 day)
- [x] Implement optional filter construction (status, activity_name)
- [x] Implement data extraction logic for curated response

### Phase 3: Tool Layer
- [x] Add `get_pipeline_activity_runs` tool to `pipeline_tools.py`
- [x] Register tool in `register_pipeline_tools()` function

### Phase 4: Unit Tests
- [x] `test_get_pipeline_activity_runs_success`
- [x] `test_get_pipeline_activity_runs_with_status_filter`
- [x] `test_get_pipeline_activity_runs_with_activity_name_filter`
- [x] `test_get_pipeline_activity_runs_failed_activity`
- [x] `test_get_pipeline_activity_runs_non_copy_activity` (covered in success test)
- [x] `test_get_pipeline_activity_runs_error_handling`

### Phase 5: Integration Test
- [ ] `test_get_pipeline_activity_runs_after_execution` - end-to-end test (skipped - requires live Fabric)

### Phase 6: Documentation
- [x] Update README.md tool count (53 → 54)
- [x] Add `get_pipeline_activity_runs` to Pipeline tools table in README.md

### Phase 7: Validation
- [x] Run full test suite (`pytest`)
- [x] Run linters (`black`, `isort`)

---

## Files to Modify/Create

| File | Action |
|------|--------|
| `src/ms_fabric_mcp_server/models/pipeline.py` | Create (new models) |
| `src/ms_fabric_mcp_server/models/__init__.py` | Modify (export new models) |
| `src/ms_fabric_mcp_server/services/pipeline.py` | Modify (add method) |
| `src/ms_fabric_mcp_server/tools/pipeline_tools.py` | Modify (add tool) |
| `tests/fabric/test_pipeline_service.py` | Modify (add unit tests) |
| `tests/fabric/test_pipeline_integration.py` | Modify (add integration test) |
| `README.md` | Modify (update tool count and table) |

---

## Notes

- Time window defaults: 7 days back, 1 day forward (covers typical usage)
- Non-Copy activities return `None` for `rows_read`/`rows_written`
- Succeeded activities return `None` for `error_message`
- Filter operators use `"Equals"` for exact match
