# Test Plan Review: Comments & Recommendations

**Document**: Review of `testing-design-and-implementation-plan.md`  
**Date**: December 24, 2025  
**Status**: Ready for Implementation (with noted corrections)

---

## Executive Summary

The test plan in `testing-design-and-implementation-plan.md` is **well-structured and thorough**, aligning properly with the existing test patterns and service architecture. This document captures all review feedback, identified gaps, and recommendations for a successful implementation.

---

## ✅ Strengths of the Plan

### 1. Sensible Priority Order
Starting with Notebook → Livy → SQL makes excellent sense because these services contain the most complex logic:
- **Notebook**: LRO polling, base64 encoding/decoding, private helper logic
- **Livy**: Session/statement waiter patterns, timeout handling, configuration injection
- **SQL**: Optional dependencies, token handling, connection lifecycle, ODBC interaction

This ordering allows building momentum with less complex services (Job, Item, Pipeline) afterward.

### 2. Test Style Alignment
The plan correctly identifies the class-based test organization with `@pytest.mark.unit` decorators, matching the existing pattern in `tests/fabric/services/test_pipeline.py`. This consistency is important for maintainability and IDE discovery.

### 3. Private Helper Coverage Decision
Good decision to test private helpers like `_resolve_notebook_path()`, `_encode_notebook_file()`, and `_create_notebook_definition()`. These contain real, reusable logic that warrants verification:
- Path resolution with optional repo_root
- File encoding with validation
- Definition structure construction

### 4. Hybrid Mocking Strategy
The approach of using `FabricDataFactory` for common data shapes (from `tests/fixtures/mocks.py`) plus explicit `Mock()` for edge cases strikes the right balance:
- Avoids over-abstraction of mock factories
- Reuses common Fabric API response shapes
- Allows fine-grained control for error scenarios

### 5. Optional Dependency Handling
The plan correctly identifies that SQL tests must work even when `pyodbc` is not installed. This is critical because the SQL service raises `ImportError` in `__init__` when `PYODBC_AVAILABLE` is False.

### 6. Coverage Enforcement
Setting a 75% threshold with `pytest-cov` is reasonable for a first pass, with clear acknowledgment that it can be raised later.

---

## ⚠️ Issues & Gaps to Address

### 1. **Missing `notebook_definition` Factory Correction**

**Current State**: The existing `FabricDataFactory.notebook_definition()` in `tests/fixtures/mocks.py` (lines 105-119) generates:
```python
{
    "path": "notebook-content.py",
    "payload": encoded_content,
    "payloadType": "InlineBase64"
}
```

**Issue**: The actual implementation in `_create_notebook_definition()` uses:
```python
"path": os.path.basename(notebook_path)
```

This means it typically generates `"my_notebook.ipynb"`, not `"notebook-content.py"`.

**Action**: When testing `get_notebook_content()`, note that matching `.ipynb` parts requires either:
- Updating the factory to accept a `path` parameter
- Creating test-specific mock responses inline
- Adjusting factory defaults to use `.ipynb` extension

**Reference**: [notebook.py#L145-L160](src/ms_fabric_mcp_server/services/notebook.py#L145-L160)

---

### 2. **Workspace Service Already Exists**

**Current State**: The plan says "Add `tests/fabric/services/test_workspace.py`" under Phase 4.

**Issue**: A file named `tests/fabric/services/test_workspace_service.py` already exists in the workspace.

**Action**: 
- Verify whether `test_workspace_service.py` is complete or needs extension
- If incomplete, the plan should say "Extend `test_workspace_service.py`" instead of "Add new file"
- If complete, remove from the plan

**Reference**: Workspace exists in workspace structure

---

### 3. **`execute_notebook` Creates a Job Service Internally**

**Current State**: In `execute_notebook()` ([notebook.py#L596-L600](src/ms_fabric_mcp_server/services/notebook.py#L596-L600)):
```python
# Import job service here to avoid circular imports
from .job import FabricJobService

# Create job service instance
job_service = FabricJobService(
    client=self.client,
    workspace_service=self.workspace_service,
    item_service=self.item_service
)
```

**Issue**: The plan mentions testing `execute_notebook` but doesn't address how to handle this internal `FabricJobService` instantiation. Direct instantiation is harder to mock than dependency injection.

**Action**: Add explicit note to Notebook tests:
- Patch `FabricJobService` at the **class level** using `@patch('ms_fabric_mcp_server.services.notebook.FabricJobService')`
- This ensures the internal `FabricJobService()` call uses the mock
- Test both success path (job created and waiter returns) and error path (job creation fails)

**Example Pattern**:
```python
@patch('ms_fabric_mcp_server.services.notebook.FabricJobService')
def test_execute_notebook_success(self, mock_job_service_class):
    # Configure mock
    mock_job_instance = Mock()
    mock_job_service_class.return_value = mock_job_instance
    # ... rest of test
```

---

### 4. **`attach_lakehouse_to_notebook` Deserves Deeper Testing**

**Current State**: The plan lists this under "Verify correct delegation" with minimal coverage.

**Issue**: This method has non-trivial logic:
- Resolves two workspaces (notebook's and optional lakehouse's different workspace)
- Fetches notebook definition via LRO (same polling pattern as `get_notebook_content`)
- Modifies metadata dependencies section
- Updates notebook with modified definition
- Returns structured result

**Action**: Promote `attach_lakehouse_to_notebook` to a primary test focus (like `get_notebook_content`). Add:
- Success path: lakehouse attached to notebook metadata
- LRO handling (202 with Location, polling, timeout)
- Missing lakehouse → raises `FabricItemNotFoundError`
- Lakehouse in different workspace → correct resolution and dependency update
- Definition update failure → error result

**Reference**: [notebook.py#L633-L700](src/ms_fabric_mcp_server/services/notebook.py#L633-L700)

---

### 5. **SQL Service `execute_query` vs `execute_sql_query` Distinction**

**Current State**: The plan lists both methods under "Tests".

**Issue**: These are different layers:
- `execute_query(query: str, parameters: List = None)` - operates on **existing** connection
- `execute_sql_query(...)` - **high-level wrapper** that calls `connect()`, executes, `close()`

**Action**: Clarify test expectations:
- For `execute_query`: Test with mock `_connection` already set
- For `execute_sql_query`: Test that `connect()` is called, `execute_query()` is called, and `close()` is called in finally block
- Verify that connection errors before query execution trigger `close()` properly

Also note the distinction between:
- `execute_statement()` - for DML (INSERT/UPDATE/DELETE), returns affected rows
- `execute_query()` - for DQL (SELECT), returns result set with rows/columns

---

### 6. **Livy Service `wait_for_session` Log Extraction**

**Current State**: The plan mentions "log extraction from session data" for error cases.

**Issue**: Implementation details not specified. Need clarity on:
- Where logs come from: `session.get("log", [])` (list of log lines)
- How they're used: error messages are built from log content
- What constitutes "meaningful" log for error reporting

**Action**: Add specific test cases:
- Session enters `error` state → extract logs and include in raised `FabricLivySessionError`
- Session enters `dead` state → extract logs
- Session enters `killed` state → extract logs
- Empty logs list → appropriate error message still provided

**Reference**: Livy service error handling patterns

---

### 7. **Job Service `get_job_status_by_url` Test Clarity**

**Current State**: Plan says "Valid URL parsed to endpoint; invalid URL yields error result."

**Issue**: Needs specificity on what constitutes "invalid":
- Malformed URL structure
- Missing job instance ID segment
- Wrong domain/scheme
- URL with insufficient path depth

**Action**: Add examples:
```python
# Invalid: missing job ID segment
"https://api.fabric.microsoft.com/v1/workspaces/ws-123/items/item-456/jobs/instances/"

# Valid: has job ID
"https://api.fabric.microsoft.com/v1/workspaces/ws-123/items/item-456/jobs/instances/job-789"

# Invalid: wrong structure
"not-a-url"
```

---

### 8. **Pipeline `_decode_definition` Does Not Exist in Codebase**

**CRITICAL ISSUE**: The plan mentions testing:
- `_decode_definition` / `_decode_definition` round-trip success
- Invalid decode raises appropriate error

**Reality**: Searching `services/pipeline.py`, only `_encode_definition()` exists.

**Action**: 
- **Option A**: Remove this test from the plan (only `_encode_definition` needs testing)
- **Option B**: Implement `_decode_definition()` first, then test both directions

Recommend **Option A** unless there's a genuine use case for decoding. Current service only encodes definitions for upload.

**Reference**: [pipeline.py](src/ms_fabric_mcp_server/services/pipeline.py) - search for "decode"

---

### 9. **Coverage Configuration Location Unclear**

**Current State**: Plan says "Add/adjust pytest config" under "Coverage Enforcement Configuration".

**Issue**: Doesn't specify where or provide exact syntax.

**Action**: Add exact configuration to `pyproject.toml` under `[tool.pytest.ini_options]`:

```toml
[tool.pytest.ini_options]
addopts = "--cov=ms_fabric_mcp_server --cov-report=term-missing --cov-fail-under=75"
testpaths = ["tests"]
python_files = ["test_*.py"]
python_classes = ["Test*"]
python_functions = ["test_*"]
markers = [
    "unit: Unit tests",
    "integration: Integration tests",
]
```

This ensures coverage is enforced on every test run.

---

## 📝 Additional Recommendations

### 1. **Extend `mocks.py` with New Factories**

Several test patterns will appear repeatedly. Reduce boilerplate by adding these factories to `tests/fixtures/mocks.py`:

```python
@staticmethod
def notebook_execution_details(
    job_instance_id: str = "job-123",
    livy_id: int = 0,
    state: str = "Completed"
) -> Dict[str, Any]:
    """Generate notebook execution details response."""
    return {
        "status": "success",
        "execution_summary": {
            "job_instance_id": job_instance_id,
            "livy_id": livy_id,
            "spark_application_id": f"app-{job_instance_id}",
            "state": state,
            "submitted_time_utc": "2025-12-24T10:00:00Z",
            "start_time_utc": "2025-12-24T10:05:00Z",
            "end_time_utc": "2025-12-24T10:15:00Z" if state == "Completed" else None,
        }
    }

@staticmethod
def lro_response(
    status: str = "Succeeded",
    location: str = "https://api.fabric.microsoft.com/v1/operations/op-123"
) -> tuple:
    """Generate LRO (202 Accepted) response tuple (response, headers)."""
    response = Mock()
    response.status_code = 202
    response.ok = True
    response.json.return_value = {"status": status}
    headers = {"Location": location}
    response.headers = headers
    return response, headers
```

---

### 2. **Time Patching Helper Fixture**

Livy, Job, and Notebook polling tests will all need to patch `time.sleep` and `time.time`. Create a shared fixture in `tests/conftest.py`:

```python
@pytest.fixture
def mock_time_progression():
    """Mock time.sleep and time.time for fast polling tests."""
    with patch('time.sleep') as mock_sleep, \
         patch('time.time') as mock_time:
        current_time = [1000.0]  # Use list to allow mutation in nested scope
        
        def advance_time(seconds):
            current_time[0] += seconds
        
        mock_time.side_effect = lambda: current_time[0]
        # Can advance time per sleep call if needed
        
        yield {
            'sleep': mock_sleep,
            'time': mock_time,
            'advance': advance_time,
            'current': current_time
        }
```

Usage in tests:
```python
def test_livy_wait_timeout(self, livy_service, mock_time_progression):
    # Tests will see zero actual sleep
    # Timeout logic verified with mock time advancement
```

---

### 3. **Parametrized Tests for Validation**

For `_validate_pipeline_inputs` (testing each required field missing), use `@pytest.mark.parametrize` to reduce code duplication:

```python
@pytest.mark.parametrize("missing_field,error_text", [
    ("pipeline_name", "pipeline_name"),
    ("source_type", "source_type"),
    ("source_connection_id", "source_connection_id"),
    ("source_schema", "source_schema"),
    ("source_table", "source_table"),
    ("destination_lakehouse_id", "destination_lakehouse_id"),
    ("destination_connection_id", "destination_connection_id"),
    ("destination_table", "destination_table"),
])
def test_validate_pipeline_inputs_missing_field(
    self, pipeline_service, missing_field, error_text
):
    """Test validation fails when each required field is empty."""
    kwargs = {
        "pipeline_name": "Test_Pipeline",
        "source_type": "AzurePostgreSqlSource",
        "source_connection_id": "conn-123",
        "source_schema": "public",
        "source_table": "movie",
        "destination_lakehouse_id": "lakehouse-456",
        "destination_connection_id": "dest-conn-789",
        "destination_table": "movie",
    }
    kwargs[missing_field] = ""  # Set to empty
    
    with pytest.raises(FabricValidationError) as exc_info:
        pipeline_service._validate_pipeline_inputs(**kwargs)
    
    assert error_text in str(exc_info.value)
```

This reduces 8 individual test methods to 1 parametrized method.

---

### 4. **Estimated Test Count**

The plan doesn't estimate test method counts. Here's a rough breakdown for tracking progress:

| Service | Est. Tests | Notes |
|---------|-----------|-------|
| Notebook | 25-30 | Complex: LRO, encoding, execution, logging |
| Livy | 18-22 | Complex: polling, timeout, environment config |
| SQL | 15-18 | Complex: optional deps, token handling, connection lifecycle |
| Job | 12-15 | Medium: URL parsing, status mapping, polling |
| Item | 10-12 | Medium: validation, list filtering, CRUD |
| Pipeline (extend) | 8-10 | Medium: validation, encoding, definition building |
| Workspace (extend) | 6-8 | Simpler: list, lookup, creation |
| **Total** | **94-115** | |

This helps track progress and estimate completion time.

---

### 5. **Test Markers for Future Use**

While the focus is unit tests, consider adding markers for future extensibility:

```python
@pytest.mark.unit
@pytest.mark.notebook
class TestFabricNotebookService:
    """Unit tests for FabricNotebookService."""
```

Then team can run:
- `pytest -m unit` - all unit tests
- `pytest -m notebook` - only notebook tests
- `pytest -m "unit and notebook"` - both filters

Add marker definitions to `conftest.py`:
```python
def pytest_configure(config):
    config.addinivalue_line(
        "markers", "notebook: Tests for FabricNotebookService"
    )
    config.addinivalue_line(
        "markers", "livy: Tests for FabricLivyService"
    )
    # ... etc
```

---

### 6. **Add Integration Test Placeholders**

While not implementing integration tests, add placeholder markers for future reference:

```python
@pytest.mark.integration
@pytest.mark.skip(reason="Integration test - requires live Fabric environment")
def test_end_to_end_notebook_import_and_execute(self):
    """Integration test: Import notebook, execute, and verify results."""
    pass
```

This documents where integration tests should live without cluttering unit tests.

---

### 7. **Test File Organization**

For clarity, each service test should follow this structure:

```python
"""Unit tests for FabricXXXService."""

import pytest
from unittest.mock import Mock, patch, MagicMock

from ms_fabric_mcp_server.services.xxx import FabricXXXService
from ms_fabric_mcp_server.client.exceptions import Fabric*Error
from tests.fixtures.mocks import FabricDataFactory


@pytest.mark.unit
class TestFabricXXXService:
    """Test suite for FabricXXXService."""
    
    @pytest.fixture
    def mock_client(self):
        """Create a mock FabricClient."""
        return Mock()
    
    @pytest.fixture
    def mock_dependency_service(self):
        """Create mock for dependent services."""
        return Mock()
    
    @pytest.fixture
    def service(self, mock_client, mock_dependency_service):
        """Create service instance with mocked dependencies."""
        return FabricXXXService(mock_client, mock_dependency_service)
    
    # Private helper tests (if any)
    def test_private_helper_success(self, service):
        """Test private helper in success case."""
        pass
    
    # Public method tests
    @pytest.mark.parametrize("...")
    def test_public_method_success(self, service):
        """Test public method in success case."""
        pass
    
    def test_public_method_error_case_1(self, service):
        """Test public method handles error case 1."""
        pass
```

---

## 🔧 Quick Fixes Required Before Implementation

| Issue | Priority | Fix | Reference |
|-------|----------|-----|-----------|
| `_decode_definition` doesn't exist | **HIGH** | Remove from Pipeline tests or implement method first | [pipeline.py](src/ms_fabric_mcp_server/services/pipeline.py) |
| `test_workspace_service.py` exists | **HIGH** | Change plan to "Extend" not "Add" | Workspace structure |
| `execute_notebook` instantiates JobService | **MEDIUM** | Add note about patching `FabricJobService` at class level | [notebook.py#L596](src/ms_fabric_mcp_server/services/notebook.py#L596) |
| Coverage config location unclear | **MEDIUM** | Specify `pyproject.toml` location and exact syntax | Plan section 8 |
| Factory path mismatch (`.py` vs `.ipynb`) | **MEDIUM** | Note factory limitations for notebook content tests | [mocks.py#L105](tests/fixtures/mocks.py#L105) |
| `attach_lakehouse_to_notebook` too shallow | **LOW** | Promote to primary test focus with LRO testing | [notebook.py#L633](src/ms_fabric_mcp_server/services/notebook.py#L633) |

---

## Summary & Next Steps

### Status: ✅ **Ready for Implementation** (with corrections)

The test plan is solid and comprehensive. The identified gaps are manageable:

1. **Fix critical issue**: Clarify/remove `_decode_definition` reference
2. **Fix high-priority issues**: 
   - Update workspace service reference (extend vs. add)
   - Add JobService patching note for execute_notebook
3. **Enhance mid-priority items**:
   - Update factory documentation for path mismatch
   - Specify coverage config location
4. **Apply optional improvements**:
   - Add new factories to mocks.py
   - Create time-patching helper
   - Use parametrized tests for validation

With these adjustments, the implementation session can proceed confidently with clear guidance on:
- What to test (comprehensive coverage of each service)
- How to test (existing patterns + recommendations)
- Where to organize tests (clear file structure)
- How to track progress (test count estimates)

**Recommended Action**: Update `testing-design-and-implementation-plan.md` with these corrections before starting implementation.
