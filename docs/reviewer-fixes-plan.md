# MCP Tool Improvements

Three enhancements to improve error visibility and expand SQL capabilities.

---

## Workplan

### 1. Improve Dataflow Error Extraction

Our experiments showed Dataflow failures often return **generic** error payloads
(`message`, `moreDetails`, or job `failureReason`) rather than `pbi.error.details`.
Currently only the top-level `message` is surfaced, which can be empty. This change
extracts useful details from multiple known shapes and from `FabricAPIError.response_body`.

- [ ] Add `_extract_dataflow_error()` helper to `services/dataflow.py`
- [ ] Parse generic error payloads (`message`, `error.message`, `moreDetails[].message`)
- [ ] Parse job-status payloads (`failureReason.message`)
- [ ] Optionally parse `pbi.error.details` if present (best effort, not assumed)
- [ ] Update `run_dataflow()` to use improved extraction when API returns error
- [ ] Add unit tests for each error shape

**Files:** `src/ms_fabric_mcp_server/services/dataflow.py`, `tests/fabric/test_dataflow_service.py`

**Implementation:**
```python
def _extract_dataflow_error(payload: Dict[str, Any]) -> str:
    """Extract useful error message from Fabric dataflow error payloads."""
    message = (
        payload.get("message")
        or payload.get("error", {}).get("message")
        or payload.get("failureReason", {}).get("message")
    )
    if message:
        return message

    for detail in payload.get("moreDetails", []) or []:
        msg = detail.get("message") or detail.get("detail", {}).get("value")
        if msg:
            return msg

    pbi_error = payload.get("pbi.error") or payload.get("error", {}).get("pbi.error")
    if isinstance(pbi_error, dict):
        for d in pbi_error.get("details", []) or []:
            if d.get("code") == "DetailsMessage":
                return d.get("detail", {}).get("value") or "Unknown dataflow error"

    return "Unknown dataflow error"
```

---

### 2. Add DDL Support to execute_sql_statement

The `execute_sql_statement` tool only accepts DML (INSERT/UPDATE/DELETE/MERGE). This change adds an `allow_ddl` parameter to optionally permit DDL statements (CREATE/ALTER/DROP/TRUNCATE).

**Note:** DDL is only supported on **Warehouse SQL endpoints**. Lakehouse SQL Analytics endpoints are read-only and will reject DDL at the API level. This was validated via ODBC experiments.

- [ ] Add `allow_ddl: bool = False` parameter to `execute_statement()` in `services/sql.py`
- [ ] Add `_is_ddl_statement()` helper method
- [ ] Update validation logic to check `allow_ddl` flag before rejecting DDL
- [ ] Update tool wrapper in `tools/sql_tools.py` to expose parameter
- [ ] Update tool docstring to document new parameter and Warehouse-only limitation
- [ ] Add unit tests for DDL acceptance/rejection

**Files:** `src/ms_fabric_mcp_server/services/sql.py`, `src/ms_fabric_mcp_server/tools/sql_tools.py`, `tests/fabric/test_sql_service.py`

**Implementation:**
```python
@staticmethod
def _is_ddl_statement(statement: str) -> bool:
    """Return True if statement starts with a DDL keyword."""
    if not statement:
        return False
    first_token = statement.lstrip().split(None, 1)
    if not first_token:
        return False
    return first_token[0].upper() in {"CREATE", "ALTER", "DROP", "TRUNCATE"}

def execute_statement(self, statement: str, allow_ddl: bool = False) -> Dict[str, Any]:
    # Existing DML check
    if not self._is_dml_statement(statement):
        # Check if it's DDL and allowed
        if allow_ddl and self._is_ddl_statement(statement):
            pass  # Allow execution
        else:
            return {"status": "error", ...}
```

---

### 3. Surface Livy Session Fallback Warnings

When creating a Livy session with custom libraries/environments, Fabric sometimes falls back to a slower startup mode (6+ minutes). The API returns this info in session tags but we don't surface it. This change extracts and returns fallback warnings (validated in live session status).

- [ ] Update `create_session()` in `services/livy.py` to extract `FallbackReasons`/`FallbackMessages` from session tags
- [ ] Include fallback info in returned session dict
- [ ] Update tool wrapper docstring to document new response fields
- [ ] Add unit test for fallback extraction

**Files:** `src/ms_fabric_mcp_server/services/livy.py`, `src/ms_fabric_mcp_server/tools/livy_tools.py`, `tests/fabric/test_livy_service.py`

**Implementation:**
```python
def _extract_fallback_info(session_data: Dict[str, Any]) -> Dict[str, Any]:
    """Extract fallback warnings from Livy session tags."""
    tags = session_data.get("tags", {})
    fallback_info = {}
    if "FallbackReasons" in tags:
        fallback_info["fallback_reasons"] = tags["FallbackReasons"]
    if "FallbackMessages" in tags:
        fallback_info["fallback_messages"] = tags["FallbackMessages"]
    return fallback_info

# Add to return dict:
result = {**session_data, **_extract_fallback_info(session_data)}
```

---

## Notes

- All changes are backward compatible (new optional parameters, additional response fields)
- No breaking changes to existing tool APIs
- Experiments confirmed Warehouse DDL support and Livy fallback tags; Dataflow error payloads were generic (no `pbi.error.details` observed)
- Run `pytest` after each change to verify no regressions
