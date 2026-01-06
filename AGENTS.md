# Repository Guidelines

## Commands
- `pip install -e ".[dev,sql,telemetry]"`: install in editable mode with all extras
- `pytest`: run full test suite with coverage
- `pytest -m unit`: run only unit tests
- `pytest -k test_name`: run specific test
- `black src tests && isort src tests`: format code
- `bandit -r src`: security linting
- `uvx ms-fabric-mcp-server`: run MCP server locally (stdio)

## Tech Stack
- **Python:** 3.11+ (CI tests 3.11, 3.12, 3.13)
- **MCP Framework:** FastMCP 2.10+
- **Models:** Pydantic 2.0+
- **Auth:** Azure Identity DefaultAzureCredential
- **Build:** Hatchling (via uv)

## Project Structure
- `src/ms_fabric_mcp_server/` – Main package
  - `client/` – HTTP client, config, exceptions
  - `services/` – Business logic (workspace, notebook, job, livy, SQL, pipeline, powerbi, semantic_model)
  - `tools/` – MCP tool definitions wrapping services
  - `models/` – Pydantic data models
- `tests/` – pytest suite (`tests/fabric/` for Fabric-specific tests)
- `docs/` – Design docs and testing notes
- `.github/workflows/` – CI: tests.yml, publish.yml, integration-tests.yml

## Code Style
- Formatter: Black (default settings)
- Import sorting: isort
- Naming: `snake_case` functions/vars, `PascalCase` classes, `UPPER_SNAKE_CASE` constants
- Type hints required for public APIs

### Example
```python
# ✅ Good
async def get_workspace_by_name(
    client: FabricClient, 
    workspace_name: str
) -> FabricWorkspace:
    """Fetch a workspace by display name."""
    workspaces = await client.get("/workspaces")
    for ws in workspaces["value"]:
        if ws["displayName"] == workspace_name:
            return FabricWorkspace(**ws)
    raise FabricWorkspaceNotFoundError(workspace_name)
```

## Testing
- Framework: pytest with strict config
- Markers: `@pytest.mark.unit`, `@pytest.mark.integration`, `@pytest.mark.slow`
- Naming: `test_*.py` files, `Test*` classes, `test_*` functions
- Integration tests require `FABRIC_INTEGRATION_TESTS=1` and live Azure resources
- Run integration tests: `cp .env.integration.example .env.integration && FABRIC_INTEGRATION_TESTS=1 pytest`

## Publishing
- Bump the version in `pyproject.toml` before any TestPyPI/PyPI publish.
- Use the GitHub workflow to publish (run `publish-testpypi.yml` or `publish.yml`) instead of local uploads.

## Commit Style
- Short imperative summaries
- Optional conventional prefixes: `docs:`, `chore:`, `fix:`, `feat:`

## Boundaries
- ✅ **Always:** Run tests before commits, use type hints, follow existing patterns
- ⚠️ **Ask first:** Adding dependencies, modifying tool APIs, changing `pyproject.toml`
- 🚫 **Never:** Commit secrets or `.env` files, modify `uv.lock` manually, run in production environments

## Security Notes
- Server is for **development only** – tools can delete items and run arbitrary code
- Never expose to untrusted networks
- Use `.env.example` as baseline; never commit real credentials
