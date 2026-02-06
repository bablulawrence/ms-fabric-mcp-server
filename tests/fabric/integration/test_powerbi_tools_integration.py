"""Integration tests for Power BI tools."""

import pytest

from tests.conftest import unique_name


@pytest.mark.integration
@pytest.mark.asyncio
async def test_powerbi_tools_flow(
    call_tool,
    delete_item_if_exists,
    poll_until,
    workspace_name,
    lakehouse_name,
    semantic_model_table,
    semantic_model_columns,
    semantic_model_schema,
):
    if not semantic_model_table or not semantic_model_columns:
        pytest.skip("Missing semantic model table/columns inputs")

    semantic_model_name = unique_name("e2e_powerbi_model")
    try:
        create_result = await call_tool(
            "create_semantic_model",
            workspace_name=workspace_name,
            semantic_model_name=semantic_model_name,
        )
        assert create_result["status"] == "success"

        async def _get_semantic_model():
            result = await call_tool(
                "list_items",
                workspace_name=workspace_name,
                item_type="SemanticModel",
            )
            if result.get("status") != "success":
                return result
            for item in result.get("items", []):
                if item.get("display_name") == semantic_model_name:
                    return result
            return None

        found = await poll_until(_get_semantic_model, timeout_seconds=120, interval_seconds=10)
        assert found is not None

        add_table_result = await call_tool(
            "add_table_to_semantic_model",
            workspace_name=workspace_name,
            semantic_model_name=semantic_model_name,
            lakehouse_name=lakehouse_name,
            table_name=semantic_model_table,
            columns=semantic_model_columns,
            table_schema=semantic_model_schema,
        )
        assert add_table_result["status"] == "success"

        measure_name = "Total Key"
        measure_expression = f"SUM('{semantic_model_table}'[{semantic_model_columns[0]['name']}])"

        add_measures_result = await call_tool(
            "add_measures_to_semantic_model",
            workspace_name=workspace_name,
            semantic_model_name=semantic_model_name,
            table_name=semantic_model_table,
            measures=[
                {
                    "name": measure_name,
                    "expression": measure_expression,
                    "format_string": "0",
                    "display_folder": "Test Measures",
                    "description": "Integration test measure",
                }
            ],
        )
        assert add_measures_result["status"] == "success"

        refresh_result = await call_tool(
            "refresh_semantic_model",
            workspace_name=workspace_name,
            semantic_model_name=semantic_model_name,
        )
        assert refresh_result["status"] == "success", f"Refresh failed: {refresh_result}"

        dax_result = await call_tool(
            "execute_dax_query",
            workspace_name=workspace_name,
            semantic_model_name=semantic_model_name,
            query=f"EVALUATE ROW(\"Result\", [{measure_name}])",
        )
        # Skip DAX assertion if service principal lacks Dataset.Read.All permission
        if dax_result.get("status") == "error":
            message = dax_result.get("message", "")
            if "PowerBINotAuthorizedException" in message or "401" in message:
                pytest.skip("DAX query requires Dataset.Read.All permission for service principal")
        assert dax_result["status"] == "success"
        assert "response" in dax_result

        delete_measures_result = await call_tool(
            "delete_measures_from_semantic_model",
            workspace_name=workspace_name,
            semantic_model_name=semantic_model_name,
            table_name=semantic_model_table,
            measure_names=[measure_name],
        )
        assert delete_measures_result["status"] == "success"

    finally:
        await delete_item_if_exists(semantic_model_name, "SemanticModel")
