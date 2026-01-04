"""Integration tests for semantic model tools."""

import pytest

from tests.conftest import unique_name


@pytest.mark.integration
@pytest.mark.asyncio
async def test_semantic_model_tools_flow(
    call_tool,
    delete_item_if_exists,
    poll_until,
    workspace_name,
    lakehouse_name,
    semantic_model_table,
    semantic_model_columns,
    semantic_model_table_2,
    semantic_model_columns_2,
):
    if not semantic_model_table or not semantic_model_columns:
        pytest.skip("Missing semantic model table/columns inputs")

    semantic_model_name = unique_name("e2e_semantic_model")
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
        )
        assert add_table_result["status"] == "success"

        if semantic_model_table_2 and semantic_model_columns_2:
            add_table_2_result = await call_tool(
                "add_table_to_semantic_model",
                workspace_name=workspace_name,
                semantic_model_name=semantic_model_name,
                lakehouse_name=lakehouse_name,
                table_name=semantic_model_table_2,
                columns=semantic_model_columns_2,
            )
            assert add_table_2_result["status"] == "success"

            from_column = semantic_model_columns[0]["name"]
            to_column = semantic_model_columns_2[0]["name"]

            add_rel_result = await call_tool(
                "add_relationship_to_semantic_model",
                workspace_name=workspace_name,
                semantic_model_name=semantic_model_name,
                from_table=semantic_model_table,
                from_column=from_column,
                to_table=semantic_model_table_2,
                to_column=to_column,
                cardinality="manyToOne",
                cross_filter_direction="oneDirection",
                is_active=True,
            )
            assert add_rel_result["status"] == "success"
    finally:
        await delete_item_if_exists(semantic_model_name, "SemanticModel")
