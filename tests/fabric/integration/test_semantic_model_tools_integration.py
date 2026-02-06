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
    semantic_model_refresh,
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

        if semantic_model_refresh:
            refresh_result = await call_tool(
                "refresh_semantic_model",
                workspace_name=workspace_name,
                semantic_model_name=semantic_model_name,
            )
            assert refresh_result["status"] == "success", refresh_result
    finally:
        await delete_item_if_exists(semantic_model_name, "SemanticModel")


@pytest.mark.integration
@pytest.mark.asyncio
async def test_get_semantic_model_details_and_definition(
    call_tool,
    delete_item_if_exists,
    poll_until,
    workspace_name,
):
    semantic_model_name = unique_name("e2e_semantic_model_details")
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

        details_result = await call_tool(
            "get_semantic_model_details",
            workspace_name=workspace_name,
            semantic_model_name=semantic_model_name,
        )
        assert details_result["status"] == "success"
        assert details_result.get("semantic_model_name") == semantic_model_name

        definition_result = await call_tool(
            "get_semantic_model_definition",
            workspace_name=workspace_name,
            semantic_model_name=semantic_model_name,
            format="TMSL",
        )
        assert definition_result["status"] == "success"
        assert definition_result.get("definition") is not None
    finally:
        await delete_item_if_exists(semantic_model_name, "SemanticModel")


@pytest.mark.integration
@pytest.mark.asyncio
async def test_semantic_model_table_with_schema_refresh(
    call_tool,
    delete_item_if_exists,
    poll_until,
    workspace_name,
    lakehouse_name,
    semantic_model_table,
    semantic_model_columns,
    semantic_model_schema,
    semantic_model_schema_refresh,
    semantic_model_table_2,
    semantic_model_columns_2,
):
    if not semantic_model_table or not semantic_model_columns or not semantic_model_schema:
        pytest.skip("Missing semantic model table/columns/schema inputs")
    if any(sep in semantic_model_table for sep in (".", "/", "\\")):
        pytest.skip("Semantic model table must be unqualified when using schema")

    semantic_model_name = unique_name("e2e_semantic_model_schema")
    model_table_name = f"{semantic_model_schema}_{semantic_model_table}"

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
            model_table_name=model_table_name,
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
                table_schema=semantic_model_schema,
                model_table_name=f"{semantic_model_schema}_{semantic_model_table_2}",
            )
            assert add_table_2_result["status"] == "success"

            from_column = semantic_model_columns[0]["name"]
            to_column = semantic_model_columns_2[0]["name"]
            add_rel_result = await call_tool(
                "add_relationship_to_semantic_model",
                workspace_name=workspace_name,
                semantic_model_name=semantic_model_name,
                from_table=model_table_name,
                from_column=from_column,
                to_table=f"{semantic_model_schema}_{semantic_model_table_2}",
                to_column=to_column,
                cardinality="manyToOne",
                cross_filter_direction="oneDirection",
                is_active=True,
            )
            assert add_rel_result["status"] == "success"

        definition_result = await call_tool(
            "get_semantic_model_definition",
            workspace_name=workspace_name,
            semantic_model_name=semantic_model_name,
            format="TMSL",
            decode_model_bim=True,
        )
        assert definition_result["status"] == "success"

        model = definition_result.get("model_bim_json", {}).get("model", {})
        tables = model.get("tables", [])
        target = next((t for t in tables if t.get("name") == model_table_name), None)
        assert target is not None
        partitions = target.get("partitions", [])
        assert partitions
        source = partitions[0].get("source", {})
        assert source.get("schemaName") == semantic_model_schema
        assert source.get("entityName") == semantic_model_table

        if semantic_model_schema_refresh:
            refresh_result = await call_tool(
                "refresh_semantic_model",
                workspace_name=workspace_name,
                semantic_model_name=semantic_model_name,
            )
            assert refresh_result["status"] == "success", refresh_result
            assert refresh_result.get("refresh_status") == "Completed", refresh_result
    finally:
        await delete_item_if_exists(semantic_model_name, "SemanticModel")


@pytest.mark.integration
@pytest.mark.asyncio
async def test_delete_table_from_semantic_model(
    call_tool,
    delete_item_if_exists,
    poll_until,
    workspace_name,
    lakehouse_name,
    semantic_model_table,
    semantic_model_columns,
):
    if not semantic_model_table or not semantic_model_columns:
        pytest.skip("Missing semantic model table/columns inputs")

    semantic_model_name = unique_name("e2e_semantic_model_delete_table")
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

        delete_result = await call_tool(
            "delete_table_from_semantic_model",
            workspace_name=workspace_name,
            semantic_model_name=semantic_model_name,
            table_name=semantic_model_table,
        )
        assert delete_result["status"] == "success"

        definition_result = await call_tool(
            "get_semantic_model_definition",
            workspace_name=workspace_name,
            semantic_model_name=semantic_model_name,
            format="TMSL",
            decode_model_bim=True,
        )
        assert definition_result["status"] == "success"
        model = definition_result.get("model_bim_json", {}).get("model", {})
        tables = model.get("tables", [])
        assert all(table.get("name") != semantic_model_table for table in tables)
    finally:
        await delete_item_if_exists(semantic_model_name, "SemanticModel")


@pytest.mark.integration
@pytest.mark.asyncio
async def test_delete_relationship_from_semantic_model(
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
    if not all([
        semantic_model_table,
        semantic_model_columns,
        semantic_model_table_2,
        semantic_model_columns_2,
    ]):
        pytest.skip("Missing semantic model tables/columns inputs for relationship delete")

    semantic_model_name = unique_name("e2e_semantic_model_delete_relationship")
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
        )
        assert add_rel_result["status"] == "success"

        delete_rel_result = await call_tool(
            "delete_relationship_from_semantic_model",
            workspace_name=workspace_name,
            semantic_model_name=semantic_model_name,
            from_table=semantic_model_table,
            from_column=from_column,
            to_table=semantic_model_table_2,
            to_column=to_column,
        )
        assert delete_rel_result["status"] == "success"
        assert delete_rel_result.get("relationships_removed", 0) >= 1

        definition_result = await call_tool(
            "get_semantic_model_definition",
            workspace_name=workspace_name,
            semantic_model_name=semantic_model_name,
            format="TMSL",
            decode_model_bim=True,
        )
        assert definition_result["status"] == "success"
        model = definition_result.get("model_bim_json", {}).get("model", {})
        relationships = model.get("relationships", [])
        assert not any(
            rel.get("fromTable") == semantic_model_table
            and rel.get("fromColumn") == from_column
            and rel.get("toTable") == semantic_model_table_2
            and rel.get("toColumn") == to_column
            for rel in relationships
        )
    finally:
        await delete_item_if_exists(semantic_model_name, "SemanticModel")
