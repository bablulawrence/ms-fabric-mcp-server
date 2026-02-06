"""Integration tests for lakehouse file tools."""

import pytest

from tests.conftest import unique_name


@pytest.mark.integration
@pytest.mark.asyncio
async def test_upload_list_delete_lakehouse_file(
    call_tool,
    workspace_name,
    delete_item_if_exists,
    poll_until,
    tmp_path,
):
    lakehouse_name = unique_name("e2e_lakehouse_files")
    file_name = unique_name("sample_file")
    destination_path = f"raw/{file_name}.csv"

    lakehouse_id = None
    try:
        create_result = await call_tool(
            "create_lakehouse",
            workspace_name=workspace_name,
            lakehouse_name=lakehouse_name,
        )
        assert create_result["status"] == "success"
        lakehouse_id = create_result.get("lakehouse_id")
        assert lakehouse_id

        local_file = tmp_path / f"{file_name}.csv"
        local_file.write_text("col1,col2\n1,2\n")

        upload_result = await call_tool(
            "upload_lakehouse_file",
            workspace_name=workspace_name,
            lakehouse_id=lakehouse_id,
            local_file_path=str(local_file),
            destination_path=destination_path,
            create_missing_directories=True,
        )
        assert upload_result["status"] == "success"

        async def _file_visible():
            list_result = await call_tool(
                "list_lakehouse_files",
                workspace_name=workspace_name,
                lakehouse_id=lakehouse_id,
                path="raw",
                recursive=True,
            )
            if list_result.get("status") != "success":
                return None
            for entry in list_result.get("files", []):
                name = entry.get("name") or ""
                if destination_path in name:
                    return list_result
            return None

        listed = await poll_until(
            _file_visible, timeout_seconds=180, interval_seconds=10
        )
        assert listed is not None

        delete_result = await call_tool(
            "delete_lakehouse_file",
            workspace_name=workspace_name,
            lakehouse_id=lakehouse_id,
            path=destination_path,
        )
        assert delete_result["status"] == "success"
    finally:
        await delete_item_if_exists(lakehouse_name, "Lakehouse")
