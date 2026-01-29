"""Unit tests for FabricLakehouseFileService."""

from pathlib import Path
from unittest.mock import Mock

import pytest

from ms_fabric_mcp_server.services.lakehouse_files import \
    FabricLakehouseFileService


@pytest.mark.unit
class TestFabricLakehouseFileService:
    def test_list_files_paginates(self, mock_fabric_client):
        service = FabricLakehouseFileService(mock_fabric_client)
        mock_fabric_client.get_auth_token = Mock(return_value="token")
        session = Mock()
        mock_fabric_client._session = session

        response_one = Mock()
        response_one.status_code = 200
        response_one.ok = True
        response_one.json.return_value = {
            "paths": [{"name": "Files/raw/file1.csv"}],
            "continuation": "token-1",
        }
        response_two = Mock()
        response_two.status_code = 200
        response_two.ok = True
        response_two.json.return_value = {
            "paths": [{"name": "Files/raw/file2.csv"}],
        }

        session.request.side_effect = [response_one, response_two]

        results = service.list_files("ws-1", "lh-1", path="raw", recursive=True)

        assert len(results) == 2
        assert results[0]["name"] == "Files/raw/file1.csv"
        assert results[1]["name"] == "Files/raw/file2.csv"

        assert session.request.call_count == 2
        first_call = session.request.call_args_list[0].kwargs["url"]
        second_call = session.request.call_args_list[1].kwargs["url"]
        assert "resource=filesystem" in first_call
        assert "directory=Files%2Fraw" in first_call
        assert "recursive=true" in first_call
        assert "continuation=token-1" in second_call

    def test_upload_file_writes_and_flushes(self, mock_fabric_client, tmp_path: Path):
        service = FabricLakehouseFileService(mock_fabric_client)
        mock_fabric_client.get_auth_token = Mock(return_value="token")
        session = Mock()
        mock_fabric_client._session = session

        sample = tmp_path / "sample.csv"
        sample.write_text("a,b\n1,2\n")

        responses = []
        for _ in range(3):
            response = Mock()
            response.status_code = 200
            response.ok = True
            response.json.return_value = {}
            responses.append(response)

        session.request.side_effect = responses

        result = service.upload_file(
            workspace_id="ws-1",
            lakehouse_id="lh-1",
            local_file_path=str(sample),
            destination_path="raw/sample.csv",
            create_missing_directories=False,
        )

        assert result["path"] == "raw/sample.csv"
        assert result["size_bytes"] > 0
        assert session.request.call_count == 3

        create_url = session.request.call_args_list[0].kwargs["url"]
        append_url = session.request.call_args_list[1].kwargs["url"]
        flush_url = session.request.call_args_list[2].kwargs["url"]
        assert "resource=file" in create_url
        assert "action=append" in append_url
        assert "action=flush" in flush_url

    def test_delete_file_recursive(self, mock_fabric_client):
        service = FabricLakehouseFileService(mock_fabric_client)
        mock_fabric_client.get_auth_token = Mock(return_value="token")
        session = Mock()
        mock_fabric_client._session = session

        response = Mock()
        response.status_code = 200
        response.ok = True
        response.json.return_value = {}
        session.request.return_value = response

        service.delete_file(
            workspace_id="ws-1",
            lakehouse_id="lh-1",
            path="raw",
            recursive=True,
        )

        assert session.request.call_count == 1
        delete_url = session.request.call_args.kwargs["url"]
        assert "recursive=true" in delete_url
