"""Unit tests for FabricNotebookService."""

import base64
import json
import os
from unittest.mock import Mock, patch

import pytest

from ms_fabric_mcp_server.client.exceptions import (
    FabricAPIError,
    FabricError,
    FabricItemNotFoundError,
    FabricValidationError,
)
from ms_fabric_mcp_server.models.item import FabricItem
from ms_fabric_mcp_server.models.job import FabricJob
from ms_fabric_mcp_server.models.results import JobStatusResult
from tests.fixtures.mocks import MockResponseFactory


def _make_response(status_code=200, json_data=None, text="", headers=None):
    response = Mock()
    response.status_code = status_code
    response.ok = 200 <= status_code < 300
    response.json.return_value = json_data or {}
    response.text = text
    response.headers = headers or {}
    return response


def _encode_ipynb(payload):
    return base64.b64encode(json.dumps(payload).encode("utf-8")).decode("utf-8")


@pytest.fixture
def mock_workspace_service():
    service = Mock()
    service.resolve_workspace_id.return_value = "workspace-123"
    return service


@pytest.fixture
def mock_item_service():
    return Mock()


@pytest.fixture
def notebook_service(mock_fabric_client, mock_item_service, mock_workspace_service):
    from ms_fabric_mcp_server.services.notebook import FabricNotebookService

    return FabricNotebookService(
        mock_fabric_client,
        mock_item_service,
        mock_workspace_service,
    )


@pytest.mark.unit
class TestFabricNotebookService:
    """Test suite for FabricNotebookService."""

    def test_resolve_notebook_path_absolute(self, mock_fabric_client, mock_item_service, mock_workspace_service, tmp_path):
        """Absolute path should be returned unchanged."""
        from ms_fabric_mcp_server.services.notebook import FabricNotebookService

        service = FabricNotebookService(mock_fabric_client, mock_item_service, mock_workspace_service, repo_root="/repo")
        absolute_path = str(tmp_path / "notebook.ipynb")
        assert service._resolve_notebook_path(absolute_path) == absolute_path

    def test_resolve_notebook_path_relative_with_repo_root(self, mock_fabric_client, mock_item_service, mock_workspace_service):
        """Relative path should be resolved against repo_root."""
        from ms_fabric_mcp_server.services.notebook import FabricNotebookService

        service = FabricNotebookService(mock_fabric_client, mock_item_service, mock_workspace_service, repo_root="/repo")
        relative_path = "notebooks/test.ipynb"
        assert service._resolve_notebook_path(relative_path) == os.path.join("/repo", relative_path)

    def test_resolve_notebook_path_relative_without_repo_root(self, mock_fabric_client, mock_item_service, mock_workspace_service):
        """Relative path should be resolved against cwd when repo_root missing."""
        from ms_fabric_mcp_server.services.notebook import FabricNotebookService

        service = FabricNotebookService(mock_fabric_client, mock_item_service, mock_workspace_service)
        relative_path = "notebooks/test.ipynb"
        assert service._resolve_notebook_path(relative_path) == os.path.abspath(relative_path)

    def test_encode_notebook_file_success(self, notebook_service, tmp_path):
        """Encode a non-empty notebook file."""
        notebook_path = tmp_path / "sample.ipynb"
        content = b'{"cells":[{"cell_type":"code","source":["print(1)"]}]}'
        notebook_path.write_bytes(content)

        encoded = notebook_service._encode_notebook_file(str(notebook_path))

        decoded = base64.b64decode(encoded.encode("utf-8"))
        assert decoded == content

    def test_encode_notebook_file_missing(self, notebook_service, tmp_path):
        """Missing notebook should raise FileNotFoundError."""
        notebook_path = tmp_path / "missing.ipynb"
        with pytest.raises(FileNotFoundError):
            notebook_service._encode_notebook_file(str(notebook_path))

    def test_encode_notebook_file_empty(self, notebook_service, tmp_path):
        """Empty notebook should raise ValueError."""
        notebook_path = tmp_path / "empty.ipynb"
        notebook_path.write_bytes(b"")
        with pytest.raises(ValueError):
            notebook_service._encode_notebook_file(str(notebook_path))

    def test_encode_notebook_file_unexpected_error(self, notebook_service, tmp_path):
        """Unexpected file read errors should raise FabricError."""
        notebook_path = tmp_path / "broken.ipynb"
        notebook_path.write_text("data")

        with patch("ms_fabric_mcp_server.services.notebook.open", side_effect=OSError("boom")):
            with pytest.raises(FabricError):
                notebook_service._encode_notebook_file(str(notebook_path))

    def test_create_notebook_definition_includes_description(self, notebook_service):
        """Notebook definition includes description when provided."""
        notebook_service._encode_notebook_file = Mock(return_value="encoded")

        definition = notebook_service._create_notebook_definition(
            notebook_name="Test Notebook",
            notebook_path="notebooks/test.ipynb",
            description="Test description",
        )

        assert definition["displayName"] == "Test Notebook"
        assert definition["type"] == "Notebook"
        assert definition["description"] == "Test description"
        part = definition["definition"]["parts"][0]
        assert part["path"] == "test.ipynb"
        assert part["payload"] == "encoded"

    def test_create_notebook_definition_without_description(self, notebook_service):
        """Notebook definition omits description when not provided."""
        notebook_service._encode_notebook_file = Mock(return_value="encoded")

        definition = notebook_service._create_notebook_definition(
            notebook_name="Test Notebook",
            notebook_path="notebooks/test.ipynb",
        )

        assert "description" not in definition

    def test_import_notebook_success(self, notebook_service, mock_item_service, mock_workspace_service):
        """Import notebook returns success result."""
        notebook_service._create_notebook_definition = Mock(return_value={"definition": {}})
        mock_workspace_service.resolve_workspace_id.return_value = "ws-1"
        mock_item_service.create_item.return_value = FabricItem(
            id="nb-123",
            display_name="Test",
            type="Notebook",
            workspace_id="ws-1",
        )

        result = notebook_service.import_notebook(
            workspace_name="Workspace",
            notebook_name="Notebook",
            local_path="notebooks/test.ipynb",
        )

        assert result.status == "success"
        assert result.artifact_id == "nb-123"
        mock_item_service.create_item.assert_called_once()

    @pytest.mark.parametrize(
        "exception",
        [
            FabricItemNotFoundError("Notebook", "Notebook", "Workspace"),
            FabricValidationError("field", "value", "bad"),
            FabricAPIError(400, "bad"),
            FileNotFoundError("missing"),
            ValueError("bad"),
        ],
    )
    def test_import_notebook_expected_errors(self, notebook_service, exception):
        """Import notebook maps known exceptions to error result."""
        notebook_service._create_notebook_definition = Mock(side_effect=exception)

        result = notebook_service.import_notebook(
            workspace_name="Workspace",
            notebook_name="Notebook",
            local_path="notebooks/test.ipynb",
        )

        assert result.status == "error"
        assert result.message

    def test_import_notebook_unexpected_error(self, notebook_service):
        """Unexpected exceptions return error with prefix."""
        notebook_service._create_notebook_definition = Mock(side_effect=RuntimeError("boom"))

        result = notebook_service.import_notebook(
            workspace_name="Workspace",
            notebook_name="Notebook",
            local_path="notebooks/test.ipynb",
        )

        assert result.status == "error"
        assert "Unexpected error" in result.message

    def test_get_notebook_content_success(self, notebook_service, mock_item_service, mock_workspace_service, mock_fabric_client):
        """Get notebook content decodes ipynb payload."""
        mock_workspace_service.resolve_workspace_id.return_value = "ws-123"
        mock_item_service.get_item_by_name.return_value = FabricItem(
            id="nb-123",
            display_name="Notebook",
            type="Notebook",
            workspace_id="ws-123",
        )

        notebook_content = {"cells": [{"cell_type": "code", "source": ["print(1)"]}]}
        definition_response = {
            "definition": {
                "parts": [
                    {
                        "path": "notebook.ipynb",
                        "payload": _encode_ipynb(notebook_content),
                        "payloadType": "InlineBase64",
                    }
                ]
            }
        }

        response = _make_response(200, definition_response)
        mock_fabric_client.make_api_request.return_value = response

        result = notebook_service.get_notebook_content("Workspace", "Notebook")

        assert result == notebook_content
        mock_fabric_client.make_api_request.assert_called_once()

    def test_get_notebook_content_lro_success(self, notebook_service, mock_item_service, mock_workspace_service, mock_fabric_client):
        """Handle 202 LRO with success status."""
        mock_workspace_service.resolve_workspace_id.return_value = "ws-123"
        mock_item_service.get_item_by_name.return_value = FabricItem(
            id="nb-123",
            display_name="Notebook",
            type="Notebook",
            workspace_id="ws-123",
        )

        notebook_content = {"cells": [{"cell_type": "markdown", "source": ["hi"]}]}
        definition_response = {
            "definition": {
                "parts": [
                    {
                        "path": "notebook.ipynb",
                        "payload": _encode_ipynb(notebook_content),
                        "payloadType": "InlineBase64",
                    }
                ]
            }
        }

        initial = _make_response(202, headers={"Location": "https://poll", "Retry-After": "0"})
        poll = _make_response(200, {"status": "Succeeded"})
        result_resp = _make_response(200, definition_response)

        mock_fabric_client.make_api_request.side_effect = [initial, poll, result_resp]

        with patch("time.sleep", return_value=None):
            result = notebook_service.get_notebook_content("Workspace", "Notebook")

        assert result == notebook_content

    def test_get_notebook_content_lro_failed(self, notebook_service, mock_item_service, mock_workspace_service, mock_fabric_client):
        """LRO failed status raises FabricError."""
        mock_workspace_service.resolve_workspace_id.return_value = "ws-123"
        mock_item_service.get_item_by_name.return_value = FabricItem(
            id="nb-123",
            display_name="Notebook",
            type="Notebook",
            workspace_id="ws-123",
        )

        initial = _make_response(202, headers={"Location": "https://poll", "Retry-After": "0"})
        poll = _make_response(200, {"status": "Failed", "error": {"message": "boom"}})
        mock_fabric_client.make_api_request.side_effect = [initial, poll]

        with patch("time.sleep", return_value=None):
            with pytest.raises(FabricError):
                notebook_service.get_notebook_content("Workspace", "Notebook")

    def test_get_notebook_content_lro_retry_after_non_integer(
        self, notebook_service, mock_item_service, mock_workspace_service, mock_fabric_client
    ):
        """LRO handles non-integer Retry-After headers."""
        mock_workspace_service.resolve_workspace_id.return_value = "ws-123"
        mock_item_service.get_item_by_name.return_value = FabricItem(
            id="nb-123",
            display_name="Notebook",
            type="Notebook",
            workspace_id="ws-123",
        )

        notebook_content = {"cells": [{"cell_type": "markdown", "source": ["hi"]}]}
        definition_response = {
            "definition": {
                "parts": [
                    {
                        "path": "notebook.ipynb",
                        "payload": _encode_ipynb(notebook_content),
                        "payloadType": "InlineBase64",
                    }
                ]
            }
        }

        initial = _make_response(
            202,
            headers={"Location": "https://poll", "Retry-After": "Wed, 21 Oct 2015 07:28:00 GMT"},
        )
        poll = _make_response(200, {"status": "Succeeded"})
        result_resp = _make_response(200, definition_response)
        mock_fabric_client.make_api_request.side_effect = [initial, poll, result_resp]

        with patch("time.sleep", return_value=None) as sleep_mock:
            result = notebook_service.get_notebook_content("Workspace", "Notebook")

        assert result == notebook_content
        sleep_mock.assert_called_once_with(5)

    def test_get_notebook_content_lro_in_progress(self, notebook_service, mock_item_service, mock_workspace_service, mock_fabric_client):
        """LRO continues polling on non-terminal status."""
        mock_workspace_service.resolve_workspace_id.return_value = "ws-123"
        mock_item_service.get_item_by_name.return_value = FabricItem(
            id="nb-123",
            display_name="Notebook",
            type="Notebook",
            workspace_id="ws-123",
        )

        notebook_content = {"cells": [{"cell_type": "markdown", "source": ["hi"]}]}
        definition_response = {
            "definition": {
                "parts": [
                    {
                        "path": "notebook.ipynb",
                        "payload": _encode_ipynb(notebook_content),
                        "payloadType": "InlineBase64",
                    }
                ]
            }
        }

        initial = _make_response(202, headers={"Location": "https://poll", "Retry-After": "0"})
        poll_running = _make_response(200, {"status": "Running"}, headers={"Retry-After": "0"})
        poll_accepted = _make_response(202, headers={"Retry-After": "0"})
        poll_succeeded = _make_response(200, {"status": "Succeeded"})
        result_resp = _make_response(200, definition_response)

        mock_fabric_client.make_api_request.side_effect = [
            initial,
            poll_running,
            poll_accepted,
            poll_succeeded,
            result_resp,
        ]

        with patch("time.sleep", return_value=None):
            result = notebook_service.get_notebook_content("Workspace", "Notebook")

        assert result == notebook_content

    def test_get_notebook_content_lro_timeout(self, notebook_service, mock_item_service, mock_workspace_service, mock_fabric_client):
        """LRO timeout raises FabricError after max retries."""
        mock_workspace_service.resolve_workspace_id.return_value = "ws-123"
        mock_item_service.get_item_by_name.return_value = FabricItem(
            id="nb-123",
            display_name="Notebook",
            type="Notebook",
            workspace_id="ws-123",
        )

        initial = _make_response(202, headers={"Location": "https://poll", "Retry-After": "0"})
        poll = _make_response(202, headers={"Retry-After": "0"})
        mock_fabric_client.make_api_request.side_effect = [initial] + [poll] * 30

        with patch("time.sleep", return_value=None):
            with pytest.raises(FabricError):
                notebook_service.get_notebook_content("Workspace", "Notebook")

    def test_get_notebook_content_lro_missing_location(self, notebook_service, mock_item_service, mock_workspace_service, mock_fabric_client):
        """LRO without Location header raises FabricError."""
        mock_workspace_service.resolve_workspace_id.return_value = "ws-123"
        mock_item_service.get_item_by_name.return_value = FabricItem(
            id="nb-123",
            display_name="Notebook",
            type="Notebook",
            workspace_id="ws-123",
        )

        initial = _make_response(202)
        mock_fabric_client.make_api_request.return_value = initial

        with pytest.raises(FabricError):
            notebook_service.get_notebook_content("Workspace", "Notebook")

    def test_get_notebook_content_returns_raw_definition(self, notebook_service, mock_item_service, mock_workspace_service, mock_fabric_client):
        """Return raw definition when no ipynb part exists."""
        mock_workspace_service.resolve_workspace_id.return_value = "ws-123"
        mock_item_service.get_item_by_name.return_value = FabricItem(
            id="nb-123",
            display_name="Notebook",
            type="Notebook",
            workspace_id="ws-123",
        )

        definition_response = {
            "definition": {
                "parts": [
                    {
                        "path": "notebook-content.py",
                        "payload": base64.b64encode(b"print(1)").decode("utf-8"),
                        "payloadType": "InlineBase64",
                    }
                ]
            }
        }
        response = _make_response(200, definition_response)
        mock_fabric_client.make_api_request.return_value = response

        result = notebook_service.get_notebook_content("Workspace", "Notebook")

        assert result == definition_response

    def test_list_notebooks(self, notebook_service, mock_item_service, mock_workspace_service):
        """List notebooks delegates to item service."""
        mock_workspace_service.resolve_workspace_id.return_value = "ws-123"
        items = [
            FabricItem(id="nb-1", display_name="A", type="Notebook", workspace_id="ws-123"),
            FabricItem(id="nb-2", display_name="B", type="Notebook", workspace_id="ws-123"),
        ]
        mock_item_service.list_items.return_value = items

        result = notebook_service.list_notebooks("Workspace")

        assert result == items
        mock_item_service.list_items.assert_called_once_with("ws-123", "Notebook")

    def test_get_notebook_by_name(self, notebook_service, mock_item_service, mock_workspace_service):
        """Get notebook by name delegates to item service."""
        mock_workspace_service.resolve_workspace_id.return_value = "ws-123"
        item = FabricItem(id="nb-1", display_name="Notebook", type="Notebook", workspace_id="ws-123")
        mock_item_service.get_item_by_name.return_value = item

        result = notebook_service.get_notebook_by_name("Workspace", "Notebook")

        assert result == item
        mock_item_service.get_item_by_name.assert_called_once_with("ws-123", "Notebook", "Notebook")

    def test_update_notebook_metadata(self, notebook_service, mock_item_service, mock_workspace_service):
        """Update notebook metadata delegates to item service."""
        mock_workspace_service.resolve_workspace_id.return_value = "ws-123"
        notebook = FabricItem(id="nb-1", display_name="Notebook", type="Notebook", workspace_id="ws-123")
        updated = FabricItem(id="nb-1", display_name="Notebook v2", type="Notebook", workspace_id="ws-123")
        mock_item_service.get_item_by_name.return_value = notebook
        mock_item_service.update_item.return_value = updated

        result = notebook_service.update_notebook_metadata("Workspace", "Notebook", {"displayName": "Notebook v2"})

        assert result == updated
        mock_item_service.update_item.assert_called_once_with("ws-123", "nb-1", {"displayName": "Notebook v2"})

    def test_delete_notebook(self, notebook_service, mock_item_service, mock_workspace_service):
        """Delete notebook delegates to item service."""
        mock_workspace_service.resolve_workspace_id.return_value = "ws-123"
        notebook = FabricItem(id="nb-1", display_name="Notebook", type="Notebook", workspace_id="ws-123")
        mock_item_service.get_item_by_name.return_value = notebook

        notebook_service.delete_notebook("Workspace", "Notebook")

        mock_item_service.delete_item.assert_called_once_with("ws-123", "nb-1")

    def test_execute_notebook_success(self, notebook_service, mock_workspace_service, mock_item_service):
        """Execute notebook returns mapped ExecuteNotebookResult."""
        job = FabricJob(
            job_instance_id="job-1",
            item_id="nb-1",
            job_type="RunNotebook",
            status="Completed",
            invoke_type="Manual",
            root_activity_id="root-1",
            start_time_utc="2025-01-01T00:00:00Z",
            end_time_utc="2025-01-01T00:05:00Z",
        )
        job_result = JobStatusResult(status="success", job=job, message="ok")

        with patch("ms_fabric_mcp_server.services.job.FabricJobService") as mock_job_service:
            mock_job_service.return_value.run_notebook_job.return_value = job_result
            result = notebook_service.execute_notebook("Workspace", "Notebook", parameters={"a": 1})

        assert result.status == "success"
        assert result.job_instance_id == "job-1"
        assert result.job_status == "Completed"

    def test_execute_notebook_error_result(self, notebook_service):
        """Execute notebook returns error when job result is error."""
        job_result = JobStatusResult(status="error", message="boom")

        with patch("ms_fabric_mcp_server.services.job.FabricJobService") as mock_job_service:
            mock_job_service.return_value.run_notebook_job.return_value = job_result
            result = notebook_service.execute_notebook("Workspace", "Notebook")

        assert result.status == "error"
        assert result.message == "boom"

    def test_execute_notebook_unexpected_error(self, notebook_service):
        """Execute notebook wraps unexpected exceptions."""
        with patch("ms_fabric_mcp_server.services.job.FabricJobService") as mock_job_service:
            mock_job_service.return_value.run_notebook_job.side_effect = RuntimeError("boom")
            result = notebook_service.execute_notebook("Workspace", "Notebook")

        assert result.status == "error"
        assert "Unexpected error" in result.message

    @pytest.mark.parametrize(
        "lakehouse_workspace_name,expected_workspace_id",
        [
            (None, "workspace-123"),
            ("Other Workspace", "workspace-999"),
        ],
    )
    def test_attach_lakehouse_to_notebook_success(
        self,
        notebook_service,
        mock_item_service,
        mock_workspace_service,
        mock_fabric_client,
        lakehouse_workspace_name,
        expected_workspace_id,
    ):
        """Attach lakehouse updates notebook dependencies and payload."""
        mock_workspace_service.resolve_workspace_id.side_effect = ["workspace-123", expected_workspace_id]

        notebook = FabricItem(id="nb-1", display_name="Notebook", type="Notebook", workspace_id="workspace-123")
        lakehouse = FabricItem(id="lh-1", display_name="Lakehouse", type="Lakehouse", workspace_id=expected_workspace_id)
        mock_item_service.get_item_by_name.side_effect = [notebook, lakehouse]

        notebook_payload = {"cells": [], "metadata": {}}
        definition_response = {
            "definition": {
                "parts": [
                    {
                        "path": "notebook.ipynb",
                        "payload": _encode_ipynb(notebook_payload),
                        "payloadType": "InlineBase64",
                    }
                ]
            }
        }

        get_def = _make_response(200, definition_response)
        update_def = MockResponseFactory.success({})
        mock_fabric_client.make_api_request.side_effect = [get_def, update_def]

        result = notebook_service.attach_lakehouse_to_notebook(
            workspace_name="Workspace",
            notebook_name="Notebook",
            lakehouse_name="Lakehouse",
            lakehouse_workspace_name=lakehouse_workspace_name,
        )

        assert result.status == "success"
        update_call = mock_fabric_client.make_api_request.call_args_list[1]
        payload = update_call.kwargs["payload"]
        encoded = payload["definition"]["parts"][0]["payload"]
        decoded = json.loads(base64.b64decode(encoded).decode("utf-8"))
        lakehouse_meta = decoded["metadata"]["dependencies"]["lakehouse"]
        assert lakehouse_meta["default_lakehouse"] == "lh-1"
        assert lakehouse_meta["default_lakehouse_name"] == "Lakehouse"
        assert lakehouse_meta["default_lakehouse_workspace_id"] == expected_workspace_id
        assert lakehouse_meta["known_lakehouses"] == [{"id": "lh-1"}]

    def test_attach_lakehouse_to_notebook_lro_success(
        self, notebook_service, mock_item_service, mock_workspace_service, mock_fabric_client
    ):
        """Attach lakehouse handles LRO getDefinition responses."""
        mock_workspace_service.resolve_workspace_id.side_effect = ["workspace-123", "workspace-123"]

        notebook = FabricItem(id="nb-1", display_name="Notebook", type="Notebook", workspace_id="workspace-123")
        lakehouse = FabricItem(id="lh-1", display_name="Lakehouse", type="Lakehouse", workspace_id="workspace-123")
        mock_item_service.get_item_by_name.side_effect = [notebook, lakehouse]

        notebook_payload = {"cells": [], "metadata": {}}
        definition_response = {
            "definition": {
                "parts": [
                    {
                        "path": "notebook.ipynb",
                        "payload": _encode_ipynb(notebook_payload),
                        "payloadType": "InlineBase64",
                    }
                ]
            }
        }

        initial = _make_response(202, headers={"Location": "https://poll", "Retry-After": "0"})
        poll = _make_response(200, {"status": "Succeeded"})
        result_resp = _make_response(200, definition_response)
        update_def = MockResponseFactory.success({})
        mock_fabric_client.make_api_request.side_effect = [initial, poll, result_resp, update_def]

        with patch("time.sleep", return_value=None):
            result = notebook_service.attach_lakehouse_to_notebook(
                workspace_name="Workspace",
                notebook_name="Notebook",
                lakehouse_name="Lakehouse",
            )

        assert result.status == "success"

    def test_attach_lakehouse_to_notebook_lro_retry_after_non_integer(
        self, notebook_service, mock_item_service, mock_workspace_service, mock_fabric_client
    ):
        """Attach lakehouse LRO handles non-integer Retry-After."""
        mock_workspace_service.resolve_workspace_id.side_effect = ["workspace-123", "workspace-123"]

        notebook = FabricItem(id="nb-1", display_name="Notebook", type="Notebook", workspace_id="workspace-123")
        lakehouse = FabricItem(id="lh-1", display_name="Lakehouse", type="Lakehouse", workspace_id="workspace-123")
        mock_item_service.get_item_by_name.side_effect = [notebook, lakehouse]

        notebook_payload = {"cells": [], "metadata": {}}
        definition_response = {
            "definition": {
                "parts": [
                    {
                        "path": "notebook.ipynb",
                        "payload": _encode_ipynb(notebook_payload),
                        "payloadType": "InlineBase64",
                    }
                ]
            }
        }

        initial = _make_response(
            202,
            headers={"Location": "https://poll", "Retry-After": "Wed, 21 Oct 2015 07:28:00 GMT"},
        )
        poll = _make_response(200, {"status": "Succeeded"})
        result_resp = _make_response(200, definition_response)
        update_def = MockResponseFactory.success({})
        mock_fabric_client.make_api_request.side_effect = [initial, poll, result_resp, update_def]

        with patch("time.sleep", return_value=None) as sleep_mock:
            result = notebook_service.attach_lakehouse_to_notebook(
                workspace_name="Workspace",
                notebook_name="Notebook",
                lakehouse_name="Lakehouse",
            )

        assert result.status == "success"
        sleep_mock.assert_called_once_with(5)

    def test_attach_lakehouse_to_notebook_missing_lakehouse(
        self, notebook_service, mock_item_service, mock_workspace_service
    ):
        """Missing lakehouse returns error result."""
        mock_workspace_service.resolve_workspace_id.side_effect = ["workspace-123", "workspace-123"]

        notebook = FabricItem(id="nb-1", display_name="Notebook", type="Notebook", workspace_id="workspace-123")
        mock_item_service.get_item_by_name.side_effect = [
            notebook,
            FabricItemNotFoundError("Lakehouse", "Lakehouse", "workspace-123"),
        ]

        result = notebook_service.attach_lakehouse_to_notebook(
            workspace_name="Workspace",
            notebook_name="Notebook",
            lakehouse_name="Lakehouse",
        )

        assert result.status == "error"

    def test_attach_lakehouse_to_notebook_update_failure(
        self, notebook_service, mock_item_service, mock_workspace_service, mock_fabric_client
    ):
        """Update failure returns error result."""
        mock_workspace_service.resolve_workspace_id.side_effect = ["workspace-123", "workspace-123"]

        notebook = FabricItem(id="nb-1", display_name="Notebook", type="Notebook", workspace_id="workspace-123")
        lakehouse = FabricItem(id="lh-1", display_name="Lakehouse", type="Lakehouse", workspace_id="workspace-123")
        mock_item_service.get_item_by_name.side_effect = [notebook, lakehouse]

        notebook_payload = {"cells": [], "metadata": {}}
        definition_response = {
            "definition": {
                "parts": [
                    {
                        "path": "notebook.ipynb",
                        "payload": _encode_ipynb(notebook_payload),
                        "payloadType": "InlineBase64",
                    }
                ]
            }
        }
        get_def = _make_response(200, definition_response)
        mock_fabric_client.make_api_request.side_effect = [
            get_def,
            FabricAPIError(500, "boom"),
        ]

        result = notebook_service.attach_lakehouse_to_notebook(
            workspace_name="Workspace",
            notebook_name="Notebook",
            lakehouse_name="Lakehouse",
        )

        assert result.status == "error"

    def test_get_notebook_execution_details_success(
        self, notebook_service, mock_item_service, mock_workspace_service, mock_fabric_client
    ):
        """Return execution details and summary."""
        mock_workspace_service.resolve_workspace_id.return_value = "workspace-123"
        notebook = FabricItem(id="nb-1", display_name="Notebook", type="Notebook", workspace_id="workspace-123")
        mock_item_service.get_item_by_name.return_value = notebook

        list_response = _make_response(
            200,
            {
                "value": [
                    {
                        "jobInstanceId": "job-1",
                        "livyId": "livy-1",
                        "state": "Success",
                        "sparkApplicationId": "app-1",
                        "operationName": "RunNotebook",
                        "submittedDateTime": "2025-01-01T00:00:00Z",
                        "startDateTime": "2025-01-01T00:01:00Z",
                        "endDateTime": "2025-01-01T00:05:00Z",
                        "totalDuration": {"value": 240},
                    }
                ]
            },
        )
        detail_response = _make_response(
            200,
            {
                "state": "Success",
                "sparkApplicationId": "app-1",
                "livyId": "livy-1",
                "jobInstanceId": "job-1",
                "operationName": "RunNotebook",
                "submittedDateTime": "2025-01-01T00:00:00Z",
                "startDateTime": "2025-01-01T00:01:00Z",
                "endDateTime": "2025-01-01T00:05:00Z",
                "queuedDuration": {"value": 5},
                "runningDuration": {"value": 235},
                "totalDuration": {"value": 240},
            },
        )
        job_response = _make_response(200, {"failureReason": {"message": "none"}})

        mock_fabric_client.make_api_request.side_effect = [
            list_response,
            detail_response,
            job_response,
        ]

        result = notebook_service.get_notebook_execution_details("Workspace", "Notebook", "job-1")

        assert result["status"] == "success"
        assert result["execution_summary"]["failure_reason"] == {"message": "none"}
        assert result["notebook_id"] == "nb-1"

    def test_get_notebook_execution_details_no_session(
        self, notebook_service, mock_item_service, mock_workspace_service, mock_fabric_client
    ):
        """Return error when no matching session found."""
        mock_workspace_service.resolve_workspace_id.return_value = "workspace-123"
        notebook = FabricItem(id="nb-1", display_name="Notebook", type="Notebook", workspace_id="workspace-123")
        mock_item_service.get_item_by_name.return_value = notebook

        list_response = _make_response(200, {"value": []})
        mock_fabric_client.make_api_request.return_value = list_response

        result = notebook_service.get_notebook_execution_details("Workspace", "Notebook", "job-1")

        assert result["status"] == "error"
        assert result["available_sessions"] == 0

    def test_get_notebook_execution_details_api_error(
        self, notebook_service, mock_item_service, mock_workspace_service, mock_fabric_client
    ):
        """API errors return error status."""
        mock_workspace_service.resolve_workspace_id.return_value = "workspace-123"
        notebook = FabricItem(id="nb-1", display_name="Notebook", type="Notebook", workspace_id="workspace-123")
        mock_item_service.get_item_by_name.return_value = notebook

        mock_fabric_client.make_api_request.side_effect = FabricAPIError(500, "boom")

        result = notebook_service.get_notebook_execution_details("Workspace", "Notebook", "job-1")

        assert result["status"] == "error"
        assert "boom" in result["message"]

    def test_list_notebook_executions_success_with_limit(
        self, notebook_service, mock_item_service, mock_workspace_service, mock_fabric_client
    ):
        """List executions applies limit to session summaries."""
        mock_workspace_service.resolve_workspace_id.return_value = "workspace-123"
        notebook = FabricItem(id="nb-1", display_name="Notebook", type="Notebook", workspace_id="workspace-123")
        mock_item_service.get_item_by_name.return_value = notebook

        list_response = _make_response(
            200,
            {
                "value": [
                    {"jobInstanceId": "job-1", "livyId": "1", "state": "Success"},
                    {"jobInstanceId": "job-2", "livyId": "2", "state": "Failed"},
                ]
            },
        )
        mock_fabric_client.make_api_request.return_value = list_response

        result = notebook_service.list_notebook_executions("Workspace", "Notebook", limit=1)

        assert result["status"] == "success"
        assert len(result["sessions"]) == 1
        assert result["total_count"] == 2

    def test_list_notebook_executions_item_not_found(
        self, notebook_service, mock_item_service, mock_workspace_service
    ):
        """Item not found maps to error response."""
        mock_workspace_service.resolve_workspace_id.return_value = "workspace-123"
        mock_item_service.get_item_by_name.side_effect = FabricItemNotFoundError("Notebook", "Notebook", "workspace-123")

        result = notebook_service.list_notebook_executions("Workspace", "Notebook")

        assert result["status"] == "error"

    def test_get_notebook_driver_logs_success_truncated(
        self, notebook_service, mock_item_service, mock_workspace_service, mock_fabric_client
    ):
        """Driver log retrieval truncates to max lines."""
        mock_workspace_service.resolve_workspace_id.return_value = "workspace-123"
        notebook = FabricItem(id="nb-1", display_name="Notebook", type="Notebook", workspace_id="workspace-123")
        mock_item_service.get_item_by_name.return_value = notebook

        notebook_service.get_notebook_execution_details = Mock(
            return_value={
                "status": "success",
                "execution_summary": {
                    "livy_id": "livy-1",
                    "spark_application_id": "app-1",
                },
            }
        )

        meta_response = _make_response(200, {"sizeInBytes": 123})
        log_response = _make_response(200, text="line1\nline2\nline3")
        mock_fabric_client.make_api_request.side_effect = [meta_response, log_response]

        result = notebook_service.get_notebook_driver_logs(
            "Workspace",
            "Notebook",
            "job-1",
            log_type="stdout",
            max_lines=2,
        )

        assert result["status"] == "success"
        assert result["truncated"] is True
        assert result["log_content"] == "line2\nline3"

    def test_get_notebook_driver_logs_invalid_log_type(self, notebook_service):
        """Invalid log_type returns error result."""
        result = notebook_service.get_notebook_driver_logs(
            "Workspace", "Notebook", "job-1", log_type="bad"
        )

        assert result["status"] == "error"
        assert "Invalid log_type" in result["message"]

    def test_get_notebook_driver_logs_exec_details_error(self, notebook_service):
        """Propagates execution detail error."""
        notebook_service.get_notebook_execution_details = Mock(
            return_value={"status": "error", "message": "boom"}
        )

        result = notebook_service.get_notebook_driver_logs(
            "Workspace", "Notebook", "job-1"
        )

        assert result["status"] == "error"
        assert result["message"] == "boom"

    def test_get_notebook_driver_logs_missing_summary_fields(self, notebook_service):
        """Missing livy or spark app IDs returns error."""
        notebook_service.get_notebook_execution_details = Mock(
            return_value={"status": "success", "execution_summary": {}}
        )

        result = notebook_service.get_notebook_driver_logs(
            "Workspace", "Notebook", "job-1"
        )

        assert result["status"] == "error"

    def test_get_notebook_driver_logs_api_error(
        self, notebook_service, mock_item_service, mock_workspace_service, mock_fabric_client
    ):
        """API errors are mapped to error result."""
        mock_workspace_service.resolve_workspace_id.return_value = "workspace-123"
        notebook = FabricItem(id="nb-1", display_name="Notebook", type="Notebook", workspace_id="workspace-123")
        mock_item_service.get_item_by_name.return_value = notebook
        notebook_service.get_notebook_execution_details = Mock(
            return_value={
                "status": "success",
                "execution_summary": {"livy_id": "livy-1", "spark_application_id": "app-1"},
            }
        )
        mock_fabric_client.make_api_request.side_effect = FabricAPIError(500, "boom")

        result = notebook_service.get_notebook_driver_logs(
            "Workspace", "Notebook", "job-1"
        )

        assert result["status"] == "error"
