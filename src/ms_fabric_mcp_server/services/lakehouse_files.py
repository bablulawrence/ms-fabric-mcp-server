# ABOUTME: Lakehouse file operations via OneLake DFS endpoint.
# ABOUTME: Provides list, upload, and delete capabilities for lakehouse Files.
"""Lakehouse file operations for OneLake (DFS API)."""

from __future__ import annotations

import logging
from email.utils import formatdate
from pathlib import Path
from typing import Any, Dict, List, Optional
from urllib.parse import quote, urlencode

from ..client.exceptions import (FabricAPIError, FabricError,
                                 FabricValidationError)
from ..client.http_client import FabricClient

logger = logging.getLogger(__name__)


class FabricLakehouseFileService:
    """Service for managing lakehouse files in OneLake."""

    ONELAKE_DFS_BASE_URL = "https://onelake.dfs.fabric.microsoft.com"
    STORAGE_SCOPE = ["https://storage.azure.com/.default"]
    STORAGE_API_VERSION = "2023-11-03"

    def __init__(self, client: FabricClient) -> None:
        self.client = client

    def list_files(
        self,
        workspace_id: str,
        lakehouse_id: str,
        path: Optional[str] = None,
        recursive: bool = True,
    ) -> List[Dict[str, Any]]:
        """List files in the lakehouse Files area."""
        filesystem_url = self._build_filesystem_url(
            workspace_id=workspace_id,
        )
        directory = self._build_directory_param(
            lakehouse_id=lakehouse_id,
            path=path,
        )

        params: Dict[str, Any] = {
            "resource": "filesystem",
            "directory": directory,
            "recursive": str(bool(recursive)).lower(),
        }

        paths: List[Dict[str, Any]] = []
        continuation: Optional[str] = None

        while True:
            request_params = dict(params)
            if continuation:
                request_params["continuation"] = continuation
            request_url = f"{filesystem_url}?{urlencode(request_params)}"
            response = self._request("GET", request_url)
            try:
                payload = response.json()
            except ValueError as exc:
                raise FabricError(f"Failed to parse list response: {exc}") from exc

            if not isinstance(payload, dict):
                raise FabricError("Invalid list response payload")

            paths.extend(payload.get("paths", []))
            continuation = payload.get("continuation")
            if not continuation:
                break

        return paths

    def upload_file(
        self,
        workspace_id: str,
        lakehouse_id: str,
        local_file_path: str,
        destination_path: str,
        create_missing_directories: bool = True,
    ) -> Dict[str, Any]:
        """Upload a local file to the lakehouse Files area."""
        local_path = Path(local_file_path)
        if not local_path.is_file():
            raise FabricValidationError(
                "local_file_path",
                local_file_path,
                "Local file does not exist or is not a file.",
            )

        normalized_destination = self._normalize_relative_path(
            destination_path, allow_empty=False
        )

        if create_missing_directories:
            self._ensure_directories(
                workspace_id,
                lakehouse_id,
                normalized_destination,
            )

        file_url = self._build_path_url(
            workspace_id=workspace_id,
            lakehouse_id=lakehouse_id,
            relative_path=normalized_destination,
            allow_empty=False,
        )

        # Create file
        create_url = f"{file_url}?{urlencode({'resource': 'file'})}"
        self._request("PUT", create_url)

        file_bytes = local_path.read_bytes()
        size_bytes = len(file_bytes)

        if size_bytes:
            append_url = f"{file_url}?{urlencode({'action': 'append', 'position': 0})}"
            self._request(
                "PATCH",
                append_url,
                data=file_bytes,
                content_type="application/octet-stream",
                content_length=size_bytes,
            )

        flush_url = (
            f"{file_url}?{urlencode({'action': 'flush', 'position': size_bytes})}"
        )
        self._request("PATCH", flush_url)

        return {
            "path": normalized_destination,
            "size_bytes": size_bytes,
        }

    def delete_file(
        self,
        workspace_id: str,
        lakehouse_id: str,
        path: str,
        recursive: bool = False,
    ) -> None:
        """Delete a file or directory in the lakehouse Files area."""
        file_url = self._build_path_url(
            workspace_id=workspace_id,
            lakehouse_id=lakehouse_id,
            relative_path=path,
            allow_empty=False,
        )

        params: Dict[str, Any] = {}
        if recursive:
            params["recursive"] = "true"

        request_url = f"{file_url}?{urlencode(params)}" if params else file_url
        self._request("DELETE", request_url)

    def _request(
        self,
        method: str,
        url: str,
        data: Optional[bytes] = None,
        content_type: Optional[str] = None,
        content_length: Optional[int] = None,
    ):
        headers = self._build_headers(
            content_type=content_type,
            content_length=content_length,
        )
        response = self.client._session.request(
            method=method,
            url=url,
            headers=headers,
            data=data,
            timeout=self.client.config.API_CALL_TIMEOUT,
        )
        self.client.handle_api_errors(response)
        return response

    def _build_headers(
        self,
        content_type: Optional[str] = None,
        content_length: Optional[int] = None,
    ) -> Dict[str, str]:
        token = self.client.get_auth_token(self.STORAGE_SCOPE)
        headers = {
            "Authorization": f"Bearer {token}",
            "x-ms-version": self.STORAGE_API_VERSION,
            "x-ms-date": formatdate(usegmt=True),
        }
        if content_type:
            headers["Content-Type"] = content_type
        if content_length is not None:
            headers["Content-Length"] = str(content_length)
        return headers

    def _normalize_relative_path(self, path: Optional[str], allow_empty: bool) -> str:
        if path is None:
            if allow_empty:
                return ""
            raise FabricValidationError("path", "None", "Path cannot be empty.")

        trimmed = str(path).strip()
        if not trimmed:
            if allow_empty:
                return ""
            raise FabricValidationError("path", trimmed, "Path cannot be empty.")

        normalized = trimmed.replace("\\", "/").lstrip("/")
        parts = [part for part in normalized.split("/") if part and part != "."]

        while parts and parts[0].lower() == "files":
            parts = parts[1:]

        # Normalize accidental duplicated-prefix inputs like
        # "mcp_tools_test/Files/mcp_tools_test/file.txt".
        for idx in range(1, len(parts)):
            if parts[idx].lower() != "files":
                continue
            prefix = parts[:idx]
            suffix = parts[idx + 1 :]
            if len(suffix) >= len(prefix) and all(
                left.lower() == right.lower() for left, right in zip(prefix, suffix)
            ):
                parts = suffix
                break

        if any(part == ".." for part in parts):
            raise FabricValidationError("path", trimmed, "Path cannot include '..'.")

        return "/".join(parts)

    def _build_path_url(
        self,
        workspace_id: str,
        lakehouse_id: str,
        relative_path: Optional[str],
        allow_empty: bool,
    ) -> str:
        normalized = self._normalize_relative_path(
            relative_path, allow_empty=allow_empty
        )
        base = f"{self.ONELAKE_DFS_BASE_URL.rstrip('/')}/{workspace_id}/{lakehouse_id}/Files"
        if not normalized:
            return base
        return f"{base}/{quote(normalized, safe='/')}"

    def _build_filesystem_url(self, workspace_id: str) -> str:
        return f"{self.ONELAKE_DFS_BASE_URL.rstrip('/')}/{workspace_id}"

    def _build_directory_param(self, lakehouse_id: str, path: Optional[str]) -> str:
        normalized = self._normalize_relative_path(path, allow_empty=True)
        if normalized:
            return f"{lakehouse_id}/Files/{normalized}"
        return f"{lakehouse_id}/Files"

    def _ensure_directories(
        self,
        workspace_id: str,
        lakehouse_id: str,
        relative_path: str,
    ) -> None:
        parts = relative_path.split("/")[:-1]
        if not parts:
            return

        accumulated: List[str] = []
        for part in parts:
            accumulated.append(part)
            directory_path = "/".join(accumulated)
            directory_url = self._build_path_url(
                workspace_id=workspace_id,
                lakehouse_id=lakehouse_id,
                relative_path=directory_path,
                allow_empty=False,
            )
            request_url = f"{directory_url}?{urlencode({'resource': 'directory'})}"
            try:
                self._request("PUT", request_url)
            except FabricAPIError as exc:
                if exc.status_code == 409:
                    continue
                raise


__all__ = ["FabricLakehouseFileService"]
