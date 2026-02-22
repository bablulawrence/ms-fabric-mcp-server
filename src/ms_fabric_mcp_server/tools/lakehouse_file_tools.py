# ABOUTME: Lakehouse file MCP tools for OneLake operations.
# ABOUTME: Provides list, upload, and delete file tools for lakehouse Files.
"""Lakehouse file tools."""

import logging
from pathlib import Path
from typing import TYPE_CHECKING, Any, Dict, List, Optional

if TYPE_CHECKING:
    from fastmcp import FastMCP

from ..client.exceptions import FabricValidationError
from ..services import (FabricItemService, FabricLakehouseFileService,
                        FabricWorkspaceService)
from .base import handle_tool_errors, log_tool_invocation

logger = logging.getLogger(__name__)


def register_lakehouse_file_tools(
    mcp: "FastMCP",
    lakehouse_file_service: FabricLakehouseFileService,
    workspace_service: FabricWorkspaceService,
    item_service: FabricItemService,
) -> None:
    """Register lakehouse file tools for OneLake operations."""

    def _resolve_lakehouse(
        workspace_id: str,
        lakehouse_id: Optional[str],
        lakehouse_name: Optional[str],
    ) -> tuple[str, Optional[str]]:
        if lakehouse_id:
            return lakehouse_id, lakehouse_name
        if not lakehouse_name or not str(lakehouse_name).strip():
            raise FabricValidationError(
                "lakehouse_name",
                str(lakehouse_name),
                "Provide lakehouse_name when lakehouse_id is not supplied.",
            )
        lakehouse = item_service.get_item_by_name(
            workspace_id,
            lakehouse_name,
            "Lakehouse",
        )
        return lakehouse.id, lakehouse.display_name

    @mcp.tool(title="List Lakehouse Files")
    @handle_tool_errors
    def list_lakehouse_files(
        workspace_name: str,
        lakehouse_name: Optional[str] = None,
        lakehouse_id: Optional[str] = None,
        path: Optional[str] = None,
        recursive: bool = True,
    ) -> dict:
        """List files from a lakehouse Files area.

        Parameters:
            workspace_name: Display name of the workspace.
            lakehouse_name: Display name of the lakehouse (required if lakehouse_id not provided).
            lakehouse_id: ID of the lakehouse (optional).
            path: Optional path under Files to list (e.g., "raw/2025").
                  This tool does not support the Tables area.
            recursive: Whether to list files recursively (default True).
        """
        log_tool_invocation(
            "list_lakehouse_files",
            workspace_name=workspace_name,
            lakehouse_name=lakehouse_name,
            lakehouse_id=lakehouse_id,
            path=path,
            recursive=recursive,
        )

        if path:
            normalized_path = str(path).strip().replace("\\", "/").lstrip("/")
            if normalized_path:
                first_segment = normalized_path.split("/", 1)[0].lower()
                if first_segment == "tables":
                    raise FabricValidationError(
                        "path",
                        str(path),
                        "list_lakehouse_files only supports the Files area. "
                        "Use a path under Files (for example, 'raw/2025').",
                    )

        workspace_id = workspace_service.resolve_workspace_id(workspace_name)
        lakehouse_id, resolved_name = _resolve_lakehouse(
            workspace_id, lakehouse_id, lakehouse_name
        )

        files = lakehouse_file_service.list_files(
            workspace_id=workspace_id,
            lakehouse_id=lakehouse_id,
            path=path,
            recursive=recursive,
        )

        lakehouse_prefix = f"{lakehouse_id}/"
        formatted: List[Dict[str, Any]] = []
        for entry in files:
            name = entry.get("name", "")
            if name.startswith(lakehouse_prefix):
                name = name[len(lakehouse_prefix) :]
            # Filter out entries outside Files/ (e.g., Tables/ delta internals)
            if not name.startswith("Files"):
                continue
            formatted.append(
                {
                    "name": name,
                    "is_directory": entry.get("isDirectory"),
                    "content_length": entry.get("contentLength"),
                    "last_modified": entry.get("lastModified"),
                    "etag": entry.get("etag"),
                }
            )

        return {
            "status": "success",
            "workspace_name": workspace_name,
            "workspace_id": workspace_id,
            "lakehouse_id": lakehouse_id,
            "lakehouse_name": resolved_name or lakehouse_name,
            "path": path,
            "recursive": recursive,
            "file_count": len(formatted),
            "files": formatted,
        }

    @mcp.tool(title="Upload Lakehouse File")
    @handle_tool_errors
    def upload_lakehouse_file(
        workspace_name: str,
        lakehouse_name: Optional[str] = None,
        lakehouse_id: Optional[str] = None,
        local_file_path: str = "",
        destination_path: Optional[str] = None,
        create_missing_directories: bool = True,
    ) -> dict:
        """Upload a local file into a lakehouse Files area.

        Parameters:
            workspace_name: Display name of the workspace.
            lakehouse_name: Display name of the lakehouse (required if lakehouse_id not provided).
            lakehouse_id: ID of the lakehouse (optional).
            local_file_path: Path to the local file to upload.
            destination_path: Destination path under Files. Defaults to local file name.
            create_missing_directories: Create missing directories (default True).
        """
        log_tool_invocation(
            "upload_lakehouse_file",
            workspace_name=workspace_name,
            lakehouse_name=lakehouse_name,
            lakehouse_id=lakehouse_id,
            local_file_path=local_file_path,
            destination_path=destination_path,
            create_missing_directories=create_missing_directories,
        )

        if not destination_path:
            destination_path = Path(local_file_path).name

        workspace_id = workspace_service.resolve_workspace_id(workspace_name)
        lakehouse_id, resolved_name = _resolve_lakehouse(
            workspace_id, lakehouse_id, lakehouse_name
        )

        result = lakehouse_file_service.upload_file(
            workspace_id=workspace_id,
            lakehouse_id=lakehouse_id,
            local_file_path=local_file_path,
            destination_path=destination_path,
            create_missing_directories=create_missing_directories,
        )

        return {
            "status": "success",
            "workspace_name": workspace_name,
            "workspace_id": workspace_id,
            "lakehouse_id": lakehouse_id,
            "lakehouse_name": resolved_name or lakehouse_name,
            "destination_path": result.get("path"),
            "size_bytes": result.get("size_bytes"),
            "message": "File uploaded successfully",
        }

    @mcp.tool(title="Delete Lakehouse File")
    @handle_tool_errors
    def delete_lakehouse_file(
        workspace_name: str,
        lakehouse_name: Optional[str] = None,
        lakehouse_id: Optional[str] = None,
        path: str = "",
        recursive: bool = False,
    ) -> dict:
        """Delete a file or directory from a lakehouse Files area.

        Parameters:
            workspace_name: Display name of the workspace.
            lakehouse_name: Display name of the lakehouse (required if lakehouse_id not provided).
            lakehouse_id: ID of the lakehouse (optional).
            path: File or directory path under Files to delete.
            recursive: Delete recursively when path is a directory (default False).
        """
        log_tool_invocation(
            "delete_lakehouse_file",
            workspace_name=workspace_name,
            lakehouse_name=lakehouse_name,
            lakehouse_id=lakehouse_id,
            path=path,
            recursive=recursive,
        )

        workspace_id = workspace_service.resolve_workspace_id(workspace_name)
        lakehouse_id, resolved_name = _resolve_lakehouse(
            workspace_id, lakehouse_id, lakehouse_name
        )

        lakehouse_file_service.delete_file(
            workspace_id=workspace_id,
            lakehouse_id=lakehouse_id,
            path=path,
            recursive=recursive,
        )

        return {
            "status": "success",
            "workspace_name": workspace_name,
            "workspace_id": workspace_id,
            "lakehouse_id": lakehouse_id,
            "lakehouse_name": resolved_name or lakehouse_name,
            "path": path,
            "message": "File deleted successfully",
        }


__all__ = ["register_lakehouse_file_tools"]
