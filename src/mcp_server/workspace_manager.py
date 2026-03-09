"""
Databricks Workspace / Notebook operations.

Provides tools to browse the workspace file tree, read notebook source code,
and inspect workspace object metadata.
"""

import base64
from typing import Any, Dict, List, Optional

from .api_client import get_api_client
from .constants import (
    DEFAULT_PAGE_LIMIT,
    READ_ONLY_ANNOTATIONS,
    clamp_pagination,
    enforce_character_limit,
    pagination_footer,
)
from .workspaces import resolve_workspace_name


def _list_workspace(
    path: str = "/",
    workspace: Optional[str] = None,
    limit: int = DEFAULT_PAGE_LIMIT,
    offset: int = 0,
) -> str:
    try:
        limit, offset = clamp_pagination(limit, offset)
        ws = resolve_workspace_name(workspace)
        client = get_api_client(ws)
        resp = client.get("/workspace/list", params={"path": path})

        all_objects = resp.get("objects", [])
        if not all_objects:
            return f"No objects found at path '{path}'."

        all_objects = sorted(
            all_objects, key=lambda o: (o.get("object_type", ""), o.get("path", ""))
        )

        total = len(all_objects)
        objects = all_objects[offset : offset + limit]
        if not objects:
            return f"No objects at offset {offset} for path '{path}' (total: {total})."

        header = "| Path | Type | Language |"
        sep = "| --- | --- | --- |"
        rows = []
        for obj in objects:
            obj_path = obj.get("path", "")
            obj_type = obj.get("object_type", "UNKNOWN")
            language = obj.get("language", "")
            rows.append(f"| {obj_path} | {obj_type} | {language} |")

        result = (
            f"Workspace objects at '{path}' ({len(objects)} shown, {total} total):\n\n"
            f"{header}\n{sep}\n"
            + "\n".join(rows)
            + pagination_footer(
                count=len(objects), offset=offset, limit=limit, total=total
            )
        )
        return enforce_character_limit(result, "Use offset and limit to paginate.")
    except Exception as e:
        return f"Error listing workspace path '{path}': {e}"


def _read_notebook(path: str, workspace: Optional[str] = None) -> str:
    try:
        ws = resolve_workspace_name(workspace)
        client = get_api_client(ws)
        resp = client.get(
            "/workspace/export",
            params={"path": path, "format": "SOURCE"},
        )

        content_b64 = resp.get("content", "")
        if not content_b64:
            return f"No content returned for notebook at '{path}'."

        try:
            content = base64.b64decode(content_b64).decode("utf-8")
        except Exception:
            return f"Failed to decode notebook content at '{path}'."

        language = resp.get("language", resp.get("file_type", ""))
        lang_tag = {"PYTHON": "python", "SQL": "sql", "SCALA": "scala", "R": "r"}.get(
            language, ""
        )

        max_chars = 50_000
        truncated = len(content) > max_chars
        if truncated:
            content = content[:max_chars]

        result = f"Notebook: **{path}** ({language})\n\n```{lang_tag}\n{content}\n```"
        if truncated:
            result += "\n\n(Truncated — notebook exceeds 50 KB display limit)"
        return result
    except Exception as e:
        return f"Error reading notebook '{path}': {e}"


def _get_workspace_object_status(path: str, workspace: Optional[str] = None) -> str:
    try:
        ws = resolve_workspace_name(workspace)
        client = get_api_client(ws)
        obj = client.get("/workspace/get-status", params={"path": path})

        lines = [f"Workspace Object: **{path}**\n"]
        lines.append(f"- **Type**: {obj.get('object_type', 'N/A')}")
        lines.append(f"- **Object ID**: {obj.get('object_id', 'N/A')}")
        if obj.get("language"):
            lines.append(f"- **Language**: {obj['language']}")
        if obj.get("created_at"):
            lines.append(f"- **Created**: {obj['created_at']}")
        if obj.get("modified_at"):
            lines.append(f"- **Modified**: {obj['modified_at']}")
        if obj.get("size") is not None:
            lines.append(f"- **Size**: {obj['size']} bytes")

        return "\n".join(lines)
    except Exception as e:
        return f"Error getting status for '{path}': {e}"


# ------------------------------------------------------------------
# Tool registration
# ------------------------------------------------------------------


def register_workspace_tools(mcp: Any) -> None:
    @mcp.tool(
        name="databricks_list_workspace_files",
        annotations={"title": "List Workspace Files", **READ_ONLY_ANNOTATIONS},
    )
    def list_workspace_files(
        path: str = "/",
        workspace: Optional[str] = None,
        limit: int = DEFAULT_PAGE_LIMIT,
        offset: int = 0,
    ) -> str:
        """
        List files and directories in the Databricks workspace.

        Use when: browsing the workspace tree to find notebooks, folders, and files.
        Do NOT use when: you already know the notebook path — use
        databricks_read_notebook to read it directly.

        Args:
            path: Workspace path to list (default: root "/")
            workspace: Target workspace name (uses default if omitted)
            limit: Max objects to return (default 25, max 100)
            offset: Number of results to skip for pagination (default 0)

        Returns:
            Markdown table of objects with path, type, and language
        """
        return _list_workspace(path, workspace, limit, offset)

    @mcp.tool(
        name="databricks_read_notebook",
        annotations={"title": "Read Notebook Source", **READ_ONLY_ANNOTATIONS},
    )
    def read_notebook(path: str, workspace: Optional[str] = None) -> str:
        """
        Read the source code of a Databricks notebook.

        Use when: understanding what a job runs, debugging failures, or reviewing
        notebook logic.
        Do NOT use when: you only need file metadata — use
        databricks_get_workspace_object_status.

        Args:
            path: Full workspace path to the notebook
                (e.g., /Repos/user/project/notebook)
            workspace: Target workspace name (uses default if omitted)

        Returns:
            Notebook source code in a fenced code block
        """
        return _read_notebook(path, workspace)

    @mcp.tool(
        name="databricks_get_workspace_object_status",
        annotations={"title": "Get Workspace Object Status", **READ_ONLY_ANNOTATIONS},
    )
    def get_workspace_object_status(path: str, workspace: Optional[str] = None) -> str:
        """
        Get metadata for a workspace object (notebook, folder, file).

        Use when: you need the object type, ID, language, or timestamps
        without reading the full content.
        Do NOT use when: you need the notebook source — use
        databricks_read_notebook.

        Args:
            path: Full workspace path to the object
            workspace: Target workspace name (uses default if omitted)

        Returns:
            Object metadata including type, ID, language, and timestamps
        """
        return _get_workspace_object_status(path, workspace)
