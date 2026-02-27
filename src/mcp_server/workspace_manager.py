"""
Databricks Workspace / Notebook operations.

Provides tools to browse the workspace file tree and read notebook source code.
"""

import base64
from typing import Any, Dict, List, Optional

from .api_client import get_api_client
from .workspaces import resolve_workspace_name


def _list_workspace(path: str = "/", workspace: Optional[str] = None) -> str:
    try:
        ws = resolve_workspace_name(workspace)
        client = get_api_client(ws)
        resp = client.get("/workspace/list", params={"path": path})

        objects = resp.get("objects", [])
        if not objects:
            return f"No objects found at path '{path}'."

        header = "| Path | Type | Language |"
        sep = "| --- | --- | --- |"
        rows = []
        for obj in sorted(
            objects, key=lambda o: (o.get("object_type", ""), o.get("path", ""))
        ):
            obj_path = obj.get("path", "")
            obj_type = obj.get("object_type", "UNKNOWN")
            language = obj.get("language", "")
            rows.append(f"| {obj_path} | {obj_type} | {language} |")

        return (
            f"Workspace objects at '{path}' ({len(objects)} found):\n\n"
            f"{header}\n{sep}\n" + "\n".join(rows)
        )
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
    @mcp.tool()
    def list_workspace_files(path: str = "/", workspace: Optional[str] = None) -> str:
        """
        List files and directories in the Databricks workspace.

        Browse the workspace tree to find notebooks, folders, and files.

        Args:
            path: Workspace path to list (default: root "/")

        Returns:
            Markdown table of objects with path, type, and language
        """
        return _list_workspace(path, workspace)

    @mcp.tool()
    def read_notebook(path: str, workspace: Optional[str] = None) -> str:
        """
        Read the source code of a Databricks notebook.

        Essential for understanding what a job actually runs, debugging failures,
        and reviewing notebook logic.

        Args:
            path: Full workspace path to the notebook (e.g., /Repos/user/project/notebook)

        Returns:
            Notebook source code in a fenced code block
        """
        return _read_notebook(path, workspace)
