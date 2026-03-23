"""
DBFS (Databricks File System) log access for the MCP server.

Provides tools to list and read cluster log files from DBFS, enabling
deep debugging of driver logs, init scripts, and executor output.
"""

import base64
from typing import Any, Optional

from .api_client import get_api_client
from .constants import (
    READ_ONLY_ANNOTATIONS,
    enforce_character_limit,
)
from .workspaces import resolve_workspace_name


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _ts_to_str(epoch_ms: int) -> str:
    """Convert epoch milliseconds to a human-readable timestamp."""
    if not epoch_ms:
        return "N/A"
    from datetime import datetime

    return datetime.fromtimestamp(epoch_ms / 1000).strftime("%Y-%m-%d %H:%M:%S UTC")


# ---------------------------------------------------------------------------
# Standalone logic
# ---------------------------------------------------------------------------


def _list_cluster_log_files(
    cluster_id: str,
    log_base_path: str = "dbfs:/cluster-logs",
    subfolder: str = "driver",
    workspace: Optional[str] = None,
) -> str:
    try:
        ws = resolve_workspace_name(workspace)
        client = get_api_client(ws)
        dbfs_path = f"{log_base_path}/{cluster_id}/{subfolder}"

        resp = client.get("/dbfs/list", params={"path": dbfs_path})

        files = resp.get("files", [])
        if not files:
            return (
                f"No log files found at `{dbfs_path}`.\n\n"
                "This could mean:\n"
                "- Cluster log delivery is not configured\n"
                "- The log base path is different (try a different `log_base_path`)\n"
                "- The cluster has not yet written logs"
            )

        result = f"## Log Files at `{dbfs_path}`\n\n"
        result += "| File Name | Size (bytes) | Last Modified |\n"
        result += "| --- | --- | --- |\n"

        for f in files:
            path = f.get("path", "N/A")
            name = path.rsplit("/", 1)[-1] if "/" in path else path
            size = f.get("file_size", 0)
            mod_time = _ts_to_str(f.get("modification_time", 0))
            is_dir = f.get("is_dir", False)
            display_name = f"📁 {name}/" if is_dir else name
            result += f"| {display_name} | {size:,} | {mod_time} |\n"

        result += (
            f"\nUse `databricks_read_cluster_log_file` with the full path to read "
            f"file contents.\n"
            f'Example: `databricks_read_cluster_log_file(file_path="{dbfs_path}/stdout")`\n'
        )
        return enforce_character_limit(result)
    except ValueError as e:
        # 4xx client errors (like 404) from api_client
        err = str(e)
        if "404" in err:
            return (
                f"Path `{log_base_path}/{cluster_id}/{subfolder}` not found. "
                "The cluster may not have log delivery configured, or the base path may differ."
            )
        return f"Error listing cluster log files: {e}"
    except Exception as e:
        return f"Error listing cluster log files: {e}"


def _read_cluster_log_file(
    file_path: str,
    offset: int = 0,
    max_bytes: int = 524288,
    workspace: Optional[str] = None,
) -> str:
    try:
        ws = resolve_workspace_name(workspace)
        client = get_api_client(ws)
        read_length = min(max_bytes, 1_048_576)

        resp = client.get(
            "/dbfs/read",
            params={"path": file_path, "offset": offset, "length": read_length},
        )

        data_b64 = resp.get("data", "")
        bytes_read = resp.get("bytes_read", 0)

        if not data_b64 or bytes_read == 0:
            return f"File `{file_path}` is empty or could not be read (bytes_read={bytes_read})."

        try:
            content = base64.b64decode(data_b64).decode("utf-8", errors="replace")
        except Exception:
            content = base64.b64decode(data_b64).decode("latin-1")

        result = f"## Log File: `{file_path}`\n\n"
        result += f"- **Offset:** {offset}\n"
        result += f"- **Bytes Read:** {bytes_read:,}\n\n"
        result += f"```\n{content}\n```\n"

        if bytes_read >= read_length:
            next_offset = offset + bytes_read
            result += f"\n_File may have more content. Read the next chunk with `offset={next_offset}`._\n"

        return enforce_character_limit(result)
    except ValueError as e:
        err = str(e)
        if "404" in err:
            return f"File `{file_path}` not found in DBFS."
        return f"Error reading log file: {e}"
    except Exception as e:
        return f"Error reading log file: {e}"


# ---------------------------------------------------------------------------
# Tool registration
# ---------------------------------------------------------------------------


def register_dbfs_tools(mcp: Any) -> None:
    @mcp.tool(
        name="databricks_list_cluster_log_files",
        annotations={"title": "List Cluster Log Files", **READ_ONLY_ANNOTATIONS},
    )
    def list_cluster_log_files(
        cluster_id: str,
        log_base_path: str = "dbfs:/cluster-logs",
        subfolder: str = "driver",
        workspace: Optional[str] = None,
    ) -> str:
        """
        List log files available in DBFS for a specific cluster.

        Databricks delivers cluster logs (stdout, stderr, log4j) to a DBFS path
        when cluster log delivery is configured.

        Use when: browsing available log files before reading their content.
        Do NOT use when: you want cluster lifecycle events — use
        databricks_get_cluster_events instead.

        Args:
            cluster_id: The Databricks cluster ID
            log_base_path: DBFS base path for logs (default: dbfs:/cluster-logs)
            subfolder: Subfolder to list — "driver" (default), "init_scripts",
                       or "executor"
            workspace: Target workspace name (uses default if omitted)

        Returns:
            Markdown table of files with name, size, and last modified time
        """
        return _list_cluster_log_files(cluster_id, log_base_path, subfolder, workspace)

    @mcp.tool(
        name="databricks_read_cluster_log_file",
        annotations={"title": "Read Cluster Log File", **READ_ONLY_ANNOTATIONS},
    )
    def read_cluster_log_file(
        file_path: str,
        offset: int = 0,
        max_bytes: int = 524288,
        workspace: Optional[str] = None,
    ) -> str:
        """
        Read the content of a specific cluster log file from DBFS.

        Use databricks_list_cluster_log_files first to discover available files,
        then pass the full DBFS path here.

        Use when: reading driver stdout/stderr, init script output, or executor logs.
        Do NOT use when: you need a quick diagnostic overview — use
        databricks_get_job_run_logs for an all-in-one view.

        Args:
            file_path: Full DBFS path (e.g. "dbfs:/cluster-logs/<id>/driver/stdout")
            offset: Byte offset to start reading (default 0). Use for pagination.
            max_bytes: Max bytes to read per call (default 512 KB, max 1 MB)
            workspace: Target workspace name (uses default if omitted)

        Returns:
            Log file content in a code block, with pagination hints if truncated
        """
        return _read_cluster_log_file(file_path, offset, max_bytes, workspace)
