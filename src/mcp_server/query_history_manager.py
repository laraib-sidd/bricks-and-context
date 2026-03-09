"""
SQL Query History and Object Permissions for Databricks.

Provides tools to retrieve recent queries, their performance, and object ACLs.
"""

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


def _list_query_history(
    max_results: int = 25,
    warehouse_id: Optional[str] = None,
    status: Optional[str] = None,
    workspace: Optional[str] = None,
    limit: int = DEFAULT_PAGE_LIMIT,
    offset: int = 0,
) -> str:
    try:
        limit, offset = clamp_pagination(limit, offset)
        ws = resolve_workspace_name(workspace)
        client = get_api_client(ws)

        body: Dict[str, Any] = {
            "max_results": min(max_results, 100),
            "include_metrics": True,
        }
        filter_by: Dict[str, Any] = {}
        if warehouse_id:
            filter_by["warehouse_ids"] = [warehouse_id]
        if status:
            filter_by["statuses"] = [status.upper()]
        if filter_by:
            body["filter_by"] = filter_by

        resp = client.post(
            "/sql/history/queries",
            json_data=body,
        )

        all_queries = resp.get("res", [])
        if not all_queries:
            return "No queries found in history."

        total = len(all_queries)
        queries = all_queries[offset : offset + limit]
        if not queries:
            return f"No queries at offset {offset} (total: {total})."

        header = "| Query ID | Status | User | Warehouse | Duration (ms) | Rows | Query Text |"
        sep = "| --- | --- | --- | --- | --- | --- | --- |"
        rows = []
        for q in queries:
            qid = q.get("query_id", "")[:12]
            q_status = q.get("status", "")
            user = q.get("user_name", "")
            wh = q.get("warehouse_id", "")[:12]
            duration = q.get("duration", q.get("execution_end_time_ms", 0)) or ""
            if isinstance(duration, (int, float)) and duration > 0:
                duration = f"{int(duration):,}"
            metrics = q.get("metrics", {})
            row_count = metrics.get("result_count_rows", "")
            if isinstance(row_count, (int, float)):
                row_count = f"{int(row_count):,}"
            query_text = (
                (q.get("query_text") or "")[:80].replace("|", "\\|").replace("\n", " ")
            )
            rows.append(
                f"| {qid} | {q_status} | {user} | {wh} | {duration} | {row_count} | {query_text} |"
            )

        result = (
            f"Query History ({len(rows)} shown, {total} total):\n\n{header}\n{sep}\n"
            + "\n".join(rows)
            + pagination_footer(
                count=len(rows), offset=offset, limit=limit, total=total
            )
        )
        return enforce_character_limit(result, "Use offset and limit to paginate.")
    except Exception as e:
        return f"Error listing query history: {e}"


def _get_object_permissions(
    object_type: str, object_id: str, workspace: Optional[str] = None
) -> str:
    """Get permissions for a Databricks object (cluster, job, warehouse, etc.)."""
    try:
        ws = resolve_workspace_name(workspace)
        client = get_api_client(ws)
        resp = client.get(f"/permissions/{object_type}/{object_id}")

        acls = resp.get("access_control_list", [])
        if not acls:
            return f"No permissions found for {object_type}/{object_id}."

        header = "| Principal | Permissions |"
        sep = "| --- | --- |"
        rows = []
        for acl in acls:
            principal = (
                acl.get("user_name")
                or acl.get("group_name")
                or acl.get("service_principal_name")
                or "unknown"
            )
            perms = acl.get("all_permissions", [])
            perm_strs = []
            for p in perms:
                level = p.get("permission_level", "")
                inherited = " (inherited)" if p.get("inherited") else ""
                perm_strs.append(f"{level}{inherited}")
            rows.append(f"| {principal} | {', '.join(perm_strs)} |")

        result = (
            f"Permissions for {object_type}/{object_id}:\n\n{header}\n{sep}\n"
            + "\n".join(rows)
        )
        return enforce_character_limit(result)
    except Exception as e:
        return f"Error getting permissions for {object_type}/{object_id}: {e}"


# ------------------------------------------------------------------
# Tool registration
# ------------------------------------------------------------------


def register_query_history_tools(mcp: Any) -> None:
    @mcp.tool(
        name="databricks_list_query_history",
        annotations={"title": "List Query History", **READ_ONLY_ANNOTATIONS},
    )
    def list_query_history(
        max_results: int = 25,
        warehouse_id: Optional[str] = None,
        status: Optional[str] = None,
        workspace: Optional[str] = None,
        limit: int = DEFAULT_PAGE_LIMIT,
        offset: int = 0,
    ) -> str:
        """
        Get recent SQL query execution history.

        Use when: auditing recent queries, analysing performance, or understanding
        workload patterns on a warehouse.
        Do NOT use when: you want to run a new query — use
        databricks_execute_sql_query.

        Args:
            max_results: Max queries to fetch from the API (default 25, max 100)
            warehouse_id: Optional filter by warehouse ID
            status: Optional filter (QUEUED, RUNNING, FINISHED, FAILED, CANCELED)
            workspace: Target workspace name (uses default if omitted)
            limit: Max results to display (default 25, max 100)
            offset: Number of results to skip for pagination (default 0)

        Returns:
            Markdown table of recent queries with ID, status, user, duration,
            and query text
        """
        return _list_query_history(
            max_results, warehouse_id, status, workspace, limit, offset
        )

    @mcp.tool(
        name="databricks_get_object_permissions",
        annotations={"title": "Get Object Permissions", **READ_ONLY_ANNOTATIONS},
    )
    def get_object_permissions(
        object_type: str, object_id: str, workspace: Optional[str] = None
    ) -> str:
        """
        Get permissions / ACLs for a Databricks object.

        Use when: auditing who has access to a cluster, job, warehouse, or other
        Databricks resource.
        Do NOT use when: you need Unity Catalog table permissions — those are
        managed through GRANT/REVOKE SQL.

        Args:
            object_type: Databricks object type (e.g. 'clusters', 'jobs',
                'sql/warehouses')
            object_id: ID of the object
            workspace: Target workspace name (uses default if omitted)

        Returns:
            Markdown table of principals and their permission levels
        """
        return _get_object_permissions(object_type, object_id, workspace)
