"""
Cluster and SQL Warehouse status for Databricks.

Read-only tools to inspect compute resources and diagnose connectivity issues.
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


def _list_clusters(
    workspace: Optional[str] = None,
    limit: int = DEFAULT_PAGE_LIMIT,
    offset: int = 0,
) -> str:
    try:
        limit, offset = clamp_pagination(limit, offset)
        ws = resolve_workspace_name(workspace)
        client = get_api_client(ws)
        resp = client.get("/clusters/list")

        all_clusters = resp.get("clusters", [])
        if not all_clusters:
            return "No clusters found."

        total = len(all_clusters)
        clusters = all_clusters[offset : offset + limit]
        if not clusters:
            return f"No clusters at offset {offset} (total: {total})."

        header = "| Cluster ID | Name | State | Spark Version | Node Type | Workers | Creator |"
        sep = "| --- | --- | --- | --- | --- | --- | --- |"
        rows = []
        for c in clusters:
            cid = c.get("cluster_id", "")
            name = c.get("cluster_name", "")
            state = c.get("state", "UNKNOWN")
            spark_ver = c.get("spark_version", "")
            node_type = c.get("node_type_id", "")
            workers = c.get(
                "num_workers", c.get("autoscale", {}).get("max_workers", "auto")
            )
            creator = c.get("creator_user_name", "")
            rows.append(
                f"| {cid} | {name} | {state} | {spark_ver} | {node_type} | {workers} | {creator} |"
            )

        result = (
            f"Clusters ({len(clusters)} shown, {total} total):\n\n{header}\n{sep}\n"
            + "\n".join(rows)
            + pagination_footer(
                count=len(clusters), offset=offset, limit=limit, total=total
            )
        )
        return enforce_character_limit(result, "Use offset and limit to paginate.")
    except Exception as e:
        return f"Error listing clusters: {e}"


def _list_warehouses(
    workspace: Optional[str] = None,
    limit: int = DEFAULT_PAGE_LIMIT,
    offset: int = 0,
) -> str:
    try:
        limit, offset = clamp_pagination(limit, offset)
        ws = resolve_workspace_name(workspace)
        client = get_api_client(ws)
        resp = client.get("/sql/warehouses")

        all_warehouses = resp.get("warehouses", [])
        if not all_warehouses:
            return "No SQL warehouses found."

        total = len(all_warehouses)
        warehouses = all_warehouses[offset : offset + limit]
        if not warehouses:
            return f"No warehouses at offset {offset} (total: {total})."

        header = "| ID | Name | State | Size | Type | Auto Stop (min) | Creator |"
        sep = "| --- | --- | --- | --- | --- | --- | --- |"
        rows = []
        for w in warehouses:
            wid = w.get("id", "")
            name = w.get("name", "")
            state = w.get("state", "UNKNOWN")
            size = w.get("cluster_size", "")
            wtype = w.get("warehouse_type", "")
            auto_stop = w.get("auto_stop_mins", "")
            creator = w.get("creator_name", "")
            rows.append(
                f"| {wid} | {name} | {state} | {size} | {wtype} | {auto_stop} | {creator} |"
            )

        result = (
            f"SQL Warehouses ({len(warehouses)} shown, {total} total):\n\n{header}\n{sep}\n"
            + "\n".join(rows)
            + pagination_footer(
                count=len(warehouses), offset=offset, limit=limit, total=total
            )
        )
        return enforce_character_limit(result, "Use offset and limit to paginate.")
    except Exception as e:
        return f"Error listing warehouses: {e}"


def _get_warehouse_status(warehouse_id: str, workspace: Optional[str] = None) -> str:
    try:
        ws = resolve_workspace_name(workspace)
        client = get_api_client(ws)
        w = client.get(f"/sql/warehouses/{warehouse_id}")

        lines = [f"SQL Warehouse: **{w.get('name', warehouse_id)}**\n"]
        lines.append(f"- **ID**: {w.get('id')}")
        lines.append(f"- **State**: {w.get('state', 'UNKNOWN')}")
        lines.append(f"- **Size**: {w.get('cluster_size', 'N/A')}")
        lines.append(f"- **Type**: {w.get('warehouse_type', 'N/A')}")
        lines.append(f"- **Auto Stop (min)**: {w.get('auto_stop_mins', 'N/A')}")
        lines.append(f"- **Creator**: {w.get('creator_name', 'N/A')}")

        if w.get("health"):
            health = w["health"]
            lines.append(f"- **Health Status**: {health.get('status', 'N/A')}")
        if w.get("num_active_sessions") is not None:
            lines.append(f"- **Active Sessions**: {w['num_active_sessions']}")
        if w.get("num_clusters") is not None:
            lines.append(f"- **Running Clusters**: {w['num_clusters']}")

        return "\n".join(lines)
    except Exception as e:
        return f"Error getting warehouse {warehouse_id}: {e}"


def register_cluster_tools(mcp: Any) -> None:
    @mcp.tool(
        name="databricks_list_clusters",
        annotations={"title": "List Clusters", **READ_ONLY_ANNOTATIONS},
    )
    def list_clusters(
        workspace: Optional[str] = None,
        limit: int = DEFAULT_PAGE_LIMIT,
        offset: int = 0,
    ) -> str:
        """
        List all Databricks clusters with their current state.

        Use when: checking compute availability, diagnosing job failures, or
        auditing cluster configurations.
        Do NOT use when: you need SQL warehouse status — use
        databricks_list_warehouses instead.

        Args:
            workspace: Target workspace name (uses default if omitted)
            limit: Max clusters to return (default 25, max 100)
            offset: Number of results to skip for pagination (default 0)

        Returns:
            Markdown table of clusters with ID, name, state, and configuration
        """
        return _list_clusters(workspace, limit, offset)

    @mcp.tool(
        name="databricks_list_warehouses",
        annotations={"title": "List SQL Warehouses", **READ_ONLY_ANNOTATIONS},
    )
    def list_warehouses(
        workspace: Optional[str] = None,
        limit: int = DEFAULT_PAGE_LIMIT,
        offset: int = 0,
    ) -> str:
        """
        List all SQL warehouses with their current state.

        Use when: diagnosing SQL connectivity issues — shows if warehouses
        are RUNNING, STOPPED, or STARTING.
        Do NOT use when: you need general-purpose cluster info — use
        databricks_list_clusters instead.

        Args:
            workspace: Target workspace name (uses default if omitted)
            limit: Max warehouses to return (default 25, max 100)
            offset: Number of results to skip for pagination (default 0)

        Returns:
            Markdown table of warehouses with ID, name, state, size, and type
        """
        return _list_warehouses(workspace, limit, offset)

    @mcp.tool(
        name="databricks_get_warehouse_status",
        annotations={"title": "Get Warehouse Status", **READ_ONLY_ANNOTATIONS},
    )
    def get_warehouse_status(warehouse_id: str, workspace: Optional[str] = None) -> str:
        """
        Get detailed status of a specific SQL warehouse.

        Use when: you need health, active sessions, and cluster counts for a
        specific warehouse.
        Do NOT use when: you only need a quick overview — use
        databricks_list_warehouses.

        Args:
            warehouse_id: SQL warehouse ID
            workspace: Target workspace name (uses default if omitted)

        Returns:
            Warehouse details including state, health, active sessions, and
            running clusters
        """
        return _get_warehouse_status(warehouse_id, workspace)
