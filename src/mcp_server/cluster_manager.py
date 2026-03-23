"""
Cluster and SQL Warehouse status for Databricks.

Read-only tools to inspect compute resources and diagnose connectivity issues.
"""

import json
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


def _ts_to_str(epoch_ms: int) -> str:
    """Convert epoch milliseconds to a human-readable timestamp."""
    if not epoch_ms:
        return "N/A"
    from datetime import datetime

    return datetime.fromtimestamp(epoch_ms / 1000).strftime("%Y-%m-%d %H:%M:%S UTC")


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


def _get_cluster_events(
    cluster_id: str,
    limit: int = 50,
    event_types: Optional[str] = None,
    workspace: Optional[str] = None,
) -> str:
    """Get activity events for a specific cluster."""
    try:
        ws = resolve_workspace_name(workspace)
        client = get_api_client(ws)
        payload: Dict[str, Any] = {
            "cluster_id": cluster_id,
            "limit": min(limit, 500),
        }

        if event_types:
            payload["event_types"] = [e.strip() for e in event_types.split(",")]

        resp = client.post("/clusters/events", json_data=payload)

        events = resp.get("events", [])
        if not events:
            return f"No events found for cluster {cluster_id}."

        total_count = resp.get("total_count", len(events))
        result = f"## Cluster Events for `{cluster_id}`\n\n"
        result += f"_Showing {len(events)} of {total_count} total events._\n\n"
        result += "| Timestamp | Event Type | Details |\n"
        result += "| --- | --- | --- |\n"

        for event in events:
            ts = _ts_to_str(event.get("timestamp", 0))
            event_type = event.get("type", "UNKNOWN")

            details_obj = event.get("details", {})
            details_parts: List[str] = []

            reason = details_obj.get("reason", {})
            if reason:
                code = reason.get("code", "")
                params = reason.get("parameters", {})
                msg = reason.get("type", "")
                detail_str = f"{code}" if code else ""
                if msg:
                    detail_str += f" ({msg})"
                if params:
                    detail_str += f" {json.dumps(params)}"
                if detail_str:
                    details_parts.append(detail_str)

            current_num = details_obj.get("current_num_workers", "")
            target_num = details_obj.get("target_num_workers", "")
            if current_num or target_num:
                details_parts.append(f"workers: {current_num} -> {target_num}")

            user = details_obj.get("user", "")
            if user:
                details_parts.append(f"by: {user}")

            detail_text = "; ".join(details_parts) if details_parts else "-"
            detail_text = detail_text.replace("|", "\\|").replace("\n", " ")

            result += f"| {ts} | {event_type} | {detail_text} |\n"

        return enforce_character_limit(result)
    except Exception as e:
        return f"Error getting cluster events: {e}"


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

    @mcp.tool(
        name="databricks_get_cluster_events",
        annotations={"title": "Get Cluster Events", **READ_ONLY_ANNOTATIONS},
    )
    def get_cluster_events(
        cluster_id: str,
        limit: int = 50,
        event_types: Optional[str] = None,
        workspace: Optional[str] = None,
    ) -> str:
        """
        Get activity events for a cluster (startup, termination, autoscaling, etc.).

        Useful for diagnosing cluster failures, seeing init script outcomes,
        and understanding cluster lifecycle.

        Use when: debugging why a cluster failed to start or terminated unexpectedly.
        Do NOT use when: you need job-level diagnostics — use
        databricks_get_job_run_logs for an all-in-one view.

        Args:
            cluster_id: The Databricks cluster ID
            limit: Max events to return (default 50, max 500)
            event_types: Optional comma-separated filter (e.g.
                         "RUNNING,TERMINATING,INIT_SCRIPTS_FINISHED")
            workspace: Target workspace name (uses default if omitted)

        Returns:
            Markdown table of cluster events with timestamps and details
        """
        return _get_cluster_events(cluster_id, limit, event_types, workspace)
