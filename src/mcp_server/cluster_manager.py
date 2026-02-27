"""
Cluster and SQL Warehouse status for Databricks.

Read-only tools to inspect compute resources and diagnose connectivity issues.
"""

from typing import Any, Dict, List, Optional

from .api_client import get_api_client
from .workspaces import resolve_workspace_name


def _list_clusters(workspace: Optional[str] = None) -> str:
    try:
        ws = resolve_workspace_name(workspace)
        client = get_api_client(ws)
        resp = client.get("/clusters/list")

        clusters = resp.get("clusters", [])
        if not clusters:
            return "No clusters found."

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

        return f"Clusters ({len(clusters)} found):\n\n{header}\n{sep}\n" + "\n".join(
            rows
        )
    except Exception as e:
        return f"Error listing clusters: {e}"


def _list_warehouses(workspace: Optional[str] = None) -> str:
    try:
        ws = resolve_workspace_name(workspace)
        client = get_api_client(ws)
        resp = client.get("/sql/warehouses")

        warehouses = resp.get("warehouses", [])
        if not warehouses:
            return "No SQL warehouses found."

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

        return (
            f"SQL Warehouses ({len(warehouses)} found):\n\n{header}\n{sep}\n"
            + "\n".join(rows)
        )
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
    @mcp.tool()
    def list_clusters(workspace: Optional[str] = None) -> str:
        """
        List all Databricks clusters with their current state.

        Useful for checking compute availability and diagnosing job failures.

        Returns:
            Markdown table of clusters with ID, name, state, and configuration
        """
        return _list_clusters(workspace)

    @mcp.tool()
    def list_warehouses(workspace: Optional[str] = None) -> str:
        """
        List all SQL warehouses with their current state.

        Critical for diagnosing SQL connectivity issues -- shows if warehouses
        are RUNNING, STOPPED, or STARTING.

        Returns:
            Markdown table of warehouses with ID, name, state, size, and type
        """
        return _list_warehouses(workspace)

    @mcp.tool()
    def get_warehouse_status(warehouse_id: str, workspace: Optional[str] = None) -> str:
        """
        Get detailed status of a specific SQL warehouse.

        Args:
            warehouse_id: SQL warehouse ID

        Returns:
            Warehouse details including state, health, active sessions, and running clusters
        """
        return _get_warehouse_status(warehouse_id, workspace)
