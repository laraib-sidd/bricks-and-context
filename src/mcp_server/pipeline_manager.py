"""
Delta Live Tables (DLT) pipeline operations for Databricks.

Provides tools to list, inspect, trigger, and monitor DLT pipelines.
"""

from typing import Any, Dict, List, Optional

from .api_client import get_api_client
from .workspaces import resolve_workspace_name


def _list_pipelines(max_results: int = 50, workspace: Optional[str] = None) -> str:
    try:
        ws = resolve_workspace_name(workspace)
        client = get_api_client(ws)
        resp = client.get("/pipelines", params={"max_results": min(max_results, 100)})

        pipelines = resp.get("statuses", [])
        if not pipelines:
            return "No DLT pipelines found."

        header = "| Pipeline ID | Name | State | Creator |"
        sep = "| --- | --- | --- | --- |"
        rows = []
        for p in pipelines:
            pid = p.get("pipeline_id", "")
            name = p.get("name", "")
            state = p.get("state", "UNKNOWN")
            creator = p.get("creator_user_name", "")
            rows.append(f"| {pid} | {name} | {state} | {creator} |")

        return (
            f"DLT Pipelines ({len(pipelines)} found):\n\n{header}\n{sep}\n"
            + "\n".join(rows)
        )
    except Exception as e:
        return f"Error listing pipelines: {e}"


def _get_pipeline_status(pipeline_id: str, workspace: Optional[str] = None) -> str:
    try:
        ws = resolve_workspace_name(workspace)
        client = get_api_client(ws)
        resp = client.get(f"/pipelines/{pipeline_id}")

        p = resp
        lines = [f"Pipeline: **{p.get('name', pipeline_id)}**\n"]
        lines.append(f"- **ID**: {p.get('pipeline_id')}")
        lines.append(f"- **State**: {p.get('state', 'N/A')}")
        lines.append(f"- **Creator**: {p.get('creator_user_name', 'N/A')}")

        spec = p.get("spec", {})
        if spec:
            lines.append(f"- **Target**: {spec.get('target', 'N/A')}")
            lines.append(f"- **Catalog**: {spec.get('catalog', 'N/A')}")
            lines.append(f"- **Storage**: {spec.get('storage', 'N/A')}")
            lines.append(f"- **Continuous**: {spec.get('continuous', False)}")
            lines.append(f"- **Development**: {spec.get('development', False)}")
            lines.append(f"- **Edition**: {spec.get('edition', 'N/A')}")
            lines.append(f"- **Photon**: {spec.get('photon', False)}")
            lines.append(f"- **Serverless**: {spec.get('serverless', False)}")

            libraries = spec.get("libraries", [])
            if libraries:
                lines.append(f"\n**Libraries** ({len(libraries)}):")
                for lib in libraries[:20]:
                    nb = lib.get("notebook", {})
                    if nb:
                        lines.append(f"  - Notebook: `{nb.get('path', '')}`")
                    file_lib = lib.get("file", {})
                    if file_lib:
                        lines.append(f"  - File: `{file_lib.get('path', '')}`")

        latest = p.get("latest_updates", [])
        if latest:
            lines.append(f"\n**Latest Updates** (last {min(len(latest), 5)}):")
            lines.append("| Update ID | State | Creation Time |")
            lines.append("| --- | --- | --- |")
            for u in latest[:5]:
                uid = u.get("update_id", "")
                state = u.get("state", "")
                created = u.get("creation_time", "")
                lines.append(f"| {uid} | {state} | {created} |")

        return "\n".join(lines)
    except Exception as e:
        return f"Error getting pipeline {pipeline_id}: {e}"


def _start_pipeline(
    pipeline_id: str, full_refresh: bool = False, workspace: Optional[str] = None
) -> str:
    try:
        ws = resolve_workspace_name(workspace)
        client = get_api_client(ws)
        data: Dict[str, Any] = {}
        if full_refresh:
            data["full_refresh"] = True
        resp = client.post(f"/pipelines/{pipeline_id}/updates", json_data=data)
        update_id = resp.get("update_id", "unknown")
        return (
            f"Pipeline {pipeline_id} update started.\n"
            f"- **Update ID**: {update_id}\n"
            f"- **Full Refresh**: {full_refresh}"
        )
    except Exception as e:
        return f"Error starting pipeline {pipeline_id}: {e}"


def _get_pipeline_events(
    pipeline_id: str, max_results: int = 25, workspace: Optional[str] = None
) -> str:
    try:
        ws = resolve_workspace_name(workspace)
        client = get_api_client(ws)
        resp = client.get(
            f"/pipelines/{pipeline_id}/events",
            params={"max_results": min(max_results, 100)},
        )

        events = resp.get("events", [])
        if not events:
            return f"No events found for pipeline {pipeline_id}."

        header = "| Timestamp | Level | Event Type | Message |"
        sep = "| --- | --- | --- | --- |"
        rows = []
        for ev in events[:max_results]:
            ts = ev.get("timestamp", "")
            level = ev.get("level", "")
            event_type = ev.get("event_type", "")
            message = (
                (ev.get("message") or "")[:150].replace("|", "\\|").replace("\n", " ")
            )
            rows.append(f"| {ts} | {level} | {event_type} | {message} |")

        return (
            f"Pipeline Events for {pipeline_id} ({len(rows)} shown):\n\n"
            f"{header}\n{sep}\n" + "\n".join(rows)
        )
    except Exception as e:
        return f"Error getting events for pipeline {pipeline_id}: {e}"


# ------------------------------------------------------------------
# Tool registration
# ------------------------------------------------------------------


def register_pipeline_tools(mcp: Any) -> None:
    @mcp.tool()
    def list_pipelines(max_results: int = 50, workspace: Optional[str] = None) -> str:
        """
        List all Delta Live Tables (DLT) pipelines.

        Returns:
            Markdown table of pipelines with ID, name, state, and creator
        """
        return _list_pipelines(max_results, workspace)

    @mcp.tool()
    def get_pipeline_status(pipeline_id: str, workspace: Optional[str] = None) -> str:
        """
        Get detailed status and configuration of a DLT pipeline.

        Args:
            pipeline_id: Pipeline ID

        Returns:
            Detailed pipeline info including spec, libraries, and latest updates
        """
        return _get_pipeline_status(pipeline_id, workspace)

    @mcp.tool()
    def get_pipeline_events(
        pipeline_id: str,
        max_results: int = 25,
        workspace: Optional[str] = None,
    ) -> str:
        """
        Get recent events/logs from a DLT pipeline.

        Essential for debugging pipeline failures and monitoring progress.

        Args:
            pipeline_id: Pipeline ID
            max_results: Maximum number of events to return (default: 25)

        Returns:
            Markdown table of pipeline events with timestamps and messages
        """
        return _get_pipeline_events(pipeline_id, max_results, workspace)
