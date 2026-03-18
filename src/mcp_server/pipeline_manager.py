"""
Delta Live Tables (DLT) pipeline operations for Databricks.

Provides tools to list, inspect, trigger, and monitor DLT pipelines.
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


def _list_pipelines(
    max_results: int = 50,
    workspace: Optional[str] = None,
    limit: int = DEFAULT_PAGE_LIMIT,
    offset: int = 0,
) -> str:
    try:
        limit, offset = clamp_pagination(limit, offset)
        ws = resolve_workspace_name(workspace)
        client = get_api_client(ws)
        resp = client.get("/pipelines", params={"max_results": min(max_results, 100)})

        all_pipelines = resp.get("statuses", [])
        if not all_pipelines:
            return "No DLT pipelines found."

        total = len(all_pipelines)
        pipelines = all_pipelines[offset : offset + limit]
        if not pipelines:
            return f"No pipelines at offset {offset} (total: {total})."

        header = "| Pipeline ID | Name | State | Creator |"
        sep = "| --- | --- | --- | --- |"
        rows = []
        for p in pipelines:
            pid = p.get("pipeline_id", "")
            name = p.get("name", "")
            state = p.get("state", "UNKNOWN")
            creator = p.get("creator_user_name", "")
            rows.append(f"| {pid} | {name} | {state} | {creator} |")

        result = (
            f"DLT Pipelines ({len(pipelines)} shown, {total} total):\n\n{header}\n{sep}\n"
            + "\n".join(rows)
            + pagination_footer(
                count=len(pipelines), offset=offset, limit=limit, total=total
            )
        )
        return enforce_character_limit(result, "Use offset and limit to paginate.")
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

        result = "\n".join(lines)
        return enforce_character_limit(result)
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
            f"- **Full Refresh**: {full_refresh}\n"
            f"- **Monitor**: Use `databricks_get_pipeline_status` or "
            f"`databricks_get_pipeline_events` to track progress."
        )
    except Exception as e:
        return f"Error starting pipeline {pipeline_id}: {e}"


def _get_pipeline_events(
    pipeline_id: str,
    max_results: int = 25,
    workspace: Optional[str] = None,
    limit: int = DEFAULT_PAGE_LIMIT,
    offset: int = 0,
) -> str:
    try:
        limit, offset = clamp_pagination(limit, offset)
        ws = resolve_workspace_name(workspace)
        client = get_api_client(ws)
        resp = client.get(
            f"/pipelines/{pipeline_id}/events",
            params={"max_results": min(max_results, 100)},
        )

        all_events = resp.get("events", [])
        if not all_events:
            return f"No events found for pipeline {pipeline_id}."

        total = len(all_events)
        events = all_events[offset : offset + limit]
        if not events:
            return f"No events at offset {offset} for pipeline {pipeline_id} (total: {total})."

        header = "| Timestamp | Level | Event Type | Message |"
        sep = "| --- | --- | --- | --- |"
        rows = []
        for ev in events:
            ts = ev.get("timestamp", "")
            level = ev.get("level", "")
            event_type = ev.get("event_type", "")
            message = (
                (ev.get("message") or "")[:150].replace("|", "\\|").replace("\n", " ")
            )
            rows.append(f"| {ts} | {level} | {event_type} | {message} |")

        result = (
            f"Pipeline Events for {pipeline_id} ({len(rows)} shown, {total} total):\n\n"
            f"{header}\n{sep}\n"
            + "\n".join(rows)
            + pagination_footer(
                count=len(rows), offset=offset, limit=limit, total=total
            )
        )
        return enforce_character_limit(result, "Use offset and limit to paginate.")
    except Exception as e:
        return f"Error getting events for pipeline {pipeline_id}: {e}"


# ------------------------------------------------------------------
# Tool registration
# ------------------------------------------------------------------


def register_pipeline_tools(mcp: Any) -> None:
    @mcp.tool(
        name="databricks_list_pipelines",
        annotations={"title": "List DLT Pipelines", **READ_ONLY_ANNOTATIONS},
    )
    def list_pipelines(
        workspace: Optional[str] = None,
        limit: int = DEFAULT_PAGE_LIMIT,
        offset: int = 0,
    ) -> str:
        """
        List all Delta Live Tables (DLT) pipelines.

        Use when: discovering available DLT pipelines or checking their state.
        Do NOT use when: you need job-level information — use
        databricks_list_jobs instead.

        Args:
            workspace: Target workspace name (uses default if omitted)
            limit: Max pipelines to return (default 25, max 100)
            offset: Number of results to skip for pagination (default 0)

        Returns:
            Markdown table of pipelines with ID, name, state, and creator
        """
        return _list_pipelines(workspace=workspace, limit=limit, offset=offset)

    @mcp.tool(
        name="databricks_get_pipeline_status",
        annotations={"title": "Get Pipeline Status", **READ_ONLY_ANNOTATIONS},
    )
    def get_pipeline_status(pipeline_id: str, workspace: Optional[str] = None) -> str:
        """
        Get detailed status and configuration of a DLT pipeline.

        Use when: you need the full pipeline spec, libraries, and latest updates.
        Do NOT use when: you only need events/logs — use
        databricks_get_pipeline_events.

        Args:
            pipeline_id: Pipeline ID
            workspace: Target workspace name (uses default if omitted)

        Returns:
            Detailed pipeline info including spec, libraries, and latest updates
        """
        return _get_pipeline_status(pipeline_id, workspace)

    @mcp.tool(
        name="databricks_start_pipeline",
        annotations={
            "title": "Start Pipeline Update",
            "readOnlyHint": False,
            "destructiveHint": False,
            "idempotentHint": False,
            "openWorldHint": True,
        },
    )
    def start_pipeline(
        pipeline_id: str,
        full_refresh: bool = False,
        workspace: Optional[str] = None,
    ) -> str:
        """
        Start a DLT pipeline update.

        Use when: you need to trigger a DLT pipeline refresh or full reload.
        Do NOT use when: you want to start a Databricks job — use
        databricks_trigger_job.

        Args:
            pipeline_id: Pipeline ID to start
            full_refresh: If True, recompute all tables from scratch (default False)
            workspace: Target workspace name (uses default if omitted)

        Returns:
            Success message with update ID, or error details

        Security: This will start actual pipeline execution — use with caution.
        """
        return _start_pipeline(pipeline_id, full_refresh, workspace)

    @mcp.tool(
        name="databricks_get_pipeline_events",
        annotations={"title": "Get Pipeline Events", **READ_ONLY_ANNOTATIONS},
    )
    def get_pipeline_events(
        pipeline_id: str,
        workspace: Optional[str] = None,
        limit: int = DEFAULT_PAGE_LIMIT,
        offset: int = 0,
    ) -> str:
        """
        Get recent events/logs from a DLT pipeline.

        Use when: debugging pipeline failures or monitoring progress.
        Do NOT use when: you need the pipeline spec — use
        databricks_get_pipeline_status.

        Args:
            pipeline_id: Pipeline ID
            workspace: Target workspace name (uses default if omitted)
            limit: Max events to return (default 25, max 100)
            offset: Number of results to skip for pagination (default 0)

        Returns:
            Markdown table of pipeline events with timestamps and messages
        """
        return _get_pipeline_events(
            pipeline_id, workspace=workspace, limit=limit, offset=offset
        )
