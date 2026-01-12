"""
Workspace configuration support.

Goal: allow configuring multiple Databricks workspaces while remaining backwards-compatible
with the legacy single-workspace env vars.

Supported env formats:
1) Legacy single workspace:
   - DATABRICKS_HOST
   - DATABRICKS_TOKEN
   - DATABRICKS_HTTP_PATH

2) Multiple workspaces via JSON:
   - DATABRICKS_WORKSPACES_JSON='[{"name":"prod","host":"...","token":"...","http_path":"..."}, ...]'
   - DEFAULT_WORKSPACE='prod' (optional; defaults to first entry)
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from typing import Dict, List, Optional


@dataclass(frozen=True)
class WorkspaceConfig:
    name: str
    host: str
    token: str
    http_path: str


def _strip_scheme(host: str) -> str:
    h = host.strip()
    if h.startswith("https://"):
        return h[len("https://") :]
    if h.startswith("http://"):
        return h[len("http://") :]
    return h


def _load_multi_workspace_configs() -> Optional[Dict[str, WorkspaceConfig]]:
    raw = os.getenv("DATABRICKS_WORKSPACES_JSON")
    if not raw:
        return None

    try:
        items = json.loads(raw)
    except json.JSONDecodeError as e:
        raise ValueError(f"DATABRICKS_WORKSPACES_JSON must be valid JSON: {e}")

    if not isinstance(items, list) or not items:
        raise ValueError("DATABRICKS_WORKSPACES_JSON must be a non-empty JSON array")

    configs: Dict[str, WorkspaceConfig] = {}
    for i, item in enumerate(items):
        if not isinstance(item, dict):
            raise ValueError(f"DATABRICKS_WORKSPACES_JSON[{i}] must be an object")

        name = str(item.get("name") or "").strip()
        host = str(item.get("host") or "").strip()
        token = str(item.get("token") or "").strip()
        http_path = str(item.get("http_path") or "").strip()

        if not all([name, host, token, http_path]):
            raise ValueError(
                f"DATABRICKS_WORKSPACES_JSON[{i}] must include name, host, token, http_path"
            )

        if name in configs:
            raise ValueError(f"Duplicate workspace name in DATABRICKS_WORKSPACES_JSON: {name}")

        configs[name] = WorkspaceConfig(
            name=name,
            host=_strip_scheme(host),
            token=token,
            http_path=http_path,
        )

    return configs


def _load_legacy_default() -> Optional[WorkspaceConfig]:
    host = os.getenv("DATABRICKS_HOST")
    token = os.getenv("DATABRICKS_TOKEN")
    http_path = os.getenv("DATABRICKS_HTTP_PATH")

    if not any([host, token, http_path]):
        return None
    if not all([host, token, http_path]):
        raise ValueError("Missing required Databricks credentials in environment")

    return WorkspaceConfig(
        name="default",
        host=_strip_scheme(host),
        token=token,
        http_path=http_path,
    )


def get_workspaces() -> Dict[str, WorkspaceConfig]:
    """Return all configured workspaces."""
    multi = _load_multi_workspace_configs()
    if multi is not None:
        return multi

    legacy = _load_legacy_default()
    if legacy is None:
        raise ValueError("No Databricks workspace configuration found in environment")
    return {"default": legacy}


def get_default_workspace_name() -> str:
    """Return the default workspace name to use when tools omit workspace."""
    multi = _load_multi_workspace_configs()
    if multi is None:
        return "default"

    requested = (os.getenv("DEFAULT_WORKSPACE") or "").strip()
    if requested:
        if requested not in multi:
            raise ValueError(f"DEFAULT_WORKSPACE={requested} is not in DATABRICKS_WORKSPACES_JSON")
        return requested

    # Fall back to first configured workspace
    return next(iter(multi.keys()))


def resolve_workspace_name(workspace: Optional[str]) -> str:
    """Resolve user input to an actual configured workspace name."""
    if workspace is None or str(workspace).strip() == "":
        return get_default_workspace_name()

    name = str(workspace).strip()
    workspaces = get_workspaces()
    if name not in workspaces:
        raise ValueError(f"Unknown workspace '{name}'. Configured: {', '.join(sorted(workspaces.keys()))}")
    return name


def get_workspace_config(workspace: Optional[str]) -> WorkspaceConfig:
    """Get the resolved workspace config (accepts None => default)."""
    name = resolve_workspace_name(workspace)
    return get_workspaces()[name]

