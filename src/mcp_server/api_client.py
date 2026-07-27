"""
Shared Databricks REST API client.

Provides a single HTTP client that all feature managers (clusters, warehouses,
pipelines, catalog, etc.) use to talk to the Databricks REST API.
"""

import threading
from typing import Any, Dict, Optional

import requests

from .config import get_setting_int
from .error_handler import with_databricks_retry
from .logger import log_databricks_event
from .workspaces import get_workspace_config, resolve_workspace_name


class DatabricksAPIClient:
    """Thin, thread-safe wrapper around ``requests.Session`` for Databricks APIs."""

    def __init__(self, *, host: str, token: str, workspace_name: str = "default"):
        self.workspace_name = workspace_name
        self.host = host.rstrip("/")
        self.token = token
        self._session = requests.Session()
        self._session.headers.update(
            {
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json",
            }
        )
        self._timeout = get_setting_int(
            "DATABRICKS_API_TIMEOUT_SECONDS", "databricks_api_timeout_seconds", 30
        )

    # ------------------------------------------------------------------
    # Core request helpers
    # ------------------------------------------------------------------

    @with_databricks_retry("databricks_api_request")
    def request(
        self,
        method: str,
        path: str,
        *,
        params: Optional[Dict[str, Any]] = None,
        json_data: Optional[Dict[str, Any]] = None,
        api_version: str = "2.0",
    ) -> Dict[str, Any]:
        url = f"https://{self.host}/api/{api_version}{path}"
        method_upper = method.upper()

        log_databricks_event(
            "API", "REQUEST", f"[{self.workspace_name}] {method_upper} {path}"
        )

        response = self._session.request(
            method_upper,
            url,
            params=params,
            json=json_data,
            timeout=self._timeout,
        )

        if response.status_code >= 400:
            body = (response.text or "").strip()[:2000]
            if response.status_code == 429:
                raise Exception(f"rate limit: HTTP 429 from Databricks API: {body}")
            if response.status_code == 401:
                raise Exception(
                    f"authentication: HTTP 401 — token may be expired or invalid: {body}"
                )
            if response.status_code == 403:
                raise Exception(
                    f"authentication: HTTP 403 — insufficient permissions: {body}"
                )
            # 4xx client errors (except 429/401/403 handled above) are NOT
            # retryable — bad input won't succeed on retry and should not
            # trip the circuit breaker.
            if response.status_code < 500:
                raise ValueError(f"client error: HTTP {response.status_code}: {body}")
            raise Exception(
                f"databricks api error: HTTP {response.status_code}: {body}"
            )

        if not response.content:
            return {}

        try:
            result: Dict[str, Any] = response.json()
        except ValueError:
            result = {"raw": response.text}

        return result

    def get(self, path: str, **kwargs: Any) -> Dict[str, Any]:
        result: Dict[str, Any] = self.request("GET", path, **kwargs)
        return result

    def post(self, path: str, **kwargs: Any) -> Dict[str, Any]:
        result: Dict[str, Any] = self.request("POST", path, **kwargs)
        return result


# ------------------------------------------------------------------
# Per-workspace singleton management
# ------------------------------------------------------------------

_clients: Dict[str, DatabricksAPIClient] = {}
_lock = threading.Lock()


def get_api_client(workspace: Optional[str] = None) -> DatabricksAPIClient:
    """Return a per-workspace API client singleton."""
    workspace_name = resolve_workspace_name(workspace)
    if workspace_name in _clients:
        return _clients[workspace_name]

    with _lock:
        if workspace_name in _clients:
            return _clients[workspace_name]

        cfg = get_workspace_config(workspace_name)
        _clients[workspace_name] = DatabricksAPIClient(
            host=cfg.host,
            token=cfg.token,
            workspace_name=workspace_name,
        )
        return _clients[workspace_name]
