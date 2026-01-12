"""
Project configuration loader.

We support:
- config.json (project properties / tuning knobs)
- environment variables overriding config.json

config.json path:
- MCP_CONFIG_PATH (default: ./config.json)
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from typing import Any, Dict, Optional


def _truthy(v: str) -> bool:
    return v.strip().lower() in {"1", "true", "yes", "y", "on"}


def _config_path() -> str:
    return os.getenv("MCP_CONFIG_PATH", "config.json")


_cached: Optional[Dict[str, Any]] = None


def load_config() -> Dict[str, Any]:
    global _cached
    if _cached is not None:
        return _cached

    path = _config_path()
    if not os.path.exists(path):
        _cached = {}
        return _cached

    with open(path, "r", encoding="utf-8") as f:
        try:
            data = json.load(f)
        except json.JSONDecodeError as e:
            raise ValueError(f"Invalid JSON in {path}: {e}")

    if not isinstance(data, dict):
        raise ValueError(f"{path} must contain a JSON object")

    _cached = data
    return _cached


def get_config_value(key: str, default: Any = None) -> Any:
    """Read from config.json only (no env override)."""
    cfg = load_config()
    return cfg.get(key, default)


def get_setting_str(env_key: str, config_key: str, default: str) -> str:
    v = os.getenv(env_key)
    if v is not None and v != "":
        return v
    return str(get_config_value(config_key, default))


def get_setting_int(env_key: str, config_key: str, default: int) -> int:
    v = os.getenv(env_key)
    if v is not None and v != "":
        return int(v)
    return int(get_config_value(config_key, default))


def get_setting_bool(env_key: str, config_key: str, default: bool) -> bool:
    v = os.getenv(env_key)
    if v is not None and v != "":
        return _truthy(v)
    return bool(get_config_value(config_key, default))
