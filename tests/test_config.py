import json
import pytest

import src.mcp_server.config as config_module
from src.mcp_server.config import get_setting_bool, get_setting_int


@pytest.fixture(autouse=True)
def reset_config_cache():
    """Reset config cache before and after each test."""
    config_module._cached = None
    yield
    config_module._cached = None


def test_config_json_defaults(tmp_path, monkeypatch):
    cfg = tmp_path / "config.json"
    cfg.write_text(
        json.dumps({"max_connections": 22, "enable_query_cache": True}),
        encoding="utf-8",
    )
    monkeypatch.setenv("MCP_CONFIG_PATH", str(cfg))
    # Clear env vars that might override
    monkeypatch.delenv("MAX_CONNECTIONS", raising=False)
    monkeypatch.delenv("ENABLE_QUERY_CACHE", raising=False)

    assert get_setting_int("MAX_CONNECTIONS", "max_connections", 10) == 22
    assert get_setting_bool("ENABLE_QUERY_CACHE", "enable_query_cache", False) is True


def test_env_overrides_config(tmp_path, monkeypatch):
    cfg = tmp_path / "config.json"
    cfg.write_text(json.dumps({"max_connections": 22}), encoding="utf-8")
    monkeypatch.setenv("MCP_CONFIG_PATH", str(cfg))
    monkeypatch.setenv("MAX_CONNECTIONS", "5")

    assert get_setting_int("MAX_CONNECTIONS", "max_connections", 10) == 5
