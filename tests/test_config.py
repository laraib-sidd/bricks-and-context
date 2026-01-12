import json

from src.mcp_server.config import get_setting_bool, get_setting_int


def test_config_json_defaults(tmp_path, monkeypatch):
    cfg = tmp_path / "config.json"
    cfg.write_text(json.dumps({"max_connections": 22, "enable_query_cache": True}), encoding="utf-8")
    monkeypatch.setenv("MCP_CONFIG_PATH", str(cfg))

    assert get_setting_int("MAX_CONNECTIONS", "max_connections", 10) == 22
    assert get_setting_bool("ENABLE_QUERY_CACHE", "enable_query_cache", False) is True


def test_env_overrides_config(tmp_path, monkeypatch):
    cfg = tmp_path / "config.json"
    cfg.write_text(json.dumps({"max_connections": 22}), encoding="utf-8")
    monkeypatch.setenv("MCP_CONFIG_PATH", str(cfg))
    monkeypatch.setenv("MAX_CONNECTIONS", "5")

    assert get_setting_int("MAX_CONNECTIONS", "max_connections", 10) == 5

