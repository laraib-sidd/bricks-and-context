import pytest

from src.mcp_server.workspaces import get_workspace_config, get_workspaces, resolve_workspace_name


def test_legacy_single_workspace_env(monkeypatch):
    monkeypatch.delenv("DATABRICKS_WORKSPACES_JSON", raising=False)
    monkeypatch.delenv("MCP_AUTH_PATH", raising=False)
    monkeypatch.setenv("DATABRICKS_HOST", "test.databricks.com")
    monkeypatch.setenv("DATABRICKS_TOKEN", "tok")
    monkeypatch.setenv("DATABRICKS_HTTP_PATH", "/sql/1.0/warehouses/x")

    ws = get_workspaces()
    assert "default" in ws
    cfg = get_workspace_config(None)
    assert cfg.name == "default"
    assert cfg.host == "test.databricks.com"


def test_multi_workspace_json(monkeypatch):
    monkeypatch.delenv("MCP_AUTH_PATH", raising=False)
    monkeypatch.delenv("DATABRICKS_HOST", raising=False)
    monkeypatch.delenv("DATABRICKS_TOKEN", raising=False)
    monkeypatch.delenv("DATABRICKS_HTTP_PATH", raising=False)
    monkeypatch.setenv(
        "DATABRICKS_WORKSPACES_JSON",
        '[{"name":"prod","host":"https://p.databricks.com","token":"t1","http_path":"/sql/1.0/warehouses/a"},'
        ' {"name":"dev","host":"d.databricks.com","token":"t2","http_path":"/sql/1.0/warehouses/b"}]',
    )
    monkeypatch.setenv("DEFAULT_WORKSPACE", "dev")

    assert resolve_workspace_name(None) == "dev"
    assert resolve_workspace_name("prod") == "prod"
    cfg = get_workspace_config("prod")
    assert cfg.host == "p.databricks.com"


def test_unknown_workspace_raises(monkeypatch):
    monkeypatch.delenv("MCP_AUTH_PATH", raising=False)
    monkeypatch.setenv(
        "DATABRICKS_WORKSPACES_JSON",
        '[{"name":"prod","host":"p.databricks.com","token":"t1","http_path":"/sql/1.0/warehouses/a"}]',
    )
    with pytest.raises(ValueError):
        resolve_workspace_name("nope")


def test_yaml_auth_file(tmp_path, monkeypatch):
    auth = tmp_path / "auth.yaml"
    auth.write_text(
        "default_workspace: dev\n"
        "workspaces:\n"
        "  - name: prod\n"
        "    host: https://p.databricks.com\n"
        "    token: t1\n"
        "    http_path: /sql/1.0/warehouses/a\n"
        "  - name: dev\n"
        "    host: d.databricks.com\n"
        "    token: t2\n"
        "    http_path: /sql/1.0/warehouses/b\n",
        encoding="utf-8",
    )
    monkeypatch.setenv("MCP_AUTH_PATH", str(auth))
    monkeypatch.delenv("DATABRICKS_WORKSPACES_JSON", raising=False)
    monkeypatch.delenv("DATABRICKS_HOST", raising=False)
    monkeypatch.delenv("DATABRICKS_TOKEN", raising=False)
    monkeypatch.delenv("DATABRICKS_HTTP_PATH", raising=False)

    assert resolve_workspace_name(None) == "dev"
    cfg = get_workspace_config("prod")
    assert cfg.host == "p.databricks.com"

