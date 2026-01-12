<div align="center">

# 🧱 Bricks and Context

**Production-grade Model Context Protocol (MCP) server for Databricks**

[![CI](https://github.com/laraib-sidd/bricks-and-context/actions/workflows/ci.yml/badge.svg)](https://github.com/laraib-sidd/bricks-and-context/actions/workflows/ci.yml)
[![Python 3.10+](https://img.shields.io/badge/python-3.10+-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)
[![MCP](https://img.shields.io/badge/MCP-Compatible-purple.svg)](https://modelcontextprotocol.io)

*SQL Warehouses · Jobs API · Multi-Workspace · Built for AI Agents*

</div>

---

## ✨ What is this?

**Bricks and Context** lets AI assistants (Cursor, Claude Desktop, etc.) talk directly to your Databricks workspaces through the [Model Context Protocol](https://modelcontextprotocol.io). 

Think of it as a bridge: your AI asks questions, this server translates them into Databricks API calls, and returns structured, AI-friendly responses.

### Why use this?

| Pain Point | How we solve it |
|------------|-----------------|
| AI gets overwhelmed by huge query results | **Bounded outputs** — configurable row/byte/cell limits |
| Flaky connections cause random failures | **Retries + circuit breakers** — automatic fault tolerance |
| Managing multiple environments is tedious | **Multi-workspace** — switch between dev/prod with one parameter |
| Raw API responses confuse AI models | **Markdown tables** — structured, LLM-optimized output |

---

## 🔧 Available Tools

<details>
<summary><strong>SQL & Schema Discovery</strong></summary>

| Tool | What it does |
|------|--------------|
| `execute_sql_query` | Run SQL with bounded, AI-safe output |
| `discover_schemas` | List all schemas in the workspace |
| `discover_tables` | List tables in a schema with metadata |
| `describe_table` | Get column types, nullability, structure |
| `get_table_sample` | Preview rows for data exploration |
| `connection_health` | Verify Databricks connectivity |

</details>

<details>
<summary><strong>Jobs Management</strong></summary>

| Tool | What it does |
|------|--------------|
| `list_jobs` | List jobs with optional name filtering |
| `get_job_details` | Full job config: schedule, cluster, tasks |
| `get_job_runs` | Run history with state and duration |
| `trigger_job` | Start a job with optional parameters |
| `cancel_job_run` | Stop a running job |
| `get_job_run_output` | Retrieve logs, errors, notebook output |

</details>

<details>
<summary><strong>Observability</strong></summary>

| Tool | What it does |
|------|--------------|
| `cache_stats` | Hit rates, memory usage, category breakdown |
| `performance_stats` | Operation latencies, error rates, health |

</details>

---

## 🚀 Quick Start

### 1. Clone & Install

```bash
git clone https://github.com/laraib-sidd/bricks-and-context.git
cd bricks-and-context
uv sync  # or: pip install -e .
```

### 2. Configure Workspaces

Copy the template and add your credentials:

```bash
cp auth.template.yaml auth.yaml
```

Edit `auth.yaml`:

```yaml
default_workspace: dev

workspaces:
  - name: dev
    host: your-dev.cloud.databricks.com
    token: dapi...
    http_path: /sql/1.0/warehouses/...

  - name: prod
    host: your-prod.cloud.databricks.com
    token: dapi...
    http_path: /sql/1.0/warehouses/...
```

> 💡 `auth.yaml` is gitignored. Your secrets stay local.

### 3. Run

```bash
python run_mcp_server.py
```

---

## 🎯 Cursor Integration

Cursor uses **stdio transport** and doesn't inherit your shell environment. You need explicit paths.

### Step 1: Ensure dependencies are installed

```bash
cd /path/to/bricks-and-context
uv sync
```

### Step 2: Open MCP settings in Cursor

`Cmd+Shift+P` → **"Open MCP Settings"** → Opens `~/.cursor/mcp.json`

### Step 3: Add this configuration

**Using `uv run` (recommended):**

```json
{
  "mcpServers": {
    "databricks": {
      "command": "uv",
      "args": [
        "--directory", "/path/to/bricks-and-context",
        "run", "python", "run_mcp_server.py"
      ],
      "env": {
        "MCP_AUTH_PATH": "/path/to/bricks-and-context/auth.yaml",
        "MCP_CONFIG_PATH": "/path/to/bricks-and-context/config.json"
      }
    }
  }
}
```

**Or using venv directly:**

```json
{
  "mcpServers": {
    "databricks": {
      "command": "/path/to/bricks-and-context/.venv/bin/python",
      "args": ["/path/to/bricks-and-context/run_mcp_server.py"],
      "env": {
        "MCP_AUTH_PATH": "/path/to/bricks-and-context/auth.yaml",
        "MCP_CONFIG_PATH": "/path/to/bricks-and-context/config.json"
      }
    }
  }
}
```

### Step 4: Restart Cursor

Reload the window to activate the MCP server.

### Test it

Ask your AI:
- *"List my Databricks jobs"*
- *"Run `SELECT 1` on Databricks"*
- *"Describe the table `catalog.schema.my_table`"*

---

## 🌐 Multi-Workspace

Define multiple workspaces in `auth.yaml`, then select per-call:

```python
execute_sql_query(sql="SELECT 1", workspace="prod")
list_jobs(limit=10, workspace="dev")
```

When `workspace` is omitted, the server uses `default_workspace`.

---

## ⚙️ Configuration

### `config.json` — Tunable settings (committed)

| Setting | Default | Description |
|---------|---------|-------------|
| `max_connections` | 10 | Connection pool size |
| `max_result_rows` | 200 | Max rows returned per query |
| `max_result_bytes` | 262144 | Max response size (256KB) |
| `max_cell_chars` | 200 | Truncate long cell values |
| `allow_write_queries` | false | Enable INSERT/UPDATE/DELETE |
| `enable_sql_retries` | true | Retry transient SQL failures |
| `enable_query_cache` | false | Cache repeated queries |
| `query_cache_ttl_seconds` | 300 | Cache TTL |
| `databricks_api_timeout_seconds` | 30 | Jobs API timeout |

> Any setting can be overridden via environment variable (uppercase, e.g., `MAX_RESULT_ROWS=500`).

---

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                   MCP Client (Cursor / Claude)                  │
└─────────────────────────────────────────────────────────────────┘
                                │ stdio
                                ▼
┌─────────────────────────────────────────────────────────────────┐
│                     FastMCP Server                              │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────────────────┐  │
│  │ SQL Tools   │  │ Job Tools   │  │ Observability           │  │
│  └──────┬──────┘  └──────┬──────┘  └───────────┬─────────────┘  │
└─────────┼────────────────┼─────────────────────┼────────────────┘
          │                │                     │
          ▼                ▼                     ▼
┌──────────────────┐ ┌──────────────────┐ ┌──────────────────────┐
│ Connection Pool  │ │  Job Manager     │ │ Cache / Perf Monitor │
│  (SQL Connector) │ │  (REST API 2.1)  │ │                      │
└────────┬─────────┘ └────────┬─────────┘ └──────────────────────┘
         │                    │
         └────────┬───────────┘
                  ▼
┌─────────────────────────────────────────────────────────────────┐
│                   Databricks Workspace(s)                       │
│              SQL Warehouse        Jobs Service                  │
└─────────────────────────────────────────────────────────────────┘
```

---

## 🛡️ Reliability Features

| Feature | Description |
|---------|-------------|
| **Bounded outputs** | Rows, bytes, and cell-character limits prevent OOM |
| **Connection pooling** | Thread-safe with per-connection health validation |
| **Retry with backoff** | Exponential backoff + jitter for transient failures |
| **Circuit breakers** | Automatic fault isolation, prevents cascading failures |
| **Query caching** | Optional TTL-based caching for repeated queries |

---

## 🧑‍💻 Development

```bash
uv sync --dev        # Install dev dependencies
uv run pytest        # Run tests
uv run black .       # Format code
uv run mypy src/     # Type check
```

---

## 📄 License

MIT — see [LICENSE](LICENSE)
