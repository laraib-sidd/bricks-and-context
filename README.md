# Bricks and Context

<p align="center">
  <strong>A production-grade Model Context Protocol (MCP) server for Databricks</strong>
</p>

<p align="center">
  <em>SQL Warehouses · Jobs API · Multi-Workspace · Built for AI Workloads</em>
</p>

---

## Overview

**Bricks and Context** enables MCP clients (Cursor, Claude Desktop, etc.) to interact with Databricks through a robust, AI-optimized interface. It provides:

- **SQL Warehouse access** — Query execution, schema discovery, table sampling
- **Jobs API integration** — List, trigger, monitor, and cancel Databricks jobs
- **Multi-workspace support** — Switch between dev/staging/prod with a single parameter
- **Production reliability** — Connection pooling, retries, circuit breakers, bounded outputs

## Features

### 🔌 SQL & Schema Discovery

| Tool | Description |
|------|-------------|
| `execute_sql_query` | Run SQL with bounded, AI-safe output (rows/bytes limits) |
| `discover_schemas` | List all schemas in the workspace |
| `discover_tables` | List tables in a schema with metadata |
| `describe_table` | Get column types, nullability, and structure |
| `get_table_sample` | Preview rows for data exploration |
| `connection_health` | Verify Databricks connectivity |

### ⚙️ Jobs Management

| Tool | Description |
|------|-------------|
| `list_jobs` | List jobs with optional name filtering |
| `get_job_details` | Full job config: schedule, cluster, tasks |
| `get_job_runs` | Run history with state and duration |
| `trigger_job` | Start a job with optional parameters |
| `cancel_job_run` | Stop a running job |
| `get_job_run_output` | Retrieve logs, errors, notebook output |

### 🌐 Multi-Workspace

Configure multiple Databricks workspaces in `auth.yaml` and select per-call:

```python
execute_sql_query(sql="SELECT 1", workspace="prod")
list_jobs(limit=10, workspace="dev")
```

If `workspace` is omitted, the server uses `default_workspace` from your config.

### 🛡️ Production Reliability

- **Bounded SQL output** — Configurable row/byte/cell limits prevent OOM and huge responses
- **Connection pooling** — Thread-safe pool with per-connection health validation
- **Retry logic** — Exponential backoff with jitter for transient failures
- **Circuit breakers** — Automatic fault isolation for cascading failure prevention
- **Query caching** — Optional TTL-based caching for repeated queries

### 📊 Observability

| Tool | Description |
|------|-------------|
| `cache_stats` | Hit rates, memory usage, category breakdown |
| `performance_stats` | Operation latencies, error rates, system health |

---

## Quick Start

### Prerequisites

- Python 3.10+
- Databricks workspace with a SQL Warehouse
- Personal Access Token (PAT) or service principal token

### Installation

   ```bash
git clone https://github.com/laraib-sidd/bricks-and-context.git
   cd bricks-and-context

# Using uv (recommended)
uv sync
   
   # Or using pip
pip install -e .
   ```

### Configuration

1. **Create `auth.yaml`** (contains secrets — not committed):

   ```bash
cp auth.template.yaml auth.yaml
```

Edit `auth.yaml` with your workspace credentials:

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

2. **Review `config.json`** (committed — tunable settings):

```json
{
  "max_connections": 10,
  "max_result_rows": 200,
  "max_result_bytes": 262144,
  "allow_write_queries": false,
  "enable_sql_retries": true
}
```

### Run Locally

   ```bash
   python run_mcp_server.py
   ```

---

## Cursor Setup

This MCP server uses **stdio transport**. Cursor doesn't inherit your shell environment, so you must provide explicit paths and environment variables.

### Step 1: Install Dependencies

```bash
cd /path/to/bricks-and-context
uv sync   # creates .venv/ with all dependencies
```

### Step 2: Open MCP Settings

1. Open Cursor
2. Press `Cmd+Shift+P` (macOS) or `Ctrl+Shift+P` (Windows/Linux)
3. Type **"Open MCP Settings"** and select it
4. This opens `~/.cursor/mcp.json`

### Step 3: Add Server Configuration

Replace `/path/to/bricks-and-context` with your actual path.

**Option A: Using `uv run` (recommended)**

```json
{
  "mcpServers": {
    "databricks": {
      "command": "uv",
      "args": [
        "--directory",
        "/path/to/bricks-and-context",
        "run",
        "python",
        "run_mcp_server.py"
      ],
      "env": {
        "MCP_AUTH_PATH": "/path/to/bricks-and-context/auth.yaml",
        "MCP_CONFIG_PATH": "/path/to/bricks-and-context/config.json"
      }
    }
  }
}
```

**Option B: Using venv Python directly**

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

> **Windows**: Use `.venv\Scripts\python.exe` instead of `.venv/bin/python`.

### Step 4: Restart Cursor

Restart Cursor (or reload the window) to load the MCP server.

### Verify

Ask the AI:
- *"List my Databricks jobs"*
- *"Run SELECT 1 on Databricks"*
- *"Describe the table catalog.schema.table_name"*

---

## Configuration Reference

### `auth.yaml` (secrets — gitignored)

```yaml
default_workspace: dev   # Used when workspace param is omitted

workspaces:
  - name: dev
    host: your-workspace.cloud.databricks.com
    token: dapi...
    http_path: /sql/1.0/warehouses/...
```

### `config.json` (project settings — committed)

| Setting | Default | Description |
|---------|---------|-------------|
| `max_connections` | 10 | Connection pool size |
| `health_check_cache_ttl` | 300 | Health check cache (seconds) |
| `max_result_rows` | 200 | Max rows per query |
| `max_result_bytes` | 262144 | Max response size (bytes) |
| `max_cell_chars` | 200 | Max chars per cell |
| `allow_write_queries` | false | Enable INSERT/UPDATE/DELETE |
| `enable_sql_retries` | true | Retry transient SQL failures |
| `enable_query_cache` | false | Cache identical queries |
| `query_cache_ttl_seconds` | 300 | Query cache TTL |
| `databricks_api_timeout_seconds` | 30 | Jobs API timeout |

### Environment Variable Overrides

Any setting can be overridden via environment variable (uppercase, e.g., `MAX_RESULT_ROWS=500`).

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                        MCP Client (Cursor/Claude)               │
└─────────────────────────────────────────────────────────────────┘
                                  │ stdio
                                  ▼
┌─────────────────────────────────────────────────────────────────┐
│                      FastMCP Server (mcp_server.py)             │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────────────────┐  │
│  │ SQL Tools   │  │ Job Tools   │  │ Observability Tools     │  │
│  └──────┬──────┘  └──────┬──────┘  └───────────┬─────────────┘  │
└─────────┼────────────────┼─────────────────────┼────────────────┘
          │                │                     │
          ▼                ▼                     ▼
┌──────────────────┐ ┌──────────────────┐ ┌──────────────────────┐
│ Connection Pool  │ │  Job Manager     │ │ Cache / Performance  │
│  (SQL Connector) │ │  (REST API 2.1)  │ │      Monitors        │
└────────┬─────────┘ └────────┬─────────┘ └──────────────────────┘
         │                    │
         ▼                    ▼
┌─────────────────────────────────────────────────────────────────┐
│                     Databricks Workspace(s)                     │
│              SQL Warehouse          Jobs Service                │
└─────────────────────────────────────────────────────────────────┘
```

---

## Development

```bash
# Install dev dependencies
uv sync --dev

# Run tests
pytest

# Format code
black src/ tests/

# Type check
mypy src/
```

---

## License

MIT — see [LICENSE](LICENSE)
