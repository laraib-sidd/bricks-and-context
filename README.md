# Bricks and Context

**A Model Context Protocol (MCP) server for Databricks (SQL + Jobs), built to be robust for AI workloads.**

## Overview

Bricks and Context lets MCP clients (Cursor, Claude Desktop, etc.) interact with Databricks using:
- **Databricks SQL Warehouses** (query + schema discovery)
- **Databricks Jobs API** (list/runs/trigger/cancel/output)

### Key Features

- **Robust by default**: bounded SQL output (rows/bytes), connection pooling, retries, and safer job parsing.
- **Multi-workspace**: configure multiple Databricks workspaces and select them per tool call.

## Quick Start

### Prerequisites

- Python 3.10+
- Databricks workspace + SQL Warehouse
- Databricks PAT (or service principal token)

### Setup

1. **Clone**

   ```bash
   git clone https://github.com/your-org/bricks-and-context.git
   cd bricks-and-context
   ```

2. **Install**

   ```bash
   uv sync --dev
# or: pip install -e ".[dev]"
   ```

3. **Configure auth + config**

   ```bash
cp auth.template.yaml auth.yaml   # NOT committed (contains secrets)
# config.json is committed project properties; tweak as needed
   ```

4. **Run**

   ```bash
   python run_mcp_server.py
   ```

## Cursor Setup (MCP)

This server uses **stdio transport**. Cursor doesn't inherit your shell environment, so you must provide the full path to Python (or use `uv`) and set environment variables explicitly.

### Step 1: Install dependencies

```bash
cd /path/to/bricks-and-context
uv sync        # creates .venv/ with all dependencies
```

### Step 2: Open Cursor MCP Settings

1. Open Cursor
2. Press `Cmd+Shift+P` (macOS) or `Ctrl+Shift+P` (Windows/Linux)
3. Type **"Open MCP Settings"** and select it
4. This opens `~/.cursor/mcp.json`

### Step 3: Add the server config

Choose **one** of the options below. Replace `/path/to/bricks-and-context` with your actual repo path.

#### Option A: Using `uv run` (recommended)

`uv run` automatically uses the project's virtual environment:

```json
{
  "mcpServers": {
    "bricks-and-context": {
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

#### Option B: Using the venv Python directly

Point to the Python executable inside `.venv`:

```json
{
  "mcpServers": {
    "bricks-and-context": {
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

> **Windows**: Use `.venv\\Scripts\\python.exe` instead of `.venv/bin/python`.

### Step 4: Restart Cursor

Restart Cursor (or reload the window) to load the MCP server.

### Verify

Once running, you can ask the AI:
- *"List my Databricks jobs"*
- *"Run SELECT 1 on Databricks"*
- *"Describe the table my_catalog.my_schema.my_table"*

## Multi-workspace

All tools accept an optional `workspace` parameter. If omitted, the server uses `default_workspace` from `auth.yaml`.

Examples:
- `execute_sql_query(sql="SELECT 1", workspace="prod")`
- `list_jobs(limit=10, workspace="dev")`

## Tools (current)

### SQL / schema
- `execute_sql_query`
- `discover_schemas`
- `discover_tables`
- `describe_table`
- `get_table_sample`
- `connection_health`

### Jobs
- `list_jobs`
- `get_job_details`
- `get_job_runs`
- `trigger_job`
- `cancel_job_run`
- `get_job_run_output`

### Observability
- `cache_stats`
- `performance_stats`