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

This server uses **stdio transport**, which Cursor expects for MCP.

### Step 1: Open Cursor Settings

1. Open Cursor
2. Press `Cmd+Shift+P` (macOS) or `Ctrl+Shift+P` (Windows/Linux)
3. Type **"Open MCP Settings"** and select it
4. This opens `~/.cursor/mcp.json`

### Step 2: Add the server config

Add the following to your `mcp.json` (create the file if it doesn't exist):

```json
{
  "mcpServers": {
    "bricks-and-context": {
      "command": "python",
      "args": ["/absolute/path/to/bricks-and-context/run_mcp_server.py"],
      "env": {
        "MCP_AUTH_PATH": "/absolute/path/to/bricks-and-context/auth.yaml",
        "MCP_CONFIG_PATH": "/absolute/path/to/bricks-and-context/config.json"
      }
    }
  }
}
```

> **Replace** `/absolute/path/to/bricks-and-context` with the actual path to this repo on your machine.

### Step 3: Restart Cursor

Restart Cursor (or reload the window) to pick up the new MCP server.

### Verify

Once running, you can ask the AI to use Databricks tools, e.g.:
- *"List my Databricks jobs"*
- *"Run SELECT 1 on Databricks"*
- *"Describe the schema of my_catalog.my_schema.my_table"*

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