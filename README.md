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

This server uses **stdio transport**, which is what Cursor’s MCP integration expects.

In Cursor, add an MCP server that runs:

```bash
python run_mcp_server.py
```

Make sure the process can find your config files:
- **Default paths**: `auth.yaml` and `config.json` in the repo root
- **Or set**: `MCP_AUTH_PATH` and `MCP_CONFIG_PATH`

Example environment values for Cursor:

```bash
MCP_AUTH_PATH=/absolute/path/to/auth.yaml
MCP_CONFIG_PATH=/absolute/path/to/config.json
```

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