# Bricks-and-Context MCP Analysis Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Complete analysis of every tool, failure mode, and stability issue in the bricks-and-context MCP server

**Architecture:** FastMCP server with 17 source files, 14 REST/SQL tools, circuit breaker + retry + connection pool resilience stack, multi-workspace YAML/JSON/env config

**Tech Stack:** Python 3.10+, FastMCP 2.x, databricks-sql-connector, requests, PyYAML, pytest, mypy

---

## Complete Tool Inventory

### SQL Tools (registered in `mcp_server.py`)
| # | Tool Name | Type | Function |
|---|-----------|------|----------|
| 1 | `databricks_execute_sql_query` | Read/Write | Execute arbitrary SQL, return markdown table |
| 2 | `databricks_discover_schemas` | Read-only | `SHOW SCHEMAS` via SQL warehouse |
| 3 | `databricks_discover_tables` | Read-only | `SHOW TABLES IN <schema>` via SQL warehouse |
| 4 | `databricks_describe_table` | Read-only | `DESCRIBE EXTENDED <schema>.<table>` |
| 5 | `databricks_get_table_sample` | Read-only | `SELECT * FROM <table> LIMIT N` (max 20) |
| 6 | `databricks_connection_health` | Read-only | `SELECT 1` health check |

### Job Tools (registered in `mcp_server.py`, logic in `job_manager.py`)
| # | Tool Name | Type | Function |
|---|-----------|------|----------|
| 7 | `databricks_list_jobs` | Read-only | List jobs with name filter + pagination |
| 8 | `databricks_get_job_details` | Read-only | Full job config, tasks, schedule |
| 9 | `databricks_get_job_runs` | Read-only | Run history with state/duration |
| 10 | `databricks_trigger_job` | Write | Trigger job run with params |
| 11 | `databricks_cancel_job_run` | Write | Cancel a running job |
| 12 | `databricks_get_job_run_output` | Read-only | Logs, notebook output, error state |

### Catalog Tools (registered in `catalog_manager.py`)
| # | Tool Name | Type | Function |
|---|-----------|------|----------|
| 13 | `databricks_list_catalogs` | Read-only | Unity Catalog catalogs list |
| 14 | `databricks_list_uc_schemas` | Read-only | Schemas in a catalog |
| 15 | `databricks_list_uc_tables` | Read-only | Tables in catalog.schema |
| 16 | `databricks_get_uc_table_info` | Read-only | Column-level table metadata |
| 17 | `databricks_list_volumes` | Read-only | Volumes in catalog.schema |

### Cluster Tools (registered in `cluster_manager.py`)
| # | Tool Name | Type | Function |
|---|-----------|------|----------|
| 18 | `databricks_list_clusters` | Read-only | All clusters with state |
| 19 | `databricks_list_warehouses` | Read-only | All SQL warehouses with state |
| 20 | `databricks_get_warehouse_status` | Read-only | Detailed warehouse health |

### Workspace Tools (registered in `workspace_manager.py`)
| # | Tool Name | Type | Function |
|---|-----------|------|----------|
| 21 | `databricks_list_workspace_files` | Read-only | Browse workspace file tree |
| 22 | `databricks_read_notebook` | Read-only | Export notebook source code |
| 23 | `databricks_get_workspace_object_status` | Read-only | Object metadata (type, id, timestamps) |

### Pipeline Tools (registered in `pipeline_manager.py`)
| # | Tool Name | Type | Function |
|---|-----------|------|----------|
| 24 | `databricks_list_pipelines` | Read-only | List DLT pipelines |
| 25 | `databricks_get_pipeline_status` | Read-only | Pipeline spec + latest updates |
| 26 | `databricks_start_pipeline` | Write | Trigger DLT pipeline update |
| 27 | `databricks_get_pipeline_events` | Read-only | Pipeline event logs |

### Query History Tools (registered in `query_history_manager.py`)
| # | Tool Name | Type | Function |
|---|-----------|------|----------|
| 28 | `databricks_list_query_history` | Read-only | Recent SQL query history |
| 29 | `databricks_get_object_permissions` | Read-only | Object ACLs/permissions |

### Observability Tools (registered in `mcp_server.py`)
| # | Tool Name | Type | Function |
|---|-----------|------|----------|
| 30 | `databricks_cache_stats` | Read-only | Cache hit rates, memory usage |
| 31 | `databricks_performance_stats` | Read-only | Health score, P95 latency, error rates |

**Total: 31 MCP tools**

---

## Architecture Analysis

### Resilience Stack
```
Request → Circuit Breaker → Retry (exponential backoff + jitter) → Connection Pool → Databricks
```

- **Circuit Breaker** (`error_handler.py`): 5 failures → OPEN, 60s recovery, 3 successes → CLOSED
- **Retry** (`error_handler.py`): 3 attempts, 2s base delay, 30s max, exponential backoff
- **Connection Pool** (`connection_pool.py`): Queue-based, per-connection validation cache, flush on warehouse restart
- **Cache** (`cache_manager.py`): TTL-based, 1000 max entries, category-specific TTLs

### Configuration Priority
1. `auth.yaml` (YAML file, recommended)
2. `DATABRICKS_WORKSPACES_JSON` (JSON env var)
3. Legacy env vars (`DATABRICKS_HOST`, `DATABRICKS_TOKEN`, `DATABRICKS_HTTP_PATH`)

### Known Failure Modes (Agent-Killing Issues)

| # | Issue | File | Impact |
|---|-------|------|--------|
| 1 | **SIGPIPE on client disconnect** | `run_mcp_server.py` | Server dies silently if MCP client disconnects — mitigated by signal handler but only for SIGPIPE, not other disconnect patterns |
| 2 | **Circuit breaker blocks ALL requests for 60s** | `error_handler.py:88-94` | After 5 failures, every tool call fails for 60s — agent sees "Circuit breaker is OPEN" and loses the MCP |
| 3 | **Connection pool exhaustion** | `connection_pool.py:270-284` | If all connections checked out and pool full, 10s timeout → agent gets TimeoutError |
| 4 | **Global cache never invalidates without restart** | `workspaces.py:169-192` | Config/workspace changes require full server restart |
| 5 | **Job manager creates its own requests.Session** | `job_manager.py:87` | Duplicates HTTP client logic instead of using `api_client.py` — different timeout/retry behavior |
| 6 | **Error classification is string-matching** | `error_handler.py:173-201` | "token" in error message → classified as AUTH (non-retryable), even if it's "token bucket" rate limiting |
| 7 | **SQL identifier validation too restrictive** | `mcp_server.py:40` | `^[\w.\-]+$` rejects valid UC names with spaces or special chars |
| 8 | **No async support** | All files | FastMCP supports async, but all tools are synchronous — blocks on IO |
| 9 | **`_discover_schemas` hardcodes description** | `mcp_server.py:319-324` | Says "User database" or "System database" — no actual metadata |
| 10 | **Query cache disabled by default** | `mcp_server.py:122-123` | `ENABLE_QUERY_CACHE` defaults to False — repeated identical queries hit warehouse every time |

---

### Task 1: Validate Current Test Coverage

**Files:**
- Read: `tests/test_mcp_server.py`
- Read: `tests/test_new_tools.py`
- Read: `tests/test_job_management.py`

- [ ] **Step 1: Run full test suite and record baseline**
```bash
uv run pytest tests/ --tb=short -q
```
Expected: 80 passed

- [ ] **Step 2: Run mypy and record baseline**
```bash
uv run mypy src/mcp_server/ --ignore-missing-imports
```
Expected: Success, no issues

- [ ] **Step 3: Document untested code paths**
Identify which tools/functions have no test coverage by comparing test files against source.

---

### Task 2: Verify MCP Server Startup & Tool Registration

**Files:**
- Read: `src/mcp_server/mcp_server.py:37-38` (FastMCP init)
- Read: `src/mcp_server/mcp_server.py` (bottom — register_* calls)

- [ ] **Step 1: Verify server can import without errors**
```bash
uv run python -c "from mcp_server.mcp_server import mcp; print(f'Server name: {mcp.name}')"
```
Expected: `Server name: databricks_mcp`

- [ ] **Step 2: Verify all 31 tools register**
```bash
uv run python -c "
from mcp_server.mcp_server import mcp
tools = list(mcp._tool_manager._tools.keys()) if hasattr(mcp, '_tool_manager') else 'unknown structure'
print(f'Tools registered: {len(tools) if isinstance(tools, list) else tools}')
if isinstance(tools, list):
    for t in sorted(tools):
        print(f'  - {t}')
"
```

---

### Task 3: Stress-Test Error Paths

- [ ] **Step 1: Test circuit breaker recovery**
Verify that after 5 failures the CB opens, and after 60s it transitions to HALF_OPEN.

- [ ] **Step 2: Test retry with non-retryable error**
Verify authentication errors (401/403) fail immediately without retry.

- [ ] **Step 3: Test connection pool under load**
Verify pool handles concurrent requests without deadlock.

---

This analysis is complete. Proceed to the enhancement plan.
