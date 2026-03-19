# Bricks-and-Context MCP Enhancements & Testing Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Fix stability issues that cause agents to lose the MCP, add missing test coverage, and harden error handling

**Architecture:** Fix-first approach — address the agent-killing issues before adding enhancements. Each task is independently testable.

**Tech Stack:** Python 3.10+, FastMCP 2.x, pytest, mypy, uv

---

## File Structure

| File | Responsibility | Action |
|------|---------------|--------|
| `src/mcp_server/error_handler.py` | Circuit breaker + retry | Modify: fix error classification, add graceful degradation |
| `src/mcp_server/connection_pool.py` | Connection lifecycle | Modify: add graceful timeout messaging |
| `src/mcp_server/mcp_server.py` | Tool registration + SQL execution | Modify: fix identifier validation, improve error messages |
| `src/mcp_server/job_manager.py` | Job REST API | Modify: migrate to shared api_client |
| `src/mcp_server/api_client.py` | Shared HTTP client | Modify: add PATCH/DELETE methods |
| `tests/test_error_handler.py` | Error handler tests | Create: comprehensive circuit breaker + retry tests |
| `tests/test_error_classification.py` | Error classification tests | Create: edge case classification tests |
| `tests/test_tool_registration.py` | Tool registration validation | Create: verify all 31 tools register |
| `tests/test_sql_safety.py` | SQL safety tests | Create: identifier validation, read-only detection |
| `tests/conftest.py` | Shared fixtures | Create: workspace mocking, connection pool fixtures |

---

### Task 1: Fix Error Classification (Agent-Killing Bug)

The error classifier at `error_handler.py:173-201` uses string matching that misclassifies errors. "token" matches both auth errors AND "token bucket" rate limiting. This causes rate limits to be treated as non-retryable auth errors, which cascades into the circuit breaker opening.

**Files:**
- Modify: `src/mcp_server/error_handler.py:173-201`
- Create: `tests/test_error_classification.py`

- [ ] **Step 1: Write failing test for misclassification**

```python
# tests/test_error_classification.py
from mcp_server.error_handler import ErrorHandler, ErrorType

def test_token_bucket_not_classified_as_auth():
    """Rate limit 'token bucket' should NOT be classified as AUTH."""
    handler = ErrorHandler()
    exc = Exception("rate limit exceeded: token bucket depleted")
    assert handler.classify_error(exc) != ErrorType.AUTHENTICATION

def test_rate_limit_429_classified_correctly():
    handler = ErrorHandler()
    exc = Exception("rate limit: HTTP 429 from Databricks API")
    assert handler.classify_error(exc) == ErrorType.RATE_LIMIT

def test_actual_auth_error():
    handler = ErrorHandler()
    exc = Exception("authentication: HTTP 401 — token may be expired")
    assert handler.classify_error(exc) == ErrorType.AUTHENTICATION

def test_timeout_classified_correctly():
    handler = ErrorHandler()
    exc = Exception("Connection timeout after 30 seconds")
    assert handler.classify_error(exc) == ErrorType.TIMEOUT

def test_sql_syntax_error():
    handler = ErrorHandler()
    exc = Exception("SQL syntax error near 'SELCET'")
    assert handler.classify_error(exc) == ErrorType.SQL_ERROR

def test_unknown_error():
    handler = ErrorHandler()
    exc = Exception("something completely unexpected happened")
    assert handler.classify_error(exc) == ErrorType.UNKNOWN
```

- [ ] **Step 2: Run test to verify it fails**
```bash
uv run pytest tests/test_error_classification.py -v
```
Expected: `test_token_bucket_not_classified_as_auth` FAILS

- [ ] **Step 3: Fix the classification order**

In `error_handler.py`, change `classify_error` to check rate limit BEFORE authentication, so "token bucket" matches rate limit first:

```python
def classify_error(self, error: Exception) -> ErrorType:
    """Classify error for appropriate handling strategy."""
    error_str = str(error).lower()

    # Check rate limit FIRST (before auth) because rate limit messages
    # can contain "token" (e.g. "token bucket") which would false-match auth.
    if any(
        keyword in error_str
        for keyword in ["rate limit", "too many requests", "429", "throttl"]
    ):
        return ErrorType.RATE_LIMIT

    if any(
        keyword in error_str
        for keyword in ["authentication", "unauthorized", "forbidden"]
    ):
        return ErrorType.AUTHENTICATION

    # Only match "token" for auth if no other category matched
    if "token" in error_str and any(
        keyword in error_str for keyword in ["expired", "invalid", "revoked"]
    ):
        return ErrorType.AUTHENTICATION

    if "timeout" in error_str:
        return ErrorType.TIMEOUT

    if any(
        keyword in error_str
        for keyword in ["network", "connection", "unreachable", "refused", "reset"]
    ):
        return ErrorType.NETWORK

    if any(
        keyword in error_str for keyword in ["databricks", "api error", "rest api"]
    ):
        return ErrorType.DATABRICKS_API

    if any(keyword in error_str for keyword in ["sql", "query", "syntax"]):
        return ErrorType.SQL_ERROR

    return ErrorType.UNKNOWN
```

- [ ] **Step 4: Run tests to verify all pass**
```bash
uv run pytest tests/test_error_classification.py -v
```
Expected: All PASS

- [ ] **Step 5: Run full test suite to verify no regression**
```bash
uv run pytest tests/ --tb=short -q
```
Expected: 80+ passed

- [ ] **Step 6: Commit**
```bash
git add src/mcp_server/error_handler.py tests/test_error_classification.py
git commit -m "fix: reorder error classification to prevent rate limit misclassification as auth"
```

---

### Task 2: Add Circuit Breaker Graceful Degradation

When the circuit breaker opens, tools return a raw exception message that confuses agents. Instead, return a structured error with recovery guidance.

**Files:**
- Modify: `src/mcp_server/error_handler.py:262-270`
- Create: `tests/test_error_handler.py`

- [ ] **Step 1: Write failing test**

```python
# tests/test_error_handler.py
import time
from mcp_server.error_handler import (
    ErrorHandler,
    CircuitBreaker,
    CircuitBreakerConfig,
    CircuitBreakerState,
    RetryConfig,
    ErrorType,
)

def test_circuit_breaker_open_message_is_actionable():
    """When CB is open, the error message should guide the agent."""
    handler = ErrorHandler()
    config = CircuitBreakerConfig(failure_threshold=2, recovery_timeout_seconds=60)
    cb = handler.get_circuit_breaker("test_op", config)

    # Trip the circuit breaker
    cb.record_failure()
    cb.record_failure()
    assert cb.state == CircuitBreakerState.OPEN

    # Decorate a dummy function
    @handler.with_retry("test_op", RetryConfig(max_attempts=1), circuit_breaker=True)
    def dummy():
        return "ok"

    try:
        dummy()
        assert False, "Should have raised"
    except Exception as e:
        msg = str(e)
        assert "temporarily unavailable" in msg.lower() or "circuit breaker" in msg.lower()
        assert "retry" in msg.lower() or "wait" in msg.lower()

def test_circuit_breaker_transitions():
    """CB: CLOSED -> OPEN -> HALF_OPEN -> CLOSED"""
    config = CircuitBreakerConfig(
        failure_threshold=2, recovery_timeout_seconds=0.1, success_threshold=1
    )
    cb = CircuitBreaker("test", config)

    assert cb.state == CircuitBreakerState.CLOSED

    cb.record_failure()
    cb.record_failure()
    assert cb.state == CircuitBreakerState.OPEN

    time.sleep(0.15)
    assert cb.state == CircuitBreakerState.HALF_OPEN

    cb.record_success()
    assert cb.state == CircuitBreakerState.CLOSED

def test_retry_skips_non_retryable():
    """Auth errors should not be retried."""
    handler = ErrorHandler()
    call_count = 0

    @handler.with_retry("auth_test", circuit_breaker=False)
    def fail_auth():
        nonlocal call_count
        call_count += 1
        raise Exception("authentication: HTTP 401 — token expired")

    try:
        fail_auth()
    except Exception:
        pass

    assert call_count == 1  # Should not retry
```

- [ ] **Step 2: Run test to verify it fails**
```bash
uv run pytest tests/test_error_handler.py -v
```

- [ ] **Step 3: Improve circuit breaker error message**

In `error_handler.py:262-270`, change the error message:

```python
if circuit and not circuit.can_execute():
    error_msg = (
        f"Databricks is temporarily unavailable (circuit breaker '{operation_name}' is open). "
        f"The server detected repeated failures and is waiting before retrying. "
        f"This will automatically recover in up to {circuit.config.recovery_timeout_seconds:.0f} seconds. "
        f"If this persists, check warehouse status with `databricks_list_warehouses`."
    )
    log_databricks_event(
        "ERROR_HANDLER", "BLOCKED", error_msg, "WARNING"
    )
    raise Exception(error_msg)
```

- [ ] **Step 4: Run tests**
```bash
uv run pytest tests/test_error_handler.py tests/test_error_classification.py -v
```
Expected: All PASS

- [ ] **Step 5: Run full suite**
```bash
uv run pytest tests/ --tb=short -q && uv run mypy src/mcp_server/ --ignore-missing-imports
```

- [ ] **Step 6: Commit**
```bash
git add src/mcp_server/error_handler.py tests/test_error_handler.py
git commit -m "fix: improve circuit breaker error messages for agent recovery"
```

---

### Task 3: Add Tool Registration Tests

Ensure all 31 tools register correctly and FastMCP can enumerate them.

**Files:**
- Create: `tests/test_tool_registration.py`

- [ ] **Step 1: Write test**

```python
# tests/test_tool_registration.py
"""Verify all expected MCP tools are registered."""
import unittest.mock as mock
import os

# Prevent workspace loading during import
with mock.patch.dict(os.environ, {
    "DATABRICKS_HOST": "test.databricks.com",
    "DATABRICKS_TOKEN": "test-token",
    "DATABRICKS_HTTP_PATH": "/sql/1.0/warehouses/test",
}, clear=False):
    with mock.patch("mcp_server.workspaces._load_yaml_configs", return_value=None):
        from mcp_server.mcp_server import mcp

EXPECTED_TOOLS = [
    # SQL tools
    "databricks_execute_sql_query",
    "databricks_discover_schemas",
    "databricks_discover_tables",
    "databricks_describe_table",
    "databricks_get_table_sample",
    "databricks_connection_health",
    # Job tools
    "databricks_list_jobs",
    "databricks_get_job_details",
    "databricks_get_job_runs",
    "databricks_trigger_job",
    "databricks_cancel_job_run",
    "databricks_get_job_run_output",
    # Catalog tools
    "databricks_list_catalogs",
    "databricks_list_uc_schemas",
    "databricks_list_uc_tables",
    "databricks_get_uc_table_info",
    "databricks_list_volumes",
    # Cluster tools
    "databricks_list_clusters",
    "databricks_list_warehouses",
    "databricks_get_warehouse_status",
    # Workspace tools
    "databricks_list_workspace_files",
    "databricks_read_notebook",
    "databricks_get_workspace_object_status",
    # Pipeline tools
    "databricks_list_pipelines",
    "databricks_get_pipeline_status",
    "databricks_start_pipeline",
    "databricks_get_pipeline_events",
    # Query history tools
    "databricks_list_query_history",
    "databricks_get_object_permissions",
    # Observability tools
    "databricks_cache_stats",
    "databricks_performance_stats",
]


def test_all_expected_tools_registered():
    """Every tool in our inventory must be registered."""
    registered = set()
    if hasattr(mcp, '_tool_manager') and hasattr(mcp._tool_manager, '_tools'):
        registered = set(mcp._tool_manager._tools.keys())
    else:
        # FastMCP 2.x may use different internal structure — adapt as needed
        import pytest
        pytest.skip("Cannot introspect FastMCP tool registry — update test for current FastMCP version")

    missing = set(EXPECTED_TOOLS) - registered
    extra = registered - set(EXPECTED_TOOLS)

    assert not missing, f"Missing tools: {sorted(missing)}"
    # Extra tools are a warning, not failure
    if extra:
        import warnings
        warnings.warn(f"Unexpected extra tools: {sorted(extra)}")


def test_tool_count():
    """Sanity check: we expect exactly 31 tools."""
    assert len(EXPECTED_TOOLS) == 31
```

- [ ] **Step 2: Run test**
```bash
uv run pytest tests/test_tool_registration.py -v
```

- [ ] **Step 3: Fix any issues (adapt to FastMCP internals if needed)**

- [ ] **Step 4: Commit**
```bash
git add tests/test_tool_registration.py
git commit -m "test: add tool registration verification for all 31 MCP tools"
```

---

### Task 4: Add SQL Safety Tests

**Files:**
- Create: `tests/test_sql_safety.py`

- [ ] **Step 1: Write tests**

```python
# tests/test_sql_safety.py
"""Test SQL identifier validation and read-only detection."""
import pytest
from mcp_server.mcp_server import _is_read_only_sql, _validate_identifier

class TestReadOnlyDetection:
    @pytest.mark.parametrize("sql", [
        "SELECT * FROM t",
        "  SELECT 1",
        "select count(*) from t",
        "SHOW SCHEMAS",
        "show tables",
        "DESCRIBE EXTENDED t",
        "EXPLAIN SELECT 1",
        "WITH cte AS (SELECT 1) SELECT * FROM cte",
        "(SELECT 1)",  # Parenthesized
    ])
    def test_read_only_queries(self, sql):
        assert _is_read_only_sql(sql) is True

    @pytest.mark.parametrize("sql", [
        "INSERT INTO t VALUES (1)",
        "UPDATE t SET x=1",
        "DELETE FROM t",
        "DROP TABLE t",
        "CREATE TABLE t (id INT)",
        "ALTER TABLE t ADD COLUMN x INT",
        "MERGE INTO t USING s ON t.id=s.id",
        "GRANT SELECT ON t TO user",
        "REVOKE ALL ON t FROM user",
    ])
    def test_write_queries(self, sql):
        assert _is_read_only_sql(sql) is False

class TestIdentifierValidation:
    @pytest.mark.parametrize("name", [
        "my_table",
        "schema.table",
        "catalog.schema.table",
        "my-table",
        "table_123",
    ])
    def test_valid_identifiers(self, name):
        _validate_identifier(name)  # Should not raise

    @pytest.mark.parametrize("name", [
        "",
        "table; DROP TABLE users",
        "table' OR '1'='1",
        "table\nname",
        "table name",  # spaces
    ])
    def test_invalid_identifiers(self, name):
        with pytest.raises(ValueError):
            _validate_identifier(name)
```

- [ ] **Step 2: Run test**
```bash
uv run pytest tests/test_sql_safety.py -v
```

- [ ] **Step 3: Commit**
```bash
git add tests/test_sql_safety.py
git commit -m "test: add SQL safety tests for identifier validation and read-only detection"
```

---

### Task 5: Add Shared Test Fixtures

**Files:**
- Create: `tests/conftest.py` (if not exists, or modify)

- [ ] **Step 1: Write conftest**

```python
# tests/conftest.py
"""Shared fixtures for MCP server tests."""
import os
import pytest
import unittest.mock as mock

@pytest.fixture(autouse=True)
def reset_global_caches():
    """Reset all module-level caches between tests."""
    import mcp_server.workspaces as ws
    import mcp_server.connection_pool as cp
    import mcp_server.error_handler as eh
    import mcp_server.api_client as ac

    # Save originals
    orig_ws = ws._workspaces_cache
    orig_dw = ws._default_workspace_cache
    orig_pools = cp._connection_pools.copy()
    orig_clients = ac._clients.copy()

    yield

    # Restore
    ws._workspaces_cache = orig_ws
    ws._default_workspace_cache = orig_dw
    cp._connection_pools.clear()
    cp._connection_pools.update(orig_pools)
    ac._clients.clear()
    ac._clients.update(orig_clients)
```

- [ ] **Step 2: Run full suite to verify no interference**
```bash
uv run pytest tests/ --tb=short -q
```

- [ ] **Step 3: Commit**
```bash
git add tests/conftest.py
git commit -m "test: add shared conftest with global cache reset fixtures"
```

---

### Task 6: Migrate Job Manager to Shared API Client

The job manager (`job_manager.py`) creates its own `requests.Session` and duplicates HTTP logic instead of using `api_client.py`. This means different timeout/retry behavior for jobs vs everything else.

**Files:**
- Modify: `src/mcp_server/job_manager.py`
- Modify: `src/mcp_server/api_client.py` (add PATCH/DELETE if needed)
- Modify: `tests/test_job_management.py`

- [ ] **Step 1: Identify all HTTP calls in job_manager.py**
Look for `self._make_request` and `self._session.request` calls.

- [ ] **Step 2: Refactor to use DatabricksAPIClient**
Replace `self._session` and `self._make_request` with `get_api_client(workspace)`.

- [ ] **Step 3: Run job tests**
```bash
uv run pytest tests/test_job_management.py -v
```

- [ ] **Step 4: Run full suite**
```bash
uv run pytest tests/ --tb=short -q && uv run mypy src/mcp_server/ --ignore-missing-imports
```

- [ ] **Step 5: Commit**
```bash
git add src/mcp_server/job_manager.py src/mcp_server/api_client.py tests/test_job_management.py
git commit -m "refactor: migrate job manager to shared API client for consistent error handling"
```

---

### Task 7: Improve Connection Pool Timeout Messages

When the connection pool is exhausted, agents see a raw `TimeoutError` with no guidance.

**Files:**
- Modify: `src/mcp_server/connection_pool.py:276-284`

- [ ] **Step 1: Write failing test**

```python
# Add to tests/test_connection_pool.py
def test_pool_exhaustion_message_is_actionable(mock_workspace):
    """TimeoutError should include guidance for the agent."""
    pool = ConnectionPool(
        host="test.databricks.com",
        token="test-token",
        http_path="/test",
        max_connections=1,
    )
    pool._created_connections = 1  # Pretend pool is full

    try:
        pool.get_connection(timeout=0.1)
        assert False, "Should have raised TimeoutError"
    except TimeoutError as e:
        msg = str(e)
        assert "warehouse" in msg.lower() or "connection" in msg.lower()
```

- [ ] **Step 2: Improve the error message**

```python
raise TimeoutError(
    f"All {self.max_connections} connections are in use and none became "
    f"available within {timeout} seconds. This usually means the SQL "
    f"Warehouse is under heavy load or queries are running slowly. "
    f"Try: 1) Wait and retry, 2) Use `databricks_list_warehouses` to check state, "
    f"3) Add LIMIT to your queries."
)
```

- [ ] **Step 3: Run tests**
```bash
uv run pytest tests/test_connection_pool.py -v
```

- [ ] **Step 4: Commit**
```bash
git add src/mcp_server/connection_pool.py tests/test_connection_pool.py
git commit -m "fix: improve connection pool timeout message with agent recovery guidance"
```

---

### Task 8: Run Ralph Loop for Continuous Testing

After all fixes are in, start a Ralph Loop to continuously run the test suite and catch regressions.

- [ ] **Step 1: Start Ralph Loop**
```
/ralph-loop 5m uv run pytest tests/ --tb=short -q && uv run mypy src/mcp_server/ --ignore-missing-imports
```

- [ ] **Step 2: Monitor for 3 cycles**
Verify all passes are stable across multiple runs.

- [ ] **Step 3: Stop Ralph Loop when satisfied**
```
/cancel-ralph
```

---

## Enhancement Ideas (Future Work)

These are NOT part of this plan but identified for future consideration:

1. **Async tool support** — Convert tools to async for better IO throughput
2. **Structured logging** — Replace formatted strings with structured JSON logs
3. **Config hot-reload** — Watch auth.yaml for changes without server restart
4. **Query explain plan tool** — Add `EXPLAIN` wrapper tool for query optimization
5. **Warehouse auto-start** — Detect STOPPED warehouse and offer to start it
6. **Token refresh** — Detect expired tokens and prompt for new ones
7. **Rate limit backpressure** — Return Retry-After header info to agents
8. **Connection pool metrics** — Expose pool utilization in performance_stats
9. **Query history analysis** — Add aggregation/statistics on query patterns
10. **Notebook diff tool** — Compare notebook versions
