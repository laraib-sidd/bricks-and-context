"""
MCP Server for Databricks Integration
Provides AI solutions with tools to interact with Databricks via MCP protocol
"""

import hashlib
import json
import os
from typing import Optional

from fastmcp import FastMCP

from .config import get_setting_bool, get_setting_int
from .cache_manager import get_cached_query_result, cache_query_result, get_cache_stats
from .connection_pool import PooledConnection, get_pool
from .error_handler import with_databricks_retry, get_error_handler
from .job_manager import get_job_manager
from .logger import log_mcp_event
from .performance_monitor import get_performance_stats
from .workspaces import get_workspaces, resolve_workspace_name

from .catalog_manager import register_catalog_tools
from .cluster_manager import register_cluster_tools
from .workspace_manager import register_workspace_tools
from .pipeline_manager import register_pipeline_tools
from .query_history_manager import register_query_history_tools


# Create FastMCP server instance optimized for AI solutions
mcp: FastMCP = FastMCP("bricks-and-context")


# Core functions (testable without MCP decorators)
def _is_read_only_sql(sql: str) -> bool:
    """
    Best-effort check for read-only SQL.

    We intentionally keep this conservative: if we can't confidently detect read-only,
    we treat it as write-capable and do not apply retries/caching by default.
    """
    s = sql.strip().lstrip("(").strip().lower()
    return s.startswith(("select", "show", "describe", "explain", "with"))


def _normalize_sql_for_cache(sql: str) -> str:
    """Normalize SQL for hashing (do not attempt to parse SQL)."""
    # Collapse whitespace to reduce cache misses for equivalent queries
    return " ".join(sql.strip().split())


def _escape_markdown_cell(value: str) -> str:
    """Escape characters that break markdown tables."""
    # Pipes break columns; newlines break rows; carriage returns can be messy.
    return value.replace("|", "\\|").replace("\r", " ").replace("\n", "\\n")


def _format_sql_error(exc: Exception, sql: str, workspace: str) -> str:
    """Turn a raw exception into an actionable error message for the AI."""
    msg = str(exc).lower()
    hint = ""
    if "connectionerror" in type(exc).__name__.lower() or "connection" in msg:
        hint = (
            "\n\nThe SQL Warehouse may be stopped or unreachable. "
            "Use `list_warehouses` to check its state."
        )
    elif "unauthorized" in msg or "401" in msg or "token" in msg:
        hint = (
            "\n\nAuthentication failed — the Databricks token for workspace "
            f"'{workspace}' may be expired. Regenerate it and update auth.yaml."
        )
    elif "timeout" in msg:
        hint = (
            "\n\nThe query timed out. Try adding a LIMIT clause or "
            "simplifying the query."
        )
    return (
        f"Error executing query on workspace '{workspace}': {exc}{hint}\n\nQuery: {sql}"
    )


def _execute_sql_query(sql: str, workspace: Optional[str] = None) -> str:
    """Core SQL execution logic with safety limits and proper cleanup."""
    sql_stripped = sql.strip()
    if not sql_stripped:
        return "Error executing query: SQL query is empty."

    workspace_name = resolve_workspace_name(workspace)

    allow_write = get_setting_bool("ALLOW_WRITE_QUERIES", "allow_write_queries", False)
    if not allow_write and not _is_read_only_sql(sql_stripped):
        return (
            "Error executing query: only read-only queries are allowed by default.\n\n"
            "Set ALLOW_WRITE_QUERIES=true to enable write queries.\n\n"
            f"Query: {sql}"
        )

    max_rows = get_setting_int("MAX_RESULT_ROWS", "max_result_rows", 200)
    max_rows = min(max(max_rows, 1), 5000)

    max_bytes = get_setting_int("MAX_RESULT_BYTES", "max_result_bytes", 256 * 1024)
    max_bytes = min(max(max_bytes, 32 * 1024), 5 * 1024 * 1024)

    max_cell_chars = get_setting_int("MAX_CELL_CHARS", "max_cell_chars", 200)
    max_cell_chars = min(max(max_cell_chars, 20), 2000)

    enable_query_cache = get_setting_bool(
        "ENABLE_QUERY_CACHE", "enable_query_cache", False
    )
    cache_ttl = get_setting_int(
        "QUERY_CACHE_TTL_SECONDS", "query_cache_ttl_seconds", 300
    )
    cache_ttl = min(max(cache_ttl, 30), 3600)
    enable_sql_retries = get_setting_bool(
        "ENABLE_SQL_RETRIES", "enable_sql_retries", True
    )

    normalized = _normalize_sql_for_cache(sql_stripped)
    sql_hash = hashlib.sha256(normalized.encode("utf-8")).hexdigest()
    cache_key = f"{workspace_name}:{sql_hash}"

    if enable_query_cache and _is_read_only_sql(sql_stripped):
        cached = get_cached_query_result(cache_key)
        if cached is not None:
            return f"✅ Cached result (workspace: {workspace_name}, TTL: {cache_ttl}s)\n\n{cached}"

    # Only retry read-only queries (safe-ish). Writes should fail fast by default.
    exec_fn = _execute_sql_query_once
    if enable_sql_retries and _is_read_only_sql(sql_stripped):
        exec_fn = with_databricks_retry("execute_sql_query")(_execute_sql_query_once)

    try:
        result = exec_fn(
            sql=sql_stripped,
            workspace=workspace_name,
            max_rows=max_rows,
            max_bytes=max_bytes,
            max_cell_chars=max_cell_chars,
        )

        if (
            enable_query_cache
            and _is_read_only_sql(sql_stripped)
            and result.startswith("Query Results")
        ):
            # Cache the rendered result to keep client consumption cheap and stable.
            cache_query_result(cache_key, result, ttl_seconds=cache_ttl)

        return result
    except Exception as e:
        return _format_sql_error(e, sql, workspace_name)


def _execute_sql_query_once(
    *, sql: str, workspace: str, max_rows: int, max_bytes: int, max_cell_chars: int
) -> str:
    """Execute SQL once (no retries here), streaming results with hard limits."""
    pool = get_pool(workspace)
    with PooledConnection(pool) as conn:
        cursor = None
        try:
            cursor = conn.cursor()
            cursor.execute(sql)

            columns = (
                [desc[0] for desc in cursor.description] if cursor.description else []
            )
            if not columns:
                # For commands that don't return rows.
                return (
                    "Query executed successfully but no column information available."
                )

            header = (
                "| " + " | ".join(_escape_markdown_cell(str(c)) for c in columns) + " |"
            )
            separator = "| " + " | ".join(["---"] * len(columns)) + " |"

            table_rows: list[str] = []
            total_rows_seen = 0
            truncated = False
            bytes_used = len(header) + len(separator) + 64

            # Pull in chunks to avoid loading huge results into memory.
            fetch_size = min(200, max_rows)
            while True:
                if total_rows_seen >= max_rows:
                    truncated = True
                    break

                remaining = max_rows - total_rows_seen
                chunk = cursor.fetchmany(min(fetch_size, remaining))
                if not chunk:
                    break

                for row in chunk:
                    cells = []
                    for cell in row:
                        cell_str = "NULL" if cell is None else str(cell)
                        cell_str = _escape_markdown_cell(cell_str)
                        if len(cell_str) > max_cell_chars:
                            cell_str = cell_str[: max_cell_chars - 1] + "…"
                        cells.append(cell_str)

                    line = "| " + " | ".join(cells) + " |"
                    bytes_used += len(line) + 1
                    if bytes_used > max_bytes:
                        truncated = True
                        break

                    table_rows.append(line)
                    total_rows_seen += 1

                if truncated:
                    break

            if total_rows_seen == 0:
                return f"Query executed successfully. No rows returned.\nColumns: {', '.join(columns)}"

            meta = f"Query Results ({total_rows_seen} rows"
            if truncated:
                meta += ", truncated"
            meta += "):\n\n"

            result = meta + f"{header}\n{separator}\n" + "\n".join(table_rows)
            if truncated:
                result += (
                    "\n\n"
                    f"⚠️ Results truncated for safety (MAX_RESULT_ROWS={max_rows}, MAX_RESULT_BYTES={max_bytes}). "
                    "Add a LIMIT clause or tighten your WHERE filters."
                )

            return result
        finally:
            # Ensure cursor is closed even if formatting or fetch fails.
            try:
                if cursor is not None:
                    cursor.close()
            except Exception:
                pass


@mcp.tool()
def execute_sql_query(sql: str, workspace: Optional[str] = None) -> str:
    """
    Execute a SQL query against Databricks and return results in markdown table format.

    Perfect for AI analysis - returns structured data that AI can easily parse and reason about.

    Args:
        sql: The SQL query to execute (SELECT statements recommended)

    Returns:
        Formatted markdown table with query results, or error message if query fails

    Example:
        execute_sql_query("SELECT * FROM sales_data LIMIT 10")

    Security: Only SELECT queries are recommended for AI safety
    """
    return _execute_sql_query(sql, workspace)


def _discover_schemas(workspace: Optional[str] = None) -> str:
    """Core schema discovery logic"""
    try:
        sql = "SHOW SCHEMAS"
        workspace_name = resolve_workspace_name(workspace)
        pool = get_pool(workspace_name)
        with PooledConnection(pool) as conn:
            cursor = conn.cursor()
            cursor.execute(sql)

            schemas = cursor.fetchall()

            if not schemas:
                return "No schemas found in the workspace."

            # Format as markdown table
            header = "| Schema Name | Description |"
            separator = "| --- | --- |"

            table_rows = []
            for schema in schemas:
                schema_name = schema[0] if schema else "Unknown"
                # Try to get schema comment/description if available
                description = (
                    "User database"
                    if schema_name not in ["default", "information_schema"]
                    else "System database"
                )
                table_rows.append(f"| {schema_name} | {description} |")

            result = (
                f"Available Schemas ({len(schemas)} found):\n\n{header}\n{separator}\n"
                + "\n".join(table_rows)
            )
            return result

    except Exception as e:
        return f"Error discovering schemas: {str(e)}"


@mcp.tool()
def discover_schemas(workspace: Optional[str] = None) -> str:
    """
    Discover all available schemas (databases) in the Databricks workspace.

    Essential for AI to understand the data landscape before querying.

    Returns:
        Markdown table listing all schemas with descriptions
    """
    return _discover_schemas(workspace)


def _discover_tables(
    schema_name: str = "default", workspace: Optional[str] = None
) -> str:
    """Core table discovery logic"""
    try:
        sql = f"SHOW TABLES IN {schema_name}"
        workspace_name = resolve_workspace_name(workspace)
        pool = get_pool(workspace_name)
        with PooledConnection(pool) as conn:
            cursor = conn.cursor()
            cursor.execute(sql)

            tables = cursor.fetchall()

            if not tables:
                return f"No tables found in schema '{schema_name}'."

            # Format as markdown table
            header = "| Table Name | Type | Description |"
            separator = "| --- | --- | --- |"

            table_rows = []
            for table in tables:
                table_name = table[1] if len(table) > 1 else table[0]
                table_type = "TABLE" if len(table) <= 2 else table[2]
                description = f"Table in {schema_name} schema"
                table_rows.append(f"| {table_name} | {table_type} | {description} |")

            result = (
                f"Tables in '{schema_name}' ({len(tables)} found):\n\n{header}\n{separator}\n"
                + "\n".join(table_rows)
            )
            return result

    except Exception as e:
        return f"Error discovering tables in schema '{schema_name}': {str(e)}"


@mcp.tool()
def discover_tables(
    schema_name: str = "default", workspace: Optional[str] = None
) -> str:
    """
    Discover all tables in a specific schema with metadata.

    Helps AI understand available data sources for analysis and querying.

    Args:
        schema_name: Name of the schema to explore (default: "default")

    Returns:
        Markdown table listing all tables with metadata
    """
    return _discover_tables(schema_name, workspace)


def _describe_table(
    table_name: str, schema_name: str = "default", workspace: Optional[str] = None
) -> str:
    """Core table description logic"""
    try:
        # Use DESCRIBE EXTENDED for comprehensive information
        sql = f"DESCRIBE EXTENDED {schema_name}.{table_name}"
        workspace_name = resolve_workspace_name(workspace)
        pool = get_pool(workspace_name)
        with PooledConnection(pool) as conn:
            cursor = conn.cursor()
            cursor.execute(sql)

            columns_info = cursor.fetchall()

            if not columns_info:
                return f"No information found for table '{schema_name}.{table_name}'."

            # Parse the description output
            header = "| Column Name | Data Type | Nullable | Description |"
            separator = "| --- | --- | --- | --- |"

            table_rows = []
            for col_info in columns_info:
                if (
                    len(col_info) >= 3
                    and col_info[0]
                    and not col_info[0].startswith("#")
                ):
                    col_name = col_info[0].strip()
                    data_type = col_info[1].strip() if col_info[1] else "unknown"
                    nullable = (
                        "Yes"
                        if col_info[2]
                        and col_info[2].strip().lower() in ["true", "yes", ""]
                        else "No"
                    )
                    description = "Data column"

                    table_rows.append(
                        f"| {col_name} | {data_type} | {nullable} | {description} |"
                    )

            if not table_rows:
                return f"Could not parse column information for table '{schema_name}.{table_name}'."

            result = (
                f"Schema for '{schema_name}.{table_name}':\n\n{header}\n{separator}\n"
                + "\n".join(table_rows)
            )
            return result

    except Exception as e:
        return f"Error describing table '{schema_name}.{table_name}': {str(e)}"


@mcp.tool()
def describe_table(
    table_name: str, schema_name: str = "default", workspace: Optional[str] = None
) -> str:
    """
    Get detailed schema information for a specific table.

    Critical for AI to understand data types, constraints, and structure before querying.

    Args:
        table_name: Name of the table to describe
        schema_name: Schema containing the table (default: "default")

    Returns:
        Markdown table with column details (name, type, nullable, etc.)
    """
    return _describe_table(table_name, schema_name, workspace)


def _get_table_sample(
    table_name: str,
    schema_name: str = "default",
    limit: int = 5,
    workspace: Optional[str] = None,
) -> str:
    """Core table sampling logic"""
    try:
        # Limit for performance and AI context efficiency
        safe_limit = min(max(1, limit), 20)
        sql = f"SELECT * FROM {schema_name}.{table_name} LIMIT {safe_limit}"

        # Reuse the _execute_sql_query logic
        result = _execute_sql_query(sql, workspace)

        # Add context about the sampling
        if result.startswith("Query Results"):
            result = (
                f"Sample Data from '{schema_name}.{table_name}' (showing {safe_limit} rows):\n\n"
                + result.split(":\n\n", 1)[1]
            )

        return result

    except Exception as e:
        return f"Error sampling table '{schema_name}.{table_name}': {str(e)}"


@mcp.tool()
def get_table_sample(
    table_name: str,
    schema_name: str = "default",
    limit: int = 5,
    workspace: Optional[str] = None,
) -> str:
    """
    Get a small sample of data from a table for AI analysis.

    Perfect for AI to understand data patterns, formats, and content before complex analysis.

    Args:
        table_name: Name of the table to sample
        schema_name: Schema containing the table (default: "default")
        limit: Number of rows to return (default: 5, max: 20 for performance)

    Returns:
        Markdown table with sample data
    """
    return _get_table_sample(table_name, schema_name, limit, workspace)


def _connection_health(workspace: Optional[str] = None) -> str:
    """Core connection health logic"""
    try:
        workspace_name = resolve_workspace_name(workspace)
        pool = get_pool(workspace_name)

        # Test connection with simple query
        test_result = _execute_sql_query("SELECT 1 as health_check", workspace_name)

        if "Error" in test_result:
            return (
                f"⚠️ Connection Health: UNHEALTHY\n\nTest query failed:\n{test_result}"
            )

        return "✅ Connection Health: HEALTHY\n\nDatabricks connection pool is working correctly.\nTest query executed successfully."

    except Exception as e:
        return f"❌ Connection Health: ERROR\n\nFailed to check connection: {str(e)}"


@mcp.tool()
def connection_health(workspace: Optional[str] = None) -> str:
    """
    Check the health of the Databricks connection pool.

    Useful for AI to understand system status and troubleshoot connectivity issues.

    Returns:
        Status information about connection pool and Databricks connectivity
    """
    return _connection_health(workspace)


# ===== JOB MANAGEMENT TOOLS =====


def _list_jobs(
    limit: int = 25, name_filter: Optional[str] = None, workspace: Optional[str] = None
) -> str:
    """Core job listing logic"""
    try:
        log_mcp_event(
            "list_jobs",
            "START",
            f"Listing jobs (limit: {limit}, filter: {name_filter})",
        )

        workspace_name = resolve_workspace_name(workspace)
        job_manager = get_job_manager(workspace_name)
        jobs = job_manager.list_jobs(limit=limit, name_filter=name_filter)

        if not jobs:
            return "No jobs found in the Databricks workspace."

        # Format as markdown table for AI consumption
        header = "| Job ID | Job Name | Type | Creator | Status | Last Run |"
        separator = "| --- | --- | --- | --- | --- | --- |"

        table_rows = []
        for job in jobs:
            last_run = job.last_run_state if job.last_run_state else "None"
            created_time = (
                job_manager._format_timestamp(job.created_time)
                if hasattr(job_manager, "_format_timestamp")
                else "Unknown"
            )

            table_rows.append(
                f"| {job.job_id} | {job.name} | {job.job_type} | {job.creator_email} | {job.status} | {last_run} |"
            )

        result = (
            f"Databricks Jobs ({len(jobs)} found):\n\n{header}\n{separator}\n"
            + "\n".join(table_rows)
        )

        log_mcp_event("list_jobs", "SUCCESS", f"Retrieved {len(jobs)} jobs")
        return result

    except Exception as e:
        error_msg = f"Error listing jobs: {str(e)}"
        log_mcp_event("list_jobs", "ERROR", error_msg, "ERROR")
        return error_msg


@mcp.tool()
def list_jobs(
    limit: int = 25, name_filter: Optional[str] = None, workspace: Optional[str] = None
) -> str:
    """
    List Databricks jobs with optional filtering.

    Essential for AI to discover available jobs for monitoring and management.

    Args:
        limit: Maximum number of jobs to return (default: 25, max: 100)
        name_filter: Optional filter for job names (case-insensitive partial match)

    Returns:
        Markdown table with job information (ID, name, type, creator, status, last run)

    Example:
        list_jobs(limit=10, name_filter="data_pipeline")
    """
    return _list_jobs(limit, name_filter, workspace)


def _get_job_details(job_id: int, workspace: Optional[str] = None) -> str:
    """Core job details logic"""
    try:
        log_mcp_event("get_job_details", "START", f"Getting details for job {job_id}")

        workspace_name = resolve_workspace_name(workspace)
        job_manager = get_job_manager(workspace_name)
        details = job_manager.get_job_details(job_id)

        # Format for AI consumption
        result = f"Job Details for ID {job_id}:\n\n"
        result += f"**Name**: {details['name']}\n"
        result += f"**Type**: {details['job_type']}\n"
        result += f"**Creator**: {details['creator']}\n"
        result += f"**Created**: {details['created_time']}\n"
        result += f"**Timeout**: {details.get('timeout_seconds', 'No limit')} seconds\n"
        result += f"**Max Concurrent Runs**: {details['max_concurrent_runs']}\n\n"

        if details["schedule"]:
            schedule = details["schedule"]
            result += f"**Schedule**:\n"
            result += f"- Cron: {schedule.get('quartz_cron_expression', 'None')}\n"
            result += f"- Timezone: {schedule.get('timezone_id', 'UTC')}\n"
            result += f"- Status: {schedule.get('pause_status', 'UNPAUSED')}\n\n"

        cluster_config = details["cluster_config"]
        result += f"**Cluster Configuration**:\n"
        result += f"- Type: {cluster_config.get('type')}\n"
        if cluster_config.get("type") == "existing":
            result += f"- Cluster ID: {cluster_config.get('cluster_id')}\n"
        elif cluster_config.get("type") == "new":
            result += f"- Spark Version: {cluster_config.get('spark_version')}\n"
            result += f"- Node Type: {cluster_config.get('node_type_id')}\n"
            result += f"- Workers: {cluster_config.get('num_workers')}\n"
        elif cluster_config.get("type") == "multi_task":
            job_clusters = cluster_config.get("job_clusters", []) or []
            result += f"- Job Clusters: {len(job_clusters)}\n"

        tasks = details.get("tasks") or []
        if tasks:
            result += f"\n**Tasks** ({len(tasks)}):\n"
            result += "| Task Key | Type | Cluster | Details |\n"
            result += "| --- | --- | --- | --- |\n"
            for t in tasks:
                details_obj = t.get("details", {}) or {}
                cluster_obj = details_obj.get("cluster", {}) or {}
                cluster_str = cluster_obj.get("type", "unknown")
                if cluster_obj.get("type") == "existing":
                    cluster_str += f":{cluster_obj.get('cluster_id')}"
                elif cluster_obj.get("type") == "job_cluster":
                    cluster_str += f":{cluster_obj.get('key')}"
                # Keep details short; this is a human-readable overview.
                extra = ""
                if t.get("type") == "NOTEBOOK":
                    extra = str(details_obj.get("notebook_path") or "")
                elif t.get("type") == "PYTHON":
                    extra = str(details_obj.get("python_file") or "")
                elif t.get("type") == "SQL":
                    extra = str(details_obj.get("warehouse_id") or "")
                result += f"| {t.get('task_key')} | {t.get('type')} | {cluster_str} | {extra} |\n"
        else:
            task_config = details["task_config"]
            result += f"\n**Task Configuration**:\n"
            result += f"- Type: {task_config['type']}\n"
            if task_config["type"] == "NOTEBOOK":
                result += f"- Notebook Path: {task_config.get('notebook_path')}\n"
                if task_config.get("base_parameters"):
                    result += f"- Parameters: {task_config['base_parameters']}\n"
            elif task_config["type"] == "PYTHON":
                result += f"- Python File: {task_config.get('python_file')}\n"
                if task_config.get("parameters"):
                    result += f"- Parameters: {task_config['parameters']}\n"
            elif task_config["type"] == "SQL":
                result += f"- Warehouse ID: {task_config.get('warehouse_id')}\n"

        log_mcp_event(
            "get_job_details", "SUCCESS", f"Retrieved details for job {job_id}"
        )
        return result

    except Exception as e:
        error_msg = f"Error getting job {job_id} details: {str(e)}"
        log_mcp_event("get_job_details", "ERROR", error_msg, "ERROR")
        return error_msg


@mcp.tool()
def get_job_details(job_id: int, workspace: Optional[str] = None) -> str:
    """
    Get detailed information about a specific Databricks job.

    Critical for AI to understand job configuration, schedule, and dependencies.

    Args:
        job_id: Databricks job ID

    Returns:
        Detailed job information including schedule, cluster config, and task settings

    Example:
        get_job_details(123)
    """
    return _get_job_details(job_id, workspace)


def _get_job_runs(
    job_id: int,
    limit: int = 10,
    active_only: bool = False,
    workspace: Optional[str] = None,
) -> str:
    """Core job runs logic"""
    try:
        log_mcp_event(
            "get_job_runs",
            "START",
            f"Getting runs for job {job_id} (limit: {limit}, active_only: {active_only})",
        )

        workspace_name = resolve_workspace_name(workspace)
        job_manager = get_job_manager(workspace_name)
        runs = job_manager.get_job_runs(job_id, limit=limit, active_only=active_only)

        if not runs:
            status = "active runs" if active_only else "runs"
            return f"No {status} found for job {job_id}."

        # Format as markdown table
        header = "| Run ID | Run Name | State | Result | Start Time | Duration (ms) | Trigger |"
        separator = "| --- | --- | --- | --- | --- | --- | --- |"

        table_rows = []
        for run in runs:
            start_time = (
                job_manager._format_timestamp(run.start_time)
                if hasattr(job_manager, "_format_timestamp")
                else "Unknown"
            )
            duration = str(run.execution_duration) if run.execution_duration else "N/A"
            result_state = run.result_state if run.result_state else "N/A"

            table_rows.append(
                f"| {run.run_id} | {run.run_name} | {run.state} | {result_state} | {start_time} | {duration} | {run.trigger} |"
            )

        status = "Active Runs" if active_only else "Recent Runs"
        result = (
            f"{status} for Job {job_id} ({len(runs)} found):\n\n{header}\n{separator}\n"
            + "\n".join(table_rows)
        )

        log_mcp_event(
            "get_job_runs", "SUCCESS", f"Retrieved {len(runs)} runs for job {job_id}"
        )
        return result

    except Exception as e:
        error_msg = f"Error getting runs for job {job_id}: {str(e)}"
        log_mcp_event("get_job_runs", "ERROR", error_msg, "ERROR")
        return error_msg


@mcp.tool()
def get_job_runs(
    job_id: int,
    limit: int = 10,
    active_only: bool = False,
    workspace: Optional[str] = None,
) -> str:
    """
    Get run history for a specific Databricks job.

    Essential for AI to monitor job performance, track failures, and analyze execution patterns.

    Args:
        job_id: Databricks job ID
        limit: Maximum number of runs to return (default: 10, max: 100)
        active_only: If True, only return currently running jobs

    Returns:
        Markdown table with run information (ID, state, result, duration, trigger)

    Example:
        get_job_runs(123, limit=5, active_only=True)
    """
    return _get_job_runs(job_id, limit, active_only, workspace)


def _trigger_job(
    job_id: int,
    notebook_params: Optional[str] = None,
    jar_params: Optional[str] = None,
    python_params: Optional[str] = None,
    job_parameters: Optional[str] = None,
    workspace: Optional[str] = None,
) -> str:
    """Core job triggering logic"""
    try:
        log_mcp_event("trigger_job", "START", f"Triggering job {job_id}")

        workspace_name = resolve_workspace_name(workspace)
        job_manager = get_job_manager(workspace_name)

        # Parse parameters if provided (expect JSON strings)
        parsed_job_parameters = None
        parsed_notebook_params = None
        parsed_jar_params = None
        parsed_python_params = None

        if job_parameters:
            try:
                parsed_job_parameters = json.loads(job_parameters)
                if not isinstance(parsed_job_parameters, dict):
                    return f"Error: job_parameters must be a JSON object string. Got: {job_parameters}"
            except json.JSONDecodeError:
                return f"Error: job_parameters must be valid JSON string. Got: {job_parameters}"

        if notebook_params:
            try:
                parsed_notebook_params = json.loads(notebook_params)
            except json.JSONDecodeError:
                return f"Error: notebook_params must be valid JSON string. Got: {notebook_params}"

        if jar_params:
            try:
                parsed_jar_params = json.loads(jar_params)
            except json.JSONDecodeError:
                return f"Error: jar_params must be valid JSON array string. Got: {jar_params}"

        if python_params:
            try:
                parsed_python_params = json.loads(python_params)
            except json.JSONDecodeError:
                return f"Error: python_params must be valid JSON array string. Got: {python_params}"

        run_id = job_manager.trigger_job(
            job_id=job_id,
            job_parameters=parsed_job_parameters,
            notebook_params=parsed_notebook_params,
            jar_params=parsed_jar_params,
            python_params=parsed_python_params,
        )

        result = f"✅ Job {job_id} triggered successfully!\n\n"
        result += f"**Run ID**: {run_id}\n"
        result += f"**Status**: Job execution started\n"
        result += f"**Monitor**: Use `get_job_runs({job_id}, active_only=True)` to check status\n"

        log_mcp_event(
            "trigger_job", "SUCCESS", f"Triggered job {job_id}, run ID: {run_id}"
        )
        return result

    except Exception as e:
        error_msg = f"Error triggering job {job_id}: {str(e)}"
        log_mcp_event("trigger_job", "ERROR", error_msg, "ERROR")
        return error_msg


@mcp.tool()
def trigger_job(
    job_id: int,
    notebook_params: Optional[str] = None,
    jar_params: Optional[str] = None,
    python_params: Optional[str] = None,
    job_parameters: Optional[str] = None,
    workspace: Optional[str] = None,
) -> str:
    """
    Trigger a Databricks job run with optional parameters.

    Powerful automation capability for AI to start data processing workflows.

    Args:
        job_id: Databricks job ID to trigger
        notebook_params: JSON string of parameters for notebook tasks (e.g., '{"param1": "value1"}')
        jar_params: JSON array string of parameters for JAR tasks (e.g., '["arg1", "arg2"]')
        python_params: JSON array string of parameters for Python tasks (e.g., '["arg1", "arg2"]')

    Returns:
        Success message with run ID, or error details

    Example:
        trigger_job(123, notebook_params='{"date": "2024-01-01", "env": "prod"}')

    Security: Use with caution - this will start actual job execution
    """
    return _trigger_job(
        job_id, notebook_params, jar_params, python_params, job_parameters, workspace
    )


def _cancel_job_run(run_id: int, workspace: Optional[str] = None) -> str:
    """Core job cancellation logic"""
    try:
        log_mcp_event("cancel_job_run", "START", f"Cancelling job run {run_id}")

        workspace_name = resolve_workspace_name(workspace)
        job_manager = get_job_manager(workspace_name)
        success = job_manager.cancel_job_run(run_id)

        if success:
            result = f"✅ Job run {run_id} cancelled successfully!\n\n"
            result += f"**Status**: Cancellation request sent\n"
            result += f"**Note**: Job may take a few moments to fully stop\n"

            log_mcp_event("cancel_job_run", "SUCCESS", f"Cancelled job run {run_id}")
            return result
        else:
            error_msg = f"Failed to cancel job run {run_id}"
            log_mcp_event("cancel_job_run", "ERROR", error_msg, "ERROR")
            return error_msg

    except Exception as e:
        error_msg = f"Error cancelling job run {run_id}: {str(e)}"
        log_mcp_event("cancel_job_run", "ERROR", error_msg, "ERROR")
        return error_msg


@mcp.tool()
def cancel_job_run(run_id: int, workspace: Optional[str] = None) -> str:
    """
    Cancel a running Databricks job.

    Critical capability for AI to stop problematic or long-running jobs.

    Args:
        run_id: Job run ID to cancel

    Returns:
        Success message or error details

    Example:
        cancel_job_run(456789)

    Security: Use carefully - this will stop actual job execution
    """
    return _cancel_job_run(run_id, workspace)


def _get_job_run_output(run_id: int, workspace: Optional[str] = None) -> str:
    """Core job output logic"""
    try:
        log_mcp_event(
            "get_job_run_output", "START", f"Getting output for job run {run_id}"
        )

        workspace_name = resolve_workspace_name(workspace)
        job_manager = get_job_manager(workspace_name)
        output = job_manager.get_job_run_output(run_id)

        result = f"Job Run Output for Run ID {run_id}:\n\n"

        if output.get("error"):
            result += f"**❌ Error**: {output['error']}\n\n"

        if output.get("error_trace"):
            result += f"**Error Trace**:\n```\n{output['error_trace']}\n```\n\n"

        if output.get("logs"):
            logs = output["logs"]
            if output.get("logs_truncated"):
                result += f"**📝 Logs** (truncated):\n```\n{logs}\n```\n\n"
            else:
                result += f"**📝 Logs**:\n```\n{logs}\n```\n\n"

        if output.get("notebook_output"):
            notebook_output = output["notebook_output"]
            result += f"**📓 Notebook Output**:\n```json\n{json.dumps(notebook_output, indent=2)}\n```\n\n"

        if output.get("metadata"):
            metadata = output["metadata"]
            result += (
                f"**ℹ️ Metadata**:\n```json\n{json.dumps(metadata, indent=2)}\n```\n\n"
            )

        if not any(
            [output.get("logs"), output.get("error"), output.get("notebook_output")]
        ):
            result += "No output available for this job run.\n"

        log_mcp_event(
            "get_job_run_output", "SUCCESS", f"Retrieved output for job run {run_id}"
        )
        return result

    except Exception as e:
        error_msg = f"Error getting output for job run {run_id}: {str(e)}"
        log_mcp_event("get_job_run_output", "ERROR", error_msg, "ERROR")
        return error_msg


@mcp.tool()
def get_job_run_output(run_id: int, workspace: Optional[str] = None) -> str:
    """
    Get output, logs, and results from a job run.

    Essential for AI to analyze job results, debug failures, and extract insights.

    Args:
        run_id: Job run ID to get output for

    Returns:
        Formatted output including logs, errors, notebook output, and metadata

    Example:
        get_job_run_output(456789)
    """
    return _get_job_run_output(run_id, workspace)


# ===== PERFORMANCE MONITORING TOOLS =====


def _cache_stats() -> str:
    """Core cache statistics logic"""
    try:
        log_mcp_event("cache_stats", "START", "Getting cache performance statistics")

        stats = get_cache_stats()

        # Format as markdown table
        lines = [
            "# 🚀 Cache Performance Statistics",
            "",
            "## Overview",
            f"- **Total Entries**: {stats['total_entries']:,}",
            f"- **Max Capacity**: {stats['max_entries']:,}",
            f"- **Hit Rate**: {stats['hit_rate_percent']:.2f}%",
            f"- **Cache Hits**: {stats['hits']:,}",
            f"- **Cache Misses**: {stats['misses']:,}",
            f"- **Expired Entries**: {stats['expired_entries']:,}",
            f"- **Memory Usage**: {stats['memory_usage_estimate']}",
            "",
        ]

        # Category breakdown
        if stats["categories"]:
            lines.extend(
                [
                    "## Cache Categories",
                    "",
                    "| Category | Count | Description |",
                    "|----------|-------|-------------|",
                ]
            )

            category_descriptions = {
                "health": "Connection health status (5 min TTL)",
                "schema": "Database schema information (30 min TTL)",
                "table": "Table metadata and structure (15 min TTL)",
                "query": "Query results (5 min TTL)",
                "job": "Job information (2 min TTL)",
                "connection": "Connection status (1 min TTL)",
            }

            for category, count in stats["categories"].items():
                description = category_descriptions.get(category, "Unknown category")
                lines.append(f"| {category} | {count:,} | {description} |")

            lines.append("")

        # Performance insights
        total_requests = stats["hits"] + stats["misses"]
        if total_requests > 0:
            lines.extend(
                [
                    "## Performance Insights",
                    "",
                    f"- **Cache Efficiency**: {'🟢 Excellent' if stats['hit_rate_percent'] > 80 else '🟡 Good' if stats['hit_rate_percent'] > 60 else '🔴 Needs Optimization'}",
                    f"- **Memory Efficiency**: {'🟢 Optimal' if stats['total_entries'] < stats['max_entries'] * 0.8 else '🟡 High Usage'}",
                    f"- **Total Requests**: {total_requests:,}",
                    "",
                ]
            )

            if stats["hit_rate_percent"] < 60:
                lines.extend(
                    [
                        "### ⚠️ Optimization Recommendations",
                        "- Consider increasing cache TTL values",
                        "- Review caching strategy for frequently accessed data",
                        "- Monitor for cache invalidation patterns",
                        "",
                    ]
                )

        log_mcp_event(
            "cache_stats",
            "SUCCESS",
            f"Cache stats retrieved - Hit rate: {stats['hit_rate_percent']:.2f}%",
        )
        return "\n".join(lines)

    except Exception as e:
        error_msg = f"Failed to get cache statistics: {str(e)}"
        log_mcp_event("cache_stats", "ERROR", error_msg, "ERROR")
        return f"**Error**: {error_msg}"


@mcp.tool()
def cache_stats(random_string: str = "dummy") -> str:
    """
    Get cache performance statistics and optimization insights.

    Essential for monitoring cache hit rates and system efficiency.

    Args:
        random_string: Dummy parameter for no-parameter tools

    Returns:
        Detailed cache statistics including hit rates, memory usage, and category breakdown
    """
    return _cache_stats()


def _performance_stats(workspace: Optional[str] = None) -> str:
    """Core performance statistics logic"""
    try:
        log_mcp_event(
            "performance_stats", "START", "Getting system performance statistics"
        )

        # Get performance stats
        perf_stats = get_performance_stats()

        pool_stats_by_workspace = {}
        if workspace is not None:
            ws = resolve_workspace_name(workspace)
            pool_stats_by_workspace[ws] = get_pool(ws).get_pool_stats()
        else:
            # If multiple workspaces are configured, show all.
            for ws in get_workspaces().keys():
                pool_stats_by_workspace[ws] = get_pool(ws).get_pool_stats()

        # Get cache stats for performance context
        cache_stats = get_cache_stats()

        # Get error handler stats
        error_handler = get_error_handler()
        error_stats = error_handler.get_stats()

        lines = [
            "# 📊 System Performance Dashboard",
            "",
            "## System Overview",
            f"- **Uptime**: {perf_stats['uptime_seconds']:.1f} seconds",
            f"- **Total Operations**: {perf_stats['total_operations']:,}",
            f"- **Operations/Second**: {perf_stats['operations_per_second']:.2f}",
            "",
        ]

        # Connection Pool Performance
        lines.extend(["## 🔌 Connection Pool Status", ""])
        if len(pool_stats_by_workspace) == 1:
            (ws, pool_stats) = next(iter(pool_stats_by_workspace.items()))
            lines.extend(
                [
                    f"- **Workspace**: {ws}",
                    f"- **Pool Utilization**: {pool_stats['pool_utilization_percent']:.1f}%",
                    f"- **Active Connections**: {pool_stats['active_connections']}/{pool_stats['max_connections']}",
                    f"- **Available Connections**: {pool_stats['available_connections']}",
                    f"- **Health Check Cache**: {pool_stats['health_check_interval_seconds']}s TTL",
                    "",
                ]
            )
        else:
            lines.extend(
                [
                    "| Workspace | Utilization | Active | Available | Health TTL (s) |",
                    "|----------|------------:|-------:|----------:|---------------:|",
                ]
            )
            for ws, pool_stats in sorted(pool_stats_by_workspace.items()):
                lines.append(
                    f"| {ws} | {pool_stats['pool_utilization_percent']:.1f}% | "
                    f"{pool_stats['active_connections']}/{pool_stats['max_connections']} | "
                    f"{pool_stats['available_connections']} | {pool_stats['health_check_interval_seconds']} |"
                )
            lines.append("")

        # Cache Performance Summary
        cache_hit_rate = cache_stats.get("hit_rate_percent", 0)
        lines.extend(
            [
                "## 🚀 Cache Performance",
                f"- **Hit Rate**: {cache_hit_rate:.2f}%",
                f"- **Cached Entries**: {cache_stats.get('total_entries', 0):,}",
                f"- **Memory Usage**: {cache_stats.get('memory_usage_estimate', 'Unknown')}",
                "",
            ]
        )

        # Operation Performance
        if "operation_stats" in perf_stats and perf_stats["operation_stats"]:
            lines.extend(
                [
                    "## ⚡ Operation Performance",
                    "",
                    "| Operation | Calls | Success Rate | Avg Duration (ms) |",
                    "|-----------|-------|--------------|-------------------|",
                ]
            )

            for op, stats in perf_stats["operation_stats"].items():
                success_rate = (
                    (stats["successful_calls"] / stats["total_calls"] * 100)
                    if stats["total_calls"] > 0
                    else 0
                )
                avg_duration = (
                    stats["total_duration_ms"] / stats["total_calls"]
                    if stats["total_calls"] > 0
                    else 0
                )

                lines.append(
                    f"| {op} | {stats['total_calls']:,} | {success_rate:.1f}% | {avg_duration:.1f} |"
                )

            lines.append("")

        # Circuit Breaker Status
        if error_stats["circuit_breakers"]:
            lines.extend(
                [
                    "## 🔄 Circuit Breaker Status",
                    "",
                    "| Operation | State | Failures | Can Execute |",
                    "|-----------|-------|----------|-------------|",
                ]
            )

            for cb_name, cb_stats in error_stats["circuit_breakers"].items():
                state_emoji = {"closed": "🟢", "open": "🔴", "half_open": "🟡"}.get(
                    cb_stats["state"], "⚪"
                )

                lines.append(
                    f"| {cb_name} | {state_emoji} {cb_stats['state']} | {cb_stats['failure_count']} | {'✅' if cb_stats['can_execute'] else '❌'} |"
                )

            lines.append("")

        # Health Assessment
        lines.extend(["## 🏥 System Health Assessment", ""])

        # Overall health scoring
        health_score = 100
        issues = []

        if cache_hit_rate < 60:
            health_score -= 20
            issues.append("Low cache hit rate")

        if pool_stats["pool_utilization_percent"] > 90:
            health_score -= 15
            issues.append("High connection pool utilization")

        # Check for any open circuit breakers
        open_circuits = [
            name
            for name, stats in error_stats["circuit_breakers"].items()
            if stats["state"] == "open"
        ]
        if open_circuits:
            health_score -= 30
            issues.append(f"Open circuit breakers: {', '.join(open_circuits)}")

        # Check operation error rates
        high_error_ops = []
        if "operation_stats" in perf_stats:
            for op, stats in perf_stats["operation_stats"].items():
                if stats["total_calls"] > 10:  # Only check ops with significant calls
                    error_rate = (
                        (stats["failed_calls"] / stats["total_calls"] * 100)
                        if stats["total_calls"] > 0
                        else 0
                    )
                    if error_rate > 10:  # More than 10% error rate
                        high_error_ops.append(f"{op} ({error_rate:.1f}%)")

        if high_error_ops:
            health_score -= 25
            issues.append(f"High error rates: {', '.join(high_error_ops)}")

        health_score = max(0, health_score)

        if health_score >= 90:
            health_status = "🟢 Excellent"
        elif health_score >= 70:
            health_status = "🟡 Good"
        elif health_score >= 50:
            health_status = "🟠 Fair"
        else:
            health_status = "🔴 Poor"

        lines.append(f"- **Overall Health**: {health_status} ({health_score}/100)")

        if issues:
            lines.extend(
                ["- **Issues Detected**:", *[f"  - {issue}" for issue in issues]]
            )
        else:
            lines.append("- **Status**: All systems operating normally")

        lines.append("")

        log_mcp_event(
            "performance_stats",
            "SUCCESS",
            f"Performance stats retrieved - Health: {health_score}/100, Operations: {perf_stats['total_operations']}",
        )
        return "\n".join(lines)

    except Exception as e:
        error_msg = f"Failed to get performance statistics: {str(e)}"
        log_mcp_event("performance_stats", "ERROR", error_msg, "ERROR")
        return f"**Error**: {error_msg}"


@mcp.tool()
def performance_stats(
    random_string: str = "dummy", workspace: Optional[str] = None
) -> str:
    """
    Get comprehensive system performance statistics and health metrics.

    Critical for monitoring system performance, identifying bottlenecks, and optimization.

    Args:
        random_string: Dummy parameter for no-parameter tools

    Returns:
        Detailed performance metrics including operation times, error rates, and system health
    """
    return _performance_stats(workspace)


# ------------------------------------------------------------------
# Register all new Databricks operations tools
# ------------------------------------------------------------------
register_catalog_tools(mcp)
register_cluster_tools(mcp)
register_workspace_tools(mcp)
register_pipeline_tools(mcp)
register_query_history_tools(mcp)


def run_server() -> None:
    """
    Run the MCP server with stdio transport (recommended for AI integrations)
    """
    mcp.run(transport="stdio")


if __name__ == "__main__":
    run_server()
