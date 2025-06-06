"""
MCP Server for Databricks Integration
Provides AI solutions with tools to interact with Databricks via MCP protocol
"""

from typing import Any, Dict, List, Optional
import asyncio
import json
from fastmcp import FastMCP
from databricks.sql.client import Connection

from .connection_pool import get_pool, PooledConnection
from .job_manager import get_job_manager
from .logger import log_mcp_event
from .cache_manager import get_cache_stats
from .performance_monitor import get_performance_stats
from .error_handler import get_error_handler


# Create FastMCP server instance optimized for AI solutions
mcp = FastMCP("bricks-and-context")


# Core functions (testable without MCP decorators)
def _execute_sql_query(sql: str) -> str:
    """Core SQL execution logic"""
    try:
        pool = get_pool()
        with PooledConnection(pool) as conn:
            cursor = conn.cursor()
            cursor.execute(sql.strip())
            
            # Get column names
            columns = [desc[0] for desc in cursor.description] if cursor.description else []
            
            # Get all results
            rows = cursor.fetchall()
            
            if not rows:
                return f"Query executed successfully. No rows returned.\nColumns: {', '.join(columns)}"
            
            # Format as markdown table for AI parsing
            if not columns:
                return "Query executed successfully but no column information available."
            
            # Create markdown table
            header = "| " + " | ".join(columns) + " |"
            separator = "| " + " | ".join(["---"] * len(columns)) + " |"
            
            table_rows = []
            for row in rows:
                # Convert each cell to string, handling None values
                cells = [str(cell) if cell is not None else "NULL" for cell in row]
                table_rows.append("| " + " | ".join(cells) + " |")
            
            result = f"Query Results ({len(rows)} rows):\n\n{header}\n{separator}\n" + "\n".join(table_rows)
            return result
            
    except Exception as e:
        return f"Error executing query: {str(e)}\n\nQuery: {sql}"


@mcp.tool()
def execute_sql_query(sql: str) -> str:
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
    return _execute_sql_query(sql)


def _discover_schemas() -> str:
    """Core schema discovery logic"""
    try:
        sql = "SHOW SCHEMAS"
        pool = get_pool()
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
                description = "User database" if schema_name not in ["default", "information_schema"] else "System database"
                table_rows.append(f"| {schema_name} | {description} |")
            
            result = f"Available Schemas ({len(schemas)} found):\n\n{header}\n{separator}\n" + "\n".join(table_rows)
            return result
            
    except Exception as e:
        return f"Error discovering schemas: {str(e)}"


@mcp.tool()
def discover_schemas() -> str:
    """
    Discover all available schemas (databases) in the Databricks workspace.
    
    Essential for AI to understand the data landscape before querying.
    
    Returns:
        Markdown table listing all schemas with descriptions
    """
    return _discover_schemas()


def _discover_tables(schema_name: str = "default") -> str:
    """Core table discovery logic"""
    try:
        sql = f"SHOW TABLES IN {schema_name}"
        pool = get_pool()
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
            
            result = f"Tables in '{schema_name}' ({len(tables)} found):\n\n{header}\n{separator}\n" + "\n".join(table_rows)
            return result
            
    except Exception as e:
        return f"Error discovering tables in schema '{schema_name}': {str(e)}"


@mcp.tool()
def discover_tables(schema_name: str = "default") -> str:
    """
    Discover all tables in a specific schema with metadata.
    
    Helps AI understand available data sources for analysis and querying.
    
    Args:
        schema_name: Name of the schema to explore (default: "default")
        
    Returns:
        Markdown table listing all tables with metadata
    """
    return _discover_tables(schema_name)


def _describe_table(table_name: str, schema_name: str = "default") -> str:
    """Core table description logic"""
    try:
        # Use DESCRIBE EXTENDED for comprehensive information
        sql = f"DESCRIBE EXTENDED {schema_name}.{table_name}"
        pool = get_pool()
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
                if len(col_info) >= 3 and col_info[0] and not col_info[0].startswith('#'):
                    col_name = col_info[0].strip()
                    data_type = col_info[1].strip() if col_info[1] else "unknown"
                    nullable = "Yes" if col_info[2] and col_info[2].strip().lower() in ['true', 'yes', ''] else "No"
                    description = "Data column"
                    
                    table_rows.append(f"| {col_name} | {data_type} | {nullable} | {description} |")
            
            if not table_rows:
                return f"Could not parse column information for table '{schema_name}.{table_name}'."
            
            result = f"Schema for '{schema_name}.{table_name}':\n\n{header}\n{separator}\n" + "\n".join(table_rows)
            return result
            
    except Exception as e:
        return f"Error describing table '{schema_name}.{table_name}': {str(e)}"


@mcp.tool()
def describe_table(table_name: str, schema_name: str = "default") -> str:
    """
    Get detailed schema information for a specific table.
    
    Critical for AI to understand data types, constraints, and structure before querying.
    
    Args:
        table_name: Name of the table to describe
        schema_name: Schema containing the table (default: "default")
        
    Returns:
        Markdown table with column details (name, type, nullable, etc.)
    """
    return _describe_table(table_name, schema_name)


def _get_table_sample(table_name: str, schema_name: str = "default", limit: int = 5) -> str:
    """Core table sampling logic"""
    try:
        # Limit for performance and AI context efficiency
        safe_limit = min(max(1, limit), 20)
        sql = f"SELECT * FROM {schema_name}.{table_name} LIMIT {safe_limit}"
        
        # Reuse the _execute_sql_query logic
        result = _execute_sql_query(sql)
        
        # Add context about the sampling
        if result.startswith("Query Results"):
            result = f"Sample Data from '{schema_name}.{table_name}' (showing {safe_limit} rows):\n\n" + result.split(":\n\n", 1)[1]
        
        return result
        
    except Exception as e:
        return f"Error sampling table '{schema_name}.{table_name}': {str(e)}"


@mcp.tool()
def get_table_sample(table_name: str, schema_name: str = "default", limit: int = 5) -> str:
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
    return _get_table_sample(table_name, schema_name, limit)


def _connection_health() -> str:
    """Core connection health logic"""
    try:
        pool = get_pool()
        
        # Test connection with simple query
        test_result = _execute_sql_query("SELECT 1 as health_check")
        
        if "Error" in test_result:
            return f"⚠️ Connection Health: UNHEALTHY\n\nTest query failed:\n{test_result}"
        
        return "✅ Connection Health: HEALTHY\n\nDatabricks connection pool is working correctly.\nTest query executed successfully."
        
    except Exception as e:
        return f"❌ Connection Health: ERROR\n\nFailed to check connection: {str(e)}"


@mcp.tool()
def connection_health() -> str:
    """
    Check the health of the Databricks connection pool.
    
    Useful for AI to understand system status and troubleshoot connectivity issues.
    
    Returns:
        Status information about connection pool and Databricks connectivity
    """
    return _connection_health()


# ===== JOB MANAGEMENT TOOLS =====

def _list_jobs(limit: int = 25, name_filter: Optional[str] = None) -> str:
    """Core job listing logic"""
    try:
        log_mcp_event("list_jobs", "START", f"Listing jobs (limit: {limit}, filter: {name_filter})")
        
        job_manager = get_job_manager()
        jobs = job_manager.list_jobs(limit=limit, name_filter=name_filter)
        
        if not jobs:
            return "No jobs found in the Databricks workspace."
        
        # Format as markdown table for AI consumption
        header = "| Job ID | Job Name | Type | Creator | Status | Last Run |"
        separator = "| --- | --- | --- | --- | --- | --- |"
        
        table_rows = []
        for job in jobs:
            last_run = job.last_run_state if job.last_run_state else "None"
            created_time = job_manager._format_timestamp(job.created_time) if hasattr(job_manager, '_format_timestamp') else "Unknown"
            
            table_rows.append(f"| {job.job_id} | {job.name} | {job.job_type} | {job.creator_email} | {job.status} | {last_run} |")
        
        result = f"Databricks Jobs ({len(jobs)} found):\n\n{header}\n{separator}\n" + "\n".join(table_rows)
        
        log_mcp_event("list_jobs", "SUCCESS", f"Retrieved {len(jobs)} jobs")
        return result
        
    except Exception as e:
        error_msg = f"Error listing jobs: {str(e)}"
        log_mcp_event("list_jobs", "ERROR", error_msg, "ERROR")
        return error_msg


@mcp.tool()
def list_jobs(limit: int = 25, name_filter: Optional[str] = None) -> str:
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
    return _list_jobs(limit, name_filter)


def _get_job_details(job_id: int) -> str:
    """Core job details logic"""
    try:
        log_mcp_event("get_job_details", "START", f"Getting details for job {job_id}")
        
        job_manager = get_job_manager()
        details = job_manager.get_job_details(job_id)
        
        # Format for AI consumption
        result = f"Job Details for ID {job_id}:\n\n"
        result += f"**Name**: {details['name']}\n"
        result += f"**Type**: {details['job_type']}\n"
        result += f"**Creator**: {details['creator']}\n"
        result += f"**Created**: {details['created_time']}\n"
        result += f"**Timeout**: {details.get('timeout_seconds', 'No limit')} seconds\n"
        result += f"**Max Concurrent Runs**: {details['max_concurrent_runs']}\n\n"
        
        if details['schedule']:
            schedule = details['schedule']
            result += f"**Schedule**:\n"
            result += f"- Cron: {schedule.get('quartz_cron_expression', 'None')}\n"
            result += f"- Timezone: {schedule.get('timezone_id', 'UTC')}\n"
            result += f"- Status: {schedule.get('pause_status', 'UNPAUSED')}\n\n"
        
        cluster_config = details['cluster_config']
        result += f"**Cluster Configuration**:\n"
        result += f"- Type: {cluster_config['type']}\n"
        if cluster_config['type'] == 'existing':
            result += f"- Cluster ID: {cluster_config.get('cluster_id')}\n"
        elif cluster_config['type'] == 'new':
            result += f"- Spark Version: {cluster_config.get('spark_version')}\n"
            result += f"- Node Type: {cluster_config.get('node_type_id')}\n"
            result += f"- Workers: {cluster_config.get('num_workers')}\n"
        
        task_config = details['task_config']
        result += f"\n**Task Configuration**:\n"
        result += f"- Type: {task_config['type']}\n"
        if task_config['type'] == 'NOTEBOOK':
            result += f"- Notebook Path: {task_config.get('notebook_path')}\n"
            if task_config.get('base_parameters'):
                result += f"- Parameters: {task_config['base_parameters']}\n"
        elif task_config['type'] == 'PYTHON':
            result += f"- Python File: {task_config.get('python_file')}\n"
            if task_config.get('parameters'):
                result += f"- Parameters: {task_config['parameters']}\n"
        elif task_config['type'] == 'SQL':
            result += f"- Warehouse ID: {task_config.get('warehouse_id')}\n"
        
        log_mcp_event("get_job_details", "SUCCESS", f"Retrieved details for job {job_id}")
        return result
        
    except Exception as e:
        error_msg = f"Error getting job {job_id} details: {str(e)}"
        log_mcp_event("get_job_details", "ERROR", error_msg, "ERROR")
        return error_msg


@mcp.tool()
def get_job_details(job_id: int) -> str:
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
    return _get_job_details(job_id)


def _get_job_runs(job_id: int, limit: int = 10, active_only: bool = False) -> str:
    """Core job runs logic"""
    try:
        log_mcp_event("get_job_runs", "START", f"Getting runs for job {job_id} (limit: {limit}, active_only: {active_only})")
        
        job_manager = get_job_manager()
        runs = job_manager.get_job_runs(job_id, limit=limit, active_only=active_only)
        
        if not runs:
            status = "active runs" if active_only else "runs"
            return f"No {status} found for job {job_id}."
        
        # Format as markdown table
        header = "| Run ID | Run Name | State | Result | Start Time | Duration (ms) | Trigger |"
        separator = "| --- | --- | --- | --- | --- | --- | --- |"
        
        table_rows = []
        for run in runs:
            start_time = job_manager._format_timestamp(run.start_time) if hasattr(job_manager, '_format_timestamp') else "Unknown"
            duration = str(run.execution_duration) if run.execution_duration else "N/A"
            result_state = run.result_state if run.result_state else "N/A"
            
            table_rows.append(f"| {run.run_id} | {run.run_name} | {run.state} | {result_state} | {start_time} | {duration} | {run.trigger} |")
        
        status = "Active Runs" if active_only else "Recent Runs"
        result = f"{status} for Job {job_id} ({len(runs)} found):\n\n{header}\n{separator}\n" + "\n".join(table_rows)
        
        log_mcp_event("get_job_runs", "SUCCESS", f"Retrieved {len(runs)} runs for job {job_id}")
        return result
        
    except Exception as e:
        error_msg = f"Error getting runs for job {job_id}: {str(e)}"
        log_mcp_event("get_job_runs", "ERROR", error_msg, "ERROR")
        return error_msg


@mcp.tool()
def get_job_runs(job_id: int, limit: int = 10, active_only: bool = False) -> str:
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
    return _get_job_runs(job_id, limit, active_only)


def _trigger_job(job_id: int, notebook_params: Optional[str] = None, 
                jar_params: Optional[str] = None, python_params: Optional[str] = None) -> str:
    """Core job triggering logic"""
    try:
        log_mcp_event("trigger_job", "START", f"Triggering job {job_id}")
        
        job_manager = get_job_manager()
        
        # Parse parameters if provided (expect JSON strings)
        parsed_notebook_params = None
        parsed_jar_params = None
        parsed_python_params = None
        
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
            notebook_params=parsed_notebook_params,
            jar_params=parsed_jar_params,
            python_params=parsed_python_params
        )
        
        result = f"✅ Job {job_id} triggered successfully!\n\n"
        result += f"**Run ID**: {run_id}\n"
        result += f"**Status**: Job execution started\n"
        result += f"**Monitor**: Use `get_job_runs({job_id}, active_only=True)` to check status\n"
        
        log_mcp_event("trigger_job", "SUCCESS", f"Triggered job {job_id}, run ID: {run_id}")
        return result
        
    except Exception as e:
        error_msg = f"Error triggering job {job_id}: {str(e)}"
        log_mcp_event("trigger_job", "ERROR", error_msg, "ERROR")
        return error_msg


@mcp.tool()
def trigger_job(job_id: int, notebook_params: Optional[str] = None, 
               jar_params: Optional[str] = None, python_params: Optional[str] = None) -> str:
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
    return _trigger_job(job_id, notebook_params, jar_params, python_params)


def _cancel_job_run(run_id: int) -> str:
    """Core job cancellation logic"""
    try:
        log_mcp_event("cancel_job_run", "START", f"Cancelling job run {run_id}")
        
        job_manager = get_job_manager()
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
def cancel_job_run(run_id: int) -> str:
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
    return _cancel_job_run(run_id)


def _get_job_run_output(run_id: int) -> str:
    """Core job output logic"""
    try:
        log_mcp_event("get_job_run_output", "START", f"Getting output for job run {run_id}")
        
        job_manager = get_job_manager()
        output = job_manager.get_job_run_output(run_id)
        
        result = f"Job Run Output for Run ID {run_id}:\n\n"
        
        if output.get('error'):
            result += f"**❌ Error**: {output['error']}\n\n"
        
        if output.get('error_trace'):
            result += f"**Error Trace**:\n```\n{output['error_trace']}\n```\n\n"
        
        if output.get('logs'):
            logs = output['logs']
            if output.get('logs_truncated'):
                result += f"**📝 Logs** (truncated):\n```\n{logs}\n```\n\n"
            else:
                result += f"**📝 Logs**:\n```\n{logs}\n```\n\n"
        
        if output.get('notebook_output'):
            notebook_output = output['notebook_output']
            result += f"**📓 Notebook Output**:\n```json\n{json.dumps(notebook_output, indent=2)}\n```\n\n"
        
        if output.get('metadata'):
            metadata = output['metadata']
            result += f"**ℹ️ Metadata**:\n```json\n{json.dumps(metadata, indent=2)}\n```\n\n"
        
        if not any([output.get('logs'), output.get('error'), output.get('notebook_output')]):
            result += "No output available for this job run.\n"
        
        log_mcp_event("get_job_run_output", "SUCCESS", f"Retrieved output for job run {run_id}")
        return result
        
    except Exception as e:
        error_msg = f"Error getting output for job run {run_id}: {str(e)}"
        log_mcp_event("get_job_run_output", "ERROR", error_msg, "ERROR")
        return error_msg


@mcp.tool()
def get_job_run_output(run_id: int) -> str:
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
    return _get_job_run_output(run_id)


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
        if stats['categories']:
            lines.extend([
                "## Cache Categories",
                "",
                "| Category | Count | Description |",
                "|----------|-------|-------------|"
            ])
            
            category_descriptions = {
                'health': 'Connection health status (5 min TTL)',
                'schema': 'Database schema information (30 min TTL)',
                'table': 'Table metadata and structure (15 min TTL)',
                'query': 'Query results (5 min TTL)',
                'job': 'Job information (2 min TTL)',
                'connection': 'Connection status (1 min TTL)'
            }
            
            for category, count in stats['categories'].items():
                description = category_descriptions.get(category, 'Unknown category')
                lines.append(f"| {category} | {count:,} | {description} |")
            
            lines.append("")
        
        # Performance insights
        total_requests = stats['hits'] + stats['misses']
        if total_requests > 0:
            lines.extend([
                "## Performance Insights",
                "",
                f"- **Cache Efficiency**: {'🟢 Excellent' if stats['hit_rate_percent'] > 80 else '🟡 Good' if stats['hit_rate_percent'] > 60 else '🔴 Needs Optimization'}",
                f"- **Memory Efficiency**: {'🟢 Optimal' if stats['total_entries'] < stats['max_entries'] * 0.8 else '🟡 High Usage'}",
                f"- **Total Requests**: {total_requests:,}",
                ""
            ])
            
            if stats['hit_rate_percent'] < 60:
                lines.extend([
                    "### ⚠️ Optimization Recommendations",
                    "- Consider increasing cache TTL values",
                    "- Review caching strategy for frequently accessed data",
                    "- Monitor for cache invalidation patterns",
                    ""
                ])
        
        log_mcp_event("cache_stats", "SUCCESS", f"Cache stats retrieved - Hit rate: {stats['hit_rate_percent']:.2f}%")
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


def _performance_stats() -> str:
    """Core performance statistics logic"""
    try:
        log_mcp_event("performance_stats", "START", "Getting system performance statistics")
        
        # Get performance stats
        perf_stats = get_performance_stats()
        
        # Get connection pool stats
        pool = get_pool()
        pool_stats = pool.get_pool_stats()
        
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
        lines.extend([
            "## 🔌 Connection Pool Status",
            f"- **Pool Utilization**: {pool_stats['pool_utilization_percent']:.1f}%",
            f"- **Active Connections**: {pool_stats['active_connections']}/{pool_stats['max_connections']}",
            f"- **Available Connections**: {pool_stats['available_connections']}",
            f"- **Health Check Cache**: {pool_stats['health_check_interval_seconds']}s TTL",
            "",
        ])
        
        # Cache Performance Summary
        cache_hit_rate = cache_stats.get('hit_rate_percent', 0)
        lines.extend([
            "## 🚀 Cache Performance",
            f"- **Hit Rate**: {cache_hit_rate:.2f}%",
            f"- **Cached Entries**: {cache_stats.get('total_entries', 0):,}",
            f"- **Memory Usage**: {cache_stats.get('memory_usage_estimate', 'Unknown')}",
            "",
        ])
        
        # Operation Performance
        if 'operation_stats' in perf_stats and perf_stats['operation_stats']:
            lines.extend([
                "## ⚡ Operation Performance",
                "",
                "| Operation | Calls | Success Rate | Avg Duration (ms) |",
                "|-----------|-------|--------------|-------------------|"
            ])
            
            for op, stats in perf_stats['operation_stats'].items():
                success_rate = (stats['successful_calls'] / stats['total_calls'] * 100) if stats['total_calls'] > 0 else 0
                avg_duration = stats['total_duration_ms'] / stats['total_calls'] if stats['total_calls'] > 0 else 0
                
                lines.append(f"| {op} | {stats['total_calls']:,} | {success_rate:.1f}% | {avg_duration:.1f} |")
            
            lines.append("")
        
        # Circuit Breaker Status
        if error_stats['circuit_breakers']:
            lines.extend([
                "## 🔄 Circuit Breaker Status",
                "",
                "| Operation | State | Failures | Can Execute |",
                "|-----------|-------|----------|-------------|"
            ])
            
            for cb_name, cb_stats in error_stats['circuit_breakers'].items():
                state_emoji = {
                    'closed': '🟢',
                    'open': '🔴', 
                    'half_open': '🟡'
                }.get(cb_stats['state'], '⚪')
                
                lines.append(f"| {cb_name} | {state_emoji} {cb_stats['state']} | {cb_stats['failure_count']} | {'✅' if cb_stats['can_execute'] else '❌'} |")
            
            lines.append("")
        
        # Health Assessment
        lines.extend([
            "## 🏥 System Health Assessment",
            ""
        ])
        
        # Overall health scoring
        health_score = 100
        issues = []
        
        if cache_hit_rate < 60:
            health_score -= 20
            issues.append("Low cache hit rate")
            
        if pool_stats['pool_utilization_percent'] > 90:
            health_score -= 15
            issues.append("High connection pool utilization")
            
        # Check for any open circuit breakers
        open_circuits = [name for name, stats in error_stats['circuit_breakers'].items() if stats['state'] == 'open']
        if open_circuits:
            health_score -= 30
            issues.append(f"Open circuit breakers: {', '.join(open_circuits)}")
            
        # Check operation error rates
        high_error_ops = []
        if 'operation_stats' in perf_stats:
            for op, stats in perf_stats['operation_stats'].items():
                if stats['total_calls'] > 10:  # Only check ops with significant calls
                    error_rate = (stats['failed_calls'] / stats['total_calls'] * 100) if stats['total_calls'] > 0 else 0
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
            lines.extend([
                "- **Issues Detected**:",
                *[f"  - {issue}" for issue in issues]
            ])
        else:
            lines.append("- **Status**: All systems operating normally")
            
        lines.append("")
        
        log_mcp_event("performance_stats", "SUCCESS", f"Performance stats retrieved - Health: {health_score}/100, Operations: {perf_stats['total_operations']}")
        return "\n".join(lines)
        
    except Exception as e:
        error_msg = f"Failed to get performance statistics: {str(e)}"
        log_mcp_event("performance_stats", "ERROR", error_msg, "ERROR")
        return f"**Error**: {error_msg}"


@mcp.tool()
def performance_stats(random_string: str = "dummy") -> str:
    """
    Get comprehensive system performance statistics and health metrics.
    
    Critical for monitoring system performance, identifying bottlenecks, and optimization.
    
    Args:
        random_string: Dummy parameter for no-parameter tools
        
    Returns:
        Detailed performance metrics including operation times, error rates, and system health
    """
    return _performance_stats()


def run_server():
    """
    Run the MCP server with stdio transport (recommended for AI integrations)
    """
    mcp.run(transport="stdio")


if __name__ == "__main__":
    run_server() 