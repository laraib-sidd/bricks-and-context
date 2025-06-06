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


def run_server():
    """
    Run the MCP server with stdio transport (recommended for AI integrations)
    """
    mcp.run(transport="stdio")


if __name__ == "__main__":
    run_server() 