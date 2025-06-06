#!/usr/bin/env python3
"""
Entry point for the Bricks and Context MCP Server

Run this script to start the MCP server for AI integrations with Databricks.
The server uses stdio transport by default, making it compatible with
Claude Desktop and other MCP clients.

Usage:
    python run_mcp_server.py
    
Environment Variables:
    DATABRICKS_HOST - Databricks workspace hostname
    DATABRICKS_TOKEN - Databricks access token  
    DATABRICKS_HTTP_PATH - SQL warehouse HTTP path
    MAX_CONNECTIONS - Maximum connection pool size (default: 10)
"""

from src.mcp_server.mcp_server import run_server
from src.mcp_server.logger import log_server_event, logger

if __name__ == "__main__":
    log_server_event("START", "Initializing Bricks and Context MCP Server")
    log_server_event("TRANSPORT", "Using stdio transport for AI client connections")
    log_server_event("CONNECTION", "Preparing Databricks connection pool")
    
    try:
        run_server()
    except KeyboardInterrupt:
        log_server_event("STOP", "MCP Server stopped by user", "INFO")
    except Exception as e:
        log_server_event("ERROR", f"MCP Server failed to start: {e}", "ERROR")
        logger.error("Check your environment variables and Databricks connection") 