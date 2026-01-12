#!/usr/bin/env python3
"""
Entry point for the Bricks and Context MCP Server

Run this script to start the MCP server for AI integrations with Databricks.
The server uses stdio transport by default, making it compatible with
Claude Desktop, Cursor, and other MCP clients.

Usage:
    python run_mcp_server.py

Configuration:
    auth.yaml           - Workspace credentials (see auth.template.yaml)
    config.json         - Tunable settings (connection pool, limits, etc.)

Environment Variables (optional overrides):
    MCP_AUTH_PATH       - Path to auth.yaml (default: ./auth.yaml)
    MCP_CONFIG_PATH     - Path to config.json (default: ./config.json)
    DATABRICKS_HOST     - Legacy: single workspace hostname
    DATABRICKS_TOKEN    - Legacy: single workspace access token
    DATABRICKS_HTTP_PATH - Legacy: single workspace SQL warehouse path
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