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

if __name__ == "__main__":
    print("🚀 Starting Bricks and Context MCP Server...")
    print("📡 Using stdio transport for AI client connections")
    print("🔗 Connecting to Databricks with connection pooling")
    print()
    
    try:
        run_server()
    except KeyboardInterrupt:
        print("\n\n⏹️  MCP Server stopped by user")
    except Exception as e:
        print(f"\n\n❌ MCP Server failed to start: {e}")
        print("🔧 Check your environment variables and Databricks connection") 