#!/usr/bin/env python3
"""
Entry point for the Bricks and Context MCP Server

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

import os
import signal
import sys


def _setup_signal_handling() -> None:
    """Prevent the server from being killed by SIGPIPE.

    When the MCP client (Cursor) disconnects, the next write to stdout
    raises BrokenPipeError / SIGPIPE.  By default Python lets SIGPIPE
    kill the process silently.  We ignore it so the server can detect
    the closed pipe via BrokenPipeError and exit cleanly instead.
    """
    if hasattr(signal, "SIGPIPE"):
        signal.signal(signal.SIGPIPE, signal.SIG_IGN)


def _install_excepthook() -> None:
    """Log unhandled exceptions to stderr before the process dies."""
    original = sys.excepthook

    def _hook(exc_type, exc_value, exc_tb):  # type: ignore[no-untyped-def]
        if exc_type is KeyboardInterrupt:
            original(exc_type, exc_value, exc_tb)
            return
        try:
            from src.mcp_server.logger import logger

            logger.critical(
                "Unhandled exception — server crashing: %s: %s",
                exc_type.__name__,
                exc_value,
                exc_info=(exc_type, exc_value, exc_tb),
            )
        except Exception:
            original(exc_type, exc_value, exc_tb)

    sys.excepthook = _hook


def main() -> None:
    _setup_signal_handling()
    _install_excepthook()

    from src.mcp_server.mcp_server import run_server
    from src.mcp_server.logger import log_server_event, logger

    log_server_event("START", "Initializing Bricks and Context MCP Server")
    log_server_event("TRANSPORT", "Using stdio transport for AI client connections")
    log_server_event("CONNECTION", "Preparing Databricks connection pool")

    try:
        run_server()
    except BrokenPipeError:
        log_server_event("STOP", "Client disconnected (broken pipe)", "WARNING")
    except KeyboardInterrupt:
        log_server_event("STOP", "MCP Server stopped by user")
    except Exception as e:
        log_server_event("ERROR", f"MCP Server crashed: {e}", "ERROR")
        logger.error(
            "Fatal error — full traceback above. "
            "Check auth.yaml, config.json, and Databricks connectivity.",
            exc_info=True,
        )
        sys.exit(1)


if __name__ == "__main__":
    main()
