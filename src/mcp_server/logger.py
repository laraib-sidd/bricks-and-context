"""
Logging configuration for the MCP Databricks Server.

This module provides structured logging with configurable levels,
proper formatting, and environment-based configuration.
"""

import logging
import os
import sys
from typing import Optional


def setup_logger(
    name: str = "mcp_databricks_server",
    level: Optional[str] = None,
    format_type: str = "detailed",
) -> logging.Logger:
    """
    Set up a structured logger for the MCP server.

    Args:
        name: Logger name (default: "mcp_databricks_server")
        level: Log level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
               If None, reads from DATABRICKS_LOG_LEVEL env var (default: INFO)
        format_type: Format type - "simple", "detailed", or "structured"

    Returns:
        Configured logger instance
    """
    logger = logging.getLogger(name)

    # Prevent duplicate handlers if logger already configured
    if logger.handlers:
        return logger

    # Set log level from parameter or environment variable
    if level is None:
        level = os.getenv("DATABRICKS_LOG_LEVEL", "INFO").upper()

    try:
        log_level = getattr(logging, level)
    except AttributeError:
        log_level = logging.INFO

    logger.setLevel(log_level)

    # Create console handler - MUST use stderr for MCP stdio transport
    # stdout is reserved for JSON-RPC protocol messages
    handler = logging.StreamHandler(sys.stderr)
    handler.setLevel(log_level)

    # Configure formatter based on type
    if format_type == "simple":
        formatter = logging.Formatter("%(levelname)s: %(message)s")
    elif format_type == "structured":
        formatter = logging.Formatter(
            "%(asctime)s | %(name)s | %(levelname)s | %(funcName)s:%(lineno)d | %(message)s"
        )
    else:  # detailed (default)
        formatter = logging.Formatter(
            "%(asctime)s [%(levelname)s] %(name)s: %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )

    handler.setFormatter(formatter)
    logger.addHandler(handler)

    return logger


def get_logger(name: str = "mcp_databricks_server") -> logging.Logger:
    """
    Get or create a logger instance.

    Args:
        name: Logger name

    Returns:
        Logger instance
    """
    return logging.getLogger(name)


# Module-level logger for this package
logger = setup_logger()


def log_connection_event(event_type: str, details: str, level: str = "INFO") -> None:
    """
    Log connection-related events with consistent formatting.

    Args:
        event_type: Type of event (CONNECT, DISCONNECT, ERROR, etc.)
        details: Event details
        level: Log level
    """
    log_level = getattr(logging, level.upper(), logging.INFO)
    logger.log(log_level, f"🔗 CONNECTION {event_type}: {details}")


def log_mcp_event(
    tool_name: str, event_type: str, details: str, level: str = "INFO"
) -> None:
    """
    Log MCP tool execution events with consistent formatting.

    Args:
        tool_name: Name of the MCP tool
        event_type: Type of event (START, SUCCESS, ERROR, etc.)
        details: Event details
        level: Log level
    """
    log_level = getattr(logging, level.upper(), logging.INFO)
    logger.log(log_level, f"🛠️  MCP TOOL {tool_name} {event_type}: {details}")


def log_databricks_event(
    operation: str, event_type: str, details: str, level: str = "INFO"
) -> None:
    """
    Log Databricks API operation events with consistent formatting.

    Args:
        operation: Databricks operation (SQL, JOBS, CLUSTERS, etc.)
        event_type: Type of event (START, SUCCESS, ERROR, etc.)
        details: Event details
        level: Log level
    """
    log_level = getattr(logging, level.upper(), logging.INFO)
    logger.log(log_level, f"🧱 DATABRICKS {operation} {event_type}: {details}")


def log_server_event(event_type: str, details: str, level: str = "INFO") -> None:
    """
    Log server lifecycle events with consistent formatting.

    Args:
        event_type: Type of event (START, STOP, ERROR, etc.)
        details: Event details
        level: Log level
    """
    log_level = getattr(logging, level.upper(), logging.INFO)
    logger.log(log_level, f"🚀 SERVER {event_type}: {details}")
