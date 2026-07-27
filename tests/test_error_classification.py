"""Tests for error classification edge cases in ErrorHandler."""

from __future__ import annotations

from mcp_server.error_handler import ErrorHandler, ErrorType


def test_token_bucket_not_classified_as_auth():
    """Rate limit 'token bucket' must NOT be classified as AUTHENTICATION."""
    handler = ErrorHandler()
    exc = Exception("rate limit exceeded: token bucket depleted")
    assert handler.classify_error(exc) != ErrorType.AUTHENTICATION


def test_rate_limit_429():
    handler = ErrorHandler()
    exc = Exception("rate limit: HTTP 429 from Databricks API")
    assert handler.classify_error(exc) == ErrorType.RATE_LIMIT


def test_actual_auth_error():
    handler = ErrorHandler()
    exc = Exception("authentication: HTTP 401 — token may be expired")
    assert handler.classify_error(exc) == ErrorType.AUTHENTICATION


def test_timeout():
    handler = ErrorHandler()
    exc = Exception("Connection timeout after 30 seconds")
    assert handler.classify_error(exc) == ErrorType.TIMEOUT


def test_sql_syntax_error():
    handler = ErrorHandler()
    exc = Exception("SQL syntax error near 'SELCET'")
    assert handler.classify_error(exc) == ErrorType.SQL_ERROR


def test_unknown():
    handler = ErrorHandler()
    exc = Exception("something completely unexpected")
    assert handler.classify_error(exc) == ErrorType.UNKNOWN


def test_token_expired_is_auth():
    handler = ErrorHandler()
    exc = Exception("token expired or invalid")
    assert handler.classify_error(exc) == ErrorType.AUTHENTICATION


def test_connection_refused_is_network():
    handler = ErrorHandler()
    exc = Exception("connection refused by host")
    assert handler.classify_error(exc) == ErrorType.NETWORK


def test_4xx_client_error_not_retryable():
    """HTTP 4xx client errors should NOT be retried or trip circuit breaker."""
    handler = ErrorHandler()
    exc = ValueError('client error: HTTP 400: {"error_code":"INVALID_PARAMETER_VALUE"}')
    assert handler.classify_error(exc) == ErrorType.UNKNOWN
    assert handler.is_retryable(exc, handler.default_retry_config) is False


def test_5xx_server_error_is_retryable():
    """HTTP 5xx server errors SHOULD be retried."""
    handler = ErrorHandler()
    exc = Exception("databricks api error: HTTP 500: internal server error")
    assert handler.classify_error(exc) == ErrorType.DATABRICKS_API
    assert handler.is_retryable(exc, handler.default_retry_config) is True


def test_404_not_found_not_retryable():
    """HTTP 404 should not be retried — the resource simply doesn't exist."""
    handler = ErrorHandler()
    exc = ValueError("client error: HTTP 404: ENDPOINT_NOT_FOUND")
    assert handler.is_retryable(exc, handler.default_retry_config) is False
