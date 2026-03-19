"""Test circuit breaker and retry logic."""
from __future__ import annotations

import time

from mcp_server.error_handler import (
    CircuitBreaker,
    CircuitBreakerConfig,
    CircuitBreakerState,
    ErrorHandler,
    ErrorType,
    RetryConfig,
)


def test_circuit_breaker_starts_closed():
    config = CircuitBreakerConfig(failure_threshold=3)
    cb = CircuitBreaker("test", config)
    assert cb.state == CircuitBreakerState.CLOSED
    assert cb.can_execute() is True


def test_circuit_breaker_opens_after_threshold():
    config = CircuitBreakerConfig(failure_threshold=2)
    cb = CircuitBreaker("test", config)
    cb.record_failure()
    assert cb.state == CircuitBreakerState.CLOSED
    cb.record_failure()
    assert cb.state == CircuitBreakerState.OPEN
    assert cb.can_execute() is False


def test_circuit_breaker_transitions_to_half_open():
    config = CircuitBreakerConfig(failure_threshold=1, recovery_timeout_seconds=0.1)
    cb = CircuitBreaker("test", config)
    cb.record_failure()
    assert cb.state == CircuitBreakerState.OPEN
    time.sleep(0.15)
    assert cb.state == CircuitBreakerState.HALF_OPEN


def test_circuit_breaker_closes_after_success_threshold():
    config = CircuitBreakerConfig(failure_threshold=1, recovery_timeout_seconds=0.1, success_threshold=2)
    cb = CircuitBreaker("test", config)
    cb.record_failure()
    time.sleep(0.15)
    assert cb.state == CircuitBreakerState.HALF_OPEN
    cb.record_success()
    assert cb.state == CircuitBreakerState.HALF_OPEN  # Need 2
    cb.record_success()
    assert cb.state == CircuitBreakerState.CLOSED


def test_circuit_breaker_reopens_on_half_open_failure():
    config = CircuitBreakerConfig(failure_threshold=1, recovery_timeout_seconds=0.1)
    cb = CircuitBreaker("test", config)
    cb.record_failure()
    time.sleep(0.15)
    assert cb.state == CircuitBreakerState.HALF_OPEN
    cb.record_failure()
    assert cb.state == CircuitBreakerState.OPEN


def test_success_resets_failure_count():
    config = CircuitBreakerConfig(failure_threshold=3)
    cb = CircuitBreaker("test", config)
    cb.record_failure()
    cb.record_failure()
    cb.record_success()  # Reset
    cb.record_failure()  # 1st failure after reset
    assert cb.state == CircuitBreakerState.CLOSED  # Not open yet


def test_retry_decorator_retries_on_retryable():
    handler = ErrorHandler()
    call_count = 0

    @handler.with_retry("test_retry", RetryConfig(max_attempts=3, base_delay_seconds=0.01), circuit_breaker=False)
    def flaky():
        nonlocal call_count
        call_count += 1
        if call_count < 3:
            raise Exception("network: connection reset")
        return "success"

    result = flaky()
    assert result == "success"
    assert call_count == 3


def test_retry_skips_non_retryable():
    handler = ErrorHandler()
    call_count = 0

    @handler.with_retry("test_no_retry", RetryConfig(max_attempts=3), circuit_breaker=False)
    def fail_auth():
        nonlocal call_count
        call_count += 1
        raise Exception("authentication: HTTP 401 — token expired")

    try:
        fail_auth()
    except Exception:
        pass
    assert call_count == 1


def test_calculate_delay_exponential():
    handler = ErrorHandler()
    config = RetryConfig(base_delay_seconds=1.0, exponential_base=2.0, jitter=False)
    assert handler.calculate_delay(1, config) == 1.0
    assert handler.calculate_delay(2, config) == 2.0
    assert handler.calculate_delay(3, config) == 4.0


def test_calculate_delay_capped():
    handler = ErrorHandler()
    config = RetryConfig(base_delay_seconds=1.0, max_delay_seconds=5.0, jitter=False)
    assert handler.calculate_delay(10, config) == 5.0


def test_circuit_breaker_stats():
    config = CircuitBreakerConfig(failure_threshold=2)
    cb = CircuitBreaker("test_stats", config)
    cb.record_failure()
    stats = cb.get_stats()
    assert stats["name"] == "test_stats"
    assert stats["state"] == "closed"
    assert stats["failure_count"] == 1
