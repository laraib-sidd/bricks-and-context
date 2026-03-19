"""
Enhanced Error Handling for MCP Databricks Server.

This module provides retry logic, circuit breaker patterns, and enhanced
error recovery mechanisms for robust operation.
"""

import time
import random
import threading
from typing import Callable, Any, Optional, Dict, List, Union
from dataclasses import dataclass
from enum import Enum
import functools

from .logger import log_databricks_event, logger


class ErrorType(Enum):
    """Classification of error types for handling strategy."""

    NETWORK = "network"
    AUTHENTICATION = "authentication"
    RATE_LIMIT = "rate_limit"
    DATABRICKS_API = "databricks_api"
    SQL_ERROR = "sql_error"
    TIMEOUT = "timeout"
    UNKNOWN = "unknown"


@dataclass
class RetryConfig:
    """Configuration for retry behavior."""

    max_attempts: int = 3
    base_delay_seconds: float = 1.0
    max_delay_seconds: float = 60.0
    exponential_base: float = 2.0
    jitter: bool = True
    retryable_errors: Optional[List[ErrorType]] = None

    def __post_init__(self) -> None:
        if self.retryable_errors is None:
            self.retryable_errors = [
                ErrorType.NETWORK,
                ErrorType.RATE_LIMIT,
                ErrorType.TIMEOUT,
                ErrorType.DATABRICKS_API,
            ]


class CircuitBreakerState(Enum):
    """Circuit breaker states."""

    CLOSED = "closed"  # Normal operation
    OPEN = "open"  # Failing, blocking requests
    HALF_OPEN = "half_open"  # Testing if service recovered


@dataclass
class CircuitBreakerConfig:
    """Configuration for circuit breaker."""

    failure_threshold: int = 5  # Failures before opening
    recovery_timeout_seconds: float = 60.0  # Time before trying recovery
    success_threshold: int = 3  # Successes needed to close


class CircuitBreaker:
    """Circuit breaker implementation for fault tolerance."""

    def __init__(self, name: str, config: CircuitBreakerConfig):
        self.name = name
        self.config = config
        self._lock = threading.RLock()

        # State tracking
        self._state = CircuitBreakerState.CLOSED
        self._failure_count = 0
        self._success_count = 0
        self._last_failure_time = 0.0

    @property
    def state(self) -> CircuitBreakerState:
        """Get current circuit breaker state."""
        with self._lock:
            # Check if we should transition from OPEN to HALF_OPEN
            if (
                self._state == CircuitBreakerState.OPEN
                and time.time() - self._last_failure_time
                > self.config.recovery_timeout_seconds
            ):
                self._state = CircuitBreakerState.HALF_OPEN
                self._success_count = 0
                log_databricks_event(
                    "CIRCUIT_BREAKER",
                    "HALF_OPEN",
                    f"{self.name} entering half-open state",
                )

            return self._state

    def can_execute(self) -> bool:
        """Check if execution is allowed."""
        return self.state != CircuitBreakerState.OPEN

    def record_success(self) -> None:
        """Record a successful operation."""
        with self._lock:
            if self._state == CircuitBreakerState.HALF_OPEN:
                self._success_count += 1
                if self._success_count >= self.config.success_threshold:
                    self._state = CircuitBreakerState.CLOSED
                    self._failure_count = 0
                    log_databricks_event(
                        "CIRCUIT_BREAKER",
                        "CLOSED",
                        f"{self.name} circuit closed after recovery",
                    )
            elif self._state == CircuitBreakerState.CLOSED:
                self._failure_count = 0  # Reset failure count on success

    def record_failure(self) -> None:
        """Record a failed operation."""
        with self._lock:
            self._failure_count += 1
            self._last_failure_time = time.time()

            if self._state == CircuitBreakerState.CLOSED:
                if self._failure_count >= self.config.failure_threshold:
                    self._state = CircuitBreakerState.OPEN
                    log_databricks_event(
                        "CIRCUIT_BREAKER",
                        "OPEN",
                        f"{self.name} circuit opened due to failures",
                    )
            elif self._state == CircuitBreakerState.HALF_OPEN:
                self._state = CircuitBreakerState.OPEN
                log_databricks_event(
                    "CIRCUIT_BREAKER",
                    "OPEN",
                    f"{self.name} circuit reopened during half-open test",
                )

    def get_stats(self) -> Dict[str, Any]:
        """Get circuit breaker statistics."""
        with self._lock:
            return {
                "name": self.name,
                "state": self._state.value,
                "failure_count": self._failure_count,
                "success_count": self._success_count,
                "last_failure_time": self._last_failure_time,
                "can_execute": self.can_execute(),
            }


class ErrorHandler:
    """
    Comprehensive error handling with retry logic and circuit breakers.
    """

    def __init__(self) -> None:
        self._circuit_breakers: Dict[str, CircuitBreaker] = {}
        self._lock = threading.RLock()

        # Default configurations
        self.default_retry_config = RetryConfig()
        self.default_circuit_config = CircuitBreakerConfig()

        logger.info("Error handler initialized with retry logic and circuit breakers")

    def classify_error(self, error: Exception) -> ErrorType:
        """Classify error for appropriate handling strategy."""
        error_str = str(error).lower()

        # Check rate limit FIRST — rate limit messages can contain "token" (e.g. "token bucket")
        # which would false-match the auth check below if order were reversed.
        if any(keyword in error_str for keyword in ["rate limit", "too many requests", "429", "throttl"]):
            return ErrorType.RATE_LIMIT

        if any(keyword in error_str for keyword in ["authentication", "unauthorized", "forbidden"]):
            return ErrorType.AUTHENTICATION

        # Narrow token match: only treat as auth when paired with a failure qualifier.
        if "token" in error_str and any(keyword in error_str for keyword in ["expired", "invalid", "revoked"]):
            return ErrorType.AUTHENTICATION

        if "timeout" in error_str:
            return ErrorType.TIMEOUT

        if any(keyword in error_str for keyword in ["network", "connection", "unreachable", "refused", "reset"]):
            return ErrorType.NETWORK

        if any(keyword in error_str for keyword in ["databricks", "api error", "rest api"]):
            return ErrorType.DATABRICKS_API

        if any(keyword in error_str for keyword in ["sql", "query", "syntax"]):
            return ErrorType.SQL_ERROR

        return ErrorType.UNKNOWN

    def is_retryable(self, error: Exception, retry_config: RetryConfig) -> bool:
        """Determine if an error is retryable based on classification."""
        error_type = self.classify_error(error)
        retryable = retry_config.retryable_errors or []
        return error_type in retryable

    def calculate_delay(self, attempt: int, config: RetryConfig) -> float:
        """Calculate delay for retry attempt with exponential backoff and jitter."""
        if attempt <= 0:
            return 0.0

        # Exponential backoff
        delay = config.base_delay_seconds * (config.exponential_base ** (attempt - 1))

        # Cap at max delay
        delay = min(delay, config.max_delay_seconds)

        # Add jitter to prevent thundering herd
        if config.jitter:
            jitter_range = delay * 0.1  # 10% jitter
            delay += random.uniform(-jitter_range, jitter_range)

        return max(0.0, delay)

    def get_circuit_breaker(
        self, name: str, config: Optional[CircuitBreakerConfig] = None
    ) -> CircuitBreaker:
        """Get or create a circuit breaker for the given operation."""
        with self._lock:
            if name not in self._circuit_breakers:
                breaker_config = config or self.default_circuit_config
                self._circuit_breakers[name] = CircuitBreaker(name, breaker_config)
            return self._circuit_breakers[name]

    def with_retry(
        self,
        operation_name: str,
        config: Optional[RetryConfig] = None,
        circuit_breaker: bool = True,
    ) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
        """
        Decorator for adding retry logic and circuit breaker to operations.

        Args:
            operation_name: Name of the operation for logging and circuit breaker
            config: Retry configuration (uses default if None)
            circuit_breaker: Whether to use circuit breaker pattern
        """

        def decorator(func: Callable[..., Any]) -> Callable[..., Any]:
            @functools.wraps(func)
            def wrapper(*args: Any, **kwargs: Any) -> Any:
                retry_config = config or self.default_retry_config
                circuit = (
                    self.get_circuit_breaker(operation_name)
                    if circuit_breaker
                    else None
                )

                # Check circuit breaker
                if circuit and not circuit.can_execute():
                    error_msg = (
                        f"Databricks is temporarily unavailable (circuit breaker '{operation_name}' is open). "
                        f"The server detected repeated failures and is waiting before retrying. "
                        f"This will automatically recover in up to {circuit.config.recovery_timeout_seconds:.0f} seconds. "
                        f"If this persists, check warehouse status with `databricks_list_warehouses`."
                    )
                    log_databricks_event(
                        "ERROR_HANDLER", "BLOCKED", error_msg, "WARNING"
                    )
                    raise Exception(error_msg)

                last_error = None

                for attempt in range(1, retry_config.max_attempts + 1):
                    try:
                        start_time = time.time()

                        # Execute the operation
                        result = func(*args, **kwargs)

                        # Record success
                        if circuit:
                            circuit.record_success()

                        # Log successful retry if not first attempt
                        if attempt > 1:
                            duration_ms = (time.time() - start_time) * 1000
                            log_databricks_event(
                                "ERROR_HANDLER",
                                "RETRY_SUCCESS",
                                f"{operation_name} succeeded on attempt {attempt} ({duration_ms:.1f}ms)",
                            )

                        return result

                    except Exception as e:
                        last_error = e
                        error_type = self.classify_error(e)

                        # Record failure in circuit breaker
                        if circuit:
                            circuit.record_failure()

                        # Check if error is retryable
                        if not self.is_retryable(e, retry_config):
                            log_databricks_event(
                                "ERROR_HANDLER",
                                "NON_RETRYABLE",
                                f"{operation_name} failed with non-retryable error: {error_type.value}",
                                "ERROR",
                            )
                            raise e

                        # Don't retry on last attempt
                        if attempt >= retry_config.max_attempts:
                            break

                        # Calculate delay and wait
                        delay = self.calculate_delay(attempt, retry_config)

                        log_databricks_event(
                            "ERROR_HANDLER",
                            "RETRY",
                            f"{operation_name} attempt {attempt} failed ({error_type.value}), retrying in {delay:.2f}s",
                            "WARNING",
                        )

                        if delay > 0:
                            time.sleep(delay)

                # All attempts failed
                log_databricks_event(
                    "ERROR_HANDLER",
                    "MAX_RETRIES",
                    f"{operation_name} failed after {retry_config.max_attempts} attempts",
                    "ERROR",
                )

                if last_error is not None:
                    raise last_error
                raise RuntimeError(f"{operation_name} failed with unknown error")

            return wrapper

        return decorator

    def get_stats(self) -> Dict[str, Any]:
        """Get error handler statistics."""
        with self._lock:
            circuit_stats = {}
            for name, breaker in self._circuit_breakers.items():
                circuit_stats[name] = breaker.get_stats()

            return {
                "circuit_breakers": circuit_stats,
                "total_circuit_breakers": len(self._circuit_breakers),
            }


# Global error handler instance
_error_handler = None
_error_lock = threading.Lock()


def get_error_handler() -> ErrorHandler:
    """Get or create the global error handler instance."""
    global _error_handler

    if _error_handler is None:
        with _error_lock:
            if _error_handler is None:
                _error_handler = ErrorHandler()

    return _error_handler


# Convenience decorators
def with_retry(
    operation_name: str,
    max_attempts: int = 3,
    base_delay: float = 1.0,
    use_circuit_breaker: bool = True,
) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
    """
    Convenience decorator for adding retry logic.

    Args:
        operation_name: Name of the operation
        max_attempts: Maximum retry attempts
        base_delay: Base delay between retries
        use_circuit_breaker: Whether to use circuit breaker
    """
    config = RetryConfig(max_attempts=max_attempts, base_delay_seconds=base_delay)

    error_handler = get_error_handler()
    return error_handler.with_retry(operation_name, config, use_circuit_breaker)


def with_databricks_retry(
    operation_name: str,
) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
    """Decorator with Databricks-optimized retry configuration."""
    config = RetryConfig(
        max_attempts=3,
        base_delay_seconds=2.0,
        max_delay_seconds=30.0,
        retryable_errors=[
            ErrorType.NETWORK,
            ErrorType.RATE_LIMIT,
            ErrorType.TIMEOUT,
            ErrorType.DATABRICKS_API,
        ],
    )

    error_handler = get_error_handler()
    return error_handler.with_retry(operation_name, config, True)
