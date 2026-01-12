"""
Performance Monitor for MCP Databricks Server.

This module tracks execution times, usage patterns, and system metrics
to provide insights for optimization and monitoring.
"""

import time
import threading
from typing import Dict, List, Optional, Any
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from collections import defaultdict, deque

from .logger import log_databricks_event, logger


@dataclass
class ExecutionMetric:
    """Individual execution metric."""

    operation: str
    start_time: float
    end_time: float
    success: bool
    error_message: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)

    @property
    def duration_ms(self) -> float:
        """Execution duration in milliseconds."""
        return (self.end_time - self.start_time) * 1000

    @property
    def timestamp(self) -> datetime:
        """Start timestamp as datetime."""
        return datetime.fromtimestamp(self.start_time)


@dataclass
class OperationStats:
    """Aggregated statistics for an operation."""

    operation: str
    total_calls: int = 0
    successful_calls: int = 0
    failed_calls: int = 0
    total_duration_ms: float = 0.0
    min_duration_ms: float = float("inf")
    max_duration_ms: float = 0.0
    recent_errors: List[str] = field(default_factory=list)

    @property
    def average_duration_ms(self) -> float:
        """Average execution duration."""
        return (
            self.total_duration_ms / self.total_calls if self.total_calls > 0 else 0.0
        )

    @property
    def success_rate_percent(self) -> float:
        """Success rate as percentage."""
        return (
            (self.successful_calls / self.total_calls * 100)
            if self.total_calls > 0
            else 0.0
        )

    @property
    def error_rate_percent(self) -> float:
        """Error rate as percentage."""
        return (
            (self.failed_calls / self.total_calls * 100)
            if self.total_calls > 0
            else 0.0
        )


class PerformanceMonitor:
    """
    Thread-safe performance monitoring system.

    Tracks execution times, success/failure rates, and usage patterns.
    """

    def __init__(self, max_metrics: int = 10000):
        """Initialize performance monitor."""
        self._lock = threading.RLock()
        self._max_metrics = max_metrics
        self._metrics: deque[ExecutionMetric] = deque(maxlen=max_metrics)
        self._operation_stats: Dict[str, Dict[str, Any]] = defaultdict(dict)
        self._start_time = time.time()

        logger.info(f"Performance monitor initialized (max metrics: {max_metrics})")

    def record_operation(
        self,
        operation: str,
        duration_ms: float,
        success: bool = True,
        error_message: Optional[str] = None,
    ) -> None:
        """Record a completed operation metric."""
        now = time.time()
        start_time = now - (duration_ms / 1000)

        metric = ExecutionMetric(
            operation=operation,
            start_time=start_time,
            end_time=now,
            success=success,
            error_message=error_message,
        )

        with self._lock:
            self._metrics.append(metric)

            # Update operation stats
            if operation not in self._operation_stats:
                self._operation_stats[operation] = {
                    "total_calls": 0,
                    "successful_calls": 0,
                    "failed_calls": 0,
                    "total_duration_ms": 0.0,
                    "min_duration_ms": float("inf"),
                    "max_duration_ms": 0.0,
                }

            stats = self._operation_stats[operation]
            stats["total_calls"] += 1
            stats["total_duration_ms"] += duration_ms
            stats["min_duration_ms"] = min(stats["min_duration_ms"], duration_ms)
            stats["max_duration_ms"] = max(stats["max_duration_ms"], duration_ms)

            if success:
                stats["successful_calls"] += 1
            else:
                stats["failed_calls"] += 1

    def get_stats(self) -> Dict[str, Any]:
        """Get comprehensive performance statistics."""
        with self._lock:
            uptime_seconds = time.time() - self._start_time
            total_operations = sum(
                stats["total_calls"] for stats in self._operation_stats.values()
            )

            return {
                "uptime_seconds": uptime_seconds,
                "total_operations": total_operations,
                "operations_per_second": (
                    total_operations / uptime_seconds if uptime_seconds > 0 else 0
                ),
                "operation_stats": dict(self._operation_stats),
            }


# Global performance monitor
_performance_monitor = None
_perf_lock = threading.Lock()


def get_performance_monitor() -> PerformanceMonitor:
    """Get or create the global performance monitor instance."""
    global _performance_monitor

    if _performance_monitor is None:
        with _perf_lock:
            if _performance_monitor is None:
                _performance_monitor = PerformanceMonitor()

    return _performance_monitor


def record_operation(
    operation: str,
    duration_ms: float,
    success: bool = True,
    error: Optional[str] = None,
) -> None:
    """Record a completed operation metric."""
    monitor = get_performance_monitor()
    monitor.record_operation(operation, duration_ms, success, error)


def get_performance_stats() -> Dict[str, Any]:
    """Get comprehensive performance statistics."""
    monitor = get_performance_monitor()
    return monitor.get_stats()
