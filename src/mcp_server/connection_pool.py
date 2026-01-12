"""
Enhanced Connection Pool for Databricks SQL connections.
Optimized for AI request patterns with caching, performance monitoring, and error handling.
"""

import os
import time
import threading
import hashlib
from queue import Queue, Empty
from typing import Optional, Any, Dict
from databricks.sql import connect
from databricks.sql.client import Connection
from dotenv import load_dotenv

from .logger import log_databricks_event, logger
from .cache_manager import cache_health_status, get_cached_health_status, get_cache_manager
from .performance_monitor import get_performance_monitor, record_operation
from .error_handler import with_databricks_retry

# Load environment variables from .env file
load_dotenv()


class ConnectionPool:
    """
    Thread-safe connection pool for Databricks SQL connections with enhanced features.
    
    Features:
    - Cached health checks (5-minute TTL) to reduce SELECT 1 queries
    - Performance monitoring for all operations
    - Error handling with retry logic
    - Connection validation and automatic recovery
    """

    def __init__(self, max_connections: int = 10, health_check_interval: int = 300):
        """
        Initialize connection pool.
        
        Args:
            max_connections: Maximum number of connections in pool
            health_check_interval: Health check cache TTL in seconds (default: 5 minutes)
        """
        self.max_connections = max_connections
        self.health_check_interval = health_check_interval
        self._pool: Queue[Connection] = Queue(maxsize=max_connections)
        self._created_connections = 0
        self._lock = threading.Lock()
        self._last_health_check = 0.0
        # Per-connection validation cache (prevents global "healthy" from masking a bad connection)
        self._conn_validated_at: Dict[int, float] = {}
        self._conn_bad: Dict[int, bool] = {}
        
        # Performance monitoring
        self._monitor = get_performance_monitor()
        self._cache = get_cache_manager()

        # Databricks connection config
        self.host = os.getenv("DATABRICKS_HOST")
        self.token = os.getenv("DATABRICKS_TOKEN")
        self.http_path = os.getenv("DATABRICKS_HTTP_PATH")

        if not all([self.host, self.token, self.http_path]):
            raise ValueError("Missing required Databricks credentials in environment")
        
        log_databricks_event("CONNECTION_POOL", "INIT", f"Pool initialized with {max_connections} max connections")

    @with_databricks_retry("create_connection")
    def _create_connection(self) -> Connection:
        """Create a new Databricks SQL connection with retry logic."""
        start_time = time.time()
        
        try:
            connection = connect(
                server_hostname=self.host,
                http_path=self.http_path,
                access_token=self.token
            )
            
            duration_ms = (time.time() - start_time) * 1000
            record_operation("create_connection", duration_ms, True)
            
            log_databricks_event("CONNECTION_POOL", "CREATE", f"New connection created ({duration_ms:.1f}ms)")
            return connection
            
        except Exception as e:
            duration_ms = (time.time() - start_time) * 1000
            record_operation("create_connection", duration_ms, False, str(e))
            raise

    def _validate_connection(self, connection: Connection) -> bool:
        """
        Validate connection health with caching to reduce SELECT 1 queries.
        
        Args:
            connection: Connection to validate
            
        Returns:
            bool: True if connection is valid
        """
        conn_id = id(connection)

        # If we've marked this connection as bad, don't hand it out again.
        if self._conn_bad.get(conn_id):
            return False

        # If we validated this specific connection recently, skip re-validating.
        last_ok = self._conn_validated_at.get(conn_id)
        if last_ok and (time.time() - last_ok) <= self.health_check_interval:
            return True
        
        # Perform actual health check
        start_time = time.time()
        
        try:
            cursor = connection.cursor()
            cursor.execute("SELECT 1")
            result = cursor.fetchone()
            cursor.close()
            
            is_healthy = result is not None
            duration_ms = (time.time() - start_time) * 1000
            
            if is_healthy:
                # Cache successful health check
                cache_health_status("healthy", "Connection validated", self.health_check_interval)
                self._conn_validated_at[conn_id] = time.time()
                self._conn_bad.pop(conn_id, None)
                record_operation("health_check", duration_ms, True)
                log_databricks_event("CONNECTION_POOL", "HEALTH_CHECK", f"Health check passed ({duration_ms:.1f}ms)")
            else:
                self._conn_bad[conn_id] = True
                record_operation("health_check", duration_ms, False, "SELECT 1 returned no result")
                
            return is_healthy
            
        except Exception as e:
            duration_ms = (time.time() - start_time) * 1000
            record_operation("health_check", duration_ms, False, str(e))
            log_databricks_event("CONNECTION_POOL", "HEALTH_CHECK", f"Health check failed: {str(e)}", "WARNING")
            self._conn_bad[conn_id] = True
            return False

    def get_connection(self, timeout: float = 10.0) -> Connection:
        """
        Get a connection from the pool with performance monitoring.

        Args:
            timeout: Maximum time to wait for a connection

        Returns:
            Connection: A Databricks SQL connection

        Raises:
            TimeoutError: If no connection available within timeout
        """
        start_time = time.time()
        
        try:
            # Try to get existing connection from pool
            connection = self._pool.get(block=False)
            
            # Quick validation using cached health status
            if self._validate_connection(connection):
                duration_ms = (time.time() - start_time) * 1000
                record_operation("get_connection_from_pool", duration_ms, True)
                return connection
            else:
                # Connection is invalid, decrement count and try creating new one
                with self._lock:
                    self._created_connections -= 1
                try:
                    connection.close()
                except Exception:
                    pass
                    
        except Empty:
            # No connections available in pool
            pass

        # Create new connection or wait for one
        with self._lock:
            if self._created_connections < self.max_connections:
                self._created_connections += 1
                try:
                    connection = self._create_connection()
                    duration_ms = (time.time() - start_time) * 1000
                    record_operation("get_connection_new", duration_ms, True)
                    return connection
                except Exception as e:
                    self._created_connections -= 1
                    raise

        # Pool is full, wait for a connection to be returned
        try:
            connection = self._pool.get(timeout=timeout)
            duration_ms = (time.time() - start_time) * 1000
            record_operation("get_connection_wait", duration_ms, True)
            return connection
        except Empty:
            duration_ms = (time.time() - start_time) * 1000
            record_operation("get_connection_wait", duration_ms, False, "Timeout waiting for connection")
            raise TimeoutError(f"No connection available within {timeout} seconds")

    def return_connection(self, connection: Connection) -> None:
        """
        Return a connection to the pool with validation.

        Args:
            connection: The connection to return
        """
        start_time = time.time()
        
        conn_id = id(connection)
        try:
            # Validate connection before returning (uses cached health if available)
            if self._validate_connection(connection):
                # Connection is healthy, return to pool
                try:
                    self._pool.put(connection, block=False)
                except Exception:
                    # Pool is full; close and decrement to avoid leaks/state drift.
                    try:
                        connection.close()
                    except Exception:
                        pass
                    with self._lock:
                        self._created_connections -= 1
                    self._conn_validated_at.pop(conn_id, None)
                    self._conn_bad.pop(conn_id, None)
                    duration_ms = (time.time() - start_time) * 1000
                    record_operation("return_connection", duration_ms, False, "Pool full; closed connection")
                    return
                duration_ms = (time.time() - start_time) * 1000
                record_operation("return_connection", duration_ms, True)
            else:
                # Connection is broken, don't return to pool
                with self._lock:
                    self._created_connections -= 1
                try:
                    connection.close()
                except Exception:
                    pass
                self._conn_validated_at.pop(conn_id, None)
                self._conn_bad.pop(conn_id, None)
                duration_ms = (time.time() - start_time) * 1000
                record_operation("return_connection", duration_ms, False, "Connection validation failed")
                
        except Exception as e:
            # Error during validation, assume connection is broken
            with self._lock:
                self._created_connections -= 1
            try:
                connection.close()
            except Exception:
                pass
            self._conn_validated_at.pop(conn_id, None)
            self._conn_bad.pop(conn_id, None)
            duration_ms = (time.time() - start_time) * 1000
            record_operation("return_connection", duration_ms, False, str(e))

    def get_pool_stats(self) -> Dict[str, Any]:
        """Get detailed pool statistics."""
        with self._lock:
            available_connections = self._pool.qsize()
            
            return {
                'max_connections': self.max_connections,
                'created_connections': self._created_connections,
                'available_connections': available_connections,
                'active_connections': self._created_connections - available_connections,
                'pool_utilization_percent': (self._created_connections / self.max_connections * 100) if self.max_connections > 0 else 0,
                'health_check_interval_seconds': self.health_check_interval
            }

    def clear_health_cache(self) -> bool:
        """Clear cached health status to force fresh health check."""
        cache_manager = get_cache_manager()
        cleared = cache_manager.invalidate('health', 'connection')
        if cleared:
            log_databricks_event("CONNECTION_POOL", "CACHE_CLEAR", "Health cache cleared")
        return cleared > 0

    def close_all(self) -> None:
        """Close all connections in the pool."""
        start_time = time.time()
        closed_count = 0
        
        while not self._pool.empty():
            try:
                connection = self._pool.get(block=False)
                connection.close()
                closed_count += 1
            except Empty:
                break
            except Exception as e:
                log_databricks_event("CONNECTION_POOL", "CLOSE_ERROR", f"Error closing connection: {str(e)}", "WARNING")

        with self._lock:
            self._created_connections = 0

        duration_ms = (time.time() - start_time) * 1000
        record_operation("close_all_connections", duration_ms, True)
        log_databricks_event("CONNECTION_POOL", "CLOSE_ALL", f"Closed {closed_count} connections ({duration_ms:.1f}ms)")


# Global connection pool instance
_connection_pool: Optional[ConnectionPool] = None
_pool_lock = threading.Lock()


def get_pool() -> ConnectionPool:
    """Get the global connection pool instance."""
    global _connection_pool
    
    if _connection_pool is None:
        with _pool_lock:
            if _connection_pool is None:
                max_conn = int(os.getenv("MAX_CONNECTIONS", "10"))
                health_interval = int(os.getenv("HEALTH_CHECK_CACHE_TTL", "300"))  # 5 minutes
                _connection_pool = ConnectionPool(max_connections=max_conn, health_check_interval=health_interval)
                
    return _connection_pool


class PooledConnection:
    """Context manager for pooled connections with performance tracking."""

    def __init__(self, pool: ConnectionPool):
        self.pool = pool
        self.connection: Optional[Connection] = None
        self.start_time = 0.0

    def __enter__(self) -> Connection:
        self.start_time = time.time()
        self.connection = self.pool.get_connection()
        return self.connection

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        if self.connection:
            self.pool.return_connection(self.connection)
            
            # Record total connection usage time
            duration_ms = (time.time() - self.start_time) * 1000
            success = exc_type is None
            error_msg = str(exc_val) if exc_val else None
            record_operation("pooled_connection_usage", duration_ms, success, error_msg)
