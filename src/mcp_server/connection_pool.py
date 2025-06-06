"""
Basic Connection Pool for Databricks SQL connections
Optimized for AI request patterns with burst handling
"""

import os
import time
import threading
from queue import Queue, Empty
from typing import Optional
from databricks.sql import connect
from databricks.sql.client import Connection
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()


class ConnectionPool:
    """Thread-safe connection pool for Databricks SQL connections"""
    
    def __init__(self, max_connections: int = 10):
        self.max_connections = max_connections
        self._pool: Queue[Connection] = Queue(maxsize=max_connections)
        self._created_connections = 0
        self._lock = threading.Lock()
        
        # Databricks connection config
        self.host = os.getenv("DATABRICKS_HOST")
        self.token = os.getenv("DATABRICKS_TOKEN") 
        self.http_path = os.getenv("DATABRICKS_HTTP_PATH")
        
        if not all([self.host, self.token, self.http_path]):
            raise ValueError("Missing required Databricks credentials in environment")
    
    def _create_connection(self) -> Connection:
        """Create a new Databricks SQL connection"""
        return connect(
            server_hostname=self.host,
            http_path=self.http_path,
            access_token=self.token
        )
    
    def get_connection(self, timeout: float = 10.0) -> Connection:
        """
        Get a connection from the pool
        
        Args:
            timeout: Maximum time to wait for a connection
            
        Returns:
            Connection: A Databricks SQL connection
            
        Raises:
            TimeoutError: If no connection available within timeout
        """
        try:
            # Try to get existing connection from pool
            return self._pool.get(block=False)
        except Empty:
            # No connections available, try to create new one
            with self._lock:
                if self._created_connections < self.max_connections:
                    self._created_connections += 1
                    return self._create_connection()
            
            # Pool is full, wait for a connection to be returned
            try:
                return self._pool.get(timeout=timeout)
            except Empty:
                raise TimeoutError(f"No connection available within {timeout} seconds")
    
    def return_connection(self, connection: Connection) -> None:
        """
        Return a connection to the pool
        
        Args:
            connection: The connection to return
        """
        try:
            # Test if connection is still valid
            cursor = connection.cursor()
            cursor.execute("SELECT 1")
            cursor.close()
            
            # Connection is healthy, return to pool
            self._pool.put(connection, block=False)
        except:
            # Connection is broken, don't return to pool
            # This will allow a new connection to be created
            with self._lock:
                self._created_connections -= 1
    
    def close_all(self) -> None:
        """Close all connections in the pool"""
        while not self._pool.empty():
            try:
                connection = self._pool.get(block=False)
                connection.close()
            except Empty:
                break
        
        with self._lock:
            self._created_connections = 0


# Global connection pool instance
_connection_pool: Optional[ConnectionPool] = None


def get_pool() -> ConnectionPool:
    """Get the global connection pool instance"""
    global _connection_pool
    if _connection_pool is None:
        max_conn = int(os.getenv("MAX_CONNECTIONS", "10"))
        _connection_pool = ConnectionPool(max_connections=max_conn)
    return _connection_pool


class PooledConnection:
    """Context manager for pooled connections"""
    
    def __init__(self, pool: ConnectionPool):
        self.pool = pool
        self.connection: Optional[Connection] = None
    
    def __enter__(self) -> Connection:
        self.connection = self.pool.get_connection()
        return self.connection
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.connection:
            self.pool.return_connection(self.connection) 