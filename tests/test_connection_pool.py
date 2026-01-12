"""
Test connection pool functionality
"""

import pytest
import os
import threading
import time
from unittest.mock import patch, MagicMock
from src.mcp_server.connection_pool import ConnectionPool, PooledConnection, get_pool


class TestConnectionPool:
    """Test cases for ConnectionPool"""

    def test_connection_pool_creation(self):
        """Test that connection pool can be created with proper environment"""
        pool = ConnectionPool(
            host="test.databricks.com",
            token="test_token",
            http_path="/sql/1.0/warehouses/test",
            max_connections=5,
            workspace_name="default",
        )
        assert pool.max_connections == 5
        assert pool._created_connections == 0

    def test_missing_credentials_raises_error(self):
        """Test that missing credentials raise ValueError"""
        with pytest.raises(ValueError, match="Missing required Databricks credentials"):
            ConnectionPool(host="", token="", http_path="", max_connections=1)

    @patch("src.mcp_server.connection_pool.connect")
    def test_get_connection_creates_new_when_pool_empty(self, mock_connect):
        """Test that new connection is created when pool is empty"""
        mock_connection = MagicMock()
        mock_connect.return_value = mock_connection
        pool = ConnectionPool(
            host="test.databricks.com",
            token="test_token",
            http_path="/sql/1.0/warehouses/test",
            max_connections=5,
            workspace_name="default",
        )

        connection = pool.get_connection()

        assert connection == mock_connection
        assert pool._created_connections == 1
        mock_connect.assert_called_once()

    @patch("src.mcp_server.connection_pool.connect")
    def test_return_connection_adds_to_pool(self, mock_connect):
        """Test that returning a healthy connection adds it back to pool"""
        mock_connection = MagicMock()
        mock_cursor = MagicMock()
        mock_connection.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_connection
        pool = ConnectionPool(
            host="test.databricks.com",
            token="test_token",
            http_path="/sql/1.0/warehouses/test",
            max_connections=5,
            workspace_name="default",
        )

        # Get a connection
        connection = pool.get_connection()

        # Return it
        pool.return_connection(connection)

        # Verify it's in the pool by getting it again without creating new one
        returned_connection = pool.get_connection()
        assert returned_connection == mock_connection

        # Should only create one connection total
        assert mock_connect.call_count == 1

    def test_pooled_connection_context_manager(self):
        """Test PooledConnection context manager"""
        mock_pool = MagicMock()
        mock_connection = MagicMock()
        mock_pool.get_connection.return_value = mock_connection

        with PooledConnection(mock_pool) as conn:
            assert conn == mock_connection
            mock_pool.get_connection.assert_called_once()

        # Should return connection after context
        mock_pool.return_connection.assert_called_once_with(mock_connection)

    def test_concurrent_connections(self):
        """Test that pool handles concurrent requests correctly"""
        pool = ConnectionPool(
            host="test.databricks.com",
            token="test_token",
            http_path="/sql/1.0/warehouses/test",
            max_connections=3,
            workspace_name="default",
        )
        connections = []
        errors = []

        def get_connection_worker():
            try:
                with patch("src.mcp_server.connection_pool.connect") as mock_connect:
                    mock_connect.return_value = MagicMock()
                    conn = pool.get_connection(timeout=1.0)
                    connections.append(conn)
                    time.sleep(0.1)  # Hold connection briefly
                    pool.return_connection(conn)
            except Exception as e:
                errors.append(e)

        # Start multiple threads
        threads = []
        for _ in range(5):
            thread = threading.Thread(target=get_connection_worker)
            threads.append(thread)
            thread.start()

        # Wait for all threads
        for thread in threads:
            thread.join()

        # Should not have errors and should have created max 3 connections
        assert len(errors) == 0
        assert pool._created_connections <= 3


def test_get_pool_singleton():
    """Test that get_pool returns the same instance"""
    with patch.dict(
        os.environ,
        {
            "DATABRICKS_HOST": "test.databricks.com",
            "DATABRICKS_TOKEN": "test_token",
            "DATABRICKS_HTTP_PATH": "/sql/1.0/warehouses/test",
        },
    ):
        pool1 = get_pool()
        pool2 = get_pool()

        assert pool1 is pool2
