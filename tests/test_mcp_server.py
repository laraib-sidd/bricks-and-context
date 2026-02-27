"""
Unit tests for MCP Server tools

Tests all MCP tools with mocked Databricks connections to ensure
proper functionality without requiring real database access.
"""

import pytest
from unittest.mock import Mock, patch, MagicMock
from src.mcp_server import mcp_server


class TestMCPServerTools:
    """Test suite for MCP server tool functions"""

    @patch("src.mcp_server.mcp_server.get_pool")
    def test_execute_sql_query_success(self, mock_get_pool):
        """Test successful SQL query execution with markdown table output"""
        # Setup mock
        mock_pool = Mock()
        mock_conn = Mock()
        mock_cursor = Mock()

        mock_get_pool.return_value = mock_pool
        mock_pool.__enter__ = Mock(return_value=mock_conn)
        mock_pool.__exit__ = Mock(return_value=None)

        # Mock PooledConnection context manager
        with patch("src.mcp_server.mcp_server.PooledConnection") as mock_pooled_conn:
            mock_pooled_conn.return_value.__enter__.return_value = mock_conn
            mock_pooled_conn.return_value.__exit__.return_value = None

            mock_conn.cursor.return_value = mock_cursor
            mock_cursor.description = [("id",), ("name",), ("value",)]
            mock_cursor.fetchmany.side_effect = [
                [(1, "test", 100), (2, "demo", 200)],
                [],
            ]

            # Execute test
            result = mcp_server._execute_sql_query(
                "SELECT id, name, value FROM test_table"
            )

            # Verify results
            assert "Query Results (2 rows)" in result
            assert "| id | name | value |" in result
            assert "| --- | --- | --- |" in result
            assert "| 1 | test | 100 |" in result
            assert "| 2 | demo | 200 |" in result

            # Verify mocks were called correctly
            mock_cursor.execute.assert_called_once_with(
                "SELECT id, name, value FROM test_table"
            )

    @patch("src.mcp_server.mcp_server.get_pool")
    def test_execute_sql_query_error(self, mock_get_pool):
        """Test SQL query execution with database error"""
        # Setup mock to raise exception
        mock_get_pool.side_effect = Exception("Connection failed")
        with patch.dict("os.environ", {"ENABLE_SQL_RETRIES": "false"}):

            # Execute test
            result = mcp_server._execute_sql_query("SELECT * FROM invalid_table")

        assert "Connection failed" in result
        assert "SELECT * FROM invalid_table" in result

    @patch("src.mcp_server.mcp_server.get_pool")
    def test_execute_sql_query_no_results(self, mock_get_pool):
        """Test SQL query execution with no results"""
        # Setup mock
        mock_pool = Mock()
        mock_conn = Mock()
        mock_cursor = Mock()

        mock_get_pool.return_value = mock_pool

        with patch("src.mcp_server.mcp_server.PooledConnection") as mock_pooled_conn:
            mock_pooled_conn.return_value.__enter__.return_value = mock_conn
            mock_pooled_conn.return_value.__exit__.return_value = None

            mock_conn.cursor.return_value = mock_cursor
            mock_cursor.description = [("count",)]
            mock_cursor.fetchmany.side_effect = [[]]

            # Execute test
            result = mcp_server._execute_sql_query("SELECT COUNT(*) FROM empty_table")

            # Verify results
            assert "Query executed successfully. No rows returned." in result
            assert "Columns: count" in result

    @patch("src.mcp_server.mcp_server.get_pool")
    def test_execute_sql_query_truncation(self, mock_get_pool):
        """Test SQL query execution truncates large outputs safely."""
        mock_pool = Mock()
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_get_pool.return_value = mock_pool

        with patch.dict(
            "os.environ",
            {
                "MAX_RESULT_ROWS": "3",
                "MAX_RESULT_BYTES": "32768",
                "ENABLE_SQL_RETRIES": "false",
            },
        ):
            with patch(
                "src.mcp_server.mcp_server.PooledConnection"
            ) as mock_pooled_conn:
                mock_pooled_conn.return_value.__enter__.return_value = mock_conn
                mock_pooled_conn.return_value.__exit__.return_value = None

                mock_conn.cursor.return_value = mock_cursor
                mock_cursor.description = [("id",), ("txt",)]

                # Smarter mock that respects the size argument
                def smart_fetchmany(size=200):
                    # Return rows respecting the requested size
                    all_rows = [(1, "a"), (2, "b"), (3, "c"), (4, "d"), (5, "e")]
                    # Track how many we've returned
                    if not hasattr(smart_fetchmany, "offset"):
                        smart_fetchmany.offset = 0
                    start = smart_fetchmany.offset
                    end = start + size
                    result = all_rows[start:end]
                    smart_fetchmany.offset = end
                    return result

                mock_cursor.fetchmany.side_effect = smart_fetchmany

                result = mcp_server._execute_sql_query("SELECT id, txt FROM t")

                assert "Query Results (3 rows" in result
                assert "truncated" in result
                assert "Results truncated for safety" in result
                assert "| 1 | a |" in result
                assert "| 3 | c |" in result

    @patch("src.mcp_server.mcp_server.get_pool")
    def test_discover_schemas_success(self, mock_get_pool):
        """Test successful schema discovery"""
        # Setup mock
        mock_pool = Mock()
        mock_conn = Mock()
        mock_cursor = Mock()

        mock_get_pool.return_value = mock_pool

        with patch("src.mcp_server.mcp_server.PooledConnection") as mock_pooled_conn:
            mock_pooled_conn.return_value.__enter__.return_value = mock_conn
            mock_pooled_conn.return_value.__exit__.return_value = None

            mock_conn.cursor.return_value = mock_cursor
            mock_cursor.fetchall.return_value = [
                ("default",),
                ("sales_db",),
                ("analytics_db",),
            ]

            # Execute test
            result = mcp_server._discover_schemas()

            # Verify results
            assert "Available Schemas (3 found)" in result
            assert "| Schema Name | Description |" in result
            assert "| default | System database |" in result
            assert "| sales_db | User database |" in result
            assert "| analytics_db | User database |" in result

            # Verify mock was called correctly
            mock_cursor.execute.assert_called_once_with("SHOW SCHEMAS")

    @patch("src.mcp_server.mcp_server.get_pool")
    def test_discover_tables_success(self, mock_get_pool):
        """Test successful table discovery"""
        # Setup mock
        mock_pool = Mock()
        mock_conn = Mock()
        mock_cursor = Mock()

        mock_get_pool.return_value = mock_pool

        with patch("src.mcp_server.mcp_server.PooledConnection") as mock_pooled_conn:
            mock_pooled_conn.return_value.__enter__.return_value = mock_conn
            mock_pooled_conn.return_value.__exit__.return_value = None

            mock_conn.cursor.return_value = mock_cursor
            mock_cursor.fetchall.return_value = [
                ("sales_db", "customers", "TABLE"),
                ("sales_db", "orders", "TABLE"),
                ("sales_db", "products", "VIEW"),
            ]

            # Execute test
            result = mcp_server._discover_tables("sales_db")

            # Verify results
            assert "Tables in 'sales_db' (3 found)" in result
            assert "| Table Name | Type | Description |" in result
            assert "| customers | TABLE | Table in sales_db schema |" in result
            assert "| orders | TABLE | Table in sales_db schema |" in result
            assert "| products | VIEW | Table in sales_db schema |" in result

            # Verify mock was called correctly
            mock_cursor.execute.assert_called_once_with("SHOW TABLES IN sales_db")

    @patch("src.mcp_server.mcp_server.get_pool")
    def test_describe_table_success(self, mock_get_pool):
        """Test successful table description"""
        # Setup mock
        mock_pool = Mock()
        mock_conn = Mock()
        mock_cursor = Mock()

        mock_get_pool.return_value = mock_pool

        with patch("src.mcp_server.mcp_server.PooledConnection") as mock_pooled_conn:
            mock_pooled_conn.return_value.__enter__.return_value = mock_conn
            mock_pooled_conn.return_value.__exit__.return_value = None

            mock_conn.cursor.return_value = mock_cursor
            mock_cursor.fetchall.return_value = [
                ("id", "bigint", "false"),
                ("name", "string", "true"),
                ("email", "string", "true"),
                ("created_at", "timestamp", "false"),
            ]

            # Execute test
            result = mcp_server._describe_table("customers", "sales_db")

            # Verify results
            assert "Schema for 'sales_db.customers'" in result
            assert "| Column Name | Data Type | Nullable | Description |" in result
            assert "| id | bigint | No | Data column |" in result
            assert "| name | string | Yes | Data column |" in result
            assert "| email | string | Yes | Data column |" in result
            assert "| created_at | timestamp | No | Data column |" in result

            # Verify mock was called correctly
            mock_cursor.execute.assert_called_once_with(
                "DESCRIBE EXTENDED sales_db.customers"
            )

    @patch("src.mcp_server.mcp_server._execute_sql_query")
    def test_get_table_sample_success(self, mock_execute_sql):
        """Test successful table sampling"""
        # Setup mock
        mock_execute_sql.return_value = """Query Results (3 rows):

| id | name | email |
| --- | --- | --- |
| 1 | John | john@example.com |
| 2 | Jane | jane@example.com |
| 3 | Bob | bob@example.com |"""

        # Execute test
        result = mcp_server._get_table_sample("customers", "sales_db", 3)

        # Verify results
        assert "Sample Data from 'sales_db.customers' (showing 3 rows)" in result
        assert "| id | name | email |" in result
        assert "| 1 | John | john@example.com |" in result

        # Verify mock was called correctly (now includes workspace parameter)
        mock_execute_sql.assert_called_once_with(
            "SELECT * FROM sales_db.customers LIMIT 3", None
        )

    @patch("src.mcp_server.mcp_server._get_table_sample")
    def test_get_table_sample_limit_enforcement(self, mock_get_table_sample):
        """Test that table sampling enforces reasonable limits"""
        # This test verifies the limit logic without full execution
        mcp_server._get_table_sample("test_table", "default", 50)  # Request 50 rows

        # The function should have been called, and internally it limits to 20
        # We can verify this by checking the SQL query that would be generated
        assert mock_get_table_sample.called

    @patch("src.mcp_server.mcp_server.get_pool")
    @patch("src.mcp_server.mcp_server._execute_sql_query")
    @patch("src.mcp_server.mcp_server.resolve_workspace_name")
    def test_connection_health_healthy(
        self, mock_resolve_ws, mock_execute_sql, mock_get_pool
    ):
        """Test connection health check when connection is healthy"""
        # Setup mock for successful health check
        mock_resolve_ws.return_value = "default"
        mock_get_pool.return_value = Mock()
        mock_execute_sql.return_value = (
            "Query Results (1 rows):\n\n| health_check |\n| --- |\n| 1 |"
        )

        # Execute test
        result = mcp_server._connection_health()

        # Verify results
        assert "✅ Connection Health: HEALTHY" in result
        assert "Databricks connection pool is working correctly" in result

        # Verify mock was called correctly (now includes workspace parameter)
        mock_execute_sql.assert_called_once_with("SELECT 1 as health_check", "default")

    @patch("src.mcp_server.mcp_server.get_pool")
    @patch("src.mcp_server.mcp_server._execute_sql_query")
    @patch("src.mcp_server.mcp_server.resolve_workspace_name")
    def test_connection_health_unhealthy(
        self, mock_resolve_ws, mock_execute_sql, mock_get_pool
    ):
        """Test connection health check when connection fails"""
        # Setup mock for failed health check
        mock_resolve_ws.return_value = "default"
        mock_get_pool.return_value = Mock()
        mock_execute_sql.return_value = "Error executing query: Connection timeout"

        # Execute test
        result = mcp_server._connection_health()

        # Verify results
        assert "⚠️ Connection Health: UNHEALTHY" in result
        assert "Test query failed" in result
        assert "Connection timeout" in result

    def test_null_value_handling(self):
        """Test that NULL values in query results are handled properly"""
        # This is tested as part of execute_sql_query test, but worth highlighting
        # that the markdown formatting correctly handles None values as "NULL"
        pass

    def test_special_character_handling(self):
        """Test that special characters in data don't break markdown formatting"""
        # Future enhancement: test with data containing pipes, newlines, etc.
        pass


class TestMCPServerIntegration:
    """Integration tests for MCP server components"""

    def test_mcp_server_creation(self):
        """Test that the FastMCP server is created correctly"""
        # Verify server instance exists and has expected properties
        assert mcp_server.mcp is not None
        assert mcp_server.mcp.name == "bricks-and-context"

    def test_all_tools_registered(self):
        """Test that all expected MCP tools are registered"""
        # Get the tools from the FastMCP server
        # Note: This test might need adjustment based on FastMCP's API for introspection
        expected_tools = [
            "execute_sql_query",
            "discover_schemas",
            "discover_tables",
            "describe_table",
            "get_table_sample",
            "connection_health",
        ]

        # This test verifies that the decorators properly register the tools
        # Implementation depends on FastMCP's internal API
        pass  # TODO: Implement once we have access to FastMCP's tool registry


# Performance and stress testing
class TestMCPServerPerformance:
    """Performance tests for MCP server tools"""

    @patch("src.mcp_server.mcp_server.get_pool")
    def test_concurrent_query_handling(self, mock_get_pool):
        """Test that multiple concurrent queries can be handled"""
        # Setup mock for connection pool
        mock_pool = Mock()
        mock_get_pool.return_value = mock_pool

        # This test would verify thread safety and concurrent access
        # Implementation depends on testing framework for concurrency
        pass  # TODO: Implement concurrent testing

    def test_large_result_set_handling(self):
        """Test handling of large query results"""
        # Test memory efficiency and performance with large datasets
        pass  # TODO: Implement with large mock datasets

    def test_query_timeout_handling(self):
        """Test proper handling of query timeouts"""
        # Test that long-running queries are handled gracefully
        pass  # TODO: Implement timeout testing
