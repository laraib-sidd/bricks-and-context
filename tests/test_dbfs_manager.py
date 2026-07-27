"""Tests for DBFS cluster log tools.

Each tool's core logic is tested with mocked API responses to verify:
- Successful formatting of results into markdown
- Error handling and user-friendly messages
- Edge cases (empty results, 404s, pagination hints)
"""

from unittest.mock import patch, MagicMock

import pytest


class TestListClusterLogFiles:
    @patch("src.mcp_server.dbfs_manager.resolve_workspace_name", return_value="dev")
    @patch("src.mcp_server.dbfs_manager.get_api_client")
    def test_list_cluster_log_files_success(
        self, mock_get_client: MagicMock, mock_ws: MagicMock
    ) -> None:
        from src.mcp_server.dbfs_manager import _list_cluster_log_files

        mock_client = MagicMock()
        mock_client.get.return_value = {
            "files": [
                {
                    "path": "dbfs:/cluster-logs/abc-123/driver/stdout",
                    "file_size": 2048,
                    "modification_time": 1700000000000,
                    "is_dir": False,
                },
                {
                    "path": "dbfs:/cluster-logs/abc-123/driver/subdir",
                    "file_size": 0,
                    "modification_time": 1700000000000,
                    "is_dir": True,
                },
            ]
        }
        mock_get_client.return_value = mock_client

        result = _list_cluster_log_files("abc-123")
        assert "stdout" in result
        assert "2,048" in result
        assert "📁 subdir/" in result
        assert "databricks_read_cluster_log_file" in result

    @patch("src.mcp_server.dbfs_manager.resolve_workspace_name", return_value="dev")
    @patch("src.mcp_server.dbfs_manager.get_api_client")
    def test_list_cluster_log_files_empty(
        self, mock_get_client: MagicMock, mock_ws: MagicMock
    ) -> None:
        from src.mcp_server.dbfs_manager import _list_cluster_log_files

        mock_client = MagicMock()
        mock_client.get.return_value = {"files": []}
        mock_get_client.return_value = mock_client

        result = _list_cluster_log_files("abc-123")
        assert "No log files found" in result

    @patch("src.mcp_server.dbfs_manager.resolve_workspace_name", return_value="dev")
    @patch("src.mcp_server.dbfs_manager.get_api_client")
    def test_list_cluster_log_files_not_found(
        self, mock_get_client: MagicMock, mock_ws: MagicMock
    ) -> None:
        from src.mcp_server.dbfs_manager import _list_cluster_log_files

        mock_client = MagicMock()
        mock_client.get.side_effect = ValueError("client error: HTTP 404 from API")
        mock_get_client.return_value = mock_client

        result = _list_cluster_log_files("missing-cluster")
        assert "not found" in result.lower()

    @patch("src.mcp_server.dbfs_manager.resolve_workspace_name", return_value="dev")
    @patch("src.mcp_server.dbfs_manager.get_api_client")
    def test_list_cluster_log_files_generic_error(
        self, mock_get_client: MagicMock, mock_ws: MagicMock
    ) -> None:
        from src.mcp_server.dbfs_manager import _list_cluster_log_files

        mock_client = MagicMock()
        mock_client.get.side_effect = Exception("boom")
        mock_get_client.return_value = mock_client

        result = _list_cluster_log_files("abc-123")
        assert "Error listing cluster log files" in result
        assert "boom" in result


class TestReadClusterLogFile:
    @patch("src.mcp_server.dbfs_manager.resolve_workspace_name", return_value="dev")
    @patch("src.mcp_server.dbfs_manager.get_api_client")
    def test_read_cluster_log_file_success(
        self, mock_get_client: MagicMock, mock_ws: MagicMock
    ) -> None:
        import base64

        from src.mcp_server.dbfs_manager import _read_cluster_log_file

        content = "line one\nline two\n"
        mock_client = MagicMock()
        mock_client.get.return_value = {
            "data": base64.b64encode(content.encode("utf-8")).decode("ascii"),
            "bytes_read": len(content),
        }
        mock_get_client.return_value = mock_client

        result = _read_cluster_log_file("dbfs:/cluster-logs/abc/driver/stdout")
        assert "line one" in result
        assert "line two" in result
        assert "Bytes Read" in result

    @patch("src.mcp_server.dbfs_manager.resolve_workspace_name", return_value="dev")
    @patch("src.mcp_server.dbfs_manager.get_api_client")
    def test_read_cluster_log_file_empty(
        self, mock_get_client: MagicMock, mock_ws: MagicMock
    ) -> None:
        from src.mcp_server.dbfs_manager import _read_cluster_log_file

        mock_client = MagicMock()
        mock_client.get.return_value = {"data": "", "bytes_read": 0}
        mock_get_client.return_value = mock_client

        result = _read_cluster_log_file("dbfs:/cluster-logs/abc/driver/stdout")
        assert "empty or could not be read" in result

    @patch("src.mcp_server.dbfs_manager.resolve_workspace_name", return_value="dev")
    @patch("src.mcp_server.dbfs_manager.get_api_client")
    def test_read_cluster_log_file_pagination_hint(
        self, mock_get_client: MagicMock, mock_ws: MagicMock
    ) -> None:
        import base64

        from src.mcp_server.dbfs_manager import _read_cluster_log_file

        content = "x" * 100
        mock_client = MagicMock()
        mock_client.get.return_value = {
            "data": base64.b64encode(content.encode("utf-8")).decode("ascii"),
            "bytes_read": 100,
        }
        mock_get_client.return_value = mock_client

        result = _read_cluster_log_file(
            "dbfs:/cluster-logs/abc/driver/stdout", offset=0, max_bytes=100
        )
        assert "next chunk" in result
        assert "offset=100" in result

    @patch("src.mcp_server.dbfs_manager.resolve_workspace_name", return_value="dev")
    @patch("src.mcp_server.dbfs_manager.get_api_client")
    def test_read_cluster_log_file_not_found(
        self, mock_get_client: MagicMock, mock_ws: MagicMock
    ) -> None:
        from src.mcp_server.dbfs_manager import _read_cluster_log_file

        mock_client = MagicMock()
        mock_client.get.side_effect = ValueError("client error: HTTP 404 from API")
        mock_get_client.return_value = mock_client

        result = _read_cluster_log_file("dbfs:/cluster-logs/missing/driver/stdout")
        assert "not found in DBFS" in result
