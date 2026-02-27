"""Tests for the shared Databricks API client."""

import os
from unittest.mock import patch, MagicMock

import pytest

from src.mcp_server.api_client import DatabricksAPIClient, get_api_client


class TestDatabricksAPIClient:
    def _make_client(self) -> DatabricksAPIClient:
        return DatabricksAPIClient(
            host="test.cloud.databricks.com",
            token="dapi_test_token",
            workspace_name="test",
        )

    def test_initialization(self) -> None:
        client = self._make_client()
        assert client.host == "test.cloud.databricks.com"
        assert client.token == "dapi_test_token"
        assert client.workspace_name == "test"

    def test_host_strip_trailing_slash(self) -> None:
        client = DatabricksAPIClient(
            host="test.cloud.databricks.com/",
            token="tok",
            workspace_name="t",
        )
        assert client.host == "test.cloud.databricks.com"

    @patch("src.mcp_server.api_client.requests.Session")
    def test_get_success(self, mock_session_cls: MagicMock) -> None:
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.content = b'{"key":"value"}'
        mock_resp.json.return_value = {"key": "value"}
        mock_session = MagicMock()
        mock_session.request.return_value = mock_resp
        mock_session_cls.return_value = mock_session

        client = self._make_client()
        client._session = mock_session
        result = client.get("/test/endpoint", params={"a": "b"})

        assert result == {"key": "value"}
        mock_session.request.assert_called_once()

    @patch("src.mcp_server.api_client.requests.Session")
    def test_post_success(self, mock_session_cls: MagicMock) -> None:
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.content = b'{"run_id": 123}'
        mock_resp.json.return_value = {"run_id": 123}
        mock_session = MagicMock()
        mock_session.request.return_value = mock_resp
        mock_session_cls.return_value = mock_session

        client = self._make_client()
        client._session = mock_session
        result = client.post("/test/start", json_data={"id": 1})

        assert result == {"run_id": 123}

    @patch("src.mcp_server.api_client.requests.Session")
    def test_401_raises_auth_error(self, mock_session_cls: MagicMock) -> None:
        mock_resp = MagicMock()
        mock_resp.status_code = 401
        mock_resp.text = "Unauthorized"
        mock_session = MagicMock()
        mock_session.request.return_value = mock_resp
        mock_session_cls.return_value = mock_session

        client = self._make_client()
        client._session = mock_session
        with pytest.raises(Exception, match="authentication.*401"):
            client.get("/test")

    @patch("src.mcp_server.api_client.requests.Session")
    def test_429_raises_rate_limit(self, mock_session_cls: MagicMock) -> None:
        mock_resp = MagicMock()
        mock_resp.status_code = 429
        mock_resp.text = "Too many requests"
        mock_session = MagicMock()
        mock_session.request.return_value = mock_resp
        mock_session_cls.return_value = mock_session

        client = self._make_client()
        client._session = mock_session
        with pytest.raises(Exception, match="rate limit.*429"):
            client.get("/test")

    @patch("src.mcp_server.api_client.requests.Session")
    def test_empty_response_returns_empty_dict(
        self, mock_session_cls: MagicMock
    ) -> None:
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.content = b""
        mock_session = MagicMock()
        mock_session.request.return_value = mock_resp
        mock_session_cls.return_value = mock_session

        client = self._make_client()
        client._session = mock_session
        assert client.get("/test") == {}
