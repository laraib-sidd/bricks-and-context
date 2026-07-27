"""Tests for Genie (AI/BI) space tools.

Each tool's core logic is tested with mocked API responses to verify:
- Successful formatting of results into markdown
- Error handling and user-friendly messages
- Edge cases (empty results, polling to completion, failed queries)
"""

from unittest.mock import patch, MagicMock

import pytest


class TestListSpaces:
    @patch("src.mcp_server.genie_manager.resolve_workspace_name", return_value="dev")
    @patch("src.mcp_server.genie_manager.get_api_client")
    def test_list_spaces_success(
        self, mock_get_client: MagicMock, mock_ws: MagicMock
    ) -> None:
        from src.mcp_server.genie_manager import _list_spaces

        mock_client = MagicMock()
        mock_client.get.return_value = {
            "spaces": [
                {"space_id": "sp1", "title": "Sales", "description": "Sales data"},
            ]
        }
        mock_get_client.return_value = mock_client

        result = _list_spaces()
        assert "Sales" in result
        assert "sp1" in result

    @patch("src.mcp_server.genie_manager.resolve_workspace_name", return_value="dev")
    @patch("src.mcp_server.genie_manager.get_api_client")
    def test_list_spaces_empty(
        self, mock_get_client: MagicMock, mock_ws: MagicMock
    ) -> None:
        from src.mcp_server.genie_manager import _list_spaces

        mock_client = MagicMock()
        mock_client.get.return_value = {"spaces": []}
        mock_get_client.return_value = mock_client

        result = _list_spaces()
        assert "No Genie spaces found" in result

    @patch("src.mcp_server.genie_manager.resolve_workspace_name", return_value="dev")
    @patch("src.mcp_server.genie_manager.get_api_client")
    def test_list_spaces_error(
        self, mock_get_client: MagicMock, mock_ws: MagicMock
    ) -> None:
        from src.mcp_server.genie_manager import _list_spaces

        mock_client = MagicMock()
        mock_client.get.side_effect = Exception("boom")
        mock_get_client.return_value = mock_client

        result = _list_spaces()
        assert "Error listing Genie spaces" in result


class TestGetSpace:
    @patch("src.mcp_server.genie_manager.resolve_workspace_name", return_value="dev")
    @patch("src.mcp_server.genie_manager.get_api_client")
    def test_get_space_with_data_sources(
        self, mock_get_client: MagicMock, mock_ws: MagicMock
    ) -> None:
        import json

        from src.mcp_server.genie_manager import _get_space

        serialized = json.dumps(
            {
                "data_sources": {
                    "tables": [
                        {
                            "identifier": "main.sales.orders",
                            "description": ["Orders table"],
                        }
                    ]
                },
                "config": {"sample_questions": [{"question": ["What were", "sales?"]}]},
            }
        )
        mock_client = MagicMock()
        mock_client.get.return_value = {
            "title": "Sales Space",
            "description": "Sales analytics",
            "warehouse_id": "wh1",
            "serialized_space": serialized,
        }
        mock_get_client.return_value = mock_client

        result = _get_space("sp1")
        assert "Sales Space" in result
        assert "main.sales.orders" in result
        assert "What were sales?" in result

    @patch("src.mcp_server.genie_manager.resolve_workspace_name", return_value="dev")
    @patch("src.mcp_server.genie_manager.get_api_client")
    def test_get_space_minimal(
        self, mock_get_client: MagicMock, mock_ws: MagicMock
    ) -> None:
        from src.mcp_server.genie_manager import _get_space

        mock_client = MagicMock()
        mock_client.get.return_value = {"title": "Empty Space"}
        mock_get_client.return_value = mock_client

        result = _get_space("sp1")
        assert "Empty Space" in result


class TestStartConversation:
    @patch("src.mcp_server.genie_manager.resolve_workspace_name", return_value="dev")
    @patch("src.mcp_server.genie_manager.get_api_client")
    def test_start_conversation_immediate_completion(
        self, mock_get_client: MagicMock, mock_ws: MagicMock
    ) -> None:
        from src.mcp_server.genie_manager import _start_conversation

        mock_client = MagicMock()
        mock_client.post.return_value = {
            "conversation": {"id": "conv1"},
            "message": {
                "id": "msg1",
                "status": "COMPLETED",
                "content": "What were sales?",
                "attachments": [],
            },
        }
        mock_get_client.return_value = mock_client

        result = _start_conversation("sp1", "What were sales?")
        assert "COMPLETED" in result
        mock_client.get.assert_not_called()

    @patch("src.mcp_server.genie_manager.time.sleep", return_value=None)
    @patch("src.mcp_server.genie_manager.resolve_workspace_name", return_value="dev")
    @patch("src.mcp_server.genie_manager.get_api_client")
    def test_start_conversation_polls_until_complete(
        self, mock_get_client: MagicMock, mock_ws: MagicMock, mock_sleep: MagicMock
    ) -> None:
        from src.mcp_server.genie_manager import _start_conversation

        mock_client = MagicMock()
        mock_client.post.return_value = {
            "conversation": {"id": "conv1"},
            "message": {"id": "msg1", "status": "IN_PROGRESS"},
        }
        mock_client.get.side_effect = [
            {"status": "IN_PROGRESS", "id": "msg1"},
            {"status": "COMPLETED", "id": "msg1", "content": "done", "attachments": []},
        ]
        mock_get_client.return_value = mock_client

        result = _start_conversation("sp1", "What were sales?")
        assert "COMPLETED" in result
        assert mock_client.get.call_count == 2

    @patch("src.mcp_server.genie_manager.resolve_workspace_name", return_value="dev")
    @patch("src.mcp_server.genie_manager.get_api_client")
    def test_start_conversation_missing_ids(
        self, mock_get_client: MagicMock, mock_ws: MagicMock
    ) -> None:
        from src.mcp_server.genie_manager import _start_conversation

        mock_client = MagicMock()
        mock_client.post.return_value = {"conversation": {}, "message": {}}
        mock_get_client.return_value = mock_client

        result = _start_conversation("sp1", "What were sales?")
        assert "Could not extract conversation/message IDs" in result

    @patch("src.mcp_server.genie_manager.resolve_workspace_name", return_value="dev")
    @patch("src.mcp_server.genie_manager.get_api_client")
    def test_start_conversation_error(
        self, mock_get_client: MagicMock, mock_ws: MagicMock
    ) -> None:
        from src.mcp_server.genie_manager import _start_conversation

        mock_client = MagicMock()
        mock_client.post.side_effect = Exception("boom")
        mock_get_client.return_value = mock_client

        result = _start_conversation("sp1", "What were sales?")
        assert "Error starting Genie conversation" in result


class TestCreateMessage:
    @patch("src.mcp_server.genie_manager.resolve_workspace_name", return_value="dev")
    @patch("src.mcp_server.genie_manager.get_api_client")
    def test_create_message_immediate_completion(
        self, mock_get_client: MagicMock, mock_ws: MagicMock
    ) -> None:
        from src.mcp_server.genie_manager import _create_message

        mock_client = MagicMock()
        mock_client.post.return_value = {
            "id": "msg2",
            "status": "COMPLETED",
            "content": "follow-up",
            "attachments": [],
        }
        mock_get_client.return_value = mock_client

        result = _create_message("sp1", "conv1", "And last month?")
        assert "COMPLETED" in result

    @patch("src.mcp_server.genie_manager.resolve_workspace_name", return_value="dev")
    @patch("src.mcp_server.genie_manager.get_api_client")
    def test_create_message_missing_id(
        self, mock_get_client: MagicMock, mock_ws: MagicMock
    ) -> None:
        from src.mcp_server.genie_manager import _create_message

        mock_client = MagicMock()
        mock_client.post.return_value = {}
        mock_get_client.return_value = mock_client

        result = _create_message("sp1", "conv1", "And last month?")
        assert "Could not extract message ID" in result


class TestGetMessage:
    @patch("src.mcp_server.genie_manager.resolve_workspace_name", return_value="dev")
    @patch("src.mcp_server.genie_manager.get_api_client")
    def test_get_message_with_sql_attachment(
        self, mock_get_client: MagicMock, mock_ws: MagicMock
    ) -> None:
        from src.mcp_server.genie_manager import _get_message

        mock_client = MagicMock()
        mock_client.get.return_value = {
            "id": "msg1",
            "status": "COMPLETED",
            "content": "What were sales?",
            "attachments": [
                {
                    "type": "QUERY",
                    "id": "att1",
                    "query": {"query": "SELECT 1", "description": "test query"},
                }
            ],
        }
        mock_get_client.return_value = mock_client

        result = _get_message("sp1", "conv1", "msg1")
        assert "SELECT 1" in result
        assert "test query" in result

    @patch("src.mcp_server.genie_manager.resolve_workspace_name", return_value="dev")
    @patch("src.mcp_server.genie_manager.get_api_client")
    def test_get_message_failed(
        self, mock_get_client: MagicMock, mock_ws: MagicMock
    ) -> None:
        from src.mcp_server.genie_manager import _get_message

        mock_client = MagicMock()
        mock_client.get.return_value = {
            "id": "msg1",
            "status": "FAILED",
            "content": "bad question",
            "error": {"message": "could not parse"},
        }
        mock_get_client.return_value = mock_client

        result = _get_message("sp1", "conv1", "msg1")
        assert "FAILED" in result
        assert "could not parse" in result


class TestGetQueryResult:
    @patch("src.mcp_server.genie_manager.resolve_workspace_name", return_value="dev")
    @patch("src.mcp_server.genie_manager.get_api_client")
    def test_get_query_result_success(
        self, mock_get_client: MagicMock, mock_ws: MagicMock
    ) -> None:
        from src.mcp_server.genie_manager import _get_query_result

        mock_client = MagicMock()
        mock_client.get.return_value = {
            "statement_response": {
                "status": {"state": "SUCCEEDED"},
                "manifest": {"schema": {"columns": [{"name": "total"}]}},
                "result": {"data_array": [["100"]], "row_count": 1},
            }
        }
        mock_get_client.return_value = mock_client

        result = _get_query_result("sp1", "conv1", "msg1", "att1")
        assert "total" in result
        assert "100" in result

    @patch("src.mcp_server.genie_manager.resolve_workspace_name", return_value="dev")
    @patch("src.mcp_server.genie_manager.get_api_client")
    def test_get_query_result_no_rows(
        self, mock_get_client: MagicMock, mock_ws: MagicMock
    ) -> None:
        from src.mcp_server.genie_manager import _get_query_result

        mock_client = MagicMock()
        mock_client.get.return_value = {
            "statement_response": {
                "status": {"state": "SUCCEEDED"},
                "manifest": {"schema": {"columns": []}},
                "result": {"data_array": [], "row_count": 0},
            }
        }
        mock_get_client.return_value = mock_client

        result = _get_query_result("sp1", "conv1", "msg1", "att1")
        assert "no rows" in result.lower()

    @patch("src.mcp_server.genie_manager.resolve_workspace_name", return_value="dev")
    @patch("src.mcp_server.genie_manager.get_api_client")
    def test_get_query_result_expired(
        self, mock_get_client: MagicMock, mock_ws: MagicMock
    ) -> None:
        from src.mcp_server.genie_manager import _get_query_result

        mock_client = MagicMock()
        mock_client.get.return_value = {
            "statement_response": {"status": {"state": "QUERY_RESULT_EXPIRED"}}
        }
        mock_get_client.return_value = mock_client

        result = _get_query_result("sp1", "conv1", "msg1", "att1")
        assert "QUERY_RESULT_EXPIRED" in result


class TestExecuteQuery:
    @patch("src.mcp_server.genie_manager.time.sleep", return_value=None)
    @patch("src.mcp_server.genie_manager.resolve_workspace_name", return_value="dev")
    @patch("src.mcp_server.genie_manager.get_api_client")
    def test_execute_query_success(
        self, mock_get_client: MagicMock, mock_ws: MagicMock, mock_sleep: MagicMock
    ) -> None:
        from src.mcp_server.genie_manager import _execute_query

        mock_client = MagicMock()
        mock_client.get.return_value = {
            "status": "COMPLETED",
            "id": "msg1",
            "content": "re-run",
            "attachments": [],
        }
        mock_get_client.return_value = mock_client

        result = _execute_query("sp1", "conv1", "msg1", "att1")
        assert "COMPLETED" in result
        mock_client.post.assert_called_once()


class TestAskQuestion:
    @patch("src.mcp_server.genie_manager.resolve_workspace_name", return_value="dev")
    @patch("src.mcp_server.genie_manager.get_api_client")
    def test_ask_question_new_conversation_no_attachments(
        self, mock_get_client: MagicMock, mock_ws: MagicMock
    ) -> None:
        from src.mcp_server.genie_manager import _ask_question

        mock_client = MagicMock()
        mock_client.post.return_value = {
            "conversation": {"id": "conv1"},
            "message": {
                "id": "msg1",
                "status": "COMPLETED",
                "content": "hello",
                "attachments": [],
            },
        }
        mock_get_client.return_value = mock_client

        result = _ask_question("sp1", "hello")
        assert "COMPLETED" in result
        mock_client.get.assert_not_called()

    @patch("src.mcp_server.genie_manager.resolve_workspace_name", return_value="dev")
    @patch("src.mcp_server.genie_manager.get_api_client")
    def test_ask_question_follow_up_with_query_result(
        self, mock_get_client: MagicMock, mock_ws: MagicMock
    ) -> None:
        from src.mcp_server.genie_manager import _ask_question

        mock_client = MagicMock()
        mock_client.post.return_value = {
            "id": "msg2",
            "status": "COMPLETED",
            "content": "sales?",
            "attachments": [
                {"type": "QUERY", "id": "att1", "query": {"query": "SELECT 1"}}
            ],
        }
        mock_client.get.return_value = {
            "statement_response": {
                "status": {"state": "SUCCEEDED"},
                "manifest": {"schema": {"columns": [{"name": "total"}]}},
                "result": {"data_array": [["42"]], "row_count": 1},
            }
        }
        mock_get_client.return_value = mock_client

        result = _ask_question("sp1", "sales?", conversation_id="conv1")
        assert "Query Results" in result
        assert "42" in result

    @patch("src.mcp_server.genie_manager.resolve_workspace_name", return_value="dev")
    @patch("src.mcp_server.genie_manager.get_api_client")
    def test_ask_question_missing_ids(
        self, mock_get_client: MagicMock, mock_ws: MagicMock
    ) -> None:
        from src.mcp_server.genie_manager import _ask_question

        mock_client = MagicMock()
        mock_client.post.return_value = {"conversation": {}, "message": {}}
        mock_get_client.return_value = mock_client

        result = _ask_question("sp1", "hello")
        assert "Could not extract IDs" in result

    @patch("src.mcp_server.genie_manager.resolve_workspace_name", return_value="dev")
    @patch("src.mcp_server.genie_manager.get_api_client")
    def test_ask_question_error(
        self, mock_get_client: MagicMock, mock_ws: MagicMock
    ) -> None:
        from src.mcp_server.genie_manager import _ask_question

        mock_client = MagicMock()
        mock_client.post.side_effect = Exception("boom")
        mock_get_client.return_value = mock_client

        result = _ask_question("sp1", "hello")
        assert "Error in Genie ask_question" in result
