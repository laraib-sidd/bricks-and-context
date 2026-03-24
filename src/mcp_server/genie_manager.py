"""
Databricks Genie (AI/BI) space tools for the MCP server.

Provides natural language querying via Genie spaces — list spaces,
start/continue conversations, poll for results, and fetch query output.
"""

import json
import time
from typing import Any, Dict, Optional

from .api_client import get_api_client
from .constants import (
    READ_ONLY_ANNOTATIONS,
    enforce_character_limit,
)
from .workspaces import resolve_workspace_name

# ---------------------------------------------------------------------------
# Polling configuration
# ---------------------------------------------------------------------------
GENIE_POLL_INTERVAL = 2  # seconds between polls
GENIE_MAX_POLL_TIME = 120  # max seconds to wait


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _genie_poll_message(
    client: Any,
    space_id: str,
    conversation_id: str,
    message_id: str,
    max_wait: int = GENIE_MAX_POLL_TIME,
) -> Dict[str, Any]:
    """Poll a Genie message until it reaches a terminal status or times out."""
    resp: Dict[str, Any] = {}
    start = time.time()
    while time.time() - start < max_wait:
        resp = client.get(
            f"/genie/spaces/{space_id}/conversations/{conversation_id}/messages/{message_id}",
        )
        status = resp.get("status", "UNKNOWN")
        if status in ("COMPLETED", "FAILED", "CANCELLED", "QUERY_RESULT_EXPIRED"):
            return resp
        time.sleep(GENIE_POLL_INTERVAL)
    return resp


def _format_genie_message(resp: Dict[str, Any]) -> str:
    """Format a Genie message response into a readable markdown string."""
    status = resp.get("status", "UNKNOWN")
    content = resp.get("content", "")
    conversation_id = resp.get("conversation_id", "N/A")
    message_id = resp.get("id", "N/A")

    result = "## Genie Response\n\n"
    result += f"- **Status:** {status}\n"
    result += f"- **Conversation ID:** {conversation_id}\n"
    result += f"- **Message ID:** {message_id}\n"
    result += f"- **Question:** {content}\n\n"

    if status == "FAILED":
        error = resp.get("error", {})
        result += f"**Error:** {json.dumps(error, indent=2)}\n"
        return result

    attachments = resp.get("attachments", []) or []
    if attachments:
        for i, attachment in enumerate(attachments):
            att_type = attachment.get("type", "UNKNOWN")
            att_id = attachment.get("id", "N/A")

            if att_type == "QUERY":
                query_info = attachment.get("query", {})
                sql = query_info.get("query", "N/A")
                description = query_info.get("description", "")
                result += f"### Generated SQL (Attachment {i + 1})\n\n"
                result += f"- **Attachment ID:** {att_id}\n"
                if description:
                    result += f"- **Description:** {description}\n"
                result += f"\n```sql\n{sql}\n```\n\n"

            elif att_type == "TEXT":
                text_content = attachment.get("text", {}).get("content", "")
                result += f"### Response Text\n\n{text_content}\n\n"

            else:
                result += f"### Attachment ({att_type})\n\n"
                result += f"- **Attachment ID:** {att_id}\n"
                result += f"- **Raw:** {json.dumps(attachment, indent=2)}\n\n"
    elif status == "COMPLETED":
        result += "_No attachments in the response._\n"

    return result


def _format_query_result(response: Dict[str, Any]) -> str:
    """Format a Genie query-result response into markdown."""
    statement_response = response.get("statement_response", {})
    status = statement_response.get("status", {}).get("state", "UNKNOWN")

    result = "## Genie Query Result\n\n"
    result += f"- **Status:** {status}\n\n"

    if status == "SUCCEEDED":
        manifest = statement_response.get("manifest", {})
        columns = manifest.get("schema", {}).get("columns", [])
        col_names = [c.get("name", f"col_{i}") for i, c in enumerate(columns)]

        data_array = statement_response.get("result", {}).get("data_array", [])
        if not data_array:
            result += "_Query returned no rows._\n"
            return result

        table = "| " + " | ".join(col_names) + " |\n"
        table += "| " + " | ".join(["---"] * len(col_names)) + " |\n"
        for row in data_array:
            cells = [str(cell) if cell is not None else "NULL" for cell in row]
            table += "| " + " | ".join(cells) + " |\n"

        result += table
        row_count = len(data_array)
        total_row_count = statement_response.get("result", {}).get(
            "row_count", row_count
        )
        result += f"\n_Showing {row_count} of {total_row_count} total rows._\n"
    else:
        result += f"**Raw response:**\n```json\n{json.dumps(response, indent=2)}\n```\n"

    return result


# ---------------------------------------------------------------------------
# Standalone logic (testable without MCP decorators)
# ---------------------------------------------------------------------------


def _list_spaces(workspace: Optional[str] = None) -> str:
    try:
        ws = resolve_workspace_name(workspace)
        client = get_api_client(ws)
        resp = client.get("/genie/spaces")

        spaces = resp.get("spaces", [])
        if not spaces:
            return "No Genie spaces found."

        header = "| Space ID | Title | Description |"
        sep = "| --- | --- | --- |"
        rows = []
        for s in spaces:
            sid = s.get("space_id", "N/A")
            title = s.get("title", "N/A")
            desc = (s.get("description", "") or "")[:100]
            rows.append(f"| {sid} | {title} | {desc} |")

        result = f"Genie Spaces ({len(spaces)} found):\n\n{header}\n{sep}\n" + "\n".join(
            rows
        )
        return enforce_character_limit(result)
    except Exception as e:
        return f"Error listing Genie spaces: {e}"


def _get_space(space_id: str, workspace: Optional[str] = None) -> str:
    try:
        ws = resolve_workspace_name(workspace)
        client = get_api_client(ws)
        resp = client.get(f"/genie/spaces/{space_id}")

        title = resp.get("title", "N/A")
        description = resp.get("description", "N/A")
        warehouse_id = resp.get("warehouse_id", "N/A")

        result = f"## Genie Space: {title}\n\n"
        result += f"- **Space ID:** {space_id}\n"
        result += f"- **Description:** {description}\n"
        result += f"- **Warehouse ID:** {warehouse_id}\n\n"

        serialized = resp.get("serialized_space", "")
        if serialized:
            try:
                space_config = json.loads(serialized)
                data_sources = space_config.get("data_sources", {})
                tables = data_sources.get("tables", [])
                if tables:
                    result += "### Data Sources (Tables)\n\n"
                    result += "| Table Identifier | Description |\n"
                    result += "| --- | --- |\n"
                    for t in tables:
                        identifier = t.get("identifier", "N/A")
                        desc = " ".join(t.get("description", ["N/A"]))
                        result += f"| {identifier} | {desc} |\n"
                    result += "\n"

                config = space_config.get("config", {})
                sample_questions = config.get("sample_questions", [])
                if sample_questions:
                    result += "### Sample Questions\n\n"
                    for q in sample_questions:
                        question_text = " ".join(q.get("question", []))
                        result += f"- {question_text}\n"
            except json.JSONDecodeError:
                result += "_Could not parse space configuration._\n"

        return enforce_character_limit(result)
    except Exception as e:
        return f"Error getting Genie space details: {e}"


def _start_conversation(
    space_id: str, question: str, workspace: Optional[str] = None
) -> str:
    try:
        ws = resolve_workspace_name(workspace)
        client = get_api_client(ws)
        resp = client.post(
            f"/genie/spaces/{space_id}/start-conversation",
            json_data={"content": question},
        )

        conversation_id = resp.get("conversation", {}).get("id", "")
        message_id = resp.get("message", {}).get("id", "")
        status = resp.get("message", {}).get("status", "UNKNOWN")

        if not conversation_id or not message_id:
            return (
                "Error: Could not extract conversation/message IDs.\n\n"
                f"Raw response:\n{json.dumps(resp, indent=2)}"
            )

        if status in ("COMPLETED", "FAILED", "CANCELLED"):
            return _format_genie_message(resp.get("message", {}))

        msg = _genie_poll_message(client, space_id, conversation_id, message_id)
        return enforce_character_limit(_format_genie_message(msg))
    except Exception as e:
        return f"Error starting Genie conversation: {e}"


def _create_message(
    space_id: str,
    conversation_id: str,
    question: str,
    workspace: Optional[str] = None,
) -> str:
    try:
        ws = resolve_workspace_name(workspace)
        client = get_api_client(ws)
        resp = client.post(
            f"/genie/spaces/{space_id}/conversations/{conversation_id}/messages",
            json_data={"content": question},
        )

        message_id = resp.get("id", "")
        status = resp.get("status", "UNKNOWN")

        if not message_id:
            return (
                "Error: Could not extract message ID.\n\n"
                f"Raw response:\n{json.dumps(resp, indent=2)}"
            )

        if status in ("COMPLETED", "FAILED", "CANCELLED"):
            return _format_genie_message(resp)

        msg = _genie_poll_message(client, space_id, conversation_id, message_id)
        return enforce_character_limit(_format_genie_message(msg))
    except Exception as e:
        return f"Error creating Genie message: {e}"


def _get_message(
    space_id: str,
    conversation_id: str,
    message_id: str,
    workspace: Optional[str] = None,
) -> str:
    try:
        ws = resolve_workspace_name(workspace)
        client = get_api_client(ws)
        resp = client.get(
            f"/genie/spaces/{space_id}/conversations/{conversation_id}/messages/{message_id}",
        )
        return enforce_character_limit(_format_genie_message(resp))
    except Exception as e:
        return f"Error getting Genie message: {e}"


def _get_query_result(
    space_id: str,
    conversation_id: str,
    message_id: str,
    attachment_id: str,
    workspace: Optional[str] = None,
) -> str:
    try:
        ws = resolve_workspace_name(workspace)
        client = get_api_client(ws)
        resp = client.get(
            f"/genie/spaces/{space_id}/conversations/{conversation_id}"
            f"/messages/{message_id}/attachments/{attachment_id}/query-result",
        )
        return enforce_character_limit(_format_query_result(resp))
    except Exception as e:
        return f"Error getting Genie query result: {e}"


def _execute_query(
    space_id: str,
    conversation_id: str,
    message_id: str,
    attachment_id: str,
    workspace: Optional[str] = None,
) -> str:
    try:
        ws = resolve_workspace_name(workspace)
        client = get_api_client(ws)
        client.post(
            f"/genie/spaces/{space_id}/conversations/{conversation_id}"
            f"/messages/{message_id}/attachments/{attachment_id}/execute-query",
        )
        msg = _genie_poll_message(client, space_id, conversation_id, message_id)
        return enforce_character_limit(_format_genie_message(msg))
    except Exception as e:
        return f"Error executing Genie query: {e}"


def _ask_question(
    space_id: str,
    question: str,
    conversation_id: Optional[str] = None,
    workspace: Optional[str] = None,
) -> str:
    """Convenience wrapper: start or continue a conversation and auto-fetch results."""
    try:
        ws = resolve_workspace_name(workspace)
        client = get_api_client(ws)

        if conversation_id:
            resp = client.post(
                f"/genie/spaces/{space_id}/conversations/{conversation_id}/messages",
                json_data={"content": question},
            )
            msg_conversation_id = conversation_id
            message_id = resp.get("id", "")
            status = resp.get("status", "UNKNOWN")
        else:
            resp = client.post(
                f"/genie/spaces/{space_id}/start-conversation",
                json_data={"content": question},
            )
            msg_conversation_id = resp.get("conversation", {}).get("id", "")
            message_id = resp.get("message", {}).get("id", "")
            status = resp.get("message", {}).get("status", "UNKNOWN")

        if not msg_conversation_id or not message_id:
            return (
                "Error: Could not extract IDs from response.\n\n"
                f"Raw: {json.dumps(resp, indent=2)}"
            )

        if status not in ("COMPLETED", "FAILED", "CANCELLED"):
            msg = _genie_poll_message(
                client, space_id, msg_conversation_id, message_id
            )
        else:
            msg = resp.get("message", resp)

        formatted = _format_genie_message(msg)

        # Auto-fetch query results for QUERY attachments
        attachments = msg.get("attachments", []) or []
        for attachment in attachments:
            if attachment.get("type") == "QUERY":
                att_id = attachment.get("id", "")
                if att_id:
                    try:
                        qr = client.get(
                            f"/genie/spaces/{space_id}/conversations/{msg_conversation_id}"
                            f"/messages/{message_id}/attachments/{att_id}/query-result",
                        )
                        stmt = qr.get("statement_response", {})
                        result_status = stmt.get("status", {}).get("state", "UNKNOWN")

                        if result_status == "SUCCEEDED":
                            manifest = stmt.get("manifest", {})
                            columns = manifest.get("schema", {}).get("columns", [])
                            col_names = [
                                c.get("name", f"col_{i}")
                                for i, c in enumerate(columns)
                            ]
                            data_array = stmt.get("result", {}).get("data_array", [])

                            if data_array:
                                formatted += "\n### Query Results\n\n"
                                table = "| " + " | ".join(col_names) + " |\n"
                                table += (
                                    "| " + " | ".join(["---"] * len(col_names)) + " |\n"
                                )
                                for row in data_array:
                                    cells = [
                                        str(cell) if cell is not None else "NULL"
                                        for cell in row
                                    ]
                                    table += "| " + " | ".join(cells) + " |\n"
                                formatted += table

                                row_count = len(data_array)
                                total = stmt.get("result", {}).get(
                                    "row_count", row_count
                                )
                                formatted += f"\n_Showing {row_count} of {total} total rows._\n"
                            else:
                                formatted += "\n_Query returned no rows._\n"
                        elif result_status == "RUNNING":
                            formatted += "\n_Query is still executing. Use `databricks_genie_get_query_result` to retrieve results later._\n"
                    except Exception as qe:
                        formatted += (
                            f"\n_Could not fetch query results: {qe}_\n"
                        )

        return enforce_character_limit(formatted)
    except Exception as e:
        return f"Error in Genie ask_question: {e}"


# ---------------------------------------------------------------------------
# Tool registration
# ---------------------------------------------------------------------------


def register_genie_tools(mcp: Any) -> None:
    @mcp.tool(
        name="databricks_genie_list_spaces",
        annotations={"title": "List Genie Spaces", **READ_ONLY_ANNOTATIONS},
    )
    def genie_list_spaces(workspace: Optional[str] = None) -> str:
        """
        List all available Genie (AI/BI) spaces in the workspace.

        Use when: discovering Genie spaces for natural language data querying.
        Do NOT use when: you need to run SQL directly — use
        databricks_execute_sql_query instead.

        Args:
            workspace: Target workspace name (uses default if omitted)

        Returns:
            Markdown table of Genie spaces with ID, title, and description
        """
        return _list_spaces(workspace)

    @mcp.tool(
        name="databricks_genie_get_space",
        annotations={"title": "Get Genie Space Details", **READ_ONLY_ANNOTATIONS},
    )
    def genie_get_space(space_id: str, workspace: Optional[str] = None) -> str:
        """
        Get detailed info about a Genie space including data sources and sample questions.

        Use when: you need to understand what tables a Genie space can query and
        see example questions before asking.
        Do NOT use when: you only need a list of spaces — use
        databricks_genie_list_spaces.

        Args:
            space_id: The Genie space ID
            workspace: Target workspace name (uses default if omitted)

        Returns:
            Space details with data sources, sample questions, and warehouse info
        """
        return _get_space(space_id, workspace)

    @mcp.tool(
        name="databricks_genie_start_conversation",
        annotations={
            "title": "Start Genie Conversation",
            "readOnlyHint": False,
            "destructiveHint": False,
            "idempotentHint": False,
            "openWorldHint": True,
        },
    )
    def genie_start_conversation(
        space_id: str, question: str, workspace: Optional[str] = None
    ) -> str:
        """
        Start a new conversation in a Genie space with a natural language question.

        Genie interprets the question and generates SQL against the space's data
        sources. Polls until the response is ready or times out (120s).

        Use when: asking a new question to a Genie space.
        Do NOT use when: following up on an existing conversation — use
        databricks_genie_create_message.

        Args:
            space_id: The Genie space ID
            question: Natural language question (e.g. "What were total sales last month?")
            workspace: Target workspace name (uses default if omitted)

        Returns:
            Genie response with generated SQL and/or text answer
        """
        return _start_conversation(space_id, question, workspace)

    @mcp.tool(
        name="databricks_genie_create_message",
        annotations={
            "title": "Send Genie Follow-up",
            "readOnlyHint": False,
            "destructiveHint": False,
            "idempotentHint": False,
            "openWorldHint": True,
        },
    )
    def genie_create_message(
        space_id: str,
        conversation_id: str,
        question: str,
        workspace: Optional[str] = None,
    ) -> str:
        """
        Send a follow-up question in an existing Genie conversation.

        Genie retains context from previous messages in the conversation.

        Use when: asking a follow-up question that builds on a prior Genie answer.
        Do NOT use when: starting a fresh question — use
        databricks_genie_start_conversation.

        Args:
            space_id: The Genie space ID
            conversation_id: Conversation ID from a previous start_conversation call
            question: Follow-up natural language question
            workspace: Target workspace name (uses default if omitted)

        Returns:
            Genie response with generated SQL and/or text answer
        """
        return _create_message(space_id, conversation_id, question, workspace)

    @mcp.tool(
        name="databricks_genie_get_message",
        annotations={"title": "Get Genie Message", **READ_ONLY_ANNOTATIONS},
    )
    def genie_get_message(
        space_id: str,
        conversation_id: str,
        message_id: str,
        workspace: Optional[str] = None,
    ) -> str:
        """
        Retrieve the status and content of a specific Genie message.

        Use when: checking on a previously submitted question or retrieving
        generated SQL.

        Args:
            space_id: The Genie space ID
            conversation_id: The conversation ID
            message_id: The message ID to retrieve
            workspace: Target workspace name (uses default if omitted)

        Returns:
            Message status, generated SQL, and response text
        """
        return _get_message(space_id, conversation_id, message_id, workspace)

    @mcp.tool(
        name="databricks_genie_get_query_result",
        annotations={"title": "Get Genie Query Result", **READ_ONLY_ANNOTATIONS},
    )
    def genie_get_query_result(
        space_id: str,
        conversation_id: str,
        message_id: str,
        attachment_id: str,
        workspace: Optional[str] = None,
    ) -> str:
        """
        Fetch SQL query results from a Genie message attachment.

        After Genie generates and executes SQL, use this to get the actual data.
        The attachment_id is found in the response from start_conversation or
        get_message.

        Use when: you need the data rows from a Genie-generated query.
        Do NOT use when: the result is expired — use
        databricks_genie_execute_query to re-run it first.

        Args:
            space_id: The Genie space ID
            conversation_id: The conversation ID
            message_id: The message ID
            attachment_id: The attachment ID containing the query result
            workspace: Target workspace name (uses default if omitted)

        Returns:
            Markdown table of query results with column headers
        """
        return _get_query_result(
            space_id, conversation_id, message_id, attachment_id, workspace
        )

    @mcp.tool(
        name="databricks_genie_execute_query",
        annotations={
            "title": "Re-execute Genie Query",
            "readOnlyHint": False,
            "destructiveHint": False,
            "idempotentHint": True,
            "openWorldHint": True,
        },
    )
    def genie_execute_query(
        space_id: str,
        conversation_id: str,
        message_id: str,
        attachment_id: str,
        workspace: Optional[str] = None,
    ) -> str:
        """
        Re-execute an expired SQL query from a Genie message attachment.

        Use when: databricks_genie_get_query_result returns QUERY_RESULT_EXPIRED.

        Args:
            space_id: The Genie space ID
            conversation_id: The conversation ID
            message_id: The message ID
            attachment_id: The attachment ID to re-execute
            workspace: Target workspace name (uses default if omitted)

        Returns:
            Updated Genie response after query re-execution
        """
        return _execute_query(
            space_id, conversation_id, message_id, attachment_id, workspace
        )

    @mcp.tool(
        name="databricks_genie_ask_question",
        annotations={
            "title": "Ask Genie Question",
            "readOnlyHint": False,
            "destructiveHint": False,
            "idempotentHint": False,
            "openWorldHint": True,
        },
    )
    def genie_ask_question(
        space_id: str,
        question: str,
        conversation_id: Optional[str] = None,
        workspace: Optional[str] = None,
    ) -> str:
        """
        Ask a natural language question to a Genie space and get the full result.

        This is the recommended primary tool for Genie interactions. It combines
        starting a conversation (or sending a follow-up), polling for completion,
        and automatically fetching query results if available.

        Use when: you want a one-call interaction with Genie — question in, data out.
        Do NOT use when: you need fine-grained control over the conversation flow —
        use the individual genie_start_conversation / genie_create_message tools.

        Args:
            space_id: The Genie space ID
            question: Natural language question to ask
            conversation_id: Optional — if provided, sends as a follow-up in the
                             existing conversation; otherwise starts a new one
            workspace: Target workspace name (uses default if omitted)

        Returns:
            Full Genie response including generated SQL and query results
        """
        return _ask_question(space_id, question, conversation_id, workspace)
