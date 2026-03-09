"""Shared constants and response utilities for the MCP server."""

from typing import Optional, Tuple

CHARACTER_LIMIT = 25_000
DEFAULT_PAGE_LIMIT = 25
MAX_PAGE_LIMIT = 100

READ_ONLY_ANNOTATIONS = {
    "readOnlyHint": True,
    "destructiveHint": False,
    "idempotentHint": True,
    "openWorldHint": True,
}


def enforce_character_limit(text: str, hint: str = "") -> str:
    """Truncate response text when it exceeds CHARACTER_LIMIT."""
    if len(text) <= CHARACTER_LIMIT:
        return text
    msg = f"\n\n---\n*Response truncated at {CHARACTER_LIMIT:,} characters."
    if hint:
        msg += f" {hint}"
    msg += "*"
    return text[:CHARACTER_LIMIT] + msg


def clamp_pagination(limit: int, offset: int) -> Tuple[int, int]:
    """Clamp limit and offset to valid ranges."""
    limit = min(max(limit, 1), MAX_PAGE_LIMIT)
    offset = max(offset, 0)
    return limit, offset


def pagination_footer(
    *, count: int, offset: int, limit: int, total: Optional[int] = None
) -> str:
    """Append a standard pagination footer to list responses."""
    if total is not None:
        has_more = offset + count < total
        header = f"*Showing {count} of {total} results (offset: {offset})."
    else:
        has_more = count >= limit
        header = f"*Showing {count} results (offset: {offset})."

    if has_more:
        next_offset = offset + count
        return f"\n\n---\n{header} More results available — use offset={next_offset}.*"
    return f"\n\n---\n{header}*"
