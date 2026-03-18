"""
Unity Catalog operations for Databricks.

Provides tools to browse catalogs, schemas, tables, and volumes
through the Unity Catalog REST API.
"""

import re
from typing import Any, Dict, List, Optional

from .api_client import get_api_client
from .constants import (
    DEFAULT_PAGE_LIMIT,
    READ_ONLY_ANNOTATIONS,
    clamp_pagination,
    enforce_character_limit,
    pagination_footer,
)
from .logger import log_mcp_event
from .workspaces import resolve_workspace_name

_SAFE_IDENTIFIER = re.compile(r"^[\w.\-]+$")


def _validate_identifier(name: str, label: str = "identifier") -> None:
    if not name or not _SAFE_IDENTIFIER.match(name):
        raise ValueError(f"Invalid {label}: {name!r}")


def _list_catalogs(
    workspace: Optional[str] = None,
    limit: int = DEFAULT_PAGE_LIMIT,
    offset: int = 0,
) -> str:
    try:
        limit, offset = clamp_pagination(limit, offset)
        ws = resolve_workspace_name(workspace)
        client = get_api_client(ws)
        resp = client.get("/unity-catalog/catalogs", api_version="2.1")

        all_catalogs = resp.get("catalogs", [])
        if not all_catalogs:
            return "No catalogs found in this workspace."

        total = len(all_catalogs)
        catalogs = all_catalogs[offset : offset + limit]
        if not catalogs:
            return f"No catalogs at offset {offset} (total: {total})."

        header = "| Catalog | Type | Owner | Comment |"
        sep = "| --- | --- | --- | --- |"
        rows = []
        for c in catalogs:
            name = c.get("name", "")
            cat_type = c.get("catalog_type", "")
            owner = c.get("owner", "")
            comment = (c.get("comment") or "")[:120]
            rows.append(f"| {name} | {cat_type} | {owner} | {comment} |")

        result = (
            f"Unity Catalogs ({len(catalogs)} shown, {total} total):\n\n{header}\n{sep}\n"
            + "\n".join(rows)
            + pagination_footer(
                count=len(catalogs), offset=offset, limit=limit, total=total
            )
        )
        return enforce_character_limit(result, "Use offset and limit to paginate.")
    except Exception as e:
        return f"Error listing catalogs: {e}"


def _list_uc_schemas(
    catalog_name: str,
    workspace: Optional[str] = None,
    limit: int = DEFAULT_PAGE_LIMIT,
    offset: int = 0,
) -> str:
    try:
        _validate_identifier(catalog_name, "catalog_name")
        limit, offset = clamp_pagination(limit, offset)
        ws = resolve_workspace_name(workspace)
        client = get_api_client(ws)
        resp = client.get(
            "/unity-catalog/schemas",
            params={"catalog_name": catalog_name},
            api_version="2.1",
        )

        all_schemas = resp.get("schemas", [])
        if not all_schemas:
            return f"No schemas found in catalog '{catalog_name}'."

        total = len(all_schemas)
        schemas = all_schemas[offset : offset + limit]
        if not schemas:
            return f"No schemas at offset {offset} in catalog '{catalog_name}' (total: {total})."

        header = "| Schema | Owner | Comment |"
        sep = "| --- | --- | --- |"
        rows = []
        for s in schemas:
            name = s.get("name", "")
            owner = s.get("owner", "")
            comment = (s.get("comment") or "")[:120]
            rows.append(f"| {name} | {owner} | {comment} |")

        result = f"Schemas in catalog '{catalog_name}' ({len(schemas)} shown, {total} total):\n\n" f"{header}\n{sep}\n" + "\n".join(
            rows
        ) + pagination_footer(
            count=len(schemas), offset=offset, limit=limit, total=total
        )
        return enforce_character_limit(result, "Use offset and limit to paginate.")
    except Exception as e:
        return f"Error listing schemas in catalog '{catalog_name}': {e}"


def _list_uc_tables(
    catalog_name: str,
    schema_name: str,
    workspace: Optional[str] = None,
    limit: int = DEFAULT_PAGE_LIMIT,
    offset: int = 0,
) -> str:
    try:
        _validate_identifier(catalog_name, "catalog_name")
        _validate_identifier(schema_name, "schema_name")
        limit, offset = clamp_pagination(limit, offset)
        ws = resolve_workspace_name(workspace)
        client = get_api_client(ws)
        resp = client.get(
            "/unity-catalog/tables",
            params={"catalog_name": catalog_name, "schema_name": schema_name},
            api_version="2.1",
        )

        all_tables = resp.get("tables", [])
        if not all_tables:
            return f"No tables found in '{catalog_name}.{schema_name}'."

        total = len(all_tables)
        tables = all_tables[offset : offset + limit]
        if not tables:
            return f"No tables at offset {offset} in '{catalog_name}.{schema_name}' (total: {total})."

        header = "| Table | Type | Data Source | Owner |"
        sep = "| --- | --- | --- | --- |"
        rows = []
        for t in tables:
            name = t.get("name", "")
            table_type = t.get("table_type", "")
            data_source = t.get("data_source_format", "")
            owner = t.get("owner", "")
            rows.append(f"| {name} | {table_type} | {data_source} | {owner} |")

        result = f"Tables in '{catalog_name}.{schema_name}' ({len(tables)} shown, {total} total):\n\n" f"{header}\n{sep}\n" + "\n".join(
            rows
        ) + pagination_footer(
            count=len(tables), offset=offset, limit=limit, total=total
        )
        return enforce_character_limit(result, "Use offset and limit to paginate.")
    except Exception as e:
        return f"Error listing tables in '{catalog_name}.{schema_name}': {e}"


def _get_uc_table_info(full_table_name: str, workspace: Optional[str] = None) -> str:
    try:
        parts = full_table_name.split(".")
        if len(parts) != 3:
            return "Error: full_table_name must be 'catalog.schema.table'."
        for p in parts:
            _validate_identifier(p, "table name part")

        ws = resolve_workspace_name(workspace)
        client = get_api_client(ws)
        resp = client.get(
            f"/unity-catalog/tables/{full_table_name}",
            api_version="2.1",
        )

        lines = [f"Table: **{full_table_name}**\n"]
        lines.append(f"- **Type**: {resp.get('table_type', 'N/A')}")
        lines.append(
            f"- **Data Source Format**: {resp.get('data_source_format', 'N/A')}"
        )
        lines.append(f"- **Owner**: {resp.get('owner', 'N/A')}")
        lines.append(f"- **Storage Location**: {resp.get('storage_location', 'N/A')}")
        if resp.get("comment"):
            lines.append(f"- **Comment**: {resp['comment']}")

        columns = resp.get("columns", [])
        if columns:
            lines.append(f"\n**Columns** ({len(columns)}):\n")
            lines.append("| Name | Type | Nullable | Comment |")
            lines.append("| --- | --- | --- | --- |")
            for col in columns:
                name = col.get("name", "")
                dtype = col.get("type_text", col.get("type_name", ""))
                nullable = "Yes" if col.get("nullable", True) else "No"
                comment = (col.get("comment") or "")[:80]
                lines.append(f"| {name} | {dtype} | {nullable} | {comment} |")

        result = "\n".join(lines)
        return enforce_character_limit(
            result, "Table has many columns; consider narrowing your query."
        )
    except Exception as e:
        return f"Error getting table info for '{full_table_name}': {e}"


def _list_volumes(
    catalog_name: str,
    schema_name: str,
    workspace: Optional[str] = None,
    limit: int = DEFAULT_PAGE_LIMIT,
    offset: int = 0,
) -> str:
    try:
        _validate_identifier(catalog_name, "catalog_name")
        _validate_identifier(schema_name, "schema_name")
        limit, offset = clamp_pagination(limit, offset)
        ws = resolve_workspace_name(workspace)
        client = get_api_client(ws)
        resp = client.get(
            "/unity-catalog/volumes",
            params={"catalog_name": catalog_name, "schema_name": schema_name},
            api_version="2.1",
        )

        all_volumes = resp.get("volumes", [])
        if not all_volumes:
            return f"No volumes found in '{catalog_name}.{schema_name}'."

        total = len(all_volumes)
        volumes = all_volumes[offset : offset + limit]
        if not volumes:
            return f"No volumes at offset {offset} in '{catalog_name}.{schema_name}' (total: {total})."

        header = "| Volume | Type | Owner | Storage Location |"
        sep = "| --- | --- | --- | --- |"
        rows = []
        for v in volumes:
            name = v.get("name", "")
            vol_type = v.get("volume_type", "")
            owner = v.get("owner", "")
            location = (v.get("storage_location") or "")[:80]
            rows.append(f"| {name} | {vol_type} | {owner} | {location} |")

        result = f"Volumes in '{catalog_name}.{schema_name}' ({len(volumes)} shown, {total} total):\n\n" f"{header}\n{sep}\n" + "\n".join(
            rows
        ) + pagination_footer(
            count=len(volumes), offset=offset, limit=limit, total=total
        )
        return enforce_character_limit(result, "Use offset and limit to paginate.")
    except Exception as e:
        return f"Error listing volumes in '{catalog_name}.{schema_name}': {e}"


# ------------------------------------------------------------------
# Tool registration
# ------------------------------------------------------------------


def register_catalog_tools(mcp: Any) -> None:
    @mcp.tool(
        name="databricks_list_catalogs",
        annotations={"title": "List Unity Catalogs", **READ_ONLY_ANNOTATIONS},
    )
    def list_catalogs(
        workspace: Optional[str] = None,
        limit: int = DEFAULT_PAGE_LIMIT,
        offset: int = 0,
    ) -> str:
        """
        List all Unity Catalog catalogs in the Databricks workspace.

        Use when: exploring the data landscape or discovering available catalogs
        in the three-level namespace (catalog.schema.table).
        Do NOT use when: you already know the catalog name — jump straight to
        databricks_list_uc_schemas instead.

        Args:
            workspace: Target workspace name (uses default if omitted)
            limit: Max catalogs to return (default 25, max 100)
            offset: Number of results to skip for pagination (default 0)

        Returns:
            Markdown table of catalogs with type, owner, and comment
        """
        return _list_catalogs(workspace, limit, offset)

    @mcp.tool(
        name="databricks_list_uc_schemas",
        annotations={"title": "List Schemas in Catalog", **READ_ONLY_ANNOTATIONS},
    )
    def list_uc_schemas(
        catalog_name: str,
        workspace: Optional[str] = None,
        limit: int = DEFAULT_PAGE_LIMIT,
        offset: int = 0,
    ) -> str:
        """
        List schemas within a Unity Catalog catalog.

        Use when: exploring schemas inside a known catalog.
        Do NOT use when: you need SQL-level schema discovery — use
        databricks_discover_schemas for the SQL warehouse's default catalog.

        Args:
            catalog_name: Name of the catalog to browse
            workspace: Target workspace name (uses default if omitted)
            limit: Max schemas to return (default 25, max 100)
            offset: Number of results to skip for pagination (default 0)

        Returns:
            Markdown table of schemas with owner and comment
        """
        return _list_uc_schemas(catalog_name, workspace, limit, offset)

    @mcp.tool(
        name="databricks_list_uc_tables",
        annotations={"title": "List Tables in Schema", **READ_ONLY_ANNOTATIONS},
    )
    def list_uc_tables(
        catalog_name: str,
        schema_name: str,
        workspace: Optional[str] = None,
        limit: int = DEFAULT_PAGE_LIMIT,
        offset: int = 0,
    ) -> str:
        """
        List tables within a Unity Catalog schema.

        Use when: browsing tables in a specific catalog.schema.
        Do NOT use when: you need SQL-level table listing — use
        databricks_discover_tables for SHOW TABLES output.

        Args:
            catalog_name: Catalog name
            schema_name: Schema name
            workspace: Target workspace name (uses default if omitted)
            limit: Max tables to return (default 25, max 100)
            offset: Number of results to skip for pagination (default 0)

        Returns:
            Markdown table of tables with type, data source, and owner
        """
        return _list_uc_tables(catalog_name, schema_name, workspace, limit, offset)

    @mcp.tool(
        name="databricks_get_uc_table_info",
        annotations={"title": "Get Table Details", **READ_ONLY_ANNOTATIONS},
    )
    def get_uc_table_info(full_table_name: str, workspace: Optional[str] = None) -> str:
        """
        Get detailed Unity Catalog table information including columns.

        Use when: you need column names, types, and metadata for a specific table.
        Do NOT use when: you only need to list tables — use
        databricks_list_uc_tables instead.

        Args:
            full_table_name: Three-part name like 'catalog.schema.table'
            workspace: Target workspace name (uses default if omitted)

        Returns:
            Detailed table info with column names, types, nullability, and comments

        Error conditions:
            Returns error if full_table_name is not three-part or contains
            invalid characters.
        """
        return _get_uc_table_info(full_table_name, workspace)

    @mcp.tool(
        name="databricks_list_volumes",
        annotations={"title": "List Volumes in Schema", **READ_ONLY_ANNOTATIONS},
    )
    def list_volumes(
        catalog_name: str,
        schema_name: str,
        workspace: Optional[str] = None,
        limit: int = DEFAULT_PAGE_LIMIT,
        offset: int = 0,
    ) -> str:
        """
        List Unity Catalog volumes within a schema.

        Use when: exploring file storage volumes available in a schema.
        Do NOT use when: you need to list tables — use databricks_list_uc_tables.

        Args:
            catalog_name: Catalog name
            schema_name: Schema name
            workspace: Target workspace name (uses default if omitted)
            limit: Max volumes to return (default 25, max 100)
            offset: Number of results to skip for pagination (default 0)

        Returns:
            Markdown table of volumes with type, owner, and storage location
        """
        return _list_volumes(catalog_name, schema_name, workspace, limit, offset)
