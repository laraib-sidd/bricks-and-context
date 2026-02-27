"""
Unity Catalog operations for Databricks.

Provides tools to browse catalogs, schemas, tables, and volumes
through the Unity Catalog REST API.
"""

import re
from typing import Any, Dict, List, Optional

from .api_client import get_api_client
from .logger import log_mcp_event
from .workspaces import resolve_workspace_name

_SAFE_IDENTIFIER = re.compile(r"^[\w.\-]+$")


def _validate_identifier(name: str, label: str = "identifier") -> None:
    if not name or not _SAFE_IDENTIFIER.match(name):
        raise ValueError(f"Invalid {label}: {name!r}")


def _list_catalogs(workspace: Optional[str] = None) -> str:
    try:
        ws = resolve_workspace_name(workspace)
        client = get_api_client(ws)
        resp = client.get("/unity-catalog/catalogs", api_version="2.1")

        catalogs = resp.get("catalogs", [])
        if not catalogs:
            return "No catalogs found in this workspace."

        header = "| Catalog | Type | Owner | Comment |"
        sep = "| --- | --- | --- | --- |"
        rows = []
        for c in catalogs:
            name = c.get("name", "")
            cat_type = c.get("catalog_type", "")
            owner = c.get("owner", "")
            comment = (c.get("comment") or "")[:120]
            rows.append(f"| {name} | {cat_type} | {owner} | {comment} |")

        return (
            f"Unity Catalogs ({len(catalogs)} found):\n\n{header}\n{sep}\n"
            + "\n".join(rows)
        )
    except Exception as e:
        return f"Error listing catalogs: {e}"


def _list_uc_schemas(catalog_name: str, workspace: Optional[str] = None) -> str:
    try:
        _validate_identifier(catalog_name, "catalog_name")
        ws = resolve_workspace_name(workspace)
        client = get_api_client(ws)
        resp = client.get(
            "/unity-catalog/schemas",
            params={"catalog_name": catalog_name},
            api_version="2.1",
        )

        schemas = resp.get("schemas", [])
        if not schemas:
            return f"No schemas found in catalog '{catalog_name}'."

        header = "| Schema | Owner | Comment |"
        sep = "| --- | --- | --- |"
        rows = []
        for s in schemas:
            name = s.get("name", "")
            owner = s.get("owner", "")
            comment = (s.get("comment") or "")[:120]
            rows.append(f"| {name} | {owner} | {comment} |")

        return (
            f"Schemas in catalog '{catalog_name}' ({len(schemas)} found):\n\n"
            f"{header}\n{sep}\n" + "\n".join(rows)
        )
    except Exception as e:
        return f"Error listing schemas in catalog '{catalog_name}': {e}"


def _list_uc_tables(
    catalog_name: str, schema_name: str, workspace: Optional[str] = None
) -> str:
    try:
        _validate_identifier(catalog_name, "catalog_name")
        _validate_identifier(schema_name, "schema_name")
        ws = resolve_workspace_name(workspace)
        client = get_api_client(ws)
        resp = client.get(
            "/unity-catalog/tables",
            params={"catalog_name": catalog_name, "schema_name": schema_name},
            api_version="2.1",
        )

        tables = resp.get("tables", [])
        if not tables:
            return f"No tables found in '{catalog_name}.{schema_name}'."

        header = "| Table | Type | Data Source | Owner |"
        sep = "| --- | --- | --- | --- |"
        rows = []
        for t in tables:
            name = t.get("name", "")
            table_type = t.get("table_type", "")
            data_source = t.get("data_source_format", "")
            owner = t.get("owner", "")
            rows.append(f"| {name} | {table_type} | {data_source} | {owner} |")

        return (
            f"Tables in '{catalog_name}.{schema_name}' ({len(tables)} found):\n\n"
            f"{header}\n{sep}\n" + "\n".join(rows)
        )
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

        return "\n".join(lines)
    except Exception as e:
        return f"Error getting table info for '{full_table_name}': {e}"


def _list_volumes(
    catalog_name: str, schema_name: str, workspace: Optional[str] = None
) -> str:
    try:
        _validate_identifier(catalog_name, "catalog_name")
        _validate_identifier(schema_name, "schema_name")
        ws = resolve_workspace_name(workspace)
        client = get_api_client(ws)
        resp = client.get(
            "/unity-catalog/volumes",
            params={"catalog_name": catalog_name, "schema_name": schema_name},
            api_version="2.1",
        )

        volumes = resp.get("volumes", [])
        if not volumes:
            return f"No volumes found in '{catalog_name}.{schema_name}'."

        header = "| Volume | Type | Owner | Storage Location |"
        sep = "| --- | --- | --- | --- |"
        rows = []
        for v in volumes:
            name = v.get("name", "")
            vol_type = v.get("volume_type", "")
            owner = v.get("owner", "")
            location = (v.get("storage_location") or "")[:80]
            rows.append(f"| {name} | {vol_type} | {owner} | {location} |")

        return (
            f"Volumes in '{catalog_name}.{schema_name}' ({len(volumes)} found):\n\n"
            f"{header}\n{sep}\n" + "\n".join(rows)
        )
    except Exception as e:
        return f"Error listing volumes in '{catalog_name}.{schema_name}': {e}"


# ------------------------------------------------------------------
# Tool registration
# ------------------------------------------------------------------


def register_catalog_tools(mcp: Any) -> None:
    @mcp.tool()
    def list_catalogs(workspace: Optional[str] = None) -> str:
        """
        List all Unity Catalog catalogs in the Databricks workspace.

        Essential for understanding the data landscape across the three-level
        namespace (catalog.schema.table).

        Returns:
            Markdown table of catalogs with type, owner, and comment
        """
        return _list_catalogs(workspace)

    @mcp.tool()
    def list_uc_schemas(catalog_name: str, workspace: Optional[str] = None) -> str:
        """
        List schemas within a Unity Catalog catalog.

        Args:
            catalog_name: Name of the catalog to browse

        Returns:
            Markdown table of schemas with owner and comment
        """
        return _list_uc_schemas(catalog_name, workspace)

    @mcp.tool()
    def list_uc_tables(
        catalog_name: str, schema_name: str, workspace: Optional[str] = None
    ) -> str:
        """
        List tables within a Unity Catalog schema.

        Args:
            catalog_name: Catalog name
            schema_name: Schema name

        Returns:
            Markdown table of tables with type, data source, and owner
        """
        return _list_uc_tables(catalog_name, schema_name, workspace)

    @mcp.tool()
    def get_uc_table_info(full_table_name: str, workspace: Optional[str] = None) -> str:
        """
        Get detailed Unity Catalog table information including columns.

        Args:
            full_table_name: Three-part name like 'catalog.schema.table'

        Returns:
            Detailed table info with column names, types, and nullability
        """
        return _get_uc_table_info(full_table_name, workspace)
