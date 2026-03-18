"""Tests for all new Databricks operations tools.

Each tool's core logic is tested with mocked API responses to verify:
- Successful formatting of results into markdown
- Error handling and user-friendly messages
- Edge cases (empty results, bad input)
"""

from unittest.mock import patch, MagicMock

import pytest


# =====================================================================
# Unity Catalog tools
# =====================================================================


class TestCatalogTools:
    @patch("src.mcp_server.catalog_manager.resolve_workspace_name", return_value="dev")
    @patch("src.mcp_server.catalog_manager.get_api_client")
    def test_list_catalogs_success(
        self, mock_get_client: MagicMock, mock_ws: MagicMock
    ) -> None:
        from src.mcp_server.catalog_manager import _list_catalogs

        mock_client = MagicMock()
        mock_client.get.return_value = {
            "catalogs": [
                {
                    "name": "main",
                    "catalog_type": "MANAGED_CATALOG",
                    "owner": "admin",
                    "comment": "Primary",
                },
                {
                    "name": "system",
                    "catalog_type": "SYSTEM_CATALOG",
                    "owner": "System user",
                    "comment": "",
                },
            ]
        }
        mock_get_client.return_value = mock_client

        result = _list_catalogs("dev")
        assert "Unity Catalogs (2 shown, 2 total)" in result
        assert "main" in result
        assert "system" in result

    @patch("src.mcp_server.catalog_manager.resolve_workspace_name", return_value="dev")
    @patch("src.mcp_server.catalog_manager.get_api_client")
    def test_list_catalogs_empty(
        self, mock_get_client: MagicMock, mock_ws: MagicMock
    ) -> None:
        from src.mcp_server.catalog_manager import _list_catalogs

        mock_client = MagicMock()
        mock_client.get.return_value = {"catalogs": []}
        mock_get_client.return_value = mock_client

        result = _list_catalogs("dev")
        assert "No catalogs found" in result

    @patch("src.mcp_server.catalog_manager.resolve_workspace_name", return_value="dev")
    @patch("src.mcp_server.catalog_manager.get_api_client")
    def test_list_uc_schemas_success(
        self, mock_get_client: MagicMock, mock_ws: MagicMock
    ) -> None:
        from src.mcp_server.catalog_manager import _list_uc_schemas

        mock_client = MagicMock()
        mock_client.get.return_value = {
            "schemas": [
                {"name": "public", "owner": "admin", "comment": "Public schema"},
            ]
        }
        mock_get_client.return_value = mock_client

        result = _list_uc_schemas("main", "dev")
        assert "Schemas in catalog 'main'" in result
        assert "public" in result

    @patch("src.mcp_server.catalog_manager.resolve_workspace_name", return_value="dev")
    @patch("src.mcp_server.catalog_manager.get_api_client")
    def test_list_uc_tables_success(
        self, mock_get_client: MagicMock, mock_ws: MagicMock
    ) -> None:
        from src.mcp_server.catalog_manager import _list_uc_tables

        mock_client = MagicMock()
        mock_client.get.return_value = {
            "tables": [
                {
                    "name": "users",
                    "table_type": "MANAGED",
                    "data_source_format": "DELTA",
                    "owner": "admin",
                },
            ]
        }
        mock_get_client.return_value = mock_client

        result = _list_uc_tables("main", "public", "dev")
        assert "Tables in 'main.public'" in result
        assert "users" in result
        assert "DELTA" in result

    @patch("src.mcp_server.catalog_manager.resolve_workspace_name", return_value="dev")
    @patch("src.mcp_server.catalog_manager.get_api_client")
    def test_get_uc_table_info_success(
        self, mock_get_client: MagicMock, mock_ws: MagicMock
    ) -> None:
        from src.mcp_server.catalog_manager import _get_uc_table_info

        mock_client = MagicMock()
        mock_client.get.return_value = {
            "table_type": "MANAGED",
            "data_source_format": "DELTA",
            "owner": "admin",
            "storage_location": "s3://bucket/path",
            "columns": [
                {"name": "id", "type_text": "bigint", "nullable": False},
                {
                    "name": "name",
                    "type_text": "string",
                    "nullable": True,
                    "comment": "User name",
                },
            ],
        }
        mock_get_client.return_value = mock_client

        result = _get_uc_table_info("main.public.users", "dev")
        assert "main.public.users" in result
        assert "bigint" in result
        assert "User name" in result

    def test_get_uc_table_info_bad_name(self) -> None:
        from src.mcp_server.catalog_manager import _get_uc_table_info

        result = _get_uc_table_info("just_a_table", "dev")
        assert "Error" in result
        assert "catalog.schema.table" in result

    def test_validate_identifier_rejects_injection(self) -> None:
        from src.mcp_server.catalog_manager import _validate_identifier

        with pytest.raises(ValueError):
            _validate_identifier("schema; DROP TABLE users", "schema_name")

    @patch("src.mcp_server.catalog_manager.resolve_workspace_name", return_value="dev")
    @patch("src.mcp_server.catalog_manager.get_api_client")
    def test_list_volumes_success(
        self, mock_get_client: MagicMock, mock_ws: MagicMock
    ) -> None:
        from src.mcp_server.catalog_manager import _list_volumes

        mock_client = MagicMock()
        mock_client.get.return_value = {
            "volumes": [
                {
                    "name": "raw_data",
                    "volume_type": "MANAGED",
                    "owner": "admin",
                    "storage_location": "s3://...",
                },
            ]
        }
        mock_get_client.return_value = mock_client

        result = _list_volumes("main", "public", "dev")
        assert "Volumes in 'main.public'" in result
        assert "raw_data" in result


# =====================================================================
# Cluster tools
# =====================================================================


class TestClusterTools:
    @patch("src.mcp_server.cluster_manager.resolve_workspace_name", return_value="dev")
    @patch("src.mcp_server.cluster_manager.get_api_client")
    def test_list_clusters_success(
        self, mock_get_client: MagicMock, mock_ws: MagicMock
    ) -> None:
        from src.mcp_server.cluster_manager import _list_clusters

        mock_client = MagicMock()
        mock_client.get.return_value = {
            "clusters": [
                {
                    "cluster_id": "abc-123",
                    "cluster_name": "test-cluster",
                    "state": "RUNNING",
                    "spark_version": "14.3.x",
                    "node_type_id": "i3.xlarge",
                    "num_workers": 2,
                    "creator_user_name": "user@test.com",
                },
            ]
        }
        mock_get_client.return_value = mock_client

        result = _list_clusters("dev")
        assert "Clusters (1 shown, 1 total)" in result
        assert "test-cluster" in result
        assert "RUNNING" in result

    @patch("src.mcp_server.cluster_manager.resolve_workspace_name", return_value="dev")
    @patch("src.mcp_server.cluster_manager.get_api_client")
    def test_list_clusters_empty(
        self, mock_get_client: MagicMock, mock_ws: MagicMock
    ) -> None:
        from src.mcp_server.cluster_manager import _list_clusters

        mock_client = MagicMock()
        mock_client.get.return_value = {"clusters": []}
        mock_get_client.return_value = mock_client

        result = _list_clusters("dev")
        assert "No clusters found" in result


# =====================================================================
# Warehouse tools
# =====================================================================


class TestWarehouseTools:
    @patch("src.mcp_server.cluster_manager.resolve_workspace_name", return_value="dev")
    @patch("src.mcp_server.cluster_manager.get_api_client")
    def test_list_warehouses_success(
        self, mock_get_client: MagicMock, mock_ws: MagicMock
    ) -> None:
        from src.mcp_server.cluster_manager import _list_warehouses

        mock_client = MagicMock()
        mock_client.get.return_value = {
            "warehouses": [
                {
                    "id": "wh-001",
                    "name": "Main Warehouse",
                    "state": "RUNNING",
                    "cluster_size": "Small",
                    "warehouse_type": "PRO",
                    "creator_name": "admin",
                },
            ]
        }
        mock_get_client.return_value = mock_client

        result = _list_warehouses("dev")
        assert "SQL Warehouses (1 shown, 1 total)" in result
        assert "Main Warehouse" in result
        assert "RUNNING" in result


# =====================================================================
# Workspace tools
# =====================================================================


class TestWorkspaceTools:
    @patch(
        "src.mcp_server.workspace_manager.resolve_workspace_name", return_value="dev"
    )
    @patch("src.mcp_server.workspace_manager.get_api_client")
    def test_list_workspace_success(
        self, mock_get_client: MagicMock, mock_ws: MagicMock
    ) -> None:
        from src.mcp_server.workspace_manager import _list_workspace

        mock_client = MagicMock()
        mock_client.get.return_value = {
            "objects": [
                {"path": "/Users", "object_type": "DIRECTORY"},
                {
                    "path": "/notebook1",
                    "object_type": "NOTEBOOK",
                    "language": "PYTHON",
                },
            ]
        }
        mock_get_client.return_value = mock_client

        result = _list_workspace("/", "dev")
        assert "Workspace objects at '/'" in result
        assert "/Users" in result
        assert "NOTEBOOK" in result

    @patch(
        "src.mcp_server.workspace_manager.resolve_workspace_name", return_value="dev"
    )
    @patch("src.mcp_server.workspace_manager.get_api_client")
    def test_read_notebook_success(
        self, mock_get_client: MagicMock, mock_ws: MagicMock
    ) -> None:
        import base64

        from src.mcp_server.workspace_manager import _read_notebook

        source = "# Databricks notebook source\nprint('hello')"
        encoded = base64.b64encode(source.encode("utf-8")).decode("utf-8")

        mock_client = MagicMock()
        mock_client.get.return_value = {"content": encoded, "language": "PYTHON"}
        mock_get_client.return_value = mock_client

        result = _read_notebook("/test/notebook", "dev")
        assert "print('hello')" in result
        assert "```python" in result


# =====================================================================
# Pipeline tools
# =====================================================================


class TestPipelineTools:
    @patch("src.mcp_server.pipeline_manager.resolve_workspace_name", return_value="dev")
    @patch("src.mcp_server.pipeline_manager.get_api_client")
    def test_list_pipelines_success(
        self, mock_get_client: MagicMock, mock_ws: MagicMock
    ) -> None:
        from src.mcp_server.pipeline_manager import _list_pipelines

        mock_client = MagicMock()
        mock_client.get.return_value = {
            "statuses": [
                {
                    "pipeline_id": "p-001",
                    "name": "ETL Pipeline",
                    "state": "IDLE",
                    "creator_user_name": "admin",
                },
            ]
        }
        mock_get_client.return_value = mock_client

        result = _list_pipelines(workspace="dev")
        assert "DLT Pipelines (1 shown, 1 total)" in result
        assert "ETL Pipeline" in result

    @patch("src.mcp_server.pipeline_manager.resolve_workspace_name", return_value="dev")
    @patch("src.mcp_server.pipeline_manager.get_api_client")
    def test_start_pipeline(
        self, mock_get_client: MagicMock, mock_ws: MagicMock
    ) -> None:
        from src.mcp_server.pipeline_manager import _start_pipeline

        mock_client = MagicMock()
        mock_client.post.return_value = {"update_id": "u-123"}
        mock_get_client.return_value = mock_client

        result = _start_pipeline("p-001", False, "dev")
        assert "update started" in result
        assert "u-123" in result


# =====================================================================
# Query History tools
# =====================================================================


class TestQueryHistoryTools:
    @patch(
        "src.mcp_server.query_history_manager.resolve_workspace_name",
        return_value="dev",
    )
    @patch("src.mcp_server.query_history_manager.get_api_client")
    def test_list_query_history_success(
        self, mock_get_client: MagicMock, mock_ws: MagicMock
    ) -> None:
        from src.mcp_server.query_history_manager import _list_query_history

        mock_client = MagicMock()
        mock_client.post.return_value = {
            "res": [
                {
                    "query_id": "q-001",
                    "status": "FINISHED",
                    "user_name": "admin",
                    "warehouse_id": "wh-001",
                    "duration": 150,
                    "metrics": {"result_count_rows": 42},
                    "query_text": "SELECT * FROM users LIMIT 10",
                },
            ]
        }
        mock_get_client.return_value = mock_client

        result = _list_query_history(10, None, None, "dev")
        assert "Query History" in result
        assert "FINISHED" in result
        assert "SELECT * FROM users" in result

    @patch(
        "src.mcp_server.query_history_manager.resolve_workspace_name",
        return_value="dev",
    )
    @patch("src.mcp_server.query_history_manager.get_api_client")
    def test_get_object_permissions_success(
        self, mock_get_client: MagicMock, mock_ws: MagicMock
    ) -> None:
        from src.mcp_server.query_history_manager import _get_object_permissions

        mock_client = MagicMock()
        mock_client.get.return_value = {
            "access_control_list": [
                {
                    "user_name": "admin",
                    "all_permissions": [
                        {"permission_level": "CAN_MANAGE", "inherited": False}
                    ],
                },
            ]
        }
        mock_get_client.return_value = mock_client

        result = _get_object_permissions("clusters", "abc-123", "dev")
        assert "Permissions for clusters/abc-123" in result
        assert "CAN_MANAGE" in result
