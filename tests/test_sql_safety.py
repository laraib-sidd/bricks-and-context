"""Test SQL identifier validation and read-only detection."""
from __future__ import annotations

import pytest

from mcp_server.mcp_server import _is_read_only_sql, _validate_identifier


class TestReadOnlyDetection:
    @pytest.mark.parametrize(
        "sql",
        [
            "SELECT * FROM t",
            "  SELECT 1",
            "select count(*) from t",
            "SHOW SCHEMAS",
            "show tables",
            "DESCRIBE EXTENDED t",
            "EXPLAIN SELECT 1",
            "WITH cte AS (SELECT 1) SELECT * FROM cte",
            "(SELECT 1)",
        ],
    )
    def test_read_only_queries(self, sql):
        assert _is_read_only_sql(sql) is True

    @pytest.mark.parametrize(
        "sql",
        [
            "INSERT INTO t VALUES (1)",
            "UPDATE t SET x=1",
            "DELETE FROM t",
            "DROP TABLE t",
            "CREATE TABLE t (id INT)",
            "ALTER TABLE t ADD COLUMN x INT",
            "MERGE INTO t USING s ON t.id=s.id",
            "GRANT SELECT ON t TO user",
            "REVOKE ALL ON t FROM user",
        ],
    )
    def test_write_queries(self, sql):
        assert _is_read_only_sql(sql) is False


class TestIdentifierValidation:
    @pytest.mark.parametrize(
        "name",
        [
            "my_table",
            "schema.table",
            "catalog.schema.table",
            "my-table",
            "table_123",
        ],
    )
    def test_valid_identifiers(self, name):
        _validate_identifier(name)

    @pytest.mark.parametrize(
        "name",
        [
            "",
            "table; DROP TABLE users",
            "table' OR '1'='1",
            "table\\nname",
            "table name",
        ],
    )
    def test_invalid_identifiers(self, name):
        with pytest.raises(ValueError):
            _validate_identifier(name)
