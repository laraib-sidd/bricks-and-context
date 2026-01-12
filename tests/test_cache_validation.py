"""
Unit tests for cache manager behavior.

This file was previously empty, which meant cache correctness could regress silently.
"""

import time
from unittest.mock import patch

from src.mcp_server.cache_manager import CacheManager


class TestCacheManager:
    def test_set_and_get_roundtrip(self):
        cache = CacheManager(max_entries=10)
        assert cache.get("schema", "all_schemas") is None

        cache.set("schema", "all_schemas", ["a", "b"], ttl_seconds=60)
        assert cache.get("schema", "all_schemas") == ["a", "b"]

    def test_expiration(self):
        cache = CacheManager(max_entries=10)
        with patch("time.time") as mock_time:
            mock_time.return_value = 1000.0
            cache.set("health", "connection", {"status": "healthy"}, ttl_seconds=5)

            mock_time.return_value = 1003.0
            assert cache.get("health", "connection") == {"status": "healthy"}

            mock_time.return_value = 1006.0
            assert cache.get("health", "connection") is None

    def test_invalidate_specific(self):
        cache = CacheManager(max_entries=10)
        cache.set("query", "abc", "result", ttl_seconds=60)
        assert cache.get("query", "abc") == "result"
        assert cache.invalidate("query", "abc") == 1
        assert cache.get("query", "abc") is None

    def test_invalidate_category(self):
        cache = CacheManager(max_entries=10)
        cache.set("table", "t1", 1, ttl_seconds=60)
        cache.set("table", "t2", 2, ttl_seconds=60)
        cache.set("schema", "s1", 3, ttl_seconds=60)
        assert cache.invalidate("table") == 2
        assert cache.get("table", "t1") is None
        assert cache.get("schema", "s1") == 3