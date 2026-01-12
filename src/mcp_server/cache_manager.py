"""
Cache Manager for MCP Databricks Server.

This module provides intelligent caching for query results, schema information,
and health status to optimize performance and reduce redundant API calls.
"""

import json
import time
import threading
from typing import Any, Dict, Optional, Tuple, Union
from dataclasses import dataclass
from datetime import datetime, timedelta

from .logger import log_databricks_event, logger


@dataclass
class CacheEntry:
    """Cache entry with value, timestamp, and TTL."""

    value: Any
    timestamp: float
    ttl_seconds: int

    @property
    def is_expired(self) -> bool:
        """Check if cache entry has expired."""
        return time.time() - self.timestamp > self.ttl_seconds

    @property
    def age_seconds(self) -> int:
        """Get age of cache entry in seconds."""
        return int(time.time() - self.timestamp)


class CacheManager:
    """
    Thread-safe cache manager with TTL support and intelligent eviction.

    Provides caching for:
    - Health check status (5 minutes TTL)
    - Schema information (30 minutes TTL)
    - Table metadata (15 minutes TTL)
    - Query results (5 minutes TTL for identical queries)
    """

    def __init__(self, max_entries: int = 1000):
        """
        Initialize cache manager.

        Args:
            max_entries: Maximum number of cache entries before eviction
        """
        self._cache: Dict[str, CacheEntry] = {}
        self._lock = threading.RLock()
        self._max_entries = max_entries
        self._hits = 0
        self._misses = 0

        # Default TTL values (in seconds)
        self.DEFAULT_TTLS = {
            "health": 300,  # 5 minutes - health checks
            "schema": 1800,  # 30 minutes - schema information
            "table": 900,  # 15 minutes - table metadata
            "query": 300,  # 5 minutes - query results
            "job": 120,  # 2 minutes - job information
            "connection": 60,  # 1 minute - connection status
        }

        logger.info(f"Cache manager initialized with max {max_entries} entries")

    def _generate_key(self, category: str, identifier: str) -> str:
        """Generate cache key from category and identifier."""
        return f"{category}:{identifier}"

    def _evict_expired(self) -> int:
        """Remove expired entries from cache."""
        expired_keys = []
        with self._lock:
            for key, entry in self._cache.items():
                if entry.is_expired:
                    expired_keys.append(key)

            for key in expired_keys:
                del self._cache[key]

        if expired_keys:
            logger.debug(f"Evicted {len(expired_keys)} expired cache entries")

        return len(expired_keys)

    def _evict_oldest(self, count: int) -> int:
        """Evict oldest entries when cache is full."""
        with self._lock:
            if len(self._cache) <= self._max_entries:
                return 0

            # Sort by timestamp (oldest first)
            sorted_items = sorted(self._cache.items(), key=lambda x: x[1].timestamp)

            evicted = 0
            for key, _ in sorted_items[:count]:
                del self._cache[key]
                evicted += 1
                if len(self._cache) <= self._max_entries:
                    break

            if evicted > 0:
                logger.debug(f"Evicted {evicted} oldest cache entries")

            return evicted

    def get(self, category: str, identifier: str) -> Optional[Any]:
        """
        Get value from cache if exists and not expired.

        Args:
            category: Cache category (health, schema, table, query, etc.)
            identifier: Unique identifier within category

        Returns:
            Cached value if found and valid, None otherwise
        """
        key = self._generate_key(category, identifier)

        with self._lock:
            entry = self._cache.get(key)

            if entry is None:
                self._misses += 1
                return None

            if entry.is_expired:
                del self._cache[key]
                self._misses += 1
                log_databricks_event(
                    "CACHE", "EXPIRED", f"Cache expired for {category}:{identifier}"
                )
                return None

            self._hits += 1
            log_databricks_event(
                "CACHE",
                "HIT",
                f"Cache hit for {category}:{identifier} (age: {entry.age_seconds}s)",
            )
            return entry.value

    def set(
        self,
        category: str,
        identifier: str,
        value: Any,
        ttl_seconds: Optional[int] = None,
    ) -> bool:
        """
        Set value in cache with TTL.

        Args:
            category: Cache category
            identifier: Unique identifier within category
            value: Value to cache
            ttl_seconds: Time to live in seconds (uses default if None)

        Returns:
            True if cached successfully
        """
        key = self._generate_key(category, identifier)

        if ttl_seconds is None:
            ttl_seconds = self.DEFAULT_TTLS.get(category, 300)  # Default 5 minutes

        with self._lock:
            # Evict expired entries first
            self._evict_expired()

            # Evict oldest if cache is full
            if len(self._cache) >= self._max_entries:
                self._evict_oldest(
                    max(1, self._max_entries // 10)
                )  # Evict 10% when full

            entry = CacheEntry(
                value=value, timestamp=time.time(), ttl_seconds=ttl_seconds
            )

            self._cache[key] = entry

            log_databricks_event(
                "CACHE", "SET", f"Cached {category}:{identifier} (TTL: {ttl_seconds}s)"
            )
            return True

    def invalidate(self, category: str, identifier: Optional[str] = None) -> int:
        """
        Invalidate cache entries.

        Args:
            category: Cache category to invalidate
            identifier: Specific identifier (None to invalidate entire category)

        Returns:
            Number of entries invalidated
        """
        with self._lock:
            if identifier:
                key = self._generate_key(category, identifier)
                if key in self._cache:
                    del self._cache[key]
                    log_databricks_event(
                        "CACHE", "INVALIDATE", f"Invalidated {category}:{identifier}"
                    )
                    return 1
                return 0
            else:
                # Invalidate entire category
                keys_to_remove = [
                    key for key in self._cache.keys() if key.startswith(f"{category}:")
                ]
                for key in keys_to_remove:
                    del self._cache[key]

                if keys_to_remove:
                    log_databricks_event(
                        "CACHE",
                        "INVALIDATE",
                        f"Invalidated {len(keys_to_remove)} {category} entries",
                    )

                return len(keys_to_remove)

    def clear(self) -> int:
        """Clear all cache entries."""
        with self._lock:
            count = len(self._cache)
            self._cache.clear()
            self._hits = 0
            self._misses = 0

            if count > 0:
                log_databricks_event("CACHE", "CLEAR", f"Cleared {count} cache entries")

            return count

    def get_stats(self) -> Dict[str, Any]:
        """Get cache statistics."""
        with self._lock:
            total_requests = self._hits + self._misses
            hit_rate = (self._hits / total_requests * 100) if total_requests > 0 else 0

            # Count entries by category
            category_counts = {}
            expired_count = 0

            for key, entry in self._cache.items():
                category = key.split(":", 1)[0]
                category_counts[category] = category_counts.get(category, 0) + 1

                if entry.is_expired:
                    expired_count += 1

            return {
                "total_entries": len(self._cache),
                "max_entries": self._max_entries,
                "hits": self._hits,
                "misses": self._misses,
                "hit_rate_percent": round(hit_rate, 2),
                "expired_entries": expired_count,
                "categories": category_counts,
                "memory_usage_estimate": self._estimate_memory_usage(),
            }

    def _estimate_memory_usage(self) -> str:
        """Estimate memory usage of cache (rough calculation)."""
        try:
            # Rough estimation: each entry ~1KB average
            estimated_bytes = len(self._cache) * 1024

            if estimated_bytes < 1024:
                return f"{estimated_bytes} B"
            elif estimated_bytes < 1024 * 1024:
                return f"{estimated_bytes / 1024:.1f} KB"
            else:
                return f"{estimated_bytes / (1024 * 1024):.1f} MB"
        except:
            return "Unknown"

    def cleanup_expired(self) -> int:
        """Manual cleanup of expired entries."""
        return self._evict_expired()


# Global cache manager instance
_cache_manager = None
_cache_lock = threading.Lock()


def get_cache_manager() -> CacheManager:
    """Get or create the global cache manager instance."""
    global _cache_manager

    if _cache_manager is None:
        with _cache_lock:
            if _cache_manager is None:
                _cache_manager = CacheManager()

    return _cache_manager


# Convenience functions for common cache operations


def cache_health_status(status: str, details: str, ttl_seconds: int = 300) -> bool:
    """Cache health check status."""
    cache = get_cache_manager()
    health_data = {
        "status": status,
        "details": details,
        "timestamp": datetime.now().isoformat(),
    }
    return cache.set("health", "connection", health_data, ttl_seconds)


def get_cached_health_status() -> Optional[Dict[str, str]]:
    """Get cached health check status."""
    cache = get_cache_manager()
    return cache.get("health", "connection")


def cache_schema_info(schemas: list, ttl_seconds: int = 1800) -> bool:
    """Cache schema discovery results."""
    cache = get_cache_manager()
    return cache.set("schema", "all_schemas", schemas, ttl_seconds)


def get_cached_schema_info() -> Optional[list]:
    """Get cached schema information."""
    cache = get_cache_manager()
    return cache.get("schema", "all_schemas")


def cache_table_info(schema_name: str, tables: list, ttl_seconds: int = 900) -> bool:
    """Cache table discovery results for a schema."""
    cache = get_cache_manager()
    return cache.set("table", f"tables_{schema_name}", tables, ttl_seconds)


def get_cached_table_info(schema_name: str) -> Optional[list]:
    """Get cached table information for a schema."""
    cache = get_cache_manager()
    return cache.get("table", f"tables_{schema_name}")


def cache_table_schema(
    schema_name: str, table_name: str, schema_info: dict, ttl_seconds: int = 900
) -> bool:
    """Cache table schema information."""
    cache = get_cache_manager()
    return cache.set(
        "table", f"schema_{schema_name}_{table_name}", schema_info, ttl_seconds
    )


def get_cached_table_schema(schema_name: str, table_name: str) -> Optional[dict]:
    """Get cached table schema information."""
    cache = get_cache_manager()
    return cache.get("table", f"schema_{schema_name}_{table_name}")


def cache_query_result(sql_hash: str, result: str, ttl_seconds: int = 300) -> bool:
    """Cache query result by SQL hash."""
    cache = get_cache_manager()
    return cache.set("query", sql_hash, result, ttl_seconds)


def get_cached_query_result(sql_hash: str) -> Optional[str]:
    """Get cached query result by SQL hash."""
    cache = get_cache_manager()
    return cache.get("query", sql_hash)


def invalidate_schema_cache() -> int:
    """Invalidate all schema-related cache entries."""
    cache = get_cache_manager()
    schema_count = cache.invalidate("schema")
    table_count = cache.invalidate("table")
    return schema_count + table_count


def get_cache_stats() -> Dict[str, Any]:
    """Get comprehensive cache statistics."""
    cache = get_cache_manager()
    return cache.get_stats()
