"""
In-memory cache for processed tables.
"""

import logging
import secrets
import time
from typing import Dict, Optional, Tuple

from data.models.tables import Table

logger = logging.getLogger(__name__)


class TableCache:
    """In-memory cache for storing processed tables."""

    def __init__(self, max_size: int = 100):
        """
        Initialize the cache.

        Args:
            max_size: Maximum number of tables to cache
        """
        self.cache: Dict[
            str, Tuple[Table, str, float]
        ] = {}  # key -> (table, version, timestamp)
        self.max_size = max_size

    def generate_key(self, table: Table) -> str:
        """
        Generate a random version string for a table.

        Args:
            table: The table to generate a version for

        Returns:
            Random version string
        """
        return f"{table.name}:{secrets.token_hex(6)}"  # 12 character random hex string

    def put(self, table: Table, key: Optional[str] = None) -> str:
        """
        Store a table in the cache.

        Args:
            table: The table to cache
            version: Optional version string. If None, will be generated

        Returns:
            The version string used for caching
        """
        if key is None:
            key = self.generate_key(table)

        timestamp = time.time()

        # Check if we need to evict entries
        if len(self.cache) >= self.max_size:
            self._evict_oldest()

        self.cache[key] = (table, key, timestamp)
        logger.info(f"Cached: {key}")
        return key

    def get(self, key: str) -> Optional[Table]:
        """
        Retrieve a table from the cache.

        Args:
            table_name: Name of the table
            version: Version string

        Returns:
            Cached table if found, None otherwise
        """

        if key in self.cache:
            table, _, timestamp = self.cache[key]
            logger.info(f"Cache hit: {key}")
            return table

        logger.info(f"Cache miss: {key}")
        return None

    def _evict_oldest(self):
        """Remove the oldest entry from the cache."""
        if not self.cache:
            return

        oldest_key = min(self.cache.keys(), key=lambda k: self.cache[k][2])
        del self.cache[oldest_key]
        logger.info(f"Evicted oldest entry: {oldest_key}")

    def clear(self):
        """Clear all entries from the cache."""
        self.cache.clear()
        logger.info("Cache cleared")

    def keys(self):
        """Get all cache keys."""
        return list(self.cache.keys())


# Global cache instance
table_cache = TableCache()
