"""Pluggable cache interface for state store metadata queries."""

from typing import Protocol

from trilogy.core.models.core import DataType


class ColumnStatsCache(Protocol):
    """Cache for table column metadata to avoid redundant DB queries.

    Multiple datasources can share a physical table; this cache ensures
    column stats are fetched once per unique table address per session.
    """

    def get_columns(self, table_name: str) -> tuple[bool, dict[str, DataType] | None]:
        """Return (hit, columns). hit=False means cache miss; columns=None means table not found."""
        ...

    def set_columns(self, table_name: str, columns: dict[str, DataType] | None) -> None:
        """Store column stats for a table. columns=None means table not found."""
        ...


class InMemoryColumnStatsCache:
    """In-memory cache for table column stats, scoped to a single session."""

    def __init__(self) -> None:
        self._cache: dict[str, dict[str, DataType] | None] = {}

    def get_columns(self, table_name: str) -> tuple[bool, dict[str, DataType] | None]:
        if table_name in self._cache:
            return True, self._cache[table_name]
        return False, None

    def set_columns(self, table_name: str, columns: dict[str, DataType] | None) -> None:
        self._cache[table_name] = columns
