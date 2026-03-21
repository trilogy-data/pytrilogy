"""E2E test: InMemoryColumnStatsCache deduplicates column metadata queries."""

from trilogy import Dialects
from trilogy.core.models.core import DataType
from trilogy.execution.state.cache import InMemoryColumnStatsCache
from trilogy.execution.state.state_store import BaseStateStore

# Two datasources pointing at the same physical table, imported under different names.
PREQL = """
key id int;
property id.value string;

datasource events_a (
    id,
    value
)
grain (id)
address raw_events;

datasource events_b (
    id,
    value
)
grain (id)
address raw_events;
"""


class _CountingCache(InMemoryColumnStatsCache):
    """Wraps InMemoryColumnStatsCache and counts actual DB fetches (set_columns calls)."""

    def __init__(self) -> None:
        super().__init__()
        self.store_count = 0

    def set_columns(self, table_name: str, columns: dict[str, DataType] | None) -> None:
        self.store_count += 1
        super().set_columns(table_name, columns)


def test_cache_deduplicates_column_queries() -> None:
    """Two datasources on the same physical table should only query DB columns once."""
    executor = Dialects.DUCK_DB.default_executor()
    executor.execute_raw_sql("CREATE TABLE raw_events (id INTEGER, value VARCHAR)")
    executor.parse_text(PREQL)

    cache = _CountingCache()
    state_store = BaseStateStore(cache=cache)
    state_store.get_stale_assets(executor.environment, executor)

    assert (
        cache.store_count == 1
    ), f"Expected 1 DB column query for raw_events, got {cache.store_count}"


def test_cache_hit_returns_same_columns() -> None:
    """Cached result must match what the DB actually returns."""
    executor = Dialects.DUCK_DB.default_executor()
    executor.execute_raw_sql("CREATE TABLE raw_events (id INTEGER, value VARCHAR)")
    executor.parse_text(PREQL)

    cache = InMemoryColumnStatsCache()
    state_store = BaseStateStore(cache=cache)
    state_store.get_stale_assets(executor.environment, executor)

    # Both datasources share the same address so the cache entry is keyed by "raw_events".
    hit, cols = cache.get_columns("raw_events")
    assert hit, "Expected a cache entry for raw_events after get_stale_assets"
    assert cols is not None
    assert set(cols.keys()) == {"id", "value"}


def test_no_cache_still_works() -> None:
    """BaseStateStore without a cache must behave identically to the cached version."""
    executor = Dialects.DUCK_DB.default_executor()
    executor.execute_raw_sql("CREATE TABLE raw_events (id INTEGER, value VARCHAR)")
    executor.parse_text(PREQL)

    state_store = BaseStateStore()
    # Should not raise and should return a list (possibly empty if schema matches).
    stale = state_store.get_stale_assets(executor.environment, executor)
    assert isinstance(stale, list)
