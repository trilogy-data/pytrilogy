"""Dialect result-summary capability: full-result column stats over the query
with its LIMIT removed (duckdb via SUMMARIZE)."""

import pytest

from trilogy.dialect.base import BaseDialect
from trilogy.dialect.duckdb import DuckDBDialect


def test_base_dialect_has_no_result_summary():
    assert BaseDialect.SUPPORTS_RESULT_SUMMARY is False
    with pytest.raises(NotImplementedError):
        BaseDialect().summarize_result(None, None)  # type: ignore[arg-type]


def test_duckdb_dialect_supports_result_summary():
    assert DuckDBDialect.SUPPORTS_RESULT_SUMMARY is True


def test_compile_without_limit_strips_output_limit(duckdb_engine):
    proc = duckdb_engine.parse_text("select item, store_id limit 2;")[-1]
    with_limit = duckdb_engine.generator.compile_statement(proc)
    without = duckdb_engine.generator.compile_without_limit(proc)
    assert "LIMIT" in with_limit.upper()
    assert "LIMIT" not in without.upper()


def test_duckdb_summarize_result_covers_full_result(duckdb_engine):
    # `items` has 4 rows at (item, store_id) grain: jeans/1, hammer/1, hammer/2,
    # hammer/3. A `limit 2` query shows 2 rows; the summary must describe all 4.
    proc = duckdb_engine.parse_text("select item, store_id limit 2;")[-1]
    stats, total = duckdb_engine.generator.summarize_result(
        proc, duckdb_engine.execute_raw_sql
    )
    assert total == 4  # the FULL result, not the LIMIT of 2
    by = {s["column"]: s for s in stats}
    assert len(by) == 2
    # approx_unique is exact at this size
    item_col = next(s for k, s in by.items() if "item" in k)
    store_col = next(s for k, s in by.items() if "store" in k)
    assert item_col["distinct"] == 2  # jeans, hammer
    assert store_col["distinct"] == 3  # 1, 2, 3
    assert item_col["non_null"] == 4 and item_col["nulls"] == 0
