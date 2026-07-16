"""Regression for q64: slicing a rowset whose body coalesces its projected key.

``filtered`` aggregates store sales and `union join`s the aggregate to a catalog
aggregate on ``sa.item_sk = ci.item_sk``. The coalescing join collapses
``sa.item_sk`` onto the canonical ``ci.item_sk`` and hides that canonical in the
body (only the collapsed side was projected), so the body CTE stopped projecting
the key column at all. Any downstream reference to ``filtered.item_sk`` — the
per-year slices here — then had no backing source and the renderer emitted a
``Missing source reference to ss.item.sk`` sentinel (and, in the full agent
query, discovery re-materialized the shared aggregate, exploding into a 23-join
plan that timed out). Un-hiding the coalesced canonical by pseudonym match makes
the body project the key so the slices source it.
"""

from trilogy.constants import CONFIG

QUERY = """
import store_sales as ss;
import catalog_sales as cs;

with ci as
select cs.item.sk as item_sk, sum(cs.ext_list_price) as amount;

with sa as
select
    ss.item.sk as item_sk,
    ss.store.sk as store_sk,
    ss.date.year as yr,
    count(ss.line_item) as n;

with filtered as
select sa.item_sk, sa.store_sk, sa.yr, sa.n
union join sa.item_sk = ci.item_sk
having ci.amount is not null;

with y99 as
where filtered.yr = 1999
select filtered.item_sk, filtered.store_sk, filtered.n;

with y00 as
where filtered.yr = 2000
select filtered.item_sk, filtered.store_sk, filtered.n;

select y99.item_sk, y99.store_sk, y99.n, y00.n
union join y99.item_sk = y00.item_sk
union join y99.store_sk = y00.store_sk
where y00.item_sk is not null;
"""


def test_q64_reused_joined_rowset_slice_renders_and_executes(engine_sf001):
    sql = engine_sf001.generate_sql(QUERY)[-1]
    assert "INVALID_REFERENCE_BUG" not in sql
    # the shared store aggregate must be materialized once, not re-discovered per
    # slice (the timeout signature was the same aggregate CTE joined repeatedly)
    assert sql.count("count(") == 1
    engine_sf001.execute_raw_sql(sql).fetchall()


def test_q64_reused_joined_rowset_slice_no_build_cache(engine_sf001):
    prior = CONFIG.generation.datasource_build_cache
    CONFIG.generation.datasource_build_cache = False
    try:
        sql = engine_sf001.generate_sql(QUERY)[-1]
    finally:
        CONFIG.generation.datasource_build_cache = prior
    assert "INVALID_REFERENCE_BUG" not in sql
    engine_sf001.execute_raw_sql(sql).fetchall()
