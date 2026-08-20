"""Assembly shape and row pins for partial-key (`~`) fact merges.

The store channel is the natural fixture: `store_sales` is complete at grain
(item.sk, ticket_number) while `store_returns` marks both keys `~`, so any
query mixing the two sides plus dimension attributes exercises the FINAL
assembly's sibling stitching.

`test_adhoc_three` pins the fixed behavior — dimension contributors join the
merge on their own key, so every base table is scanned once and the output is
exactly one row per (item, ticket) pair in the data.
"""

from pathlib import Path

import pytest

from trilogy import Executor
from trilogy.core.models.environment import Environment

working_path = Path(__file__).parent


def _scans(sql: str, table: str) -> int:
    return sql.count(f'"memory"."{table}"')


# A custom reporting fact at a pure dimension-pair grain: the only datasource
# relating item to customer in this model, with both keys bound `~` (the
# customer/product/sales-list shape). Distinct from the store channel, where a
# complete sibling (store_sales) anchors store_returns' `~` grain keys.
_PAIR_FACT_MODEL = """import std.money;

import item as item;
import customer as customer;

properties <item.sk, customer.sk> (
    pair_paid numeric::usd,
);

datasource item_customer_sales (
    item_sk: ~item.sk,
    customer_sk: ~customer.sk,
    total_paid: pair_paid,
)
grain (item.sk, customer.sk)
address memory.item_customer_sales;
"""

_PAIR_FACT_SELECT = """select
    item.sk,
    customer.sk,
    item.brand_name,
    customer.current_address.state,
    sum(pair_paid) as total_pair_paid,
;"""


@pytest.fixture(scope="module")
def pair_fact_engine(engine_sf001: Executor) -> Executor:
    engine_sf001.execute_raw_sql(
        """create or replace table memory.item_customer_sales as
        select ss_item_sk as item_sk, ss_customer_sk as customer_sk,
               sum(ss_net_paid) as total_paid
        from memory.store_sales
        where ss_customer_sk is not null
        group by 1, 2"""
    )
    return engine_sf001


def test_pair_fact_span(pair_fact_engine: Executor):
    """item x customer attributes related only through the `~`-keyed pair
    fact: every real pair exactly once, one extension row per unmatched member
    of each `~` key, and no cross-paired or all-null rows."""
    pair_fact_engine.environment = Environment(working_path=working_path)
    pair_fact_engine.parse_text(_PAIR_FACT_MODEL)
    sql = pair_fact_engine.generate_sql(_PAIR_FACT_SELECT)[-1]
    rows = pair_fact_engine.execute_raw_sql(sql).fetchall()
    truth_pairs, truth_paid = pair_fact_engine.execute_raw_sql(
        "select count(*), sum(total_paid) from memory.item_customer_sales"
    ).fetchone()
    never_items = pair_fact_engine.execute_raw_sql(
        "select count(*) from memory.item where i_item_sk not in "
        "(select item_sk from memory.item_customer_sales)"
    ).fetchone()[0]
    never_cust = pair_fact_engine.execute_raw_sql(
        "select count(*) from memory.customer where c_customer_sk not in "
        "(select customer_sk from memory.item_customer_sales)"
    ).fetchone()[0]
    paired = [r for r in rows if r[0] is not None and r[1] is not None]
    item_ext = [r for r in rows if r[0] is not None and r[1] is None]
    cust_ext = [r for r in rows if r[0] is None and r[1] is not None]
    all_null = [r for r in rows if r[0] is None and r[1] is None]
    assert len(paired) == truth_pairs
    assert len({(r[0], r[1]) for r in paired}) == truth_pairs
    assert len(item_ext) == never_items
    assert len(cust_ext) == never_cust
    assert not all_null
    total = sum(r[4] for r in rows if r[4] is not None)
    assert round(total, 2) == round(truth_paid, 2)


def test_pair_fact_pinned_star(pair_fact_engine: Executor):
    """The suggested pin heals both `~` keys (each key's extension rows carry
    the OTHER key as NULL, so the pin filters them all out) and the query plans
    as a plain star over the pair fact's own rows."""
    pair_fact_engine.environment = Environment(working_path=working_path)
    pair_fact_engine.parse_text(_PAIR_FACT_MODEL)
    sql = pair_fact_engine.generate_sql(
        "where item.sk is not null and customer.sk is not null\n" + _PAIR_FACT_SELECT
    )[-1]
    assert sql.count("FULL JOIN") == 0, sql
    for table in ("item_customer_sales", "item", "customer", "customer_address"):
        assert _scans(sql, table) == 1, (table, sql)

    rows = pair_fact_engine.execute_raw_sql(sql).fetchall()
    keys = [(r[0], r[1]) for r in rows]
    truth = pair_fact_engine.execute_raw_sql(
        "select count(*), sum(total_paid) from memory.item_customer_sales"
    ).fetchone()
    assert len(rows) == truth[0]
    assert all(k[0] is not None and k[1] is not None for k in keys)
    assert len(set(keys)) == len(keys)
    total = sum(r[4] for r in rows if r[4] is not None)
    assert round(total, 2) == round(truth[1], 2)


def test_adhoc_three(engine_sf001: Executor):
    engine_sf001.environment = Environment(working_path=working_path)
    text = (working_path / "adhoc03.preql").read_text()
    sql = engine_sf001.generate_sql(text)[-1]

    # One scan per base table: the item lookup must not be re-sourced through
    # a fact table just to carry the sibling merge's grain keys.
    assert _scans(sql, "store_sales") == 1, sql
    assert _scans(sql, "store_returns") == 1, sql
    assert _scans(sql, "item") == 1, sql
    # One FULL stitch is legitimate — the `~`-keyed returns side must not drop
    # sales rows — but the dim join must not multiply it.
    assert sql.count("FULL JOIN") <= 1, sql

    rows = engine_sf001.execute_raw_sql(sql).fetchall()
    keys = [(r[0], r[1]) for r in rows]
    truth = engine_sf001.execute_raw_sql(
        "select count(*), sum(ss_net_paid) from memory.store_sales"
    ).fetchone()
    # Exactly one output row per (item, ticket) pair in the data: no
    # manufactured NULL-key rows, no fan-out duplicates, nothing dropped.
    assert len(rows) == truth[0]
    assert all(k[0] is not None and k[1] is not None for k in keys)
    assert len(set(keys)) == len(keys)
    total_paid = sum(r[3] for r in rows if r[3] is not None)
    assert total_paid == truth[1]
    total_returned = sum(r[4] for r in rows if r[4] is not None)
    truth_returned = engine_sf001.execute_raw_sql(
        "select sum(sr_return_amt) from memory.store_returns"
    ).fetchone()[0]
    assert total_returned == truth_returned


def test_partial_grain_star_under_not_null(engine_sf001: Executor):
    """A not-null pin on the grain keys licenses the pure star.

    `where <grain keys> is not null` proves the population is the fact's own
    rows, so the `~` domain-extension machinery has nothing to preserve: the
    WHERE-proves-non-null completion un-partials the keys, joins narrow, and
    the same query that fans out unconditioned plans as a star and returns
    exactly the fact's rows. This is the anchor shape the unconditioned plan
    should generalize from (star + one extension block per partial dim)."""
    engine_sf001.environment = Environment(working_path=working_path)
    sql = engine_sf001.generate_sql("""import store_sales as ss;

where ss.item.sk is not null and ss.ticket_number is not null
select
    ss.item.sk,
    ss.ticket_number,
    ss.item.brand_name,
    ss.customer.current_address.state,
    sum(ss.net_paid) as total_paid,
    sum(ss.return_amount) as total_returned,
;""")[-1]
    assert sql.count("FULL JOIN") == 0, sql
    for table in ("store_sales", "store_returns", "item", "customer"):
        assert _scans(sql, table) == 1, (table, sql)
    rows = engine_sf001.execute_raw_sql(sql).fetchall()
    keys = [(r[0], r[1]) for r in rows]
    truth_count = engine_sf001.execute_raw_sql(
        "select count(*) from memory.store_sales"
    ).fetchone()[0]
    assert len(rows) == truth_count
    assert all(k[0] is not None and k[1] is not None for k in keys)
    assert len(set(keys)) == len(keys)


def test_partial_grain_with_customer_dim(engine_sf001: Executor):
    """customer.sk is bound `?` (nullable), not `~` (partial), so no domain
    extension is licensed: no NULL-key rows for never-purchasing customers."""
    engine_sf001.environment = Environment(working_path=working_path)
    sql = engine_sf001.generate_sql("""import store_sales as ss;

select
    ss.item.sk,
    ss.ticket_number,
    ss.item.brand_name,
    ss.customer.current_address.state,
    sum(ss.net_paid) as total_paid,
    sum(ss.return_amount) as total_returned,
;""")[-1]
    rows = engine_sf001.execute_raw_sql(sql).fetchall()
    keys = [(r[0], r[1]) for r in rows]
    truth_count = engine_sf001.execute_raw_sql(
        "select count(*) from memory.store_sales"
    ).fetchone()[0]
    assert len(rows) == truth_count
    assert all(k[0] is not None and k[1] is not None for k in keys)


def test_item_customer_grain(engine_sf001: Executor):
    """No manufactured NULL-key rows or duplicate key pairs at (item,
    customer) grain — the unlicensed dim stitch stays LEFT, not FULL."""
    engine_sf001.environment = Environment(working_path=working_path)
    sql = engine_sf001.generate_sql("""import store_sales as ss;

select
    ss.item.sk,
    ss.customer.sk,
    ss.item.brand_name,
    ss.customer.current_address.state,
    sum(ss.net_paid) as total_paid,
    sum(ss.return_amount) as total_returned,
;""")[-1]
    rows = engine_sf001.execute_raw_sql(sql).fetchall()
    keys = [(r[0], r[1]) for r in rows]
    truth_count = engine_sf001.execute_raw_sql(
        "select count(*) from (select distinct ss_item_sk, ss_customer_sk from memory.store_sales)"
    ).fetchone()[0]
    assert len(rows) == truth_count
    assert all(k[0] is not None for k in keys)
    assert len(set(keys)) == len(keys)


def test_partial_grain_with_by_key_aggregate(engine_sf001: Executor):
    engine_sf001.environment = Environment(working_path=working_path)
    sql = engine_sf001.generate_sql("""import store_sales as ss;

auto customer_first_sale <- min(ss.sale_date.date) by ss.customer.sk;
auto sale_recency <- case when ss.sale_date.date = customer_first_sale then 'FIRST' else 'REPEAT' end;

select
    ss.item.sk,
    ss.ticket_number,
    ss.item.brand_name,
    sale_recency,
    sum(ss.net_paid) as total_paid,
    sum(ss.return_amount) as total_returned,
;""")[-1]
    rows = engine_sf001.execute_raw_sql(sql).fetchall()
    assert rows
