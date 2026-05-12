"""Regression tests for the RowsetNode boundary semantics.

Each test pins a specific failure mode that we hit while building the
typed RowsetNode infrastructure. The TPC-DS suite exercises these in
context but is too coarse for catching the underlying bug — a fault
shows up only as a ~5% SQL bloat, a missing WHERE clause, or a wrong
row count buried in 100+ rows. These tests target the planner output
directly so a regression flips a single assertion.
"""

from trilogy.dialect.enums import Dialects


def _executor():
    return Dialects.DUCK_DB.default_executor()


# -----------------------------------------------------------------------------
# 1. Frozen full-grain dedup (q75 shape)
# -----------------------------------------------------------------------------

_DEDUP_FIXTURE = """
key row_id int;
property row_id.year int;
property row_id.group_key int;
property row_id.val1 int;
property row_id.val2 int;
property row_id.return_val1 int?;
property row_id.return_val2 int?;
property row_id.is_returned bool;

datasource src (
    row_id, year, group_key, val1, val2,
)
grain (row_id)
query '''
select 1 as row_id, 2001 as year, 100 as group_key, 10 as val1, 100 as val2 union all
select 2 as row_id, 2001 as year, 100 as group_key, 10 as val1, 200 as val2 union all
select 3 as row_id, 2001 as year, 100 as group_key, 20 as val1, 100 as val2 union all
select 4 as row_id, 2001 as year, 100 as group_key, 20 as val1, 200 as val2 union all
select 5 as row_id, 2002 as year, 100 as group_key, 11 as val1, 110 as val2 union all
select 6 as row_id, 2002 as year, 100 as group_key, 11 as val1, 220 as val2 union all
select 7 as row_id, 2002 as year, 100 as group_key, 22 as val1, 110 as val2 union all
select 8 as row_id, 2002 as year, 100 as group_key, 22 as val1, 220 as val2
''';

datasource src_returns (
    row_id: ~row_id, return_val1, return_val2, raw(''' True '''): is_returned,
)
grain (row_id)
complete where is_returned is True
query '''select 0 as row_id, 0 as return_val1, 0 as return_val2 where false''';
"""


def test_rowset_boundary_dedups_one_cte_across_filtered_aggregates():
    """q75 shape: four ``sum(... ? year=YYYY)`` over the same rowset.

    Each aggregate plans independently and asks the rowset for a tighter
    grain than its declared SELECT DISTINCT shape. RowsetNode pins the
    boundary so all four consumers see one shared rowset CTE rendered
    at the full ``(year, group_key, net_val1, net_val2)`` grain. Counts
    a regression as either a wrong sum (per-column dedup splits the
    grain) or multiple rowset materializations in the SQL.
    """
    e = _executor()
    e.execute_text(_DEDUP_FIXTURE)
    rows = e.execute_text("""
auto net_val1 <- val1 - coalesce(return_val1, 0);
auto net_val2 <- val2 - coalesce(return_val2, 0);

with deduped as
SELECT year, group_key, net_val1, net_val2,
;

SELECT
    deduped.group_key as gk,
    sum(deduped.net_val1 ? deduped.year = 2001) as v1_2001,
    sum(deduped.net_val1 ? deduped.year = 2002) as v1_2002,
    sum(deduped.net_val2 ? deduped.year = 2001) as v2_2001,
    sum(deduped.net_val2 ? deduped.year = 2002) as v2_2002,
;
""")[0].fetchall()
    assert len(rows) == 1
    row = rows[0]
    # 2001 rows: val1 ∈ {10,20} × val2 ∈ {100,200} → 4 distinct tuples.
    # If the rowset's grain were collapsed to (group_key, net_val1)
    # only, sum(net_val1) would shrink from 60 to 30 (per-column dedup).
    assert row.v1_2001 == 60
    assert row.v2_2001 == 600
    assert row.v1_2002 == 66
    assert row.v2_2002 == 660


# -----------------------------------------------------------------------------
# 2. Multi-select align: one column per safe_address (q68 shape)
# -----------------------------------------------------------------------------


def test_multiselect_align_does_not_duplicate_projection_by_safe_address():
    """q68 shape: a multi-select aligns ``store_sales.customer.id`` and
    ``customer.id`` into a canonical ``customer_id``. Both
    ``dn.customer.id`` and ``dn.customer_id`` are declared rowset
    items, but they share a ``safe_address`` (their namespace+name
    collapse to the same SQL identifier). Rendering both as separate
    CTE projections produces a duplicate ``"customer_id"`` column in
    the rowset CTE. The dedup-by-safe_address in
    ``gen_rowset_node`` collapses them to a single projection.
    """
    e = _executor()
    e.execute_text("""
key customer_id int;
property customer_id.first_name string;
property customer_id.last_name string;
key sale_id int;
property sale_id.customer_id int;
property sale_id.amount float;

datasource customers (
    id: customer_id,
    first_name: first_name,
    last_name: last_name,
)
grain (customer_id)
query '''
select 1 as id, 'Ada' as first_name, 'Lovelace' as last_name union all
select 2 as id, 'Alan' as first_name, 'Turing' as last_name
''';

datasource sales (
    id: sale_id,
    customer_id: customer_id,
    amount: amount,
)
grain (sale_id)
query '''select 1 as id, 1 as customer_id, 100.0 as amount union all
         select 2 as id, 2 as customer_id, 200.0 as amount''';
""")
    sql = e.generate_sql("""
rowset combined <- select
    customer_id,
    last_name,
MERGE
select
    customer_id,
    sum(amount) as total_amount,
align
    cid: customer_id, customer_id
;

select
    combined.last_name,
    combined.total_amount,
;
""")[-1]
    # The rowset CTE used to emit two ``"customer_id"`` aliases against
    # the same source column when ``combined.customer_id`` (the align
    # alias) and ``combined.<branch>.customer_id`` (the source-side)
    # had matching ``safe_address``. Count occurrences of the literal
    # alias to make a regression loud.
    duplicate_alias_count = sql.count(' as "customer_id",') + sql.count(
        ' as "customer_id"\n'
    )
    assert duplicate_alias_count <= 2, (
        f"customer_id aliased {duplicate_alias_count} times — "
        f"one per branch CTE is fine, more means safe_address dedup regressed.\n{sql}"
    )


# -----------------------------------------------------------------------------
# 3. Existence-only IN-subselect doesn't cross-join (q23 shape)
# -----------------------------------------------------------------------------


def test_rowset_used_only_in_where_in_does_not_cross_join():
    """q23 shape: outer WHERE filters via ``fact.id in rowset.id``.
    The rowset CTE is rendered as a subselect inside WHERE, never as a
    join. A regression silently adds ``INNER JOIN <rowset> on 1=1``,
    multiplying the fact's row count by the rowset's row count.
    """
    e = _executor()
    e.execute_text("""
key item_id int;
property item_id.name string;

datasource items (
    id: item_id,
    name: name,
)
grain (item_id)
query '''
select 1 as id, 'A' as name union all
select 2 as id, 'B' as name union all
select 3 as id, 'C' as name
''';

key sale_id int;
property sale_id.item_id int;
property sale_id.qty int;

datasource sales (
    id: sale_id,
    item_id: item_id,
    qty: qty,
)
grain (sale_id)
query '''
select 1 as id, 1 as item_id, 5 as qty union all
select 2 as id, 1 as item_id, 7 as qty union all
select 3 as id, 2 as item_id, 3 as qty union all
select 4 as id, 3 as item_id, 9 as qty
''';
""")
    sql = e.generate_sql("""
with hot_items as
where qty > 5
select item_id as hot_item_id;

where item_id in hot_items.hot_item_id
select item_id, sum(qty) as total_qty;
""")[-1]
    # The hot_items rowset CTE name is dialect-randomized, but the
    # ``INNER JOIN <something> on 1=1`` shape is the regression marker.
    assert (
        "on 1=1" not in sql
    ), f"Existence-only rowset got cross-joined into the fact CTE.\n{sql}"
    # Functional check: results match what a hand-written subquery would
    # produce.
    rows = e.execute_text("""
with hot_items as
where qty > 5
select item_id as hot_item_id;

where item_id in hot_items.hot_item_id
select item_id, sum(qty) as total_qty
order by item_id asc;
""")[0].fetchall()
    # item 1 has rows with qty=5,7 → only qty=7 is "hot", but the WHERE
    # is applied at the rowset level and surfaces *items* with any hot
    # row. Then we sum ALL rows for those items.
    # item_id 1: rows qty=5+7=12; item_id 3: rows qty=9
    assert sorted(rows) == [(1, 12), (3, 9)], rows


# -----------------------------------------------------------------------------
# 4. WHERE clause on rowset items propagates through the boundary (q64 shape)
# -----------------------------------------------------------------------------


def test_outer_where_on_rowset_items_is_applied():
    """q64 shape: an outer SELECT filters by rowset items
    (``where rowset.cnt_a <= rowset.cnt_b``). The boundary delegates
    its resolve to the inner select; the outer's WHERE clause must be
    propagated onto that inner so the rendered CTE actually filters.
    A regression silently drops the WHERE — query parses, runs, and
    returns (wrong) extra rows.
    """
    e = _executor()
    e.execute_text("""
key id int;
property id.value int;

datasource src (
    id: id,
    value: value,
)
grain (id)
query '''
select 1 as id, 10 as value union all
select 2 as id, 20 as value union all
select 3 as id, 30 as value
''';
""")
    sql = e.generate_sql("""
rowset r <- select id, value;

where r.value > 15
select r.id, r.value;
""")[-1]
    assert (
        'value" > 15' in sql or "value > 15" in sql
    ), f"WHERE on rowset item didn't reach the rendered SQL.\n{sql}"
    rows = e.execute_text("""
rowset r <- select id, value;

where r.value > 15
select r.id, r.value
order by r.id asc;
""")[0].fetchall()
    assert rows == [(2, 20), (3, 30)]


# -----------------------------------------------------------------------------
# 5. Rowset CTE is shared across multiple aggregate consumers
# -----------------------------------------------------------------------------


def test_two_aggregates_over_one_rowset_share_one_cte():
    """The frozen boundary's whole point: when multiple consumers each
    pull a different scalar through the same rowset, the rowset
    materialization renders once. Counts ``select`` keywords as a
    proxy — the rowset's source CTE plus one CTE per consumer beats N
    separate rowset materializations.
    """
    e = _executor()
    e.execute_text(_DEDUP_FIXTURE)
    sql = e.generate_sql("""
auto net_val1 <- val1 - coalesce(return_val1, 0);
auto net_val2 <- val2 - coalesce(return_val2, 0);

with deduped as
SELECT year, group_key, net_val1, net_val2,
;

SELECT
    deduped.group_key as gk,
    sum(deduped.net_val1) as t1,
    sum(deduped.net_val2) as t2,
;
""")[-1]
    # The rowset materialization is the CTE that contains the
    # ``deduped_*`` columns and the FULL grain GROUP BY (1, 2, 3, plus
    # the unkept ``year``). If we materialize it twice, two CTEs would
    # both project ``deduped_net_val1`` and the source table would be
    # scanned twice. Detect by counting the inline-data block.
    inline_data_scans = sql.count("select 1 as row_id, 2001 as year")
    assert inline_data_scans == 1, (
        f"Inline source data scanned {inline_data_scans} times — rowset "
        f"is being materialized more than once.\n{sql}"
    )
