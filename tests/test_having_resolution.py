"""HAVING / ORDER BY projection-validation permutations.

These exercise the parse-time validator that requires every bare HAVING/ORDER BY
reference to be a genuine output column (or the shared input of a matching SELECT
aggregate). The window cases are regression coverage for the q75 bug where a
dimension appearing only inside a SELECT window's partition/order was wrongly
treated as projected, bypassing the validator and leaking the
``INVALID_REFERENCE_BUG`` sentinel into rendered SQL.
"""

import pytest

from trilogy import Dialects
from trilogy.core.exceptions import InvalidSyntaxException
from trilogy.core.models.environment import Environment
from trilogy.parser import parse

MODEL = """
key item_id int;
property item_id.brand_id int;
property item_id.class_id int;
key sale_id int;
property sale_id.item_id int;
property sale_id.year int;
property sale_id.quantity int;

datasource items (
    id: item_id,
    brand: brand_id,
    class: class_id,
)
grain (item_id)
address items;

datasource sales (
    id: sale_id,
    item: item_id,
    yr: year,
    qty: quantity,
)
grain (sale_id)
address sales;

rowset aggregated <- select
    year as yr,
    brand_id as brand_id,
    class_id as class_id,
    sum(quantity) as total_qty;
"""


def _executor():
    e = Dialects.DUCK_DB.default_executor(environment=Environment())
    e.parse_text(MODEL)
    return e


def _generate(body: str) -> str:
    return "\n".join(_executor().generate_sql(body))


# --- window-internal dimension references -----------------------------------


def test_window_partition_order_dim_in_having_resolves_as_semijoin():
    """A HAVING on a dimension (yr) finer than the select grain is a
    post-aggregation semijoin: keep the (brand, class) groups that have a yr=2002
    row, leaving the windowed measure unchanged. Resolves to a composite key
    membership rather than erroring."""
    body = """
select
    aggregated.brand_id,
    aggregated.class_id,
    aggregated.total_qty
        - lag(aggregated.total_qty, 1) over (
              partition by aggregated.brand_id, aggregated.class_id
              order by aggregated.yr asc) as qty_diff
having aggregated.yr = 2002;
"""
    sql = _generate(body)
    assert "INVALID_REFERENCE_BUG" not in sql
    assert "exists (select" in sql.lower()


def test_window_partition_order_dim_in_order_by_not_in_select_errors_cleanly():
    body = """
select
    aggregated.brand_id,
    aggregated.class_id,
    aggregated.total_qty
        - lag(aggregated.total_qty, 1) over (
              partition by aggregated.brand_id, aggregated.class_id
              order by aggregated.yr asc) as qty_diff
order by aggregated.yr asc;
"""
    with pytest.raises(InvalidSyntaxException) as exc:
        _generate(body)
    assert "aggregated.yr" in str(exc.value)
    assert "INVALID_REFERENCE_BUG" not in str(exc.value)


def test_numbering_window_partition_dim_in_having_resolves_as_semijoin():
    body = """
select
    aggregated.brand_id,
    rank() over (
        partition by aggregated.brand_id
        order by aggregated.yr asc) as yr_rank
having aggregated.yr = 2002;
"""
    sql = _generate(body)
    assert "INVALID_REFERENCE_BUG" not in sql
    assert "exists (select" in sql.lower()


def test_window_dim_in_having_succeeds_when_hidden_in_select():
    body = """
select
    aggregated.brand_id,
    aggregated.class_id,
    --aggregated.yr,
    aggregated.total_qty
        - lag(aggregated.total_qty, 1) over (
              partition by aggregated.brand_id, aggregated.class_id
              order by aggregated.yr asc) as qty_diff
having aggregated.yr = 2002;
"""
    sql = _generate(body)
    assert "INVALID_REFERENCE_BUG" not in sql


def test_window_dim_in_having_succeeds_when_visible_in_select():
    body = """
select
    aggregated.brand_id,
    aggregated.yr,
    aggregated.total_qty
        - lag(aggregated.total_qty, 1) over (
              partition by aggregated.brand_id
              order by aggregated.yr asc) as qty_diff
having aggregated.yr = 2002;
"""
    sql = _generate(body)
    assert "INVALID_REFERENCE_BUG" not in sql


# --- bare-dimension references (no window) ----------------------------------


def test_bare_dim_in_having_resolves_as_semijoin():
    body = """
select
    aggregated.brand_id,
    aggregated.total_qty
having aggregated.yr = 2002;
"""
    sql = _generate(body)
    assert "INVALID_REFERENCE_BUG" not in sql
    assert "exists (select" in sql.lower()


def test_bare_dim_in_having_succeeds_when_in_select():
    body = """
select
    aggregated.brand_id,
    aggregated.yr,
    aggregated.total_qty
having aggregated.yr = 2002;
"""
    sql = _generate(body)
    assert "INVALID_REFERENCE_BUG" not in sql


# --- aggregate references ----------------------------------------------------


def test_having_aggregate_matching_select_accepted():
    """A `sum(x)` in HAVING resolves against the `sum(x) as total` output via the
    shared input `x` — the aggregate's source must stay an allowed reference."""
    parse(MODEL + """
select
    aggregated.brand_id,
    sum(aggregated.total_qty) as brand_qty
having sum(aggregated.total_qty) > 15;
""")


def test_having_off_grain_aggregate_promoted_to_hidden_output():
    """An off-grain aggregate in HAVING that is not a SELECT output is promoted to
    a hidden output (computed in its own CTE) rather than erroring."""
    body = """
select
    aggregated.brand_id,
    aggregated.class_id,
    sum(aggregated.total_qty) as total
having sum(aggregated.total_qty) > 1.2 * sum(aggregated.total_qty) by aggregated.brand_id;
"""
    sql = _generate(body)
    assert "INVALID_REFERENCE_BUG" not in sql


# --- scalar-derived column sources ------------------------------------------


def test_having_on_scalar_derived_source_dim_accepted():
    """A scalar derivation keeps its operands at the output grain, so HAVING may
    filter on a dimension that feeds a derived (non-window) output column."""
    body = """
select
    aggregated.brand_id,
    aggregated.class_id + aggregated.brand_id as combo
having aggregated.class_id = 3;
"""
    sql = _generate(body)
    assert "INVALID_REFERENCE_BUG" not in sql


# --- aggregate-operand dimension in a bare HAVING (q72) ----------------------
#
# A raw concept consumed by a selected aggregate (as its argument or inside a
# filtered aggregate's condition) is NOT materialized at the output grain — the
# grouping collapses it. A bare HAVING reference to it must still resolve as the
# documented post-aggregation semijoin, exactly as if the concept fed no select
# expression at all. Before the fix it was treated as projected, skipping the
# rewrite: single-source plans silently pushed the predicate pre-aggregation
# (corrupting every aggregate) and multi-source scoped-join plans reached
# rendering with no backing CTE (INVALID_REFERENCE_BUG).

ORACLE_MODEL = """
key sale_id int;
property sale_id.brand_id int;
property sale_id.promo_id int?;
property sale_id.qty int;

datasource sales (
    id: sale_id,
    brand: brand_id,
    promo: promo_id,
    qty: qty,
)
grain (sale_id)
query '''select 1 as id, 1 as brand, null as promo, 10 as qty union all
         select 2, 1, 5, 20 union all
         select 3, 2, 7, 30 union all
         select 4, 2, 8, 40 union all
         select 5, 3, null, 50''';
"""


def _oracle_executor():
    e = Dialects.DUCK_DB.default_executor(environment=Environment())
    e.parse_text(ORACLE_MODEL)
    return e


def _run_oracle(body: str) -> tuple[str, list[tuple]]:
    e = _oracle_executor()
    sql = "\n".join(e.generate_sql(body))
    rows = e.execute_text(body)[-1].fetchall()
    return sql, [tuple(r) for r in rows]


def _assert_semijoin(sql: str) -> None:
    assert "INVALID_REFERENCE_BUG" not in sql
    assert "exists (select" in sql.lower()


def test_filtered_count_input_dim_in_having_semijoins():
    """The q72 shape: `promo_id` feeds a selected filtered count and a bare
    HAVING `is null` predicate, and is not projected. Groups with a promo-null
    row survive; the aggregates stay computed over the full pre-HAVING set."""
    body = """
select
    brand_id,
    count(sale_id) as total_orders,
    count(sale_id ? promo_id is not null) as with_promo_orders
having promo_id is null
order by brand_id asc;
"""
    sql, rows = _run_oracle(body)
    _assert_semijoin(sql)
    assert rows == [(1, 2, 1), (3, 1, 0)]


def test_filtered_count_input_dim_in_having_is_not_null_semijoins():
    body = """
select
    brand_id,
    count(sale_id) as total_orders,
    count(sale_id ? promo_id is not null) as with_promo_orders
having promo_id is not null
order by brand_id asc;
"""
    sql, rows = _run_oracle(body)
    _assert_semijoin(sql)
    assert rows == [(1, 2, 1), (2, 2, 2)]


def test_plain_aggregate_operand_dim_in_having_semijoins():
    """The dim is the aggregate's own argument (`count(promo_id)`), not a filter
    condition — same contract: not projected means semijoin, not pushdown."""
    body = """
select
    brand_id,
    count(promo_id) as promo_count
having promo_id is null
order by brand_id asc;
"""
    sql, rows = _run_oracle(body)
    _assert_semijoin(sql)
    assert rows == [(1, 1), (3, 0)]


def test_filtered_sum_input_dim_in_having_semijoins():
    body = """
select
    brand_id,
    sum(qty ? promo_id is not null) as promo_qty
having promo_id is null
order by brand_id asc;
"""
    sql, rows = _run_oracle(body)
    _assert_semijoin(sql)
    assert rows == [(1, 20), (3, None)]


def test_filtered_count_distinct_input_dim_in_having_semijoins():
    body = """
select
    brand_id,
    count_distinct(sale_id ? promo_id is not null) as promo_sales
having promo_id is null
order by brand_id asc;
"""
    sql, rows = _run_oracle(body)
    _assert_semijoin(sql)
    assert rows == [(1, 1), (3, 0)]


def test_control_projected_dim_with_filtered_count_stays_direct():
    """Projecting the raw dim keeps it a plain output-grain HAVING filter — no
    semijoin required and no sentinel."""
    body = """
select
    brand_id,
    --promo_id,
    count(sale_id) as total_orders,
    count(sale_id ? promo_id is not null) as with_promo_orders
having promo_id is null
order by brand_id asc;
"""
    sql, rows = _run_oracle(body)
    assert "INVALID_REFERENCE_BUG" not in sql
    assert rows == [(1, 1, 0), (3, 1, 0)]


def test_control_aggregate_alias_in_having_stays_direct():
    body = """
select
    brand_id,
    count(sale_id) as total_orders,
    count(sale_id ? promo_id is not null) as with_promo_orders
having with_promo_orders = 1
order by brand_id asc;
"""
    sql, rows = _run_oracle(body)
    assert "INVALID_REFERENCE_BUG" not in sql
    assert rows == [(1, 2, 1)]


SCOPED_JOIN_MODEL = """
key s_id int;
property s_id.s_item int;
property s_id.s_week int;
property s_id.s_promo int?;
property s_id.s_qty int;

datasource sales_src (
    id: s_id,
    item: s_item,
    week: s_week,
    promo: s_promo,
    qty: s_qty,
)
grain (s_id)
query '''select 1 as id, 1 as item, 1 as week, null as promo, 10 as qty union all
         select 2, 1, 1, 5, 10 union all
         select 3, 2, 1, 6, 10''';

key i_id int;
property i_id.i_item int;
property i_id.i_week int;
property i_id.i_qoh int;

datasource inventory_src (
    id: i_id,
    item: i_item,
    week: i_week,
    qoh: i_qoh,
)
grain (i_id)
query '''select 1 as id, 1 as item, 1 as week, 5 as qoh union all
         select 2, 2, 1, 20 as qoh''';
"""


def test_scoped_composite_join_filtered_count_having_semijoins():
    """The full q72 combination: two datasources under scoped composite subset
    joins, a WHERE spanning both, a selected filtered count consuming the dim,
    and a bare finer-grain HAVING on it. Must render the semijoin (with the
    query WHERE inside the existence set) instead of leaking the missing-source
    sentinel."""
    e = Dialects.DUCK_DB.default_executor(environment=Environment())
    e.parse_text(SCOPED_JOIN_MODEL)
    body = """
where i_qoh < s_qty

subset join s_item = i_item
subset join s_week = i_week

select
    s_item,
    s_week,
    count(s_id) as total_orders,
    count(s_id ? s_promo is not null) as with_promo_orders
having s_promo is null
order by s_item asc;
"""
    sql = "\n".join(e.generate_sql(body))
    _assert_semijoin(sql)
    rows = [tuple(r) for r in e.execute_text(body)[-1].fetchall()]
    assert rows == [(1, 1, 2, 1)]


def test_scoped_composite_join_having_is_not_null_semijoins():
    e = Dialects.DUCK_DB.default_executor(environment=Environment())
    e.parse_text(SCOPED_JOIN_MODEL)
    body = """
where i_qoh < s_qty

subset join s_item = i_item
subset join s_week = i_week

select
    s_item,
    s_week,
    count(s_id) as total_orders,
    count(s_id ? s_promo is not null) as with_promo_orders
having s_promo is not null
order by s_item asc;
"""
    sql = "\n".join(e.generate_sql(body))
    _assert_semijoin(sql)
    rows = [tuple(r) for r in e.execute_text(body)[-1].fetchall()]
    assert rows == [(1, 1, 2, 1)]
