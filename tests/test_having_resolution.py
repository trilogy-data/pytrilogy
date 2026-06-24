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


def test_window_partition_order_dim_in_having_not_in_select_errors_cleanly():
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
    with pytest.raises(InvalidSyntaxException) as exc:
        _generate(body)
    msg = str(exc.value)
    assert "aggregated.yr" in msg and "not in the SELECT projection" in msg
    assert "INVALID_REFERENCE_BUG" not in msg


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


def test_numbering_window_partition_dim_in_having_not_in_select_errors_cleanly():
    body = """
select
    aggregated.brand_id,
    rank() over (
        partition by aggregated.brand_id
        order by aggregated.yr asc) as yr_rank
having aggregated.yr = 2002;
"""
    with pytest.raises(InvalidSyntaxException) as exc:
        _generate(body)
    assert "aggregated.yr" in str(exc.value)


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


def test_bare_dim_in_having_not_in_select_errors_cleanly():
    body = """
select
    aggregated.brand_id,
    aggregated.total_qty
having aggregated.yr = 2002;
"""
    with pytest.raises(InvalidSyntaxException) as exc:
        _generate(body)
    assert "aggregated.yr" in str(exc.value)


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


def test_having_off_grain_aggregate_not_in_select_errors_cleanly():
    body = """
select
    aggregated.brand_id,
    aggregated.class_id,
    sum(aggregated.total_qty) as total
having sum(aggregated.total_qty) > 1.2 * sum(aggregated.total_qty) by aggregated.brand_id;
"""
    with pytest.raises(InvalidSyntaxException) as exc:
        _generate(body)
    assert "INVALID_REFERENCE_BUG" not in str(exc.value)


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
