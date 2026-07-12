"""WHERE must filter rows BEFORE window derivations, regardless of whether the
filtered concept is a base column or a derived row-scalar, and regardless of
whether that concept also appears in the select list.

Regression: a derived scalar (concat label) present in both select and WHERE
forced condition evaluation at the top level, so windows were computed over the
unfiltered universe — `where vehicle_label = 'A-v1' select vehicle_label, rank
... having rank = 1` silently returned zero rows.

Every matrix cell is checked against a duckdb oracle that applies the filter
first and computes the window over the filtered rows only. The data is chosen
so every window/filter combination is order-sensitive: dropping vehicle B (the
middle vehicle by payload sum) shifts both asc and desc ranks, and dropping the
v2 variant rows shifts within-partition ranks.
"""

from typing import Any

import pytest

from trilogy import Dialects, Environment

DATA = """(values
    (1, 'A', 'v1', 100.0),
    (2, 'A', 'v2', 50.0),
    (3, 'B', 'v1', 70.0),
    (4, 'B', 'v2', 200.0),
    (5, 'C', 'v1', 400.0),
    (6, 'C', 'v2', 500.0)
) as t(launch_id, vehicle_name, vehicle_variant, orb_pay)"""
# Name-level sums: C=900, B=270, A=150 — B is the middle vehicle, so removing
# it shifts ranks in both directions; all orb_pay values are distinct so every
# ordering is total.

MODEL = f"""
key launch_id int;
property launch_id.vehicle_name string;
property launch_id.vehicle_variant string;
property launch_id.orb_pay float;

property vehicle_label <- concat(vehicle_name, '-', vehicle_variant);

datasource launches (
    launch_id,
    vehicle_name,
    vehicle_variant,
    orb_pay
)
grain (launch_id)
query '''
select * from {DATA}
''';
"""

ORACLE_BASE = f"""
select *, concat(vehicle_name, '-', vehicle_variant) as vehicle_label
from {DATA}
"""

FILTERS = [
    ("derived_label", "vehicle_label not like 'B-%'"),
    ("base_name", "vehicle_name != 'B'"),
    ("derived_variant", "vehicle_label like '%-v1'"),
    ("base_variant", "vehicle_variant != 'v2'"),
]

# (id, trilogy window expr, oracle window sql, oracle grain)
# grain "name" = window over per-vehicle_name aggregate; "row" = per launch row.
WINDOWS = [
    (
        "rank_agg_desc",
        "rank vehicle_name order by sum(orb_pay) by vehicle_name desc",
        "rank() over (order by s desc)",
        "name",
    ),
    (
        "rank_agg_asc",
        "rank vehicle_name order by sum(orb_pay) by vehicle_name asc",
        "rank() over (order by s asc)",
        "name",
    ),
    (
        "dense_rank_agg_desc",
        "dense_rank vehicle_name order by sum(orb_pay) by vehicle_name desc",
        "dense_rank() over (order by s desc)",
        "name",
    ),
    (
        "rank_row_desc",
        "rank launch_id order by orb_pay desc",
        "rank() over (order by orb_pay desc)",
        "row",
    ),
    (
        "rank_row_asc",
        "rank launch_id order by orb_pay asc",
        "rank() over (order by orb_pay asc)",
        "row",
    ),
    (
        "row_number_asc",
        "row_number launch_id order by orb_pay asc",
        "row_number() over (order by orb_pay asc)",
        "row",
    ),
    (
        "rank_partitioned_desc",
        "rank launch_id over vehicle_name order by orb_pay desc",
        "rank() over (partition by vehicle_name order by orb_pay desc)",
        "row",
    ),
    (
        "lag_row",
        "lag 1 orb_pay order by launch_id asc",
        "lag(orb_pay, 1) over (order by launch_id asc)",
        "row",
    ),
    (
        "lead_row",
        "lead 1 orb_pay order by launch_id asc",
        "lead(orb_pay, 1) over (order by launch_id asc)",
        "row",
    ),
]

# vehicle_label is the derived scalar; selecting it alongside a derived-filter
# WHERE is the regression trigger. vehicle_name is the base-concept control.
SELECTS = ["vehicle_label", "vehicle_name"]


def _oracle_sql(select_col: str, window_sql: str, grain: str, filter_sql: str) -> str:
    if grain == "name":
        return f"""
with f as ({ORACLE_BASE} where {filter_sql}),
a as (select vehicle_name, sum(orb_pay) as s from f group by 1),
w as (select vehicle_name, {window_sql} as w from a)
select distinct f.{select_col}, w.w from f inner join w using (vehicle_name)
"""
    return f"""
with f as ({ORACLE_BASE} where {filter_sql})
select distinct {select_col}, {window_sql} as w from f
"""


def _sorted(rows: list[tuple[Any, ...]]) -> list[tuple[Any, ...]]:
    return sorted(rows, key=lambda t: tuple((v is None, v) for v in t))


def _trilogy_rows(query: str) -> list[tuple[Any, ...]]:
    env = Environment()
    env.parse(MODEL)
    executor = Dialects.DUCK_DB.default_executor(environment=env)
    return _sorted(
        [tuple(r) for r in executor.execute_query(query).fetchall()]  # type: ignore[union-attr]
    )


def _oracle_rows(sql: str) -> list[tuple[Any, ...]]:
    executor = Dialects.DUCK_DB.default_executor(environment=Environment())
    return _sorted([tuple(r) for r in executor.execute_raw_sql(sql).fetchall()])


@pytest.mark.parametrize("select_col", SELECTS)
@pytest.mark.parametrize("filter_id,filter_expr", FILTERS, ids=[f[0] for f in FILTERS])
@pytest.mark.parametrize(
    "window_id,window_expr,window_sql,grain",
    WINDOWS,
    ids=[w[0] for w in WINDOWS],
)
def test_where_filters_rows_before_window(
    window_id: str,
    window_expr: str,
    window_sql: str,
    grain: str,
    filter_id: str,
    filter_expr: str,
    select_col: str,
) -> None:
    query = f"where {filter_expr}\nselect {select_col}, {window_expr} as w;"
    expected = _oracle_rows(_oracle_sql(select_col, window_sql, grain, filter_expr))
    actual = _trilogy_rows(query)
    assert actual == expected, (
        f"[{window_id}/{filter_id}/select {select_col}]\nquery:\n{query}\n"
        f"expected {expected}\ngot      {actual}"
    )


def test_rank_respects_where_on_derived_scalar_in_select() -> None:
    # The original repro: equality filter on the concat label leaves only
    # vehicle A rows, so A's rank within the filtered set must be 1, not its
    # global rank.
    rank = "rank vehicle_name order by sum(orb_pay) by vehicle_name desc"
    rows = _trilogy_rows(
        f"where vehicle_label = 'A-v1'\nselect vehicle_label, {rank} as vehicle_rank;"
    )
    assert rows == [("A-v1", 1)], rows


def test_having_on_filtered_rank_not_silently_empty() -> None:
    # User-facing symptom: `having vehicle_rank = 1` over the globally-computed
    # rank discarded every row.
    rank = "rank vehicle_name order by sum(orb_pay) by vehicle_name desc"
    rows = _trilogy_rows(
        f"where vehicle_label = 'A-v1'\n"
        f"select vehicle_label, {rank} as vehicle_rank\n"
        f"having vehicle_rank = 1;"
    )
    assert rows == [("A-v1", 1)], rows
