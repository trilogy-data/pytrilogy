"""`<agg>(<key> ? <cond>) by <key>` must not compile to a nested aggregate.

Regression for TPC-DS q95 (`evals/tpcds_agent/bug_q95_nested_aggregate_codegen.md`),
a fallout of the q16 count(key) double-count fix. That fix excludes a filter
virtual whose keys are covered by the grouping CTE's grain -- but whose predicate
reads outside it -- from GROUP BY, and wraps its rendered `CASE WHEN` in `MAX(...)`
to collapse the per-row `{content, NULL}` fan-out. The wrap fires on every render
of the concept in that CTE, including when the render is an *argument* of an
aggregate composed in the same CTE, emitting `count(max(CASE ...))`: rejected by
DuckDB as a nested aggregate, and not the denoted per-group value even where the
nesting is legal.

The wrap has already reduced the group to the one grain-determined value the
filter denotes, so the enclosing aggregate is an aggregate of a single row and
renders as its AGGREGATE_GRAIN_MATCH_MAP collapse formula. Every aggregate was
affected, not just `count`; the filter may also sit inside a larger argument
expression.

Needs all of: filtered content keyed inside the `by` grain, predicate reading a
column outside it, and the aggregate computed in the grouping CTE itself.
"""

import re

import pytest

from trilogy import Dialects
from trilogy.core.models.environment import Environment
from trilogy.executor import Executor

# order 1: one flagged line of two; order 2: none flagged; order 3: both flagged.
MODEL = """
key order_number int;
key item int;
property <order_number, item>.flag bool;

datasource lines (
  o: order_number,
  i: item,
  f: flag
)
grain (order_number, item)
query '''
select 1 o, 1 i, false f union all select 1 o, 2 i, true f union all
select 2 o, 1 i, false f union all
select 3 o, 1 i, true f union all select 3 o, 2 i, true f''';
"""

# an aggregate call wrapping the collapse MAX; the projected (un-aggregated)
# collapse of the q16 fix is a bare `max(CASE ...)` and must still be allowed.
NESTED_AGGREGATE = re.compile(
    r"(count|count_distinct|sum|avg|min|max|array_agg)\s*\(\s*(distinct\s+)?max\s*\(",
    re.IGNORECASE,
)

# `count` collapses to a 0/1 presence flag; value aggregates collapse to the one
# deduplicated key the filter kept (NULL where the group kept none).
PRESENCE = [(1, 1), (2, 0), (3, 1)]
VALUE = [(1, 1), (2, None), (3, 3)]

# the same filtered count with no `by` regroup, which always compiled cleanly
TOP_LEVEL_QUERY = MODEL + "select count(order_number ? flag) as flagged;"
REGROUPED_COUNT = (
    MODEL + "select order_number, count(order_number ? flag) by order_number as v"
    " order by order_number asc;"
)


def _select(projection: str) -> str:
    return f"{MODEL}select order_number, {projection} as v order by order_number asc;"


@pytest.fixture
def executor() -> Executor:
    return Dialects.DUCK_DB.default_executor(environment=Environment())


@pytest.mark.parametrize(
    "projection,expected",
    [
        pytest.param(
            "count(order_number ? flag) by order_number", PRESENCE, id="count"
        ),
        pytest.param(
            "count_distinct(order_number ? flag) by order_number",
            PRESENCE,
            id="count_distinct",
        ),
        pytest.param("sum(order_number ? flag) by order_number", VALUE, id="sum"),
        pytest.param("max(order_number ? flag) by order_number", VALUE, id="max"),
        pytest.param("min(order_number ? flag) by order_number", VALUE, id="min"),
        pytest.param("avg(order_number ? flag) by order_number", VALUE, id="avg"),
        # the filter nested inside a larger argument expression: coalesce makes
        # every group present, so the count is 1 even for the unflagged order.
        pytest.param(
            "count(coalesce(order_number ? flag, 0)) by order_number",
            [(1, 1), (2, 1), (3, 1)],
            id="inside_coalesce",
        ),
        # the aggregate consumed by an enclosing expression rather than projected
        pytest.param(
            "(count(order_number ? flag) by order_number) * 10",
            [(1, 10), (2, 0), (3, 10)],
            id="basic_over_aggregate",
        ),
    ],
)
def test_aggregate_over_collapsed_filter_is_not_nested(
    executor: Executor, projection: str, expected: list[tuple[int, int | None]]
):
    sql = executor.generate_sql(_select(projection))[-1]
    assert not NESTED_AGGREGATE.search(sql), sql
    assert [tuple(r) for r in executor.execute_raw_sql(sql).fetchall()] == expected


def test_filtered_count_of_grain_key_matches_top_level_total(executor: Executor):
    grouped = executor.execute_text(REGROUPED_COUNT)[-1].fetchall()
    total = executor.execute_text(TOP_LEVEL_QUERY)[-1].fetchall()
    assert sum(r[1] for r in grouped) == total[0][0] == 2
