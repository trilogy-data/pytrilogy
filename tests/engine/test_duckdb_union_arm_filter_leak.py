"""A union node is one stacked source, but the planner joins its arms instead.

A `BuildUnionDatasource` (the partial-to-full injection) presents its arms as one
source and pushes a WHERE into each arm, exactly as for a single datasource. The
concept-level `union(...)` does not: the group graph hands the UNION group a
MergeNode that JOINS the arms on nothing, and `gen_union` re-plans per arm and
discards it. With no predicate that join is silently dropped and the answer is
right; give a predicate somewhere to live and the same join surfaces, either as a
keyless-join failure (filter on the stacked output) or as a filter that reaches no
node in the plan (filter on an arm's private column).
"""

from decimal import Decimal

from tests.engine.test_duckdb_union_arm_cast import MODEL
from trilogy import Dialects
from trilogy.core.exceptions import (
    DisconnectedConceptsException,
    UnresolvableQueryException,
)

REJECTIONS = (UnresolvableQueryException, DisconnectedConceptsException)


def _rows(query: str) -> list[tuple]:
    exec = Dialects.DUCK_DB.default_executor()
    return [tuple(r) for r in exec.execute_query(MODEL + "\n" + query).fetchall()]


def test_stacked_output_value_filter():
    assert _rows("select all_k, all_amt where all_amt > 0.15 order by all_k asc;") == [
        (2, Decimal("0.20"))
    ]


def test_stacked_output_key_filter():
    assert _rows("select all_k, all_amt where all_k > 2 order by all_k asc;") == [
        (3, Decimal("0.00")),
        (4, Decimal("0.00")),
    ]


def test_stacked_output_key_filter_without_value_output():
    assert _rows("select all_k where all_k > 2 order by all_k asc;") == [(3,), (4,)]


def test_stacked_output_filter_under_aggregate():
    assert _rows("select sum(all_amt) -> t where all_k > 2;") == [(Decimal("0.00"),)]


def test_having_on_stacked_output_control():
    assert _rows("select all_k, all_amt having all_amt > 0.15 order by all_k asc;") == [
        (2, Decimal("0.20"))
    ]


def test_arm_private_column_filter_is_not_silently_ignored():
    """`amt` is arm 1's own column, not a column of the stacked source. Rejecting
    the query is a fine answer; returning every row is not."""
    try:
        rows = _rows("select all_k, all_amt where amt > 0.15 order by all_k asc;")
    except REJECTIONS:
        return
    assert (1, Decimal("0.10")) not in rows


def test_other_arm_private_column_filter_is_not_silently_ignored():
    try:
        rows = _rows("select all_k, all_amt where pad > 0.15 order by all_k asc;")
    except REJECTIONS:
        return
    assert [k for k, _ in rows if k in (3, 4)] == []


CONSTANT_ARM_MODEL = (
    MODEL
    + "\nauto zero <- cast(0.0 as numeric(15,2));\nauto all_v <- union(amt, zero);"
)


def test_constant_arm_stacks_rather_than_cross_joins():
    """A constant arm has no key, so the arm join the group graph proposes
    degenerates into a visible cross product instead of being discarded."""
    exec = Dialects.DUCK_DB.default_executor()
    rows = exec.execute_query(
        CONSTANT_ARM_MODEL + "\nselect all_k, all_v order by all_k asc, all_v asc;"
    ).fetchall()
    assert len(rows) == 4
