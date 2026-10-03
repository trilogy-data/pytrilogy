"""A `union join` axis is the union of both sides' keys, whichever side's
columns the statement reads; a value absent on a side is NULL there."""

import pytest

from tests.helpers.rows import executor_for, sorted_rows

MODEL = """
key aid int;
property aid.a int?;
key bid int;
property bid.b int?;

datasource ta (aid: aid, a: a)
grain (aid)
query '''select 1 as aid, 10 as a union all select 2, 20''';

datasource tb (id: bid, b: b)
grain (bid)
query '''select 2 as id, 200 as b union all select 3, 300''';

auto id_label <- concat('x', cast(aid as string));
"""

ONE_SIDED_REASON = (
    "pre-existing on main: a request whose non-axis output sits at one arm's "
    "row grain is read arm-locally (`_axis_arm_pinned` in "
    "network_coalescing.py, meant for an aggregate's parent that a later "
    "assembly coalesces); at the statement's top level nothing coalesces the "
    "other arm, so its keys are dropped"
)
DERIVED_REASON = (
    "pre-existing on main: a scalar over the axis key is computed inside one "
    "arm's scan rather than over the coalesced key, so it is NULL on the "
    "other arm's rows (main and this branch pick different arms)"
)


@pytest.mark.parametrize(
    "query,expected",
    [
        ("select aid union join aid = bid", [(1,), (2,), (3,)]),
        (
            "select aid, a, b union join aid = bid",
            [(1, 10, None), (2, 20, 200), (3, None, 300)],
        ),
        (
            "select aid, coalesce(a, 0) as a0, b union join aid = bid",
            [(1, 10, None), (2, 20, 200), (3, 0, 300)],
        ),
        (
            "select aid, id_label union join aid = bid",
            [(1, "x1"), (2, "x2"), (3, "x3")],
        ),
        pytest.param(
            "select aid, b union join aid = bid",
            [(1, None), (2, 200), (3, 300)],
            marks=pytest.mark.xfail(strict=True, reason=ONE_SIDED_REASON),
        ),
        pytest.param(
            "select aid, id_label, b union join aid = bid",
            [(1, "x1", None), (2, "x2", 200), (3, "x3", 300)],
            marks=pytest.mark.xfail(strict=True, reason=ONE_SIDED_REASON),
        ),
        pytest.param(
            "select aid, id_label, a union join aid = bid",
            [(1, "x1", 10), (2, "x2", 20), (3, "x3", None)],
            marks=pytest.mark.xfail(strict=True, reason=ONE_SIDED_REASON),
        ),
        pytest.param(
            "select aid, sum(b) as sb union join aid = bid",
            [(1, None), (2, 200), (3, 300)],
            marks=pytest.mark.xfail(strict=True, reason=ONE_SIDED_REASON),
        ),
        pytest.param(
            "select aid, id_label, a, b union join aid = bid",
            [(1, "x1", 10, None), (2, "x2", 20, 200), (3, "x3", None, 300)],
            marks=pytest.mark.xfail(strict=True, reason=DERIVED_REASON),
        ),
    ],
)
def test_union_join_axis_keeps_both_sides(query: str, expected: list[tuple]):
    assert sorted_rows(executor_for(MODEL), query) == expected
