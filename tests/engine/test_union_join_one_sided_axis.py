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
        (
            "select aid, b union join aid = bid",
            [(1, None), (2, 200), (3, 300)],
        ),
        (
            "select aid, id_label, b union join aid = bid",
            [(1, "x1", None), (2, "x2", 200), (3, "x3", 300)],
        ),
        (
            "select aid, id_label, a union join aid = bid",
            [(1, "x1", 10), (2, "x2", 20), (3, "x3", None)],
        ),
        (
            "select aid, sum(b) as sb union join aid = bid",
            [(1, None), (2, 200), (3, 300)],
        ),
        (
            "select aid, id_label, a, b union join aid = bid",
            [(1, "x1", 10, None), (2, "x2", 20, 200), (3, "x3", None, 300)],
        ),
        ("where b > 250 select aid, b union join aid = bid", [(3, 300)]),
        (
            "select aid, sum(a) as sa, sum(b) as sb union join aid = bid",
            [(1, 10, None), (2, 20, 200), (3, None, 300)],
        ),
        (
            "select aid, count(a) as ca union join aid = bid",
            [(1, 1), (2, 1), (3, 0)],
        ),
        (
            "select aid, a, sum(b) by * as t union join aid = bid",
            [(1, 10, 500), (2, 20, 500), (3, None, 500)],
        ),
        (
            "select aid, b, sum(a) by * as t union join aid = bid",
            [(1, None, 30), (2, 200, 30), (3, 300, 30)],
        ),
    ],
)
def test_union_join_axis_keeps_both_sides(query: str, expected: list[tuple]):
    assert sorted_rows(executor_for(MODEL), query) == expected
