"""`formal/Trilogy/Windows.lean`, "Grouping by a window's value": a select of a
window beside an aggregate groups the rows by the window's value
(`Windows.regroup_by_value_exact`). Grouping by the tuple a numbering window
ranks and projecting the rank splits one rank into a group per tuple
(`Windows.group_by_tuple_splits_value`), and a value window's operand does not
determine it at all (`Windows.lag_reads_rows_not_values`). Every case runs on
DuckDB and SQLite, as an `auto` concept and inline, and must agree with the
reference."""

from typing import Any

import pytest

from tests.windows.test_window_laws import (
    DIALECTS,
    SEEDS,
    Order,
    fixture,
    frame_value,
    make_rows,
    over_clause,
    partitions,
    rank_tuples,
    sort_key,
)
from trilogy import Dialects, Environment

BY_ID: list[Order] = [("id", False, False)]
BY_X: list[Order] = [("x", False, False), ("id", True, False)]
PEERS: list[Order] = [("x", False, False)]

# (function, operand or rank arguments, partition, order)
WINDOWS: list[tuple[str, tuple[str, ...], tuple[str, ...], list[Order]]] = (
    [
        (fn, (operand,), partition, order)
        for fn in ("lag", "lead")
        for operand in ("x", "id")
        for partition in ((), ("g",))
        for order in (BY_ID, BY_X)
    ]
    + [
        (fn, ("x",), partition, order)
        for fn in ("sum", "count", "max")
        for partition in ((), ("g",))
        for order in (BY_ID, PEERS)
    ]
    + [
        (fn, args, partition, [order])
        for fn in ("rank", "dense_rank")
        for args in (("x",), ("g",), ("g", "x"))
        for partition in ((), ("g",))
        for order in (("x", False, False), ("g", True, False))
    ]
)

SELECTS = {
    "count": ((), "count(id) as c"),
    "sum": ((), "sum(x) as c"),
    "by_g": (("g",), "count(id) as c"),
    "by_x": (("x",), "count(id) as c"),
}
SPELLINGS = ("auto", "inline")


def window_sql(fn: str, args: tuple[str, ...], partition, order) -> str:
    return f"{fn}({', '.join(args)}) {over_clause(partition, order)}"


def query(window: str, select: str, spelling: str) -> str:
    dims, aggregate = SELECTS[select]
    if spelling == "auto":
        return f"auto w <- {window}; select {', '.join([*dims, 'w', aggregate])};"
    return f"select {', '.join([*dims, f'{window} as w', aggregate])};"


def window_values(
    rows: list[dict[str, Any]], fn: str, args: tuple[str, ...], partition, order
) -> None:
    if fn in ("rank", "dense_rank"):
        grain = sorted({*args, *partition, order[0][0]}, key=["id", "g", "x"].index)
        ranks = rank_tuples(rows, grain, partition, order, fn == "dense_rank")
        for row in rows:
            row["w"] = ranks[tuple(row[c] for c in grain)]
        return
    for members in partitions(rows, partition):
        members.sort(key=lambda r: sort_key(r, order))
        keys = [sort_key(r, order) for r in members]
        for i, row in enumerate(members):
            row["w"] = frame_value(fn, members, i, keys, args[0])


def reference(rows: list[dict[str, Any]], select: str) -> list[tuple]:
    dims, _ = SELECTS[select]
    groups: dict[tuple, list[dict[str, Any]]] = {}
    for row in rows:
        groups.setdefault((*(row[d] for d in dims), row["w"]), []).append(row)
    out = []
    for key, members in groups.items():
        if select == "sum":
            present = [m["x"] for m in members if m["x"] is not None]
            value: Any = sum(present) if present else None
        else:
            value = len(members)
        out.append((*key, value))
    return sorted(out, key=repr)


CASES = [
    (window, select, spelling)
    for window in WINDOWS
    for select in SELECTS
    for spelling in SPELLINGS
]


@pytest.mark.parametrize("dialect", DIALECTS, ids=lambda d: d.value)
@pytest.mark.parametrize("window, select, spelling", CASES, ids=repr)
def test_group_by_window_value(
    dialect: Dialects, window: tuple, select: str, spelling: str
) -> None:
    fn, args, partition, order = window
    text = query(window_sql(fn, args, partition, order), select, spelling)
    for seed in SEEDS:
        rows = make_rows(seed)
        window_values(rows, fn, args, partition, order)
        executor = dialect.default_executor(environment=Environment())
        got = sorted(
            map(tuple, executor.execute_text(fixture(rows) + text)[-1].fetchall()),
            key=repr,
        )
        assert got == reference(rows, select), (seed, text)
