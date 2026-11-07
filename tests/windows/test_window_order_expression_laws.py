"""`formal/Trilogy/Windows.lean`, "Wherever the aggregate sits, and however it
is spelled": every bare aggregate a window's order reads is pinned to the
tuple the window ranks, nested in an expression (a CASE included) or behind a
name defined outside the select, so the order is a function of that tuple (`Windows.pinned_expression_of_tuple`). Pinned to
the select's grain instead, one argument ranked twice
(`Windows.select_grain_order_splits_rank`). A named aggregate `by` keys finer
than the window's arguments makes those keys the window's, or the read-back
fans out (`Windows.order_keys_dropped_fans_out`). Every case runs on DuckDB and
SQLite, as an `auto` concept and inline, and must agree with the reference."""

from collections.abc import Callable
from typing import Any

import pytest

from tests.windows.test_window_laws import DIALECTS
from tests.windows.test_window_order_aggregate_laws import (
    aggregate_value,
    desc_nulls_last,
    fixture,
    make_rows,
)
from trilogy import Dialects, Environment

SEEDS = range(4)
FUNCTIONS = ("rank", "dense_rank")
Rows = list[dict[str, Any]]


def _total(fn: str, operand: str) -> Callable[[Rows], Any]:
    return lambda rows: aggregate_value(fn, [r[operand] for r in rows])


def _plus(a: Any, b: Any) -> Any:
    return None if a is None or b is None else a + b


def _double(a: Any) -> Any:
    return None if a is None else a * 2


# name -> (definition, order spelled inline, the order over a tuple's rows)
ORDERS: dict[str, tuple[str | None, str, Callable[[Rows], Any]]] = {
    "nested": (
        None,
        "sum(y) * 2 + count(id)",
        lambda m: _plus(_double(_total("sum", "y")(m)), _total("count", "id")(m)),
    ),
    "case": (
        None,
        "case when sum(y) > 2 then 1 else 0 end",
        lambda m: int((_total("sum", "y")(m) or 0) > 2),
    ),
    "named": ("auto s <- sum(y);", "s", _total("sum", "y")),
    "named_expr": (
        "auto s <- max(y) + count(id);",
        "s",
        lambda m: _plus(_total("max", "y")(m), _total("count", "id")(m)),
    ),
    "named_nested": ("auto s <- sum(y); auto s2 <- s + 1;", "s2", _total("sum", "y")),
}
PARTITIONS: tuple[tuple[str, ...], ...] = ((), ("p",))
SELECTS = ("dims", "fine", "alone", "count", "sum")
SPELLINGS = ("auto", "inline")


def window_sql(fn: str, order: str, partition: tuple[str, ...]) -> str:
    over = f"partition by {', '.join(partition)} " if partition else ""
    return f"{fn}(g) over ({over}order by {ORDERS[order][1]} desc)"


def outputs(select: str, partition: tuple[str, ...]) -> list[str]:
    return {
        "dims": ["g", *partition, "w"],
        "fine": ["g", "p", "w"],
        "alone": ["w"],
        "count": ["w", "count(id) as c"],
        "sum": ["w", "sum(y) as c"],
    }[select]


def query(
    fn: str, order: str, partition: tuple[str, ...], select: str, spelling: str
) -> str:
    defined = ORDERS[order][0] or ""
    window = window_sql(fn, order, partition)
    cols = outputs(select, partition)
    if spelling == "auto":
        return f"{defined} auto w <- {window}; select {', '.join(cols)};"
    cols = [f"{window} as w" if c == "w" else c for c in cols]
    return f"{defined} select {', '.join(cols)};"


def rank_tuples(
    keyed: dict[tuple, Any], partition_of: Callable[[tuple], tuple], fn: str
) -> dict[tuple, int]:
    groups: dict[tuple, list[tuple]] = {}
    for t in keyed:
        groups.setdefault(partition_of(t), []).append(t)
    value: dict[tuple, int] = {}
    for members in groups.values():
        members.sort(key=lambda t: desc_nulls_last(keyed[t]))
        rank = dense = 0
        for i, t in enumerate(members):
            if i == 0 or keyed[t] != keyed[members[i - 1]]:
                rank, dense = i + 1, dense + 1
            value[t] = dense if fn == "dense_rank" else rank
    return value


def window_values(rows: Rows, fn: str, order: str, partition: tuple[str, ...]) -> None:
    """Rank each `(g, *partition)` tuple within its partition by the order
    expression over that tuple's rows (`Windows.pinned_expression_of_tuple`)."""
    tuples: dict[tuple, Rows] = {}
    for row in rows:
        tuples.setdefault((row["g"], *(row[c] for c in partition)), []).append(row)
    keyed = {t: ORDERS[order][2](members) for t, members in tuples.items()}
    value = rank_tuples(keyed, lambda t: t[1:], fn)
    for row in rows:
        row["w"] = value[(row["g"], *(row[c] for c in partition))]


def reference(rows: Rows, select: str, partition: tuple[str, ...]) -> list[tuple]:
    if select in ("dims", "fine"):
        cols = [c.split(" ")[0] for c in outputs(select, partition)]
        return sorted({tuple(r[c] for c in cols) for r in rows}, key=repr)
    if select == "alone":
        return sorted({(r["w"],) for r in rows}, key=repr)
    groups: dict[Any, Rows] = {}
    for row in rows:
        groups.setdefault(row["w"], []).append(row)
    total = _total("count", "id") if select == "count" else _total("sum", "y")
    return sorted(((w, total(m)) for w, m in groups.items()), key=repr)


def fetch(dialect: Dialects, text: str) -> list[tuple]:
    executor = dialect.default_executor(environment=Environment())
    return sorted(map(tuple, executor.execute_text(text)[-1].fetchall()), key=repr)


CASES = [
    (fn, order, partition, select, spelling)
    for fn in FUNCTIONS
    for order in ORDERS
    for partition in PARTITIONS
    for select in SELECTS
    for spelling in SPELLINGS
    if not (select == "fine" and partition)
]


@pytest.mark.parametrize("dialect", DIALECTS, ids=lambda d: d.value)
@pytest.mark.parametrize("fn, order, partition, select, spelling", CASES, ids=repr)
def test_window_order_expression_is_per_tuple(
    dialect: Dialects,
    fn: str,
    order: str,
    partition: tuple[str, ...],
    select: str,
    spelling: str,
) -> None:
    text = query(fn, order, partition, select, spelling)
    for seed in SEEDS:
        rows = make_rows(seed)
        window_values(rows, fn, order, partition)
        assert fetch(dialect, fixture(rows) + text) == reference(
            rows, select, partition
        ), (seed, text)


FINE_SELECTS = {
    "dims": "g, p, w",
    "rows": "id, w",
    "alone": "w",
    "count": "w, count(id) as c",
}


@pytest.mark.parametrize("dialect", DIALECTS, ids=lambda d: d.value)
@pytest.mark.parametrize("fn", FUNCTIONS)
@pytest.mark.parametrize("select", FINE_SELECTS)
@pytest.mark.parametrize("spelling", ("named", "named_expr", "inline"))
def test_window_keys_include_order_by_keys(
    dialect: Dialects, fn: str, select: str, spelling: str
) -> None:
    """`rank(g) over (order by s)` with `s <- sum(y) by g, p` is a value per
    `(g, p)`: its grain is the keys its order is a function of, named or
    inline, and it ranks the tuples of that grain."""
    defined, order = {
        "named": ("auto s <- sum(y) by g, p;", "s"),
        "named_expr": ("auto s <- sum(y) by g, p; auto s2 <- s + 1;", "s2"),
        "inline": ("", "sum(y) by g, p"),
    }[spelling]
    text = (
        f"{defined} auto w <- {fn}(g) over (order by {order} desc); "
        f"select {FINE_SELECTS[select]};"
    )
    for seed in SEEDS:
        rows = make_rows(seed)
        per_gp: dict[tuple, Rows] = {}
        for row in rows:
            per_gp.setdefault((row["g"], row["p"]), []).append(row)
        keyed = {t: _total("sum", "y")(m) for t, m in per_gp.items()}
        value = rank_tuples(keyed, lambda t: (), fn)
        for row in rows:
            row["w"] = value[(row["g"], row["p"])]
        if select in ("dims", "rows"):
            cols = FINE_SELECTS[select].split(", ")
            expected = sorted({tuple(r[c] for c in cols) for r in rows}, key=repr)
        else:
            expected = reference(rows, select, ())
        assert fetch(dialect, fixture(rows) + text) == expected, (seed, text)


@pytest.mark.parametrize("dialect", DIALECTS, ids=lambda d: d.value)
@pytest.mark.parametrize("fn", FUNCTIONS)
def test_select_output_keeps_its_projected_value(dialect: Dialects, fn: str) -> None:
    """A name the select itself defines has the one value it projects, here
    per `(g, p)`; only names defined outside the select are read as their
    inline spelling."""
    text = f"select g, p, sum(y) as s, {fn}(g) over (order by s desc) as w;"
    for seed in SEEDS:
        rows = make_rows(seed)
        per_gp: dict[tuple, Rows] = {}
        for row in rows:
            per_gp.setdefault((row["g"], row["p"]), []).append(row)
        keyed = {t: _total("sum", "y")(m) for t, m in per_gp.items()}
        value = rank_tuples(keyed, lambda t: (), fn)
        expected = sorted(
            ((g, p, keyed[(g, p)], value[(g, p)]) for g, p in keyed), key=repr
        )
        assert fetch(dialect, fixture(rows) + text) == expected, (seed, text)
