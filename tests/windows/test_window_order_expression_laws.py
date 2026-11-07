"""`formal/Trilogy/SelectGrain.lean`: an aggregate with no `by` is at the
select's raw grain wherever it appears (`SelectGrain.bare_is_select_grain`).
In a window's order that holds nested in an expression or a CASE, and behind a
name, so any of them is a function of the select grain
(`SelectGrain.bare_expression_on_grain`) and a name orders as its inline
spelling does. An aggregate `by` fixed keys is the same in every select
(`SelectGrain.specified_ignores_select`): a window ordered by one is a value of
those keys, and its keys hold them or the read-back fans out
(`Windows.order_keys_dropped_fans_out`). Every case runs on DuckDB and SQLite,
as an `auto` concept and inline, and must agree with the reference."""

from collections.abc import Callable
from typing import Any

import pytest

from tests.windows.test_window_laws import DIALECTS
from tests.windows.test_window_order_aggregate_laws import (
    Rows,
    fixture,
    make_rows,
    select_grain_reference,
    specified_reference,
    total,
)
from trilogy import Dialects, Environment

SEEDS = range(4)
FUNCTIONS = ("rank", "dense_rank")


def _plus(a: Any, b: Any) -> Any:
    return None if a is None or b is None else a + b


def _double(a: Any) -> Any:
    return None if a is None else a * 2


# name -> (definition, the order as written, the order over a group's rows)
ORDERS: dict[str, tuple[str, str, Callable[[Rows], Any]]] = {
    "nested": (
        "",
        "sum(y) * 2 + count(id)",
        lambda m: _plus(_double(total("sum", m)), total("count", m)),
    ),
    "case": (
        "",
        "case when sum(y) > 2 then 1 else 0 end",
        lambda m: int((total("sum", m) or 0) > 2),
    ),
    "named": ("auto s <- sum(y);", "s", lambda m: total("sum", m)),
    "named_expr": (
        "auto s <- max(y) + count(id);",
        "s",
        lambda m: _plus(total("max", m), total("count", m)),
    ),
    "named_nested": (
        "auto s <- sum(y); auto s2 <- s + 1;",
        "s2",
        lambda m: total("sum", m),
    ),
}
PARTITIONS: tuple[tuple[str, ...], ...] = ((), ("p",))
# select -> (dimensions beside the window, measure beside it)
SELECTS: dict[str, tuple[tuple[str, ...] | None, str | None]] = {
    "dims": (None, None),
    "fine": (("g", "p"), None),
    "alone": ((), None),
    "count": ((), "count"),
    "sum": ((), "sum"),
}
SPELLINGS = ("auto", "inline")
MEASURES = {"count": "count(id) as c", "sum": "sum(y) as c"}


def query(
    fn: str, order: str, partition: tuple[str, ...], select: str, spelling: str
) -> str:
    defined, spelled, _ = ORDERS[order]
    over = f"partition by {', '.join(partition)} " if partition else ""
    window = f"{fn}(g) over ({over}order by {spelled} desc)"
    dims, measure = SELECTS[select]
    cols = [*(("g", *partition) if dims is None else dims), "w"]
    if measure:
        cols.append(MEASURES[measure])
    if spelling == "auto":
        return f"{defined} auto w <- {window}; select {', '.join(cols)};"
    cols = [f"{window} as w" if c == "w" else c for c in cols]
    return f"{defined} select {', '.join(cols)};"


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
def test_window_order_expression_is_at_select_grain(
    dialect: Dialects,
    fn: str,
    order: str,
    partition: tuple[str, ...],
    select: str,
    spelling: str,
) -> None:
    text = query(fn, order, partition, select, spelling)
    dims, measure = SELECTS[select]
    for seed in SEEDS:
        rows = make_rows(seed)
        expected = select_grain_reference(
            rows,
            fn,
            ORDERS[order][2],
            partition,
            ("g", *partition) if dims is None else dims,
            measure,
        )
        assert fetch(dialect, fixture(rows) + text) == expected, (seed, text)


SPECIFIED_SELECTS: dict[str, tuple[tuple[str, ...], str | None]] = {
    "dims": (("g", "p"), None),
    "rows": (("id",), None),
    "alone": ((), None),
    "count": ((), "count"),
}


@pytest.mark.parametrize("dialect", DIALECTS, ids=lambda d: d.value)
@pytest.mark.parametrize("fn", FUNCTIONS)
@pytest.mark.parametrize("select", SPECIFIED_SELECTS)
@pytest.mark.parametrize("spelling", ("named", "named_expr", "inline"))
def test_window_keys_include_order_by_keys(
    dialect: Dialects, fn: str, select: str, spelling: str
) -> None:
    """`rank(g) over (order by s)` with `s <- sum(y) by g, p` is a value per
    `(g, p)`, named or inline."""
    defined, order = {
        "named": ("auto s <- sum(y) by g, p;", "s"),
        "named_expr": ("auto s <- sum(y) by g, p; auto s2 <- s + 1;", "s2"),
        "inline": ("", "sum(y) by g, p"),
    }[spelling]
    dims, measure = SPECIFIED_SELECTS[select]
    cols = [*dims, "w", *([MEASURES[measure]] if measure else [])]
    text = (
        f"{defined} auto w <- {fn}(g) over (order by {order} desc); "
        f"select {', '.join(cols)};"
    )
    for seed in SEEDS:
        rows = make_rows(seed)
        expected = specified_reference(
            rows, fn, lambda m: total("sum", m), ["g", "p"], (), dims, measure
        )
        assert fetch(dialect, fixture(rows) + text) == expected, (seed, text)


@pytest.mark.parametrize("dialect", DIALECTS, ids=lambda d: d.value)
@pytest.mark.parametrize("fn", FUNCTIONS)
def test_window_orders_by_the_projected_aggregate(dialect: Dialects, fn: str) -> None:
    """`sum(y)` projected and ordered by is one value, at the select grain."""
    text = f"select g, p, {fn}(g) over (order by sum(y) desc) as w, sum(y) as s;"
    for seed in SEEDS:
        rows = make_rows(seed)
        expected = select_grain_reference(
            rows, fn, lambda m: total("sum", m), (), ("g", "p"), "sum"
        )
        assert fetch(dialect, fixture(rows) + text) == expected, (seed, text)


@pytest.mark.parametrize("dialect", DIALECTS, ids=lambda d: d.value)
@pytest.mark.parametrize("dims", [(), ("g",)], ids=["grand_total", "by_g"])
def test_window_in_where_reads_the_select_grain(
    dialect: Dialects, dims: tuple[str, ...]
) -> None:
    """In a WHERE too: with no keys selected the order's `sum(y)` is the grand
    total, every `g` ties and every row passes; with `g` selected it is per
    `g`, as `sum(y) by g` is."""
    cols = ", ".join([*dims, "count(id) as c"])
    text = f"where rank(g) over (order by sum(y) desc) <= 1 select {cols};"
    for seed in SEEDS:
        rows = make_rows(seed)
        ranks = specified_reference(
            rows, "rank", lambda m: total("sum", m), ["g"], (), ("g",), None
        )
        top = {g for g, w in ranks if w == 1}
        kept = rows if not dims else [r for r in rows if r["g"] in top]
        expected = (
            [(len(kept),)]
            if not dims
            else sorted(
                {(g, sum(1 for r in kept if r["g"] == g)) for g in top}, key=repr
            )
        )
        assert fetch(dialect, fixture(rows) + text) == expected, (seed, text)
