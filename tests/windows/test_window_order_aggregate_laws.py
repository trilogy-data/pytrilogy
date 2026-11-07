"""`formal/Trilogy/SelectGrain.lean`, `Windows.lean` "An aggregate a window
orders by": an aggregate with no `by` in a window's order is at the select's
raw grain, as if written `by` it (`SelectGrain.bare_is_select_grain`), and the
window gives that grain its keys. When the grain holds the partition the sum
reads only the partition's rows (`Windows.order_aggregate_per_partition`);
pinned to the window's first argument it read every partition's
(`Windows.anchor_sum_crosses_partitions`). An aggregate `by` fixed keys is the
same in every select (`SelectGrain.specified_ignores_select`), so the window
is a value the select groups by (`Windows.group_by_tuple_splits_value`). Every
case runs on DuckDB and SQLite, as an `auto` concept and inline, and must agree
with the reference."""

import random
from collections.abc import Callable, Sequence
from typing import Any

import pytest

from tests.windows.test_window_laws import DIALECTS, literal
from trilogy import Dialects, Environment

SEEDS = range(4)
FUNCTIONS = ("rank", "dense_rank")
# (spelling in the order, aggregate, operand); `named` is `by` the window's tuple
AGGREGATES = {
    "sum": ("sum(y)", "sum", "y"),
    "count": ("count(id)", "count", "id"),
    "max": ("max(y)", "max", "y"),
    "named": ("a", "sum", "y"),
}
PARTITIONS: tuple[tuple[str, ...], ...] = ((), ("p",))
SELECTS = ("dims", "alone", "count", "sum")
SPELLINGS = ("auto", "inline")


def make_rows(seed: int) -> list[dict[str, Any]]:
    rng = random.Random(seed)
    return [
        {
            "id": i,
            "g": rng.choice(["a", "b", "c", None]),
            "p": rng.choice(["P", "Q", None]),
            "y": rng.choice([1, 2, 2, 3, None]),
        }
        for i in range(1, rng.randint(5, 14))
    ]


def fixture(rows: list[dict[str, Any]]) -> str:
    body = " union all\n".join(
        f"select {r['id']} as id, {literal(r['g'], 'varchar')} as g, "
        f"{literal(r['p'], 'varchar')} as p, {literal(r['y'], 'int')} as y"
        for r in rows
    )
    return f"""
key id int;
property id.g string?;
property id.p string?;
property id.y int?;
datasource t (id: id, g: g, p: p, y: y) grain (id) query '''
{body}
''';
"""


def window_sql(fn: str, aggregate: str, partition: tuple[str, ...]) -> str:
    spelled = AGGREGATES[aggregate][0]
    over = f"partition by {', '.join(partition)} " if partition else ""
    return f"{fn}(g) over ({over}order by {spelled} desc)"


def query(
    fn: str, aggregate: str, partition: tuple[str, ...], select: str, spelling: str
) -> str:
    named = (
        f"auto a <- sum(y) by {', '.join(('g', *partition))};"
        if aggregate == "named"
        else ""
    )
    window = window_sql(fn, aggregate, partition)
    outputs = {
        "dims": ["g", *partition, "w"],
        "alone": ["w"],
        "count": ["w", "count(id) as c"],
        "sum": ["w", "sum(y) as c"],
    }[select]
    if spelling == "auto":
        return f"{named} auto w <- {window}; select {', '.join(outputs)};"
    outputs = [f"{window} as w" if o == "w" else o for o in outputs]
    return f"{named} select {', '.join(outputs)};"


def aggregate_value(fn: str, values: list[Any]) -> Any:
    if fn == "count":
        return len([v for v in values if v is not None])
    present = [v for v in values if v is not None]
    if not present:
        return None
    return sum(present) if fn == "sum" else max(present)


def desc_nulls_last(value: Any) -> tuple:
    return (1, ()) if value is None else (0, -value)


Rows = list[dict[str, Any]]


def rank_ordered(
    keyed: dict[tuple, Any], fn: str, partition_of: Callable[[tuple], tuple]
) -> dict[tuple, int]:
    """`Windows.rankD`: rank the distinct tuples within their partition, by
    their order value descending, NULLs last."""
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


def group_rows(rows: Rows, cols: Sequence[str]) -> dict[tuple, Rows]:
    groups: dict[tuple, Rows] = {}
    for row in rows:
        groups.setdefault(tuple(row[c] for c in cols), []).append(row)
    return groups


def total(name: str, members: Rows) -> Any:
    operand = "id" if name == "count" else "y"
    return aggregate_value(name, [r[operand] for r in members])


def select_grain_reference(
    rows: Rows,
    fn: str,
    order: Callable[[Rows], Any],
    partition: tuple[str, ...],
    dims: Sequence[str],
    measure: str | None,
) -> list[tuple]:
    """`select <dims>, w[, measure]` with `w <- fn(g) over (partition by ...
    order by <bare aggregates>)`. A bare aggregate is at the select grain
    (`SelectGrain.bare_is_select_grain`); the window gives that grain its
    keys, `g` and the partition. The window ranks the select-grain tuples
    within their partition, exactly as for `by <select grain>`, and each is
    one row."""
    grain = list(dict.fromkeys(["g", *partition, *dims]))
    groups = group_rows(rows, grain)
    value = {t: order(m) for t, m in groups.items()}
    ranks = rank_ordered(
        value, fn, lambda t: tuple(t[grain.index(c)] for c in partition)
    )
    out = []
    for t, members in groups.items():
        w = ranks[t]
        row = [*(t[grain.index(c)] for c in dims), w]
        if measure:
            row.append(total(measure, members))
        out.append(tuple(row))
    return sorted(out, key=repr)


def specified_reference(
    rows: Rows,
    fn: str,
    order: Callable[[Rows], Any],
    keys: Sequence[str],
    partition: tuple[str, ...],
    dims: Sequence[str],
    measure: str | None,
) -> list[tuple]:
    """The same select with the order aggregate `by keys`: it is the same in
    every select (`SelectGrain.specified_ignores_select`), so the window is a
    value of `keys`, ranking their tuples within its partition, and the
    select groups by it."""
    per_key = {t: order(m) for t, m in group_rows(rows, keys).items()}
    ranks = rank_ordered(
        per_key, fn, lambda t: tuple(t[keys.index(c)] for c in partition)
    )
    for row in rows:
        row["w"] = ranks[tuple(row[c] for c in keys)]
    out = []
    for t, members in group_rows(rows, [*dims, "w"]).items():
        out.append((*t, total(measure, members)) if measure else t)
    return sorted(set(out), key=repr)


SELECT_SHAPES: dict[str, tuple[tuple[str, ...], str | None]] = {
    "alone": ((), None),
    "count": ((), "count"),
    "sum": ((), "sum"),
}


def reference(
    rows: Rows, fn: str, aggregate: str, partition: tuple[str, ...], select: str
) -> list[tuple]:
    _, agg_fn, _ = AGGREGATES[aggregate]
    dims, measure = (
        (("g", *partition), None) if select == "dims" else SELECT_SHAPES[select]
    )
    order = lambda m: total(agg_fn, m)
    if aggregate == "named":
        keys = ["g", *partition]
        return specified_reference(rows, fn, order, keys, partition, dims, measure)
    return select_grain_reference(rows, fn, order, partition, dims, measure)


CASES = [
    (fn, aggregate, partition, select, spelling)
    for fn in FUNCTIONS
    for aggregate in AGGREGATES
    for partition in PARTITIONS
    for select in SELECTS
    for spelling in SPELLINGS
]


@pytest.mark.parametrize("dialect", DIALECTS, ids=lambda d: d.value)
@pytest.mark.parametrize("fn, aggregate, partition, select, spelling", CASES, ids=repr)
def test_window_ordered_by_aggregate(
    dialect: Dialects,
    fn: str,
    aggregate: str,
    partition: tuple[str, ...],
    select: str,
    spelling: str,
) -> None:
    text = query(fn, aggregate, partition, select, spelling)
    for seed in SEEDS:
        rows = make_rows(seed)
        executor = dialect.default_executor(environment=Environment())
        got = sorted(
            map(tuple, executor.execute_text(fixture(rows) + text)[-1].fetchall()),
            key=repr,
        )
        assert got == reference(rows, fn, aggregate, partition, select), (
            seed,
            text,
        )
