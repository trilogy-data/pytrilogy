"""`formal/Trilogy/Windows.lean`, "An aggregate a window orders by": a bare
aggregate in a window's order is evaluated per the tuple the window ranks, its
arguments and partition (`Windows.order_aggregate_per_partition`); per its
first argument alone it read every partition's rows
(`Windows.anchor_sum_crosses_partitions`). A window ordered by an aggregate is
still a value a select groups by, so two arguments that tie are one group
(`Windows.group_by_tuple_splits_value`). Every case runs on DuckDB and SQLite,
as an `auto` concept and inline, and must agree with the reference."""

import random
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


def window_values(
    rows: list[dict[str, Any]], fn: str, aggregate: str, partition: tuple[str, ...]
) -> None:
    """Rank each `(g, *partition)` tuple within its partition by the aggregate
    over that tuple's rows (`Windows.order_aggregate_per_partition`)."""
    _, agg_fn, operand = AGGREGATES[aggregate]
    tuples: dict[tuple, list[Any]] = {}
    for row in rows:
        tuples.setdefault((row["g"], *(row[c] for c in partition)), []).append(
            row[operand]
        )
    keyed = {t: aggregate_value(agg_fn, values) for t, values in tuples.items()}
    groups: dict[tuple, list[tuple]] = {}
    for t in keyed:
        groups.setdefault(t[1:], []).append(t)
    value: dict[tuple, Any] = {}
    for members in groups.values():
        members.sort(key=lambda t: desc_nulls_last(keyed[t]))
        rank = dense = 0
        for i, t in enumerate(members):
            if i == 0 or keyed[t] != keyed[members[i - 1]]:
                rank, dense = i + 1, dense + 1
            value[t] = dense if fn == "dense_rank" else rank
    for row in rows:
        row["w"] = value[(row["g"], *(row[c] for c in partition))]


def reference(
    rows: list[dict[str, Any]], partition: tuple[str, ...], select: str
) -> list[tuple]:
    if select == "dims":
        return sorted(
            {(r["g"], *(r[c] for c in partition), r["w"]) for r in rows}, key=repr
        )
    if select == "alone":
        return sorted({(r["w"],) for r in rows}, key=repr)
    groups: dict[Any, list[dict[str, Any]]] = {}
    for row in rows:
        groups.setdefault(row["w"], []).append(row)
    if select == "count":
        return sorted(((w, len(m)) for w, m in groups.items()), key=repr)
    return sorted(
        ((w, aggregate_value("sum", [r["y"] for r in m])) for w, m in groups.items()),
        key=repr,
    )


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
        window_values(rows, fn, aggregate, partition)
        executor = dialect.default_executor(environment=Environment())
        got = sorted(
            map(tuple, executor.execute_text(fixture(rows) + text)[-1].fetchall()),
            key=repr,
        )
        assert got == reference(rows, partition, select), (seed, text)
