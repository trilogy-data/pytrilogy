"""`formal/Trilogy/SelectGrain.lean` "An aggregate `by` fixed keys in a
select": such an aggregate is a value of its keys, and a select that projects
it groups by that value, not by its keys. Its rows are the distinct tuples of
what it projects (`Rows.lean`), whatever grain the aggregate was computed at
(`SelectGrain.fixed_value_rows_distinct`), and an aggregate with no `by` beside
it is at the select grain, the value included
(`SelectGrain.bare_beside_fixed_value`). Reading the aggregate's keys in its
place splits a select-grain tuple into one row per key
(`SelectGrain.by_keys_split_tuple`). Every case runs on DuckDB, SQLite and ClickHouse, as
an `auto` concept and inline, and must agree with the reference."""

import random
from collections.abc import Sequence
from typing import Any

import pytest

from tests.windows.test_window_laws import DIALECTS, literal, new_executor, seeds_for
from trilogy import Dialects

SEEDS = range(4)
# name -> (spelling, function, operand, keys)
FIXED = {
    "row_sum": ("sum(y) by id", "sum", "y", ("id",)),
    "g_sum": ("sum(y) by g", "sum", "y", ("g",)),
    "gh_count": ("count(id) by g, h", "count", "id", ("g", "h")),
    "h_max": ("max(y) by h", "max", "y", ("h",)),
}
DIMS: tuple[tuple[str, ...], ...] = ((), ("g",), ("h",), ("g", "h"), ("id",))
MEASURES = (None, "count", "sum")
SPELLINGS = ("auto", "inline")

Rows = list[dict[str, Any]]


def make_rows(seed: int) -> Rows:
    rng = random.Random(seed)
    return [
        {
            "id": i,
            "g": rng.choice(["a", "b", "c", None]),
            "h": rng.choice(["P", "Q", None]),
            "y": rng.choice([1, 2, 2, 3, None]),
        }
        for i in range(1, rng.randint(5, 14))
    ]


def fixture(rows: Rows) -> str:
    body = " union all\n".join(
        f"select {r['id']} as id, {literal(r['g'], 'varchar')} as g, "
        f"{literal(r['h'], 'varchar')} as h, {literal(r['y'], 'int')} as y"
        for r in rows
    )
    return f"""
key id int;
property id.g string?;
property id.h string?;
property id.y int?;
datasource t (id: id, g: g, h: h, y: y) grain (id) query '''
{body}
''';
"""


def aggregate_value(fn: str, values: list[Any]) -> Any:
    present = [v for v in values if v is not None]
    if fn == "count":
        return len(present)
    if not present:
        return None
    return sum(present) if fn == "sum" else max(present)


def group_rows(rows: Rows, cols: Sequence[str]) -> dict[tuple, Rows]:
    groups: dict[tuple, Rows] = {}
    for row in rows:
        groups.setdefault(tuple(row[c] for c in cols), []).append(row)
    return groups


MEASURE_SQL = {"count": "count(id) as m", "sum": "sum(y) as m"}
MEASURE_OPERAND = {"count": "id", "sum": "y"}


def query(fixed: str, dims: tuple[str, ...], measure: str | None, spelling: str) -> str:
    spelled = FIXED[fixed][0]
    outputs = [*dims, "s" if spelling == "auto" else f"{spelled} as s"]
    if measure:
        outputs.append(MEASURE_SQL[measure])
    prefix = f"auto s <- {spelled}; " if spelling == "auto" else ""
    return f"{prefix}select {', '.join(outputs)};"


def reference(
    rows: Rows, fixed: str, dims: tuple[str, ...], measure: str | None
) -> list[tuple]:
    """`s` is a value of its keys (`SelectGrain.specified_ignores_select`); the
    select's grain is its projected tuple `(dims, s)`, a bare measure is per
    that tuple, and each tuple is one row."""
    _, fn, operand, keys = FIXED[fixed]
    per_key = {
        k: aggregate_value(fn, [r[operand] for r in m])
        for k, m in group_rows(rows, keys).items()
    }
    valued = [{**r, "s": per_key[tuple(r[c] for c in keys)]} for r in rows]
    out = []
    for t, members in group_rows(valued, [*dims, "s"]).items():
        if measure:
            op = MEASURE_OPERAND[measure]
            t = (*t, aggregate_value(measure, [r[op] for r in members]))
        out.append(t)
    return sorted(out, key=repr)


CASES = [
    (fixed, dims, measure, spelling)
    for fixed in FIXED
    for dims in DIMS
    for measure in MEASURES
    for spelling in SPELLINGS
]


@pytest.mark.parametrize("dialect", DIALECTS, ids=lambda d: d.value)
@pytest.mark.parametrize("fixed, dims, measure, spelling", CASES, ids=repr)
def test_select_of_fixed_aggregate(
    dialect: Dialects,
    fixed: str,
    dims: tuple[str, ...],
    measure: str | None,
    spelling: str,
) -> None:
    text = query(fixed, dims, measure, spelling)
    for seed in seeds_for(dialect, SEEDS):
        rows = make_rows(seed)
        executor = new_executor(dialect)
        got = sorted(
            map(tuple, executor.execute_text(fixture(rows) + text)[-1].fetchall()),
            key=repr,
        )
        assert got == reference(rows, fixed, dims, measure), (seed, text)
