"""`formal/Trilogy/SetOps.lean`: `except` / `intersect` are set operators on
NULL-safe row equality, unlike `not in` (`SetOps.except_keeps_no_match`,
`SetOps.intersect_iff`, `SetOps.except_fold`, `SetOps.not_in_drops_null`).
Each case builds random rows with NULLs and duplicates and holds DuckDB and
SQLite to the reference computed here."""

import itertools
import random
from collections.abc import Callable
from typing import Any

import pytest

from trilogy import Dialects, Environment, Executor

ENGINES = [Dialects.DUCK_DB, Dialects.SQLITE]
SEEDS = range(3)
Row = tuple[int, int | None, int | None]

CONDITIONS: dict[str, Callable[[Row], bool]] = {
    "g = 1": lambda r: r[1] == 1,
    "g = 2": lambda r: r[1] == 2,
    "g is null": lambda r: r[1] is None,
    "v > 1": lambda r: r[2] is not None and r[2] > 1,
    "v is null": lambda r: r[2] is None,
    "id > 0": lambda r: True,
}
_EXECUTORS: dict[tuple[Dialects, int], Executor] = {}


def rows(seed: int) -> list[Row]:
    rnd = random.Random(seed)
    return [
        (i, rnd.choice([None, 1, 2]), rnd.choice([None, 1, 2, 3]))
        for i in range(1, rnd.randint(4, 10))
    ]


def literal(x: int | None) -> str:
    return "cast(null as int)" if x is None else str(x)


def model(seed: int) -> str:
    body = " union all\n".join(
        f"select {i} as id, {literal(g)} as g, {literal(v)} as v"
        for i, g, v in rows(seed)
    )
    return f"""
key id int;
property id.g int?;
property id.v int?;
datasource t (id: id, g: g, v: v) grain (id) query '''
{body}
''';
"""


def executor(engine: Dialects, seed: int) -> Executor:
    if (engine, seed) not in _EXECUTORS:
        ex = engine.default_executor(environment=Environment())
        ex.execute_text(model(seed))
        _EXECUTORS[(engine, seed)] = ex
    return _EXECUTORS[(engine, seed)]


def fetch(engine: Dialects, seed: int, query: str) -> list[tuple[Any, ...]]:
    got = executor(engine, seed).execute_text(query)[-1].fetchall()
    return sorted((tuple(r) for r in got), key=lambda t: (t[0] is None, t[0] or 0))


def set_op(op: str, arms: list[set]) -> set:
    out = arms[0]
    for arm in arms[1:]:
        out = (
            out - arm
            if op == "except"
            else out & arm if op == "intersect" else out | arm
        )
    return out


@pytest.mark.parametrize("seed", SEEDS)
@pytest.mark.parametrize("engine", ENGINES, ids=lambda e: e.value)
def test_set_operators_are_null_safe_sets(engine: Dialects, seed: int) -> None:
    data = rows(seed)
    rnd = random.Random(seed)
    for op, arity in itertools.product(["except", "intersect", "union"], [2, 3]):
        for _ in range(3):
            conditions = [rnd.choice(list(CONDITIONS)) for _ in range(arity)]
            col = rnd.choice(["g", "v"])
            index = 1 if col == "g" else 2
            arms = ", ".join(f"(where {c} select {col} -> k)" for c in conditions)
            values = [{r[index] for r in data if CONDITIONS[c](r)} for c in conditions]
            expected = set_op(op, values)
            head = f"with c as {op}({arms}) -> (k); "
            assert fetch(engine, seed, head + "select c.k;") == sorted(
                ((x,) for x in expected), key=lambda t: (t[0] is None, t[0] or 0)
            ), (head, values)
            counted = [(len(expected - {None}),)] if expected else []
            assert fetch(engine, seed, head + "select count(c.k) as n;") == counted, (
                head,
                values,
            )
