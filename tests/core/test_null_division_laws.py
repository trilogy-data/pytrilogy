"""A zero divisor and a persisted copy of a nullable column
(formal/Trilogy/NullReject.lean, 0.3.467), on DuckDB, SQLite and ClickHouse
through chdb where it installs, against a Python reference.

- `x / 0` and `x % 0` are NULL, as a NULL operand is (`guarded_div_strict`),
  so a value is NULL by its own expression and `is not null` keeps its filter
  (`div_zero_not_total`); a nonzero literal divisor keeps a non-null dividend
  non-null (`literal_divisor_total`).
- a persisted copy declares `?` every column the write can hold NULL in, so a
  read of the copy keeps `x is not null` and `count(x)` counts no NULL
  (`inferred_copy_sound`, `undeclared_copy_keeps_null`,
  `not_null_column_stores_zero`).
"""

import math
import random
from decimal import Decimal

import pytest

from tests.windows.test_window_laws import CHDB, new_executor
from trilogy import Dialects, Executor
from trilogy.core.enums import CreateMode, Modifier
from trilogy.core.models.core import DataType
from trilogy.core.models.environment import Environment
from trilogy.core.statements.execute import ColumnInfo, CreateTableInfo
from trilogy.dialect.clickhouse import ClickhouseDialect
from trilogy.dialect.duckdb import DuckDBDialect

DIALECTS = [Dialects.DUCK_DB, Dialects.SQLITE, *CHDB]
SEEDS = range(4)

MODEL = """
key id int;
property id.x int;
property id.y int;
property id.f float;
property id.n numeric(12,4);
key oid int;
property oid.v int;
datasource raw (id: id, x: ?x, y: ?y, f: ?f, n: ?n) grain (id) address raw;
datasource facts (oid: oid, id: ~id, v: v) grain (oid) address facts;
"""

DDL = {
    Dialects.DUCK_DB: [
        "create table raw (id int, x int, y int, f double, n decimal(12,4))",
        "create table facts (oid int, id int, v int)",
    ],
    Dialects.SQLITE: [
        "create table raw (id int, x int, y int, f real, n real)",
        "create table facts (oid int, id int, v int)",
    ],
    Dialects.CLICKHOUSE: [
        (
            "create table raw (id Int64, x Nullable(Int64), y Nullable(Int64),"
            " f Nullable(Float64), n Nullable(Decimal(12,4))) engine = Memory"
        ),
        "create table facts (oid Int64, id Int64, v Int64) engine = Memory",
    ],
}

QUOTIENTS = {
    "x / y": ("x", "y"),
    "f / y": ("f", "y"),
    "x / f": ("x", "f"),
    "n / y": ("n", "y"),
}


def _value(rng: random.Random, pick: list) -> object:
    return rng.choice([None, 0, *pick])


def _rows(rng: random.Random) -> list[dict]:
    return [
        {
            "id": i,
            "x": _value(rng, list(range(-9, 10))),
            "y": _value(rng, [-3, -2, 2, 3, 0]),
            "f": _value(rng, [0.0, 1.5, -2.5, 4.0]),
            "n": _value(rng, [Decimal("7.5000"), Decimal("-2.2500"), Decimal(0)]),
        }
        for i in range(30)
    ]


def _facts(rng: random.Random) -> list[tuple]:
    ids = rng.sample(range(30), 18)
    return [(i, ids[i % len(ids)], rng.randrange(1, 9)) for i in range(25)]


def _literal(v: object) -> str:
    return "NULL" if v is None else str(v)


def _executor(dialect: Dialects, rows: list[dict], facts: list[tuple]) -> Executor:
    executor = new_executor(dialect)
    for table in ("raw", "facts", "copied"):
        executor.execute_raw_sql(f"drop table if exists {table}")
    for ddl in DDL[dialect]:
        executor.execute_raw_sql(ddl)
    values = ", ".join(
        "(" + ", ".join(_literal(r[c]) for c in ("id", "x", "y", "f", "n")) + ")"
        for r in rows
    )
    executor.execute_raw_sql(f"insert into raw values {values}")
    executor.execute_raw_sql(
        "insert into facts values " + ", ".join(repr(f) for f in facts)
    )
    executor.parse_text(MODEL)
    return executor


def _select(executor: Executor, text: str) -> list[tuple]:
    return [tuple(r) for r in executor.execute_text(text)[-1].fetchall()]


def guarded_div(a: object, b: object) -> float | None:
    if a is None or b is None or b == 0:
        return None
    return float(a) / float(b)  # type: ignore[arg-type]


def guarded_mod(a: int | None, b: int | None) -> int | None:
    if a is None or b is None or b == 0:
        return None
    return a - b * math.trunc(a / b)


def _close(got: object, want: float | None) -> bool:
    if want is None:
        return got is None
    return got is not None and float(got) == pytest.approx(want)  # type: ignore[arg-type]


@pytest.fixture(params=DIALECTS, ids=lambda d: d.value)
def dialect(request) -> Dialects:
    return request.param


@pytest.mark.parametrize("seed", SEEDS)
def test_zero_divisor_is_null(dialect: Dialects, seed: int) -> None:
    rng = random.Random(seed)
    rows = _rows(rng)
    executor = _executor(dialect, rows, _facts(rng))
    for expr, (a, b) in QUOTIENTS.items():
        out = _select(executor, f"select id, {expr} as q order by id asc;")
        for row, (_, q) in zip(rows, out):
            assert _close(q, guarded_div(row[a], row[b])), (expr, row, q)
        kept = _select(executor, f"select id where {expr} is not null order by id asc;")
        assert [k[0] for k in kept] == [
            r["id"] for r in rows if guarded_div(r[a], r[b]) is not None
        ], expr
    out = _select(executor, "select id, x % y as m order by id asc;")
    for row, (_, m) in zip(rows, out):
        assert m == guarded_mod(row["x"], row["y"]), (row, m)
    ((count,),) = _select(executor, "select count(x / y) as c;")
    assert count == sum(guarded_div(r["x"], r["y"]) is not None for r in rows)


@pytest.mark.parametrize("seed", SEEDS)
def test_zero_divisor_is_null_inside_an_expression(
    dialect: Dialects, seed: int
) -> None:
    rng = random.Random(seed)
    rows = _rows(rng)
    executor = _executor(dialect, rows, _facts(rng))
    out = _select(
        executor,
        "select id, coalesce(x / y, -1) as c, x / 4 as quarter order by id asc;",
    )
    for row, (_, c, quarter) in zip(rows, out):
        q = guarded_div(row["x"], row["y"])
        assert _close(c, -1.0 if q is None else q), (row, c)
        assert _close(quarter, guarded_div(row["x"], 4)), (row, quarter)


def test_nullability_of_a_quotient() -> None:
    env = Environment()
    env.parse("""key id int; property id.x int; property id.y int;
        auto q <- x / y; auto m <- x % y; auto lit <- x / 4; auto neg <- x / -2.5;
        auto zero <- x / 0; auto rem <- x % 3; auto half <- x / (4 / 2);""")
    nullable = {
        name
        for name in ("q", "m", "lit", "neg", "zero", "rem", "half")
        if Modifier.NULLABLE in env.concepts[name].modifiers
    }
    assert nullable == {"q", "m", "zero", "half"}


COPIED = {
    "x": lambda r, s: r["x"],
    "q": lambda r, s: guarded_div(r["x"], r["y"]),
    "z": lambda r, s: None if r["x"] is None else r["x"] + 1,
    "sv": lambda r, s: s.get(r["id"]),
}


@pytest.mark.parametrize("seed", SEEDS[:2])
def test_persisted_copy_keeps_nulls(dialect: Dialects, seed: int) -> None:
    rng = random.Random(seed)
    rows, facts = _rows(rng), _facts(rng)
    sums: dict[int, int] = {}
    counts: dict[int, int] = {}
    for _, rid, v in facts:
        sums[rid] = sums.get(rid, 0) + v
        counts[rid] = counts.get(rid, 0) + 1
    executor = _executor(dialect, rows, facts)
    executor.execute_text("""property id.q <- x / y; property id.z <- x + 1;
        persist copied into copied from
        select id, x, q, z, sum(v) as sv, count(oid) as cnt;""")
    columns = {
        c.alias: c.is_nullable
        for c in executor.environment.datasources["copied"].columns
    }
    assert columns == {
        "id": False,
        "x": True,
        "q": True,
        "z": True,
        "sv": True,
        "cnt": False,
    }
    for name in ("x", "q", "z", "sv"):
        query = f"select id, {name} where {name} is not null order by id asc;"
        assert "copied" in executor.generate_sql(query)[-1], name
        want = [
            (r["id"], COPIED[name](r, sums))
            for r in rows
            if COPIED[name](r, sums) is not None
        ]
        got = _select(executor, query)
        assert [g[0] for g in got] == [w[0] for w in want], name
        assert all(_close(g[1], w[1]) for g, w in zip(got, want)), name
        ((count,),) = _select(executor, f"select count({name}) as c;")
        assert count == len(want), name
    cnt = _select(executor, "select id, cnt order by id asc;")
    assert cnt == [(r["id"], counts.get(r["id"], 0)) for r in rows]


def test_clickhouse_declares_a_nullable_column_nullable() -> None:
    target = CreateTableInfo(
        name="t",
        columns=[
            ColumnInfo(name="id", type=DataType.INTEGER, nullable=False),
            ColumnInfo(name="x", type=DataType.INTEGER),
            ColumnInfo(name="xs", type=DataType.ARRAY),
        ],
    )
    ddl = ClickhouseDialect().compile_create_table_statement(
        target, create_mode=CreateMode.CREATE
    )
    assert "id Int64" in ddl and "x Nullable(Int64)" in ddl
    assert "Nullable(Array" not in ddl
    assert "Nullable" not in DuckDBDialect().compile_create_table_statement(
        target, create_mode=CreateMode.CREATE
    )
