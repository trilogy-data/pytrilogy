"""Declared types hold their values (formal/Trilogy/Types.lean): an
expression persisted into a table typed by its declared type reads back the
live value, on DuckDB, SQLite and ClickHouse (chdb). A remainder is also held
to a reference, which a round trip cannot check: both sides share an error.

- a product of decimals carries the sum of their scales; a decimal beside a
  float, a decimal quotient, `sqrt` and `**` are doubles.
- `%` keeps its dividend's fraction; `greatest` / `least` take the join of
  their arguments' types.
- a float persists as a double.
- a cast to a date or a datetime is one, so it equals the stored value.
"""

import math
import random
from datetime import date, datetime, timedelta
from decimal import Decimal

import pytest

from tests.windows.test_window_laws import CHDB, new_executor
from trilogy import Dialects, Executor
from trilogy.core.models.core import DataType, NumericType
from trilogy.core.models.environment import Environment

DIALECTS = [Dialects.DUCK_DB, Dialects.SQLITE, *CHDB]
SEEDS = range(4)

MODEL = """
key id int;
property id.i int;
property id.f float;
property id.n numeric(12,4);
property id.m numeric(12,4);
property id.b bool;
property id.d date;
property id.t datetime;
datasource raw (id: id, i: i, f: f, n: n, m: m, b: b, d: d, t: t) grain (id) address raw;
"""

MODEL_COLUMNS = ["id", "i", "f", "n", "m", "b", "d", "t"]

DDL = {
    Dialects.DUCK_DB: "create table raw (id int, i int, f double, n decimal(12,4), m decimal(12,4), b boolean, d date, t timestamp)",
    Dialects.SQLITE: "create table raw (id int, i int, f real, n numeric, m numeric, b int, d text, t text)",
    Dialects.CLICKHOUSE: "create table raw (id Int64, i Int64, f Float64, n Decimal(12,4), m Decimal(12,4), b Bool, d Date, t DateTime) engine = Memory",
}

EXPRESSIONS = {
    "sqrt(abs(f))": DataType.FLOAT,
    "i ** 2": DataType.FLOAT,
    "f % 3": DataType.FLOAT,
    "n % 3": NumericType(precision=12, scale=4),
    "i % 3": DataType.INTEGER,
    "greatest(i, f)": DataType.FLOAT,
    "least(i, n)": NumericType(precision=12, scale=4),
    "greatest(n, f)": DataType.FLOAT,
    "f * n": DataType.FLOAT,
    "n * m": NumericType(precision=24, scale=8),
    "i * n": NumericType(precision=22, scale=4),
    "n / i": DataType.DOUBLE,
    "coalesce(n, f)": DataType.FLOAT,
    "f * 1": DataType.FLOAT,
    "floor(f)": DataType.INTEGER,
    "ceil(f)": DataType.INTEGER,
}

DATE_EXPRESSIONS = [
    "t::date",
    "d::datetime",
    "date_trunc(t, month)",
    "date_trunc(t, day)",
]


def _decimal(rng: random.Random) -> Decimal:
    return Decimal(rng.randrange(-99999, 99999)).scaleb(-4)


def _rows(rng: random.Random) -> list[tuple]:
    rows = []
    for idx in range(30):
        d = date(2020, 1, 1) + timedelta(days=rng.randrange(0, 900))
        t = datetime.combine(d, datetime.min.time()) + timedelta(
            seconds=rng.randrange(0, 86400)
        )
        i = rng.choice([v for v in range(-40, 40) if v != 0])
        f = rng.choice([rng.uniform(-1e7, 1e7), rng.uniform(-3, 3), 16777217.0])
        rows.append((idx, i, f, _decimal(rng), _decimal(rng), rng.random() < 0.5, d, t))
    return rows


def _literal(value: object) -> str:
    if isinstance(value, bool):
        return "1" if value else "0"
    if isinstance(value, (date, datetime)):
        return f"'{value}'"
    return repr(value) if isinstance(value, float) else str(value)


def _executor(dialect: Dialects, rows: list[tuple]) -> Executor:
    executor = new_executor(dialect)
    executor.execute_raw_sql("drop table if exists raw")
    executor.execute_raw_sql(DDL[dialect])
    values = ", ".join("(" + ", ".join(_literal(v) for v in row) + ")" for row in rows)
    executor.execute_raw_sql(f"insert into raw values {values}")
    executor.parse_text(MODEL)
    return executor


def _persisted(executor: Executor, expr: str) -> tuple[list, list]:
    executor.parse_text(f"property id.v <- {expr};")
    live = executor.execute_text("select id, v order by id asc;")[-1].fetchall()
    executor.execute_text("persist copy into copy from select id, v;")
    stored = executor.execute_raw_sql("select id, v from copy order by id").fetchall()
    return [r[1] for r in live], [r[1] for r in stored]


def _same(live: object, stored: object) -> bool:
    if live is None or stored is None:
        return live is stored
    if isinstance(live, (Decimal, int)) and isinstance(stored, (Decimal, int)):
        return Decimal(live) == Decimal(stored)
    return float(stored) == pytest.approx(float(live), rel=1e-12, abs=1e-12)


def _as_text(value: object) -> str:
    if isinstance(value, datetime):
        return value.isoformat(sep=" ")
    return str(value)


@pytest.fixture(params=DIALECTS, ids=lambda d: d.value)
def dialect(request) -> Dialects:
    return request.param


def test_declared_types() -> None:
    env = Environment()
    env.parse(MODEL)
    for expr, expected in EXPRESSIONS.items():
        env.parse(f"auto probe <- {expr};")
        assert env.concepts["probe"].datatype == expected, expr


@pytest.mark.parametrize("seed", SEEDS)
@pytest.mark.parametrize("expr", list(EXPRESSIONS))
def test_copy_reads_back(dialect: Dialects, seed: int, expr: str) -> None:
    executor = _executor(dialect, _rows(random.Random(seed)))
    live, stored = _persisted(executor, expr)
    for a, b in zip(live, stored, strict=True):
        assert _same(a, b), (expr, a, b)


@pytest.mark.parametrize("expr", DATE_EXPRESSIONS)
def test_date_copy_reads_back(dialect: Dialects, expr: str) -> None:
    executor = _executor(dialect, _rows(random.Random(0)))
    live, stored = _persisted(executor, expr)
    assert [_as_text(a) for a in live] == [_as_text(b) for b in stored], expr


def test_casts_compare_to_stored_dates(dialect: Dialects) -> None:
    rows = _rows(random.Random(1))
    executor = _executor(dialect, rows)
    ((count,),) = executor.execute_text(
        """where t::date = d and d::datetime <= t and date_add(d::datetime, day, 1) > t
        select count(id) as c;"""
    )[-1].fetchall()
    assert count == len(rows)


def test_floor_and_ceil_of_null(dialect: Dialects) -> None:
    executor = new_executor(dialect)
    rows = executor.execute_text(
        "select floor(null::float) as a, ceil(null::float) as b, floor(-0.125) as c, ceil(-0.125) as d;"
    )[-1].fetchall()
    assert [tuple(r) for r in rows] == [(None, None, -1, 0)]


def truncated_rem(value: Decimal | float, m: int) -> Decimal | float:
    if isinstance(value, float):
        return math.fmod(value, m)
    return Decimal(value) % m


@pytest.mark.parametrize("seed", SEEDS)
@pytest.mark.parametrize("column", ["i", "f", "n"])
def test_remainder_keeps_the_fraction(
    dialect: Dialects, seed: int, column: str
) -> None:
    rows = _rows(random.Random(seed))
    executor = _executor(dialect, rows)
    out = executor.execute_text(
        f"select id, {column} % 3 as r, {column} % -7 as s order by id asc;"
    )[-1].fetchall()
    index = MODEL_COLUMNS.index(column)
    for row, (_, r, s) in zip(rows, out, strict=True):
        assert _same(truncated_rem(row[index], 3), r), (row[index], r)
        assert _same(truncated_rem(row[index], -7), s), (row[index], s)
