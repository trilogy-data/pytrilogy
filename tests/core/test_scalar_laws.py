"""Scalar function laws (formal/Trilogy/Scalars.lean), checked on DuckDB,
SQLite and ClickHouse (chdb) against a Python reference with the model's definitions.

- `date_add` / `date_sub` by months, quarters and years clamp to the month's
  last day, so `date_diff` counts back the offset.
- a whole-day offset or truncation of a date is a date: it equals the stored
  date, and is typed DATE.
- `greatest` / `least` are over the present arguments.
- `log(x, b)` is the log of `x` to base `b`.
"""

import calendar
import math
import random
from datetime import date, datetime, timedelta

import pytest

from tests.windows.test_window_laws import CHDB, new_executor
from trilogy import Dialects, Executor
from trilogy.core.models.core import DataType
from trilogy.core.models.environment import Environment

DIALECTS = [Dialects.DUCK_DB, Dialects.SQLITE, *CHDB]
SEEDS = range(6)
MONTHS = {"month": 1, "quarter": 3, "year": 12}

MODEL = """
key id int;
property id.d date;
property id.t datetime;
property id.n int;
property id.e date;
property id.p int;
property id.q int;
property id.r int;
property id.x float;
property id.s string;
property id.needle string;
datasource raw (id: id, d: d, t: t, n: n, e: e, p: ?p, q: ?q, r: ?r, x: x, s: s, needle: needle)
grain (id) address raw;
"""

DDL = {
    Dialects.DUCK_DB: "create table raw (id int, d date, t timestamp, n int, e date, p int, q int, r int, x double, s varchar, needle varchar)",
    Dialects.SQLITE: "create table raw (id int, d text, t text, n int, e text, p int, q int, r int, x real, s text, needle text)",
    Dialects.CLICKHOUSE: "create table raw (id Int64, d Date, t DateTime, n Int64, e Date, p Nullable(Int64), q Nullable(Int64), r Nullable(Int64), x Float64, s String, needle String) engine = Memory",
}


def add_months(value: date, months: int) -> date:
    index = value.year * 12 + value.month - 1 + months
    year, month = divmod(index, 12)
    last = calendar.monthrange(year, month + 1)[1]
    return value.replace(year=year, month=month + 1, day=min(value.day, last))


def _day(rng: random.Random) -> date:
    base = date(2019, 1, 1) + timedelta(days=rng.randrange(0, 365 * 3))
    if rng.random() < 0.5:
        # a month end, where the clamp matters
        return base.replace(day=calendar.monthrange(base.year, base.month)[1])
    return base


def _optional(rng: random.Random) -> int | None:
    return None if rng.random() < 0.3 else rng.randrange(-20, 20)


def _rows(rng: random.Random) -> list[tuple]:
    rows = []
    for i in range(40):
        d = _day(rng)
        t = datetime.combine(d, datetime.min.time()) + timedelta(
            seconds=rng.randrange(0, 86400)
        )
        n = rng.randrange(-30, 30)
        s = "".join(rng.choice("abc") for _ in range(6))
        rows.append(
            (
                i,
                d,
                t,
                n,
                d + timedelta(days=n),
                _optional(rng),
                _optional(rng),
                _optional(rng),
                rng.uniform(0.5, 5000),
                s,
                rng.choice("abcd"),
            )
        )
    return rows


def _literal(value: object) -> str:
    if value is None:
        return "null"
    if isinstance(value, (date, datetime)):
        return f"'{value}'"
    return repr(value)


def _executor(dialect: Dialects, rows: list[tuple]) -> Executor:
    executor = new_executor(dialect)
    executor.execute_raw_sql("drop table if exists raw")
    executor.execute_raw_sql(DDL[dialect])
    values = ", ".join("(" + ", ".join(_literal(v) for v in row) + ")" for row in rows)
    executor.execute_raw_sql(f"insert into raw values {values}")
    executor.parse_text(MODEL)
    return executor


def _select(executor: Executor, text: str) -> list[tuple]:
    return [tuple(r) for r in executor.execute_text(text)[-1].fetchall()]


def _as_date(value: object) -> date:
    if isinstance(value, datetime):
        assert value.time() == datetime.min.time(), value
        return value.date()
    if isinstance(value, date):
        return value
    text = str(value)
    assert len(text) == 10, f"{text!r} is not a date"
    return date.fromisoformat(text)


def _as_datetime(value: object) -> datetime:
    return value if isinstance(value, datetime) else datetime.fromisoformat(str(value))


@pytest.fixture(params=DIALECTS, ids=lambda d: d.value)
def dialect(request) -> Dialects:
    return request.param


@pytest.mark.parametrize("seed", SEEDS)
@pytest.mark.parametrize("unit", list(MONTHS))
def test_month_offsets_clamp(dialect: Dialects, seed: int, unit: str) -> None:
    rows = _rows(random.Random(seed))
    executor = _executor(dialect, rows)
    out = _select(
        executor,
        f"""select id, date_add(d, {unit}, n) as a, date_sub(d, {unit}, n) as b,
        date_diff(d, date_add(d, {unit}, n), month) as back,
        date_add(t, {unit}, n) as ta order by id asc;""",
    )
    factor = MONTHS[unit]
    for (_, d, t, n, *_), (_, a, b, back, ta) in zip(rows, out):
        assert _as_date(a) == add_months(d, n * factor), (d, n)
        assert _as_date(b) == add_months(d, -n * factor), (d, n)
        assert back == n * factor, (d, n)
        assert _as_datetime(ta) == datetime.combine(
            add_months(d, n * factor), t.time()
        ), (t, n)


@pytest.mark.parametrize("seed", SEEDS)
def test_day_offset_of_a_date_is_a_date(dialect: Dialects, seed: int) -> None:
    rows = _rows(random.Random(seed))
    executor = _executor(dialect, rows)
    out = _select(
        executor,
        """select id, date_add(d, day, n) as a, date_sub(e, day, n) as b,
        date_add(d, week, 1) as w, date_add(d, hour, 1) as h order by id asc;""",
    )
    for (_, d, _, n, e, *_), (_, a, b, w, h) in zip(rows, out):
        assert _as_date(a) == e and _as_date(b) == d, (d, n)
        assert _as_date(w) == d + timedelta(days=7)
        assert _as_datetime(h) == datetime.combine(d, datetime.min.time()).replace(
            hour=1
        )
    ((matched,),) = _select(
        executor, "where date_add(d, day, n) = e select count(id) as c;"
    )
    assert matched == len(rows)


@pytest.mark.parametrize("seed", SEEDS)
def test_truncation_to_a_day_is_a_date(dialect: Dialects, seed: int) -> None:
    rows = _rows(random.Random(seed))
    executor = _executor(dialect, rows)
    ((count,),) = _select(
        executor,
        """where date_trunc(t, day) = d and date_trunc(d, month) = date_add(d, day, 1 - day(d))
        select count(id) as c;""",
    )
    assert count == len(rows)


def test_offset_types() -> None:
    env = Environment()
    env.parse(MODEL)
    expected = {
        "date_add(d, day, 1)": DataType.DATE,
        "date_sub(d, month, 1)": DataType.DATE,
        "date_add(d, hour, 1)": DataType.DATETIME,
        "date_add(t, day, 1)": DataType.DATETIME,
        "date_sub(t, year, 1)": DataType.DATETIME,
    }
    for expr, datatype in expected.items():
        env.parse(f"auto probe <- {expr};")
        assert env.concepts["probe"].datatype == datatype, expr


def present_extreme(fn, values: tuple) -> int | None:
    present = [v for v in values if v is not None]
    return fn(present) if present else None


@pytest.mark.parametrize("seed", SEEDS)
def test_greatest_and_least_skip_nulls(dialect: Dialects, seed: int) -> None:
    rows = _rows(random.Random(seed))
    executor = _executor(dialect, rows)
    out = _select(
        executor,
        """select id, greatest(p, q, r) as g, least(p, q, r) as l,
        greatest(p, q) as g2, least(r) as l1 order by id asc;""",
    )
    for row, (_, g, l, g2, l1) in zip(rows, out):
        p, q, r = row[5:8]
        assert g == present_extreme(max, (p, q, r)), (p, q, r)
        assert l == present_extreme(min, (p, q, r)), (p, q, r)
        assert g2 == present_extreme(max, (p, q)), (p, q)
        assert l1 == r


@pytest.mark.parametrize("seed", SEEDS)
def test_log_takes_the_base_second(dialect: Dialects, seed: int) -> None:
    rows = _rows(random.Random(seed))
    executor = _executor(dialect, rows)
    out = _select(
        executor,
        """select id, log(x) as ten, log(x, 2) as two, log(x, n + 32) as var, log(x, x / 1000 + 1.5) as frac,
        log(100) as hundred order by id asc;""",
    )
    for (_, _, _, n, _, _, _, _, x, *_), (_, ten, two, var, frac, hundred) in zip(
        rows, out
    ):
        assert ten == pytest.approx(math.log10(x)), x
        assert two == pytest.approx(math.log2(x)), x
        assert var == pytest.approx(math.log(x, n + 32)), (x, n)
        assert frac == pytest.approx(math.log(x, x / 1000 + 1.5)), x
        assert hundred == pytest.approx(2.0)


@pytest.mark.parametrize("seed", SEEDS)
def test_strpos_is_one_based(dialect: Dialects, seed: int) -> None:
    rows = _rows(random.Random(seed))
    executor = _executor(dialect, rows)
    out = _select(executor, "select id, strpos(s, needle) as pos order by id asc;")
    for row, (_, pos) in zip(rows, out):
        s, needle = row[9], row[10]
        assert pos == s.find(needle) + 1, (s, needle)
