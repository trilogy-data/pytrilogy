"""Scalar function laws (formal/Trilogy/Functions.lean), checked on every
dialect we can run against DuckDB's answers.

- `/` is true division, and a materialized quotient reads back unchanged.
- a cast to an integer is `round`, half away from zero.
- `date_diff` counts part boundaries: it adds along a chain and matches the
  truncated difference; `week` is whole 7-day spans of the day difference.
- `week` is the ISO week and `day_of_week` is 1 on Sunday.
"""

import math
import random
from datetime import datetime, timedelta

import pytest

from trilogy import Dialects, Executor
from trilogy.core.models.core import DataType
from trilogy.core.models.environment import Environment

DIALECTS = [Dialects.DUCK_DB, Dialects.SQLITE]
SEEDS = range(8)
EPOCH = datetime(1970, 1, 1)

MODEL = """
key id int;
property id.x int;
property id.y int;
property id.f float;
property id.a datetime;
property id.b datetime;
property id.c datetime;
datasource raw (id: id, x: x, y: y, f: f, a: a, b: b, c: c) grain (id) address raw;
"""

DDL = {
    Dialects.DUCK_DB: "create table raw (id int, x int, y int, f double, a timestamp, b timestamp, c timestamp)",
    Dialects.SQLITE: "create table raw (id int, x int, y int, f real, a text, b text, c text)",
}


def _instant(rng: random.Random) -> datetime:
    return datetime(2019, 12, 25) + timedelta(seconds=rng.randrange(0, 86400 * 400))


def _rows(rng: random.Random) -> list[tuple]:
    rows = []
    for i in range(40):
        y = rng.choice([v for v in range(-9, 10) if v != 0])
        f = rng.choice([rng.uniform(-50, 50), rng.randrange(-20, 20) + 0.5])
        rows.append(
            (i, rng.randrange(-60, 60), y, f, *sorted(_instant(rng) for _ in range(3)))
        )
    return rows


def _executor(dialect: Dialects, rows: list[tuple]) -> Executor:
    executor = dialect.default_executor(environment=Environment())
    executor.execute_raw_sql(DDL[dialect])
    values = ", ".join(
        "("
        + ", ".join(f"'{v}'" if isinstance(v, datetime) else repr(v) for v in row)
        + ")"
        for row in rows
    )
    executor.execute_raw_sql(f"insert into raw values {values}")
    executor.parse_text(MODEL)
    return executor


def _select(executor: Executor, text: str) -> list[tuple]:
    return [tuple(r) for r in executor.execute_text(text)[-1].fetchall()]


def _as_datetime(value: object) -> datetime:
    return value if isinstance(value, datetime) else datetime.fromisoformat(str(value))


def round_half_away(value: float) -> int:
    return int(math.copysign(math.floor(abs(value) + 0.5), value))


def _seconds(t: datetime) -> int:
    return int((t - EPOCH).total_seconds())


def boundary_diff(a: datetime, b: datetime, part: str) -> int:
    if part == "year":
        return b.year - a.year
    if part == "month":
        return (b.year - a.year) * 12 + b.month - a.month
    width = {"day": 86400, "hour": 3600, "minute": 60, "second": 1}[part]
    return _seconds(b) // width - _seconds(a) // width


@pytest.fixture(params=DIALECTS, ids=lambda d: d.value)
def dialect(request) -> Dialects:
    return request.param


@pytest.mark.parametrize("seed", SEEDS)
def test_divide_is_true_division(dialect: Dialects, seed: int) -> None:
    rows = _rows(random.Random(seed))
    executor = _executor(dialect, rows)
    out = _select(
        executor, "select id, x / y as q, (x / y) * y as back order by id asc;"
    )
    for (_, x, y, *_), (_, q, back) in zip(rows, out):
        assert q == pytest.approx(x / y), (x, y, q)
        assert back == pytest.approx(x), (x, y, back)
    ((ratio, avg),) = _select(
        executor, "select sum(x) / count(id) as ratio, avg(x) as mean;"
    )
    assert ratio == pytest.approx(avg)


def test_integer_quotient_is_typed_fractional() -> None:
    env = Environment()
    env.parse("key x int; auto q <- x / x; auto m <- avg(x);")
    assert env.concepts["q"].datatype == DataType.DOUBLE
    assert env.concepts["m"].datatype == DataType.DOUBLE


def test_materialized_quotient_reads_back(dialect: Dialects) -> None:
    rows = _rows(random.Random(0))
    executor = _executor(dialect, rows)
    live = _select(executor, "select id, x / y as q order by id asc;")
    executor.execute_text(
        "property id.ratio <- x / y; persist ratios into ratios from select id, ratio;"
    )
    stored = _select(executor, "select id, ratio order by id asc;")
    assert "ratios" in executor.generate_sql("select id, ratio;")[-1]
    assert [r[1] for r in stored] == pytest.approx([r[1] for r in live])


@pytest.mark.parametrize("seed", SEEDS)
def test_cast_to_int_is_round(dialect: Dialects, seed: int) -> None:
    rows = _rows(random.Random(seed))
    executor = _executor(dialect, rows)
    out = _select(
        executor,
        "select id, f::int as c, round(f, 0) as r, (x / 2)::int as h order by id asc;",
    )
    for (_, x, _, f, *_), (_, c, r, h) in zip(rows, out):
        assert c == round_half_away(f) == r, (f, c, r)
        assert h == round_half_away(x / 2), (x, h)


@pytest.mark.parametrize("seed", SEEDS)
@pytest.mark.parametrize("part", ["year", "month", "day", "hour", "minute", "second"])
def test_date_diff_counts_boundaries(dialect: Dialects, seed: int, part: str) -> None:
    rows = _rows(random.Random(seed))
    executor = _executor(dialect, rows)
    out = _select(
        executor,
        f"""select id, date_diff(a, b, {part}) as ab, date_diff(b, c, {part}) as bc,
        date_diff(a, c, {part}) as ac, date_diff(c, a, {part}) as ca order by id asc;""",
    )
    for (*_, a, b, c), (_, ab, bc, ac, ca) in zip(rows, out):
        assert ab == boundary_diff(a, b, part), (a, b)
        assert ab + bc == ac == -ca, (a, b, c)


@pytest.mark.parametrize("seed", SEEDS)
def test_week_diff_is_whole_spans_of_days(dialect: Dialects, seed: int) -> None:
    rows = _rows(random.Random(seed))
    executor = _executor(dialect, rows)
    out = _select(
        executor,
        "select id, date_diff(a, b, week) as w, date_diff(b, a, week) as back order by id asc;",
    )
    for (*_, a, b, _), (_, w, back) in zip(rows, out):
        days = boundary_diff(a, b, "day")
        assert w == int(days / 7) == -back, (a, b)


@pytest.mark.parametrize("seed", SEEDS)
def test_week_and_day_of_week(dialect: Dialects, seed: int) -> None:
    rows = _rows(random.Random(seed))
    executor = _executor(dialect, rows)
    out = _select(
        executor,
        """select id, week(a) as w, date_part(a, week) as pw, day_of_week(a) as d,
        date_part(a, day_of_week) as pd, date_trunc(a, week) as t order by id asc;""",
    )
    for (*_, a, _, _), (_, w, pw, d, pd, t) in zip(rows, out):
        assert w == pw == a.isocalendar().week, a
        assert d == pd == a.isoweekday() % 7 + 1, a
        assert (
            _as_datetime(t).isoweekday() == 1
            and _as_datetime(t).isocalendar().week == w
        )
