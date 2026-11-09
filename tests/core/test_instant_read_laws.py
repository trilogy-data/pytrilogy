"""A string read as a datetime or a timestamp (formal/Trilogy/Instants.lean)
and a number read past its target's range (formal/Trilogy/Readings.lean,
0.3.474) on DuckDB, SQLite and ClickHouse through chdb where it installs,
against `trilogy.core.casts`.

- a string reads as an instant when it is a date, an optional time of day and
  an optional zone; the value is the UTC instant (`read_instant_exact`,
  `duckdb_drops_zone`, `hour_24_is_not_a_clock`,
  `rolled_instant_is_not_the_instant`, `clickhouse_epoch_seconds`,
  `sqlite_zone_limit`), kept at the engine's precision by truncation
  (`trunc_trunc`), so a comparison with a whole second is exact
  (`trunc_ge_exact`) and the order of reads is the order of instants;
- `datetime(s)`, `timestamp(s)` and `date(s)` of a string are its cast;
- a folded literal reads as a column does (`bound_text_is_not_datetime_text`);
- a numeral whose rounded value the target cannot hold is NULL
  (`saturation_is_not_the_value`, `clickhouse_overflow_raises`,
  `decimal_precision_unchecked`, `round_can_leave_range`).
"""

import random
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal

import pytest

from tests.core.test_text_read_laws import _column, _date_value, _decimal, _executor
from tests.windows.test_window_laws import CHDB, new_executor
from trilogy import Dialects
from trilogy.core.casts import INT64, text_to_date, text_to_datetime, text_to_decimal
from trilogy.core.casts import text_to_int as read_int
from trilogy.parsing.v2.model import HydrationError

DIALECTS = [Dialects.DUCK_DB, Dialects.SQLITE, *CHDB]
SEEDS = range(3)
# DuckDB keeps microseconds, ClickHouse's DateTime64(3) milliseconds and
# SQLite's `datetime()` whole seconds
PRECISION = {Dialects.DUCK_DB: 6, Dialects.SQLITE: 0, Dialects.CLICKHOUSE: 3}
INT32 = (-(2**31), 2**31 - 1)

UNREADABLE = [
    "",
    "abc",
    "1.5",
    "2147483648",
    "20200105",
    "2020-02-30 00:00:00",
    "2019-02-29 12:00:00",
    "2020-13-01 00:00:00",
    "2020-00-10 00:00:00",
    "2020-01-00",
    "2020-01-05 24:00:00",
    "2020-01-05 12:60:00",
    "2020-01-05 12:34",
    "2020-01-05 12:34:56.",
    "2020-01-05 12:34:565",
    "2020-01-05 12:34:56.5.5",
    "2020-01-05 12:34:56 UTC",
    "2020-01-05 12:34:56+2",
    "2020-01-05 12:34:56+24:00",
    "2020-01-05 12:34:56+02:60",
    "2020-01-05 12:34:56z",
    "2020-01-05Z",
    "2020-01-05 12:34:56.5x",
    "2020-1-5 1:02:03",
    "2020-01-05  12:34:56",
]
SPACE = ["", " ", "\t", " \n"]


def _instant(rng: random.Random) -> str:
    day = date(rng.randint(1971, 2100), rng.randint(1, 12), rng.randint(1, 28))
    if rng.random() < 0.3:
        text = rng.choice("-/").join([str(day.year), str(day.month), str(day.day)])
    else:
        text = day.isoformat()
    if rng.random() < 0.85:
        hour, minute, second = (
            rng.randint(0, 23),
            rng.randint(0, 59),
            rng.randint(0, 59),
        )
        text += rng.choice(" T") + f"{hour:02}:{minute:02}:{second:02}"
        if rng.random() < 0.5:
            text += "." + "".join(
                rng.choice("0123456789") for _ in range(rng.randint(1, 9))
            )
        zone = rng.random()
        if zone < 0.15:
            text += "Z"
        elif zone < 0.6:
            offset = f"{rng.randint(0, 23):02}"
            minutes = f"{rng.choice([0, 15, 30, 45, 59]):02}"
            text += rng.choice("+-") + offset + rng.choice(["", minutes, f":{minutes}"])
    return rng.choice(SPACE) + text + rng.choice(SPACE)


def _strings(rng: random.Random) -> list[str]:
    return [
        _instant(rng) if rng.random() < 0.75 else rng.choice(UNREADABLE)
        for _ in range(60)
    ]


def _at(value: datetime | None, digits: int) -> datetime | None:
    if value is None:
        return None
    unit = 10 ** (6 - digits)
    return value.replace(microsecond=value.microsecond // unit * unit)


def _utc(x) -> datetime | None:
    if x is None:
        return None
    if isinstance(x, str):
        x = datetime.fromisoformat(x)
    if x.tzinfo is not None:
        x = x.astimezone(UTC).replace(tzinfo=None)
    return x


def _want(dialect: Dialects, strings: list[str]) -> list[datetime | None]:
    return [_at(text_to_datetime(s), PRECISION[dialect]) for s in strings]


@pytest.fixture(params=DIALECTS, ids=lambda d: d.value)
def dialect(request) -> Dialects:
    return request.param


@pytest.mark.parametrize("seed", SEEDS)
@pytest.mark.parametrize(
    "expr", ["s::datetime", "s::timestamp", "datetime(s)", "timestamp(s)"]
)
def test_instant_reads(dialect: Dialects, seed: int, expr: str) -> None:
    strings = _strings(random.Random(seed))
    executor = _executor(dialect, strings)
    assert [_utc(x) for x in _column(executor, expr)] == _want(dialect, strings)


def test_instant_reads_ignore_session_zone() -> None:
    """A string with no zone is UTC, not the session's zone."""
    strings = _strings(random.Random(5))
    executor = _executor(Dialects.DUCK_DB, strings)
    executor.execute_raw_sql("SET TimeZone = 'America/New_York'")
    for expr in ("s::datetime", "s::timestamp"):
        got = [_utc(x) for x in _column(executor, expr)]
        assert got == _want(Dialects.DUCK_DB, strings), expr


def test_unreadable_instant_is_null(dialect: Dialects) -> None:
    strings = _strings(random.Random(1))
    executor = _executor(dialect, strings)
    kept = _column(executor, "s", "where s::datetime is not null")
    assert kept == [s for s in strings if text_to_datetime(s) is not None]


@pytest.mark.parametrize("operator", [">=", "<"])
def test_comparison_with_a_whole_second(dialect: Dialects, operator: str) -> None:
    strings = _strings(random.Random(2))
    executor = _executor(dialect, strings)
    bound = datetime(2040, 6, 1, 12, 0, 0)
    kept = _column(
        executor,
        "s",
        f"where s::datetime {operator} '{bound.isoformat(' ')}'::datetime",
    )
    reads = [(s, text_to_datetime(s)) for s in strings]
    assert kept == [
        s
        for s, r in reads
        if r is not None and (r >= bound if operator == ">=" else r < bound)
    ]


def test_order_of_reads_is_order_of_instants(dialect: Dialects) -> None:
    strings = _strings(random.Random(3))
    executor = _executor(dialect, strings)
    got = executor.execute_text(
        "where s::datetime is not null select id, s::datetime as x order by x asc, id asc;"
    )[-1].fetchall()
    want = sorted(
        (r, k) for k, r in enumerate(_want(dialect, strings)) if r is not None
    )
    assert [row[0] for row in got] == [k for _, k in want]


def test_date_function_is_the_cast(dialect: Dialects) -> None:
    strings = _strings(random.Random(4))
    executor = _executor(dialect, strings)
    got = [_date_value(x) for x in _column(executor, "date(s)")]
    assert got == [text_to_date(s) for s in strings]


def test_persisted_instant_is_declared_nullable(dialect: Dialects) -> None:
    strings = _strings(random.Random(6))
    executor = _executor(dialect, strings)
    executor.execute_raw_sql("drop table if exists instant_copy")
    executor.parse_text("auto t <- datetime(s);")
    executor.execute_text("persist instant_copy into instant_copy from select id, t;")
    nullable = {
        c.address
        for c in executor.environment.datasources["instant_copy"].nullable_concepts
    }
    assert executor.environment.concepts["t"].address in nullable


LITERALS = [
    "2020-01-05 12:34:56",
    " 2020-01-05T12:34:56 ",
    "2020/1/5",
    "2020-01-05 23:34:56-05:30",
    "2020-01-05 00:30:00+0100",
    "2020-01-05T12:34:56Z",
    "2020-01-05 12:34:56+23",
]


@pytest.mark.parametrize("text", LITERALS)
@pytest.mark.parametrize("target", ["datetime", "timestamp"])
def test_instant_literal_reads_as_column(
    dialect: Dialects, text: str, target: str
) -> None:
    executor = _executor(dialect, [text])
    folded = executor.execute_text(f"select '{text}'::{target} as x;")[-1].fetchall()
    want = _at(text_to_datetime(text), PRECISION[dialect])
    assert _utc(folded[0][0]) == _utc(_column(executor, f"s::{target}")[0]) == want
    matched = _column(executor, "s", f"where s::{target} = '{text}'::{target}")
    assert matched == [text]


@pytest.mark.parametrize("target", ["datetime", "timestamp"])
def test_bound_instant_matches_column(dialect: Dialects, target: str) -> None:
    """A constant is bound as a parameter; on SQLite its isoformat text never
    equalled the `datetime()` text of a read."""
    strings = ["2020-01-05 12:34:56", "2020-01-05 12:34:56.5", "2020-01-05 14:34:56+02"]
    executor = _executor(dialect, strings)
    executor.parse_text(f"const c <- '2020-01-05 12:34:56'::{target};")
    want = [
        s
        for s in strings
        if _at(text_to_datetime(s), PRECISION[dialect])
        == datetime(2020, 1, 5, 12, 34, 56)
    ]
    assert _column(executor, "s", f"where s::{target} = c") == want


@pytest.mark.parametrize(
    "literal",
    ["'20200105'::datetime", "'2020-02-30 00:00:00'::timestamp", "datetime('abc')"],
)
def test_unreadable_instant_literal_raises(literal: str) -> None:
    executor = new_executor(Dialects.DUCK_DB)
    with pytest.raises((HydrationError, Exception), match="Cannot cast"):
        executor.parse_text(f"select {literal} as x;")


def test_folded_instant_is_utc() -> None:
    executor = new_executor(Dialects.DUCK_DB)
    got = executor.execute_text("select '2020-01-05 12:34:56+02:00'::timestamp as x;")[
        -1
    ].fetchall()
    assert got[0][0] == datetime(2020, 1, 5, 10, 34, 56, tzinfo=UTC)
    assert _utc(got[0][0]) - datetime(2020, 1, 5, 12, 34, 56) == timedelta(hours=-2)


BOUNDARIES = [
    "2147483647",
    "2147483648",
    "-2147483648",
    "-2147483649",
    "2147483647.4",
    "2147483647.5",
    "9223372036854775807",
    "9223372036854775808",
    "-9223372036854775808",
    "-9223372036854775809",
    "+0009223372036854775807",
    "99999999999999999999",
    "1e18",
    "1e19",
    "-1e19",
    "1e400",
    "9999999999.99",
    "-9999999999.99",
    "9999999999.995",
    "12345678901.25",
    "12",
]


def _int_bounds(dialect: Dialects, target: str) -> tuple[int, int]:
    """A Trilogy `int` is INTEGER (32-bit) on DuckDB and 64-bit elsewhere."""
    return INT32 if dialect == Dialects.DUCK_DB and target == "int" else INT64


@pytest.mark.parametrize("target", ["int", "bigint"])
def test_integer_read_in_range(dialect: Dialects, target: str) -> None:
    executor = _executor(dialect, BOUNDARIES)
    bounds = _int_bounds(dialect, target)
    assert _column(executor, f"s::{target}") == [
        read_int(s, bounds) for s in BOUNDARIES
    ]


@pytest.mark.parametrize("precision, scale", [(12, 2), (5, 0), (18, 4)])
def test_decimal_read_in_precision(
    dialect: Dialects, precision: int, scale: int
) -> None:
    executor = _executor(dialect, BOUNDARIES)
    got = [_decimal(x) for x in _column(executor, f"s::numeric({precision},{scale})")]
    want = [text_to_decimal(s, scale, precision) for s in BOUNDARIES]
    if dialect == Dialects.SQLITE:
        # a REAL holds 15 significant digits
        want = [
            w if w is None or abs(w) < Decimal(10) ** 15 else _decimal(float(w))
            for w in want
        ]
    assert got == want


@pytest.mark.parametrize(
    "literal",
    ["'99999999999999999999'::int", "'12345678901.25'::numeric(12,2)"],
)
def test_literal_past_range_raises(literal: str) -> None:
    executor = new_executor(Dialects.DUCK_DB)
    with pytest.raises(HydrationError, match="Cannot cast"):
        executor.parse_text(f"select {literal} as x;")
