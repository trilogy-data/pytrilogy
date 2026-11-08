"""A string read as a number, a date or a bool (formal/Trilogy/Readings.lean,
0.3.473) on DuckDB, SQLite and ClickHouse through chdb where it installs,
against `trilogy.core.casts`.

- a string reads as a number when, between whitespace, it is a signed decimal
  with an optional exponent; an integer rounds that value half away, a decimal
  rescales it half away, and every other string is NULL (`read_number_exact`,
  `sqlite_reads_prefix`, `clickhouse_refuses_space`, `duckdb_raises`);
- SQLite's GLOB guard is the pattern (`glob_number_exact`), and ClickHouse's
  18-digit read rounds as the exact value (`truncate_then_round_exact`);
- a string reads as a date when it is `YYYY-MM-DD` naming a real day, with an
  optional time of day (`read_date_exact`, `rolled_day_is_not_the_day`,
  `sqlite_julian_day`);
- an unreadable bool spelling is NULL, as on SQLite (`read_bool_total`);
- so a cast from a string is NULL-introducing (`read_null_introducing`): a
  non-null string keeps `is not null` and a persisted copy declares it `?`;
- a literal reads as a column does (`fold_reads_as_column`), and one with no
  reading is refused at parse time.
"""

import random
from datetime import date
from decimal import Decimal

import pytest

from tests.core.test_cast_laws import SPELLINGS
from tests.windows.test_window_laws import CHDB, new_executor, seeds_for
from trilogy import Dialects, Executor
from trilogy.core.casts import (
    text_to_bool,
    text_to_date,
    text_to_decimal,
    text_to_float,
    text_to_int,
)
from trilogy.parsing.v2.model import HydrationError

DIALECTS = [Dialects.DUCK_DB, Dialects.SQLITE, *CHDB]
SEEDS = range(3)

MODEL = """
key id int;
property id.s string;
property id.i int;
datasource reads (id: id, s: s, i: ?i) grain (id) address reads;
"""

DDL = {
    Dialects.DUCK_DB: "create table reads (id int, s varchar, i int)",
    Dialects.SQLITE: "create table reads (id int, s varchar, i int)",
    Dialects.CLICKHOUSE: (
        "create table reads (id Int64, s String, i Nullable(Int64)) engine = Memory"
    ),
}

UNREADABLE_NUMBERS = [
    "",
    " ",
    "abc",
    "1_000",
    "0x10",
    "inf",
    "-inf",
    "nan",
    "1e",
    "e3",
    "1e+",
    ".",
    "+",
    "-",
    "+-1",
    "--1",
    "1.2.3",
    "12abc",
    "1,5",
    "1 2",
    "1e2.5",
    "١٢",
]
UNREADABLE_DATES = [
    "2020-02-30",
    "2019-02-29",
    "2020-13-01",
    "2020-00-10",
    "2020-01-00",
    "2020-1-5",
    "20200105",
    "2020-01-05x",
    "2020-01-05 24:00:00",
    "2020-01-05 12:60:00",
    "2020-01-05 12:00",
    "2020-01-05 12:00:00.",
    "1.5",
    "2020/01/05",
]
UNREADABLE_BOOLS = ["on", "off", "banana", "2", "1.0", " true", ""]
SPACE = ["", " ", "  ", "\t", "\n", " \t"]


def _mantissa(rng: random.Random, binary_exact: bool) -> str:
    """A decimal string near the claims: ties at 0 and 2 digits, leading
    zeros, a bare '.5' or '5.'. `binary_exact` keeps the fraction a multiple
    of 1/8, which SQLite (a REAL) reads exactly."""
    whole = rng.choice([0, 1, 2, 7, 12, 99, 250, 1234, 65536])
    eighths = rng.randint(0, 7)
    if binary_exact or rng.random() < 0.5:
        frac = str(Decimal(eighths) / 8)[2:] if eighths else ""
    else:
        frac = str(rng.randint(0, 9999)).zfill(rng.randint(1, 4))
    shape = rng.random()
    if shape < 0.1 and frac:
        return f".{frac}"
    if shape < 0.2:
        return f"{whole}."
    if shape < 0.3:
        return f"00{whole}"
    return f"{whole}.{frac}" if frac else str(whole)


def _number(rng: random.Random, binary_exact: bool) -> str:
    text = rng.choice(["", "", "+", "-"]) + _mantissa(rng, binary_exact)
    if rng.random() < 0.25:
        signs = ["", "+"] if binary_exact else ["", "+", "-"]
        text += rng.choice("eE") + rng.choice(signs) + str(rng.randint(0, 2))
    return rng.choice(SPACE) + text + rng.choice(SPACE)


def _date(rng: random.Random) -> str:
    day = date(rng.randint(1971, 2100), rng.randint(1, 12), rng.randint(1, 28))
    text = day.isoformat()
    if rng.random() < 0.3:
        text += rng.choice(" T") + f"{rng.randint(0, 23):02}:{rng.randint(0, 59):02}"
        text += f":{rng.randint(0, 59):02}" + rng.choice(["", ".5", ".123456"])
    return rng.choice(SPACE) + text + rng.choice(SPACE)


def _strings(rng: random.Random, binary_exact: bool) -> list[str]:
    values: list[str] = []
    for _ in range(60):
        pick = rng.random()
        if pick < 0.45:
            values.append(_number(rng, binary_exact))
        elif pick < 0.6:
            values.append(rng.choice(UNREADABLE_NUMBERS))
        elif pick < 0.75:
            values.append(_date(rng))
        elif pick < 0.85:
            values.append(rng.choice(UNREADABLE_DATES))
        else:
            values.append(rng.choice([*SPELLINGS, *UNREADABLE_BOOLS]))
    return values


def _executor(dialect: Dialects, strings: list[str]) -> Executor:
    executor = new_executor(dialect)
    executor.execute_raw_sql("drop table if exists reads")
    executor.execute_raw_sql(DDL[dialect])
    literal = executor.generator.render_string_literal
    rows = [
        f"({k}, {literal(s)}, {k % 7 - 3 if k % 5 else 'NULL'})"
        for k, s in enumerate(strings)
    ]
    executor.execute_raw_sql(f"insert into reads values {', '.join(rows)}")
    executor.parse_text(MODEL)
    return executor


def _column(executor: Executor, expr: str, where: str = "") -> list:
    got = executor.execute_text(f"select id, {expr} as x {where} order by id asc;")
    return [r[1] for r in got[-1].fetchall()]


def _decimal(x) -> Decimal | None:
    return None if x is None else Decimal(str(x))


def _date_value(x) -> date | None:
    if x is None or isinstance(x, date):
        return x
    return date.fromisoformat(x)


def _bool(x) -> bool | None:
    return None if x is None else bool(x)


def _read_bool(s: str) -> bool | None:
    try:
        return text_to_bool(s)
    except ValueError:
        return None


@pytest.fixture(params=DIALECTS, ids=lambda d: d.value)
def dialect(request) -> Dialects:
    return request.param


@pytest.mark.parametrize("seed", SEEDS)
def test_number_reads(dialect: Dialects, seed: int) -> None:
    if seed not in seeds_for(dialect, SEEDS):
        pytest.skip("ClickHouse runs the first seed")
    strings = _strings(random.Random(seed), dialect == Dialects.SQLITE)
    executor = _executor(dialect, strings)
    assert _column(executor, "s::int") == [text_to_int(s) for s in strings]
    for scale in (0, 2, 3):
        got = [_decimal(x) for x in _column(executor, f"s::numeric(12,{scale})")]
        assert got == [text_to_decimal(s, scale) for s in strings], scale
    assert _column(executor, "s::float") == [text_to_float(s) for s in strings]


@pytest.mark.parametrize("seed", SEEDS)
def test_date_reads(dialect: Dialects, seed: int) -> None:
    if seed not in seeds_for(dialect, SEEDS):
        pytest.skip("ClickHouse runs the first seed")
    strings = _strings(random.Random(seed), True)
    executor = _executor(dialect, strings)
    got = [_date_value(x) for x in _column(executor, "s::date")]
    assert got == [text_to_date(s) for s in strings]


def test_bool_reads(dialect: Dialects) -> None:
    strings = _strings(random.Random(0), True)
    executor = _executor(dialect, strings)
    got = [_bool(x) for x in _column(executor, "s::bool")]
    assert got == [_read_bool(s) for s in strings]


@pytest.mark.parametrize("target", ["int", "numeric(12,2)", "float", "date", "bool"])
def test_unreadable_is_not_null_kept(dialect: Dialects, target: str) -> None:
    strings = _strings(random.Random(1), True)
    executor = _executor(dialect, strings)
    read = {
        "int": text_to_int,
        "numeric(12,2)": lambda s: text_to_decimal(s, 2),
        "float": text_to_float,
        "date": text_to_date,
        "bool": _read_bool,
    }[target]
    kept = _column(executor, "s", f"where s::{target} is not null")
    assert kept == [s for s in strings if read(s) is not None]
    dropped = _column(executor, "s", f"where s::{target} is null")
    assert dropped == [s for s in strings if read(s) is None]


def test_persisted_read_is_declared_nullable(dialect: Dialects) -> None:
    strings = _strings(random.Random(2), True)
    executor = _executor(dialect, strings)
    executor.execute_raw_sql("drop table if exists read_copy")
    executor.parse_text("auto r <- s::int; auto d <- s::date;")
    executor.execute_text("persist read_copy into read_copy from select id, r, d;")
    nullable = {
        c.address
        for c in executor.environment.datasources["read_copy"].nullable_concepts
    }
    for name in ("r", "d"):
        assert executor.environment.concepts[name].address in nullable, name
    assert _column(executor, "r", "where r is not null") == [
        text_to_int(s) for s in strings if text_to_int(s) is not None
    ]


def test_membership_in_a_read_keeps_null(dialect: Dialects) -> None:
    """Membership matches by identity, and an unreadable string puts NULL in
    the set, so a NULL `i` is kept."""
    strings = _strings(random.Random(3), True)
    executor = _executor(dialect, strings)
    executor.parse_text("auto r <- s::int;")
    members = {text_to_int(s) for s in strings}
    assert None in members
    ints = [k % 7 - 3 if k % 5 else None for k in range(len(strings))]
    assert _column(executor, "i", "where i in r") == [i for i in ints if i in members]


@pytest.mark.parametrize("member", ["i + 0", "coalesce(i, 0) + 1"])
@pytest.mark.parametrize("operator", ["in", "not in"])
def test_membership_is_identity(dialect: Dialects, member: str, operator: str) -> None:
    """A NULL probe matches a NULL member and `not in` is the complement; on
    ClickHouse a correlated `<=>` lost the NULL match both ways."""
    strings = _strings(random.Random(4), True)
    executor = _executor(dialect, strings)
    executor.parse_text(f"auto m <- {member};")
    ints = [k % 7 - 3 if k % 5 else None for k in range(len(strings))]
    members = set(ints) if member == "i + 0" else {(i or 0) + 1 for i in ints}
    kept = _column(executor, "id", f"where i {operator} m")
    assert kept == [
        k for k, i in enumerate(ints) if (i in members) == (operator == "in")
    ]


LITERALS = [
    "12",
    " 12 ",
    "+12",
    "-2.5",
    "2.5",
    ".5",
    "5.",
    "1e3",
    "1E3",
    "-1.5e2",
    "0.125",
    "1e-3",
    "007",
]


@pytest.mark.parametrize("text", LITERALS)
def test_number_literal_reads_as_column(dialect: Dialects, text: str) -> None:
    executor = _executor(dialect, [text])
    for target, read in [
        ("int", text_to_int(text)),
        ("float", text_to_float(text)),
        ("numeric(12,2)", text_to_decimal(text, 2)),
    ]:
        got = executor.execute_text(f"select '{text}'::{target} as x;")[-1]
        literal = _decimal(got.fetchall()[0][0])
        column = _decimal(_column(executor, f"s::{target}")[0])
        assert literal == column == _decimal(read), target


@pytest.mark.parametrize("text", ["2020-01-05", " 2020-01-05 ", "2020-01-05 12:00:00"])
def test_date_literal_reads_as_column(dialect: Dialects, text: str) -> None:
    executor = _executor(dialect, [text])
    got = executor.execute_text(f"select '{text}'::date as x;")[-1].fetchall()[0][0]
    assert _date_value(got) == text_to_date(text)
    assert _date_value(_column(executor, "s::date")[0]) == text_to_date(text)


@pytest.mark.parametrize(
    "literal", ["'abc'::int", "'1_000'::int", "'nan'::float", "'2020-02-30'::date"]
)
def test_unreadable_literal_raises(literal: str) -> None:
    executor = new_executor(Dialects.DUCK_DB)
    with pytest.raises(HydrationError, match="Cannot cast"):
        executor.parse_text(f"select {literal} as x;")
