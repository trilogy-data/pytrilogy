"""A value cast to a string (formal/Trilogy/Writings.lean, 0.3.475) on DuckDB,
SQLite and ClickHouse through chdb where it installs, against
`trilogy.core.casts`.

- a float is written as Python's `repr`: its shortest digits, fixed or
  `e±XX` (`layout_read_exact`, `fixed_iff`, `clickhouse_drops_point`), and
  the text reads back as the value (`sqlite_fifteen_digits`); SQLite's
  printf is exact to 15 digits (`round15_exact`), so a value needing 16 or
  17 is held to the round trip only there;
- a decimal keeps its declared scale (`decimal_read_write`, `scale_dropped`);
- an instant is written with its fraction's trailing zeros dropped and a
  timestamp at UTC with `+00:00` (`fraction_read_write`,
  `fraction_precision_free`, `clickhouse_pads_fraction`,
  `session_zone_written`);
- a decimal quotient is the double it is declared as
  (`clickhouse_decimal_quotient`).
"""

import math
import random
import struct
from datetime import UTC
from decimal import Decimal

import pytest

from tests.core.test_cast_laws import rescale
from tests.core.test_instant_read_laws import PRECISION, _at, _strings
from tests.core.test_text_read_laws import _executor as _text_executor
from tests.windows.test_window_laws import CHDB, new_executor, seeds_for
from trilogy import Dialects, Executor
from trilogy.core.casts import (
    decimal_to_text,
    float_to_text,
    instant_to_text,
    text_to_datetime,
)

DIALECTS = [Dialects.DUCK_DB, Dialects.SQLITE, *CHDB]
SEEDS = range(3)
INF = float("inf")

MODEL = """
key id int;
property id.f float;
property id.n numeric(12,4);
datasource writes (id: id, f: ?f, n: ?n) grain (id) address writes;
"""

DDL = {
    Dialects.DUCK_DB: "create table writes (id int, f double, n decimal(12,4))",
    Dialects.SQLITE: "create table writes (id int, f double, n decimal(12,4))",
    Dialects.CLICKHOUSE: (
        "create table writes (id Int64, f Nullable(Float64),"
        " n Nullable(Decimal(12,4))) engine = Memory"
    ),
}

SPECIAL = [
    0.0,
    1.0,
    -1.0,
    0.5,
    1e15,
    1e16,
    9999999999999998.0,
    1e20,
    1e21,
    1e22,
    1e-4,
    1e-5,
    9.999e-5,
    123456789012.5,
    1.5e300,
    -2.5e-300,
    2.0**60,
    INF,
    -INF,
]

Row = tuple[int, float | None, Decimal | None]


def _float_literal(dialect: Dialects, value: float) -> str:
    """inf, NaN and -0.0 have no literal (DuckDB's `-0.0` is a DECIMAL)."""
    if math.isfinite(value) and not (value == 0 and math.copysign(1, value) < 0):
        return repr(value)
    if dialect == Dialects.SQLITE:
        return "-9e999" if value < 0 else "9e999"
    if dialect == Dialects.CLICKHOUSE:
        return f"toFloat64('{value!r}')"
    return f"'{value!r}'::double"


def _short(rng: random.Random) -> float:
    """A double whose shortest form has at most 15 digits."""
    digits = rng.randint(1, 15)
    mantissa = rng.randint(10 ** (digits - 1), 10**digits - 1)
    exponent = rng.choice([rng.randint(-8, 22), rng.randint(-300, 300)])
    return float(f"{rng.choice('+-')}{mantissa}e{exponent - digits + 1}")


def _any_double(rng: random.Random) -> float:
    while True:
        value = struct.unpack("d", struct.pack("Q", rng.getrandbits(64)))[0]
        if math.isfinite(value) and abs(value) > 1e-307:
            return value


def _rows(rng: random.Random, floats: list[float]) -> list[Row]:
    rows: list[Row] = []
    for k, f in enumerate(floats):
        units = rng.randint(-(10**11), 10**11) // rng.choice([1, 10, 1000, 10**6])
        n = Decimal(units).scaleb(-4)
        rows.append(
            (
                k,
                None if rng.random() < 0.1 else f,
                None if rng.random() < 0.1 else n,
            )
        )
    return rows


def _executor(dialect: Dialects, rows: list[Row]) -> Executor:
    executor = new_executor(dialect)
    executor.execute_raw_sql("drop table if exists writes")
    executor.execute_raw_sql(DDL[dialect])
    values = ", ".join(
        f"({k}, {'NULL' if f is None else _float_literal(dialect, f)},"
        f" {'NULL' if n is None else n})"
        for k, f, n in rows
    )
    executor.execute_raw_sql(f"insert into writes values {values}")
    executor.parse_text(MODEL)
    return executor


def _column(executor: Executor, expr: str, where: str = "") -> list:
    got = executor.execute_text(f"select id, {expr} as x {where} order by id asc;")
    return [r[1] for r in got[-1].fetchall()]


@pytest.fixture(params=DIALECTS, ids=lambda d: d.value)
def dialect(request) -> Dialects:
    return request.param


@pytest.mark.parametrize("seed", SEEDS)
def test_float_text_is_repr(dialect: Dialects, seed: int) -> None:
    if seed not in seeds_for(dialect, SEEDS):
        pytest.skip("fewer seeds on ClickHouse")
    rng = random.Random(seed)
    floats = SPECIAL + [_short(rng) for _ in range(150)]
    rows = _rows(rng, floats)
    executor = _executor(dialect, rows)
    assert _column(executor, "f::string") == [
        None if f is None else float_to_text(f) for _, f, _ in rows
    ]


def test_seventeen_digit_float_text(dialect: Dialects) -> None:
    """SQLite's printf picks the 16th and 17th digits inexactly; the text
    still reads back as the value there, and is `repr` elsewhere."""
    rng = random.Random(7)
    rows = _rows(rng, [_any_double(rng) for _ in range(200)])
    executor = _executor(dialect, rows)
    got = _column(executor, "f::string")
    if dialect == Dialects.SQLITE:
        assert [None if x is None else float(x) for x in got] == [f for _, f, _ in rows]
    else:
        assert got == [None if f is None else float_to_text(f) for _, f, _ in rows]


@pytest.mark.parametrize("value", [0.30000000000000004, 1 / 3, 2 / 3 * 1e-10])
def test_float_text_reads_back(dialect: Dialects, value: float) -> None:
    executor = _executor(dialect, [(0, value, None)])
    assert _column(executor, "f", "where (f::string)::float = f") == [value]


@pytest.mark.parametrize("dialect", [Dialects.DUCK_DB, *CHDB], ids=lambda d: d.value)
def test_signed_zero_and_nan_text(dialect: Dialects) -> None:
    """SQLite stores neither -0.0 nor NaN; the other engines write `repr`."""
    executor = _executor(dialect, [(0, float("nan"), None), (1, -0.0, None)])
    assert _column(executor, "f::string") == ["nan", "-0.0"]


@pytest.mark.parametrize("literal, text", [("1.50", "1.5"), ("1.0", "1.0")])
def test_float_literal_text(dialect: Dialects, literal: str, text: str) -> None:
    executor = _executor(dialect, [(0, 1.0, None)])
    assert _column(executor, f"{literal}::string") == [text]


@pytest.mark.parametrize("seed", SEEDS)
def test_decimal_text_keeps_scale(dialect: Dialects, seed: int) -> None:
    rng = random.Random(seed)
    rows = _rows(rng, [0.0] * 80)
    executor = _executor(dialect, rows)
    want = [None if n is None else decimal_to_text(n, 4) for _, _, n in rows]
    assert _column(executor, "n::string") == want
    doubled = [None if n is None else decimal_to_text(n * 2, 4) for _, _, n in rows]
    assert _column(executor, "(n * 2)::string") == doubled
    # a REAL rounds a tie by its binary value (`Casts.sqlite_keeps_digits`)
    kept = [
        dialect != Dialects.SQLITE or n is None or abs(n * 100) % 1 != Decimal("0.5")
        for _, _, n in rows
    ]
    rescaled = [None if n is None else rescale(n, 2) for _, _, n in rows]
    for expr, scale in [("(n::numeric(14,2))::string", 2), ("round(n, 2)::string", 4)]:
        got = _column(executor, expr)
        want = [None if n is None else decimal_to_text(n, scale) for n in rescaled]
        assert [x for x, k in zip(got, kept) if k] == [
            x for x, k in zip(want, kept) if k
        ], expr


def test_decimal_text_reads_back(dialect: Dialects) -> None:
    rows = _rows(random.Random(4), [0.0] * 60)
    executor = _executor(dialect, rows)
    kept = _column(executor, "n", "where (n::string)::numeric(12,4) = n")
    assert len(kept) == sum(n is not None for _, _, n in rows)


def test_decimal_quotient_is_a_double(dialect: Dialects) -> None:
    rows = _rows(random.Random(5), [0.0] * 40)
    executor = _executor(dialect, rows)
    for got, (_, _, n) in zip(_column(executor, "n / 3"), rows):
        if n is None:
            assert got is None
        else:
            assert got == pytest.approx(float(n) / 3, rel=1e-12, abs=1e-15)


def _instant_text(dialect: Dialects, s: str, aware: bool) -> str | None:
    read = _at(text_to_datetime(s), PRECISION[dialect])
    if read is None:
        return None
    return instant_to_text(read.replace(tzinfo=UTC) if aware else read)


@pytest.mark.parametrize("target", ["datetime", "timestamp"])
def test_instant_text(dialect: Dialects, target: str) -> None:
    strings = _strings(random.Random(11))
    executor = _text_executor(dialect, strings)
    aware = target == "timestamp"
    assert _column(executor, f"(s::{target})::string") == [
        _instant_text(dialect, s, aware) for s in strings
    ]


@pytest.mark.parametrize("target", ["datetime", "timestamp"])
def test_instant_text_reads_back(dialect: Dialects, target: str) -> None:
    strings = _strings(random.Random(12))
    executor = _text_executor(dialect, strings)
    kept = _column(
        executor, "s", f"where ((s::{target})::string)::{target} = s::{target}"
    )
    assert kept == [s for s in strings if text_to_datetime(s) is not None]


def test_timestamp_text_ignores_session_zone() -> None:
    strings = _strings(random.Random(13))
    executor = _text_executor(Dialects.DUCK_DB, strings)
    executor.execute_raw_sql("SET TimeZone = 'Europe/Paris'")
    assert _column(executor, "(s::timestamp)::string") == [
        _instant_text(Dialects.DUCK_DB, s, True) for s in strings
    ]
