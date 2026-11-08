"""Casts and folded constants (formal/Trilogy/Casts.lean, 0.3.472) on DuckDB,
SQLite and ClickHouse through chdb where it installs, against a Python
reference.

- a cast to `numeric(p, s)` rounds half away to `s` digits (`rescale_exact`,
  `clickhouse_rescale_truncates`, `sqlite_keeps_digits`);
- a string reads as a bool by `trilogy.core.casts.text_to_bool`, a number is
  true iff not zero, and a bool reads back as 'true' / 'false'
  (`text_bool_exact`, `sqlite_true_is_false`, `ne_zero_exact`,
  `sqlite_int_bool_keeps_value`, `bool_text_is_digit`);
- `sqrt` and `log` are NULL outside their domain, and `is not null` over
  them is not implied by their argument (`domain_guard_exact`,
  `nan_is_not_null`, `domain_null_introducing`);
- a literal folded by the parser is the value the engine computes for the
  same values in columns: a NULL comparison is UNKNOWN (`fold_agrees`,
  `null_eq_null_folded_true`), a CASE with every arm false and no ELSE is
  NULL (`no_arm_is_null`), and a string literal cast is the column cast.
"""

import math
import random
from decimal import ROUND_HALF_UP, Decimal

import pytest

from tests.windows.test_window_laws import CHDB, new_executor, seeds_for
from trilogy import Dialects, Executor
from trilogy.core.casts import FALSE_TEXT, TRUE_TEXT, text_to_bool
from trilogy.parsing.v2.model import HydrationError

DIALECTS = [Dialects.DUCK_DB, Dialects.SQLITE, *CHDB]
SEEDS = range(3)
SPELLINGS = [*TRUE_TEXT, *FALSE_TEXT, "TRUE", "False", "Yes", "N", "T"]

MODEL = """
key id int;
property id.n numeric(12,4);
property id.f float;
property id.i int;
property id.s string;
property id.b bool;
datasource casts (id: id, n: ?n, f: ?f, i: ?i, s: ?s, b: ?b) grain (id) address casts;
"""

DDL = {
    Dialects.DUCK_DB: (
        "create table casts (id int, n decimal(12,4), f double, i int,"
        " s varchar, b boolean)"
    ),
    Dialects.SQLITE: (
        "create table casts (id int, n decimal(12,4), f double, i int,"
        " s varchar, b boolean)"
    ),
    Dialects.CLICKHOUSE: (
        "create table casts (id Int64, n Nullable(Decimal(12,4)),"
        " f Nullable(Float64), i Nullable(Int64), s Nullable(String),"
        " b Nullable(Bool)) engine = Memory"
    ),
}

Row = tuple[int, Decimal | None, float | None, int | None, str | None, bool | None]


def rescale(value: Decimal, scale: int) -> Decimal:
    """`Casts.rescale`: half away from zero (Python's ROUND_HALF_UP)."""
    return value.quantize(Decimal(1).scaleb(-scale), rounding=ROUND_HALF_UP)


def _maybe(rng: random.Random, value):
    return None if rng.random() < 0.15 else value


def _rows(rng: random.Random, binary_exact: bool) -> list[Row]:
    """`binary_exact` keeps every decimal a multiple of 1/8, which SQLite (a
    decimal is a REAL there) and a float column store exactly."""
    rows: list[Row] = []
    for k in range(40):
        units = rng.randint(-40000, 40000)
        if binary_exact:
            units = units // 1250 * 1250
        n = Decimal(units).scaleb(-4)
        rows.append(
            (
                k,
                _maybe(rng, n),
                _maybe(rng, float(Decimal(rng.randint(-200, 200)) / 8)),
                _maybe(rng, rng.randint(-5, 6)),
                _maybe(rng, rng.choice(SPELLINGS)),
                _maybe(rng, rng.random() < 0.5),
            )
        )
    return rows


def _executor(dialect: Dialects, rows: list[Row]) -> Executor:
    executor = new_executor(dialect)
    executor.execute_raw_sql("drop table if exists casts")
    executor.execute_raw_sql(DDL[dialect])
    literal = executor.generator.render_string_literal

    def value(x) -> str:
        if x is None:
            return "NULL"
        if isinstance(x, bool):
            return "true" if x else "false"
        return literal(x) if isinstance(x, str) else str(x)

    values = ", ".join("(" + ", ".join(value(x) for x in row) + ")" for row in rows)
    executor.execute_raw_sql(f"insert into casts values {values}")
    executor.parse_text(MODEL)
    return executor


def _column(executor: Executor, expr: str, where: str = "") -> list:
    got = executor.execute_text(f"select id, {expr} as x {where} order by id asc;")
    return [r[1] for r in got[-1].fetchall()]


def _bool(x) -> bool | None:
    return None if x is None else bool(x)


def _decimal(x) -> Decimal | None:
    return None if x is None else Decimal(str(x))


@pytest.fixture(params=DIALECTS, ids=lambda d: d.value)
def dialect(request) -> Dialects:
    return request.param


@pytest.mark.parametrize("seed", SEEDS)
def test_numeric_cast_rounds_half_away(dialect: Dialects, seed: int) -> None:
    if seed not in seeds_for(dialect, SEEDS):
        pytest.skip("ClickHouse runs the first seed")
    exact = dialect == Dialects.SQLITE
    rows = _rows(random.Random(seed), binary_exact=exact)
    executor = _executor(dialect, rows)
    for scale in (0, 1, 2, 3):
        got = _column(executor, f"n::numeric(12,{scale})")
        assert [_decimal(x) for x in got] == [
            None if n is None else rescale(n, scale) for _, n, *_ in rows
        ], scale
    got = _column(executor, "f::numeric(12,2)")
    assert [_decimal(x) for x in got] == [
        None if f is None else rescale(Decimal(f), 2) for _, _, f, *_ in rows
    ]


def test_bool_casts(dialect: Dialects) -> None:
    rows = _rows(random.Random(0), binary_exact=True)
    executor = _executor(dialect, rows)
    assert [_bool(x) for x in _column(executor, "s::bool")] == [
        None if s is None else text_to_bool(s) for *_, s, _ in rows
    ]
    assert [_bool(x) for x in _column(executor, "i::bool")] == [
        None if i is None else i != 0 for _, _, _, i, _, _ in rows
    ]
    assert _column(executor, "b::string") == [
        None if b is None else str(b).lower() for *_, b in rows
    ]
    assert [_bool(x) for x in _column(executor, "(i::bool) = true")] == [
        None if i is None else i != 0 for _, _, _, i, _, _ in rows
    ]


def _reference(fn, x) -> float | None:
    try:
        return None if x is None else fn(x)
    except (ValueError, ZeroDivisionError):
        return None


def _close(got: list, want: list) -> bool:
    return len(got) == len(want) and all(
        (g is None and w is None)
        or (g is not None and w is not None and math.isclose(g, w, abs_tol=1e-9))
        for g, w in zip(got, want)
    )


def test_out_of_domain_is_null(dialect: Dialects) -> None:
    rows = _rows(random.Random(1), binary_exact=True)
    executor = _executor(dialect, rows)
    ints = [i for _, _, _, i, _, _ in rows]
    cases = [
        ("sqrt(i)", math.sqrt),
        ("log(i, 10)", math.log10),
        ("log(i, 2)", math.log2),
        ("log(10, i)", lambda x: math.log(10) / math.log(x)),
    ]
    for expr, fn in cases:
        assert _close(_column(executor, expr), [_reference(fn, i) for i in ints]), expr
    kept = _column(executor, "i", "where sqrt(i) is not null and i is not null")
    assert kept == [i for i in ints if i is not None and i >= 0]
    assert [_bool(x) for x in _column(executor, "sqrt(i) is null")] == [
        i is None or i < 0 for i in ints
    ]


FOLDED_NULL = ["null = null", "1 = null", "null != 1", "1 < null", "null >= 2"]


def test_folded_comparison_is_kleene(dialect: Dialects) -> None:
    rows = _rows(random.Random(2), binary_exact=True)
    executor = _executor(dialect, rows)
    for expr in FOLDED_NULL:
        assert set(_column(executor, expr)) == {None}, expr
        assert _column(executor, "id", f"where {expr}") == [], expr
    assert [_bool(x) for x in _column(executor, "null is null")] == [True] * len(rows)
    assert [_bool(x) for x in _column(executor, "1 = 1.0")] == [True] * len(rows)


def test_case_with_no_arm_left_is_null(dialect: Dialects) -> None:
    rows = _rows(random.Random(3), binary_exact=True)
    executor = _executor(dialect, rows)
    assert set(_column(executor, "case when 1 = 2 then 'a' end")) == {None}
    assert _column(executor, "case when 1 = 2 then 'a' when i > 0 then 'b' end") == [
        "b" if i is not None and i > 0 else None for _, _, _, i, _, _ in rows
    ]


@pytest.mark.parametrize("spelling", SPELLINGS)
def test_bool_literal_fold_is_the_cast(dialect: Dialects, spelling: str) -> None:
    executor = new_executor(dialect)
    got = executor.execute_text(f"select '{spelling}'::bool as x;")[-1].fetchall()
    assert _bool(got[0][0]) is text_to_bool(spelling)


def test_unreadable_bool_literal_raises() -> None:
    executor = new_executor(Dialects.DUCK_DB)
    with pytest.raises(HydrationError, match="Cannot cast 'banana' to bool"):
        executor.parse_text("select 'banana'::bool as x;")


@pytest.mark.parametrize("text", ["2.675", "-2.675", "1.005", "3.14159", "0.004"])
def test_numeric_literal_fold_is_the_cast(dialect: Dialects, text: str) -> None:
    executor = new_executor(dialect)
    got = executor.execute_text(f"select '{text}'::numeric(12,2) as x;")[-1]
    assert _decimal(got.fetchall()[0][0]) == rescale(Decimal(text), 2)


def test_persisted_domain_null_is_declared(dialect: Dialects) -> None:
    rows = _rows(random.Random(4), binary_exact=True)
    executor = _executor(dialect, rows)
    executor.execute_raw_sql("drop table if exists copied")
    executor.parse_text("auto r <- sqrt(i); auto l <- log(i, 2);")
    executor.execute_text("persist copied into copied from select id, r, l;")
    ints = [i for _, _, _, i, _, _ in rows]
    for name in ("r", "l"):
        assert executor.environment.concepts[name].address in {
            c.address
            for c in executor.environment.datasources["copied"].nullable_concepts
        }, name
    kept = _column(executor, "r", "where r is not null")
    assert len(kept) == sum(1 for i in ints if i is not None and i >= 0)
    assert None not in kept
