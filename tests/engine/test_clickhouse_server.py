"""Smoke tests against a real ClickHouse server.

Reads `.env.secrets` (then `.env`) from the repo root if present and only sets
vars that aren't already in the environment.

Skipped unless TRILOGY_CLICKHOUSE_HOST is set. Required env vars:
  TRILOGY_CLICKHOUSE_HOST
  TRILOGY_CLICKHOUSE_PASSWORD

Everything else is defaulted to ClickHouse Cloud-style settings below.
"""

import os
from datetime import date, datetime
from pathlib import Path
from typing import Any, Generator

import pytest

from trilogy import Dialects, Executor, parse
from trilogy.constants import Rendering
from trilogy.core.models.environment import Environment
from trilogy.dialect.config import ClickhouseConfig

pytestmark = pytest.mark.clickhouse_server

REPO_ROOT = Path(__file__).resolve().parents[2]


def _load_dotenv(path: Path) -> None:
    if not path.is_file():
        return
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        if not value:
            continue
        os.environ.setdefault(key, value)


for candidate in (".env.secrets", ".env"):
    _load_dotenv(REPO_ROOT / candidate)


def _env(name: str) -> str | None:
    """Return env var, treating empty string as unset."""
    value = os.environ.get(name)
    return value if value else None


def _config_from_env() -> ClickhouseConfig | None:
    host = _env("TRILOGY_CLICKHOUSE_HOST")
    if not host:
        return None
    port_str = _env("TRILOGY_CLICKHOUSE_PORT")
    return ClickhouseConfig(
        mode="server",
        host=host,
        port=int(port_str) if port_str else None,
        username=_env("TRILOGY_CLICKHOUSE_USER"),
        password=_env("TRILOGY_CLICKHOUSE_PASSWORD") or "",
        database=_env("TRILOGY_CLICKHOUSE_DATABASE"),
        secure=True,
    )


@pytest.fixture(scope="module")
def clickhouse_server_executor() -> Generator[Executor, None, None]:
    conf = _config_from_env()
    if conf is None:
        pytest.skip("TRILOGY_CLICKHOUSE_HOST not set")
    executor = Dialects.CLICKHOUSE.default_executor(
        environment=Environment(),
        conf=conf,
        rendering=Rendering(parameters=False),
    )
    try:
        yield executor
    finally:
        executor.close()


def test_raw_select_one(clickhouse_server_executor: Executor):
    rows = clickhouse_server_executor.execute_raw_sql("select 1 as answer").fetchall()
    assert len(rows) == 1
    assert rows[0][0] == 1


def test_today(clickhouse_server_executor: Executor):
    rows = clickhouse_server_executor.execute_raw_sql("select today() as d").fetchall()
    assert rows[0][0] is not None


def test_trilogy_const(clickhouse_server_executor: Executor):
    env, _ = parse(
        "const answer <- 42;", environment=clickhouse_server_executor.environment
    )
    rows = clickhouse_server_executor.execute_text("select answer;")[0].fetchall()
    assert rows[0].answer == 42


# Parametrized constant-input smoke tests for each FUNCTION_MAP override.
# Each case: (id, trilogy_expression, expected_value).
# Names must be unique across the run since the executor's environment is shared.
SMOKE_CASES: list[tuple[str, str, Any]] = [
    # math
    ("add", "1 + 2", 3),
    ("subtract", "5 - 3", 2),
    ("multiply", "3 * 4", 12),
    ("divide", "10.0 / 4", 2.5),
    ("mod", "mod(10, 3)", 1),
    ("power", "2 ** 10", 1024),
    ("abs", "abs(-5)", 5),
    ("floor", "floor(3.7)", 3),
    ("ceil", "ceil(3.2)", 4),
    ("round", "round(3.567, 2)", 3.57),
    ("sqrt", "sqrt(16.0)", 4.0),
    ("log10", "log(100.0, 10)", 2.0),
    ("log2", "log(8.0, 2)", 3.0),
    # string
    ("upper", "upper('hello')", "HELLO"),
    ("lower", "lower('HELLO')", "hello"),
    ("len", "len('hello')", 5),
    ("substring", "substring('hello', 1, 3)", "hel"),
    ("strpos", "strpos('hello world', 'world')", 7),
    ("contains", "contains('hello world', 'world')", True),
    ("trim", "trim('  hello  ')", "hello"),
    ("ltrim", "ltrim('  hello')", "hello"),
    ("rtrim", "rtrim('hello  ')", "hello"),
    ("replace", "replace('hello', 'l', 'L')", "heLLo"),
    ("concat", "concat('hello', ' ', 'world')", "hello world"),
    # CH HTTP driver returns Array(String) as a literal Python-list string
    ("split", "split('a,b,c', ',')", "['a','b','c']"),
    ("hex", "hex('AB')", "4142"),
    ("hash_md5", "hash('abc', md5)", "900150983cd24fb0d6963f7d28e17f72"),
    ("hash_sha256", "len(hash('abc', sha256))", 64),
    # regex
    ("regexp_contains", "regexp_contains('hello123', '[0-9]+')", True),
    ("regexp_extract", "regexp_extract('hello123', '[0-9]+')", "123"),
    ("regexp_replace", "regexp_replace('hello123', '[0-9]+', 'X')", "helloX"),
    # like
    ("like", "like('hello', 'h%')", True),
    ("ilike", "ilike('Hello', 'h%')", True),
    # date extract
    ("year", "year(cast('2024-03-15' as date))", 2024),
    ("month", "month(cast('2024-03-15' as date))", 3),
    ("day", "day(cast('2024-03-15' as date))", 15),
    ("quarter", "quarter(cast('2024-08-15' as date))", 3),
    ("week", "week(cast('2024-03-15' as date))", 11),
    ("day_of_week", "day_of_week(cast('2024-03-15' as date))", 5),  # Friday=5
    ("day_name", "day_name(cast('2024-03-15' as date))", "Friday"),
    ("month_name", "month_name(cast('2024-03-15' as date))", "March"),
    ("hour", "hour(cast('2024-03-15 10:20:30' as datetime))", 10),
    ("minute", "minute(cast('2024-03-15 10:20:30' as datetime))", 20),
    ("second", "second(cast('2024-03-15 10:20:30' as datetime))", 30),
    # date manipulation
    (
        "date_trunc",
        "date_trunc(cast('2024-03-15' as date), month)",
        date(2024, 3, 1),
    ),
    (
        "date_add",
        "date_add(cast('2024-03-15' as date), day, 5)",
        date(2024, 3, 20),
    ),
    (
        "date_sub",
        "date_sub(cast('2024-03-15' as date), day, 5)",
        date(2024, 3, 10),
    ),
    (
        "date_diff",
        "date_diff(cast('2024-03-01' as date), cast('2024-03-15' as date), day)",
        14,
    ),
    ("date_cast", "date('2024-03-15')", date(2024, 3, 15)),
    (
        "datetime_cast",
        "datetime('2024-03-15 10:20:30')",
        datetime(2024, 3, 15, 10, 20, 30),
    ),
    (
        "format_time",
        "format_time(cast('2024-03-15' as date), '%Y/%m/%d')",
        "2024/03/15",
    ),
    # constants
    ("current_date_not_null", "current_date()", None),  # special: just non-null check
    ("current_datetime_not_null", "current_datetime()", None),
]


# Aggregate / array cases. Each defines an unnest source then aggregates over it.
# Format: (id, body, select_concept, expected). body must define the aggregate
# concept under `select_concept`.
AGG_CASES: list[tuple[str, str, str, Any]] = [
    (
        "sum",
        "auto __VALS__ <- unnest([1,2,3,4,5]); auto __AGG__ <- sum(__VALS__);",
        "agg",
        15,
    ),
    (
        "avg",
        "auto __VALS__ <- unnest([2.0,4.0,6.0]); auto __AGG__ <- avg(__VALS__);",
        "agg",
        4.0,
    ),
    (
        "max",
        "auto __VALS__ <- unnest([3,1,4,1,5,9,2,6]); auto __AGG__ <- max(__VALS__);",
        "agg",
        9,
    ),
    (
        "min",
        "auto __VALS__ <- unnest([3,1,4,1,5,9,2,6]); auto __AGG__ <- min(__VALS__);",
        "agg",
        1,
    ),
    (
        "count",
        "auto __VALS__ <- unnest([1,2,3,4,5]); auto __AGG__ <- count(__VALS__);",
        "agg",
        5,
    ),
    (
        "count_distinct",
        "auto __VALS__ <- unnest([1,1,2,2,3]); auto __AGG__ <- count_distinct(__VALS__);",
        "agg",
        3,
    ),
    (
        "any",
        "auto __VALS__ <- unnest([7,7,7]); auto __AGG__ <- any(__VALS__);",
        "agg",
        7,
    ),
    (
        "bool_or_true",
        "auto __VALS__ <- unnest([false, true, false]); auto __AGG__ <- bool_or(__VALS__);",
        "agg",
        True,
    ),
    (
        "bool_or_false",
        "auto __VALS__ <- unnest([false, false, false]); auto __AGG__ <- bool_or(__VALS__);",
        "agg",
        False,
    ),
    (
        "bool_and_true",
        "auto __VALS__ <- unnest([true, true, true]); auto __AGG__ <- bool_and(__VALS__);",
        "agg",
        True,
    ),
    (
        "bool_and_false",
        "auto __VALS__ <- unnest([true, false, true]); auto __AGG__ <- bool_and(__VALS__);",
        "agg",
        False,
    ),
    (
        "array_agg",
        "auto __VALS__ <- unnest([1,2,3]); auto __AGG__ <- array_agg(__VALS__);",
        "agg",
        "[1,2,3]",
    ),
    (
        "array_sort",
        "auto __VALS__ <- unnest([3,1,2]); auto __AGG__ <- array_sort([3,1,2]);",
        "agg",
        "[1,2,3]",
    ),
    (
        "array_distinct",
        "auto __VALS__ <- unnest([1,2,2,3]); auto __AGG__ <- array_distinct([1,2,2,3]);",
        "agg",
        "[1,2,3]",
    ),
    (
        "array_sum",
        "auto __VALS__ <- unnest([1,2,3]); auto __AGG__ <- array_sum([1,2,3]);",
        "agg",
        6,
    ),
    (
        "array_to_string",
        "auto __VALS__ <- unnest(['a','b','c']); auto __AGG__ <- array_to_string(['a','b','c'], ',');",
        "agg",
        "a,b,c",
    ),
]


@pytest.mark.parametrize(
    "case_id, body, select_name, expected",
    AGG_CASES,
    ids=[c[0] for c in AGG_CASES],
)
def test_aggregate_smoke(
    clickhouse_server_executor: Executor,
    case_id: str,
    body: str,
    select_name: str,
    expected: Any,
):
    # Replace the __VALS__ / __AGG__ sentinels with case-unique names so
    # parametrized cases don't collide in the shared environment.
    suffix = f"_{case_id}"
    vals_name = f"vals{suffix}"
    agg_name = f"agg{suffix}"
    text = body.replace("__VALS__", vals_name).replace("__AGG__", agg_name)
    full_name = agg_name if select_name == "agg" else f"{select_name}{suffix}"
    parse(text, environment=clickhouse_server_executor.environment)
    rows = clickhouse_server_executor.execute_text(f"select {full_name};")[0].fetchall()
    actual = rows[0][0]
    assert actual == expected, f"{case_id}: got {actual!r}, want {expected!r}"


@pytest.mark.parametrize(
    "case_id, expr, expected",
    SMOKE_CASES,
    ids=[c[0] for c in SMOKE_CASES],
)
def test_function_smoke(
    clickhouse_server_executor: Executor,
    case_id: str,
    expr: str,
    expected: Any,
):
    name = f"smoke_{case_id}"
    text = f"const {name} <- {expr}; select {name};"
    parse(text, environment=clickhouse_server_executor.environment)
    rows = clickhouse_server_executor.execute_text(f"select {name};")[0].fetchall()
    actual = rows[0][0]
    if expected is None:
        # "not null" check for time-dependent constants
        assert actual is not None, f"{case_id}: got None"
        return
    assert actual == expected, f"{case_id}: got {actual!r}, want {expected!r}"
