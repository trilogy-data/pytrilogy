"""Smoke tests against a real ClickHouse server.

Reads `.env.secrets` (then `.env`) from the repo root if present and only sets
vars that aren't already in the environment.

Skipped unless TRILOGY_CLICKHOUSE_HOST is set. Required env vars:
  TRILOGY_CLICKHOUSE_HOST
  TRILOGY_CLICKHOUSE_PASSWORD

Everything else is defaulted to ClickHouse Cloud-style settings below.
"""

import os
from pathlib import Path
from typing import Any, Generator

import pytest

from trilogy import Dialects, Executor, parse
from trilogy.constants import Rendering
from trilogy.core.models.environment import Environment
from trilogy.dialect.config import ClickhouseConfig

from ._clickhouse_cases import (
    AGG_CASES,
    SMOKE_CASES,
    run_agg_case,
    run_smoke_case,
)

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
    parse("const answer <- 42;", environment=clickhouse_server_executor.environment)
    rows = clickhouse_server_executor.execute_text("select answer;")[0].fetchall()
    assert rows[0].answer == 42


@pytest.mark.xfail(
    reason="date_spine has no native CH equivalent; needs range + addDays glue"
)
def test_date_spine_xfail(clickhouse_server_executor: Executor):
    parse(
        "auto spine <- date_spine(cast('2024-01-01' as date), cast('2024-01-03' as date)); "
        "select spine;",
        environment=clickhouse_server_executor.environment,
    )
    rows = clickhouse_server_executor.execute_text("select spine;")[0].fetchall()
    assert len(rows) == 3


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
    run_smoke_case(clickhouse_server_executor, case_id, expr, expected)


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
    run_agg_case(clickhouse_server_executor, case_id, body, select_name, expected)
