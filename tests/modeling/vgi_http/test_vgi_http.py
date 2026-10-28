"""Trilogy models over VGI catalogs served on local HTTP: a rust worker serving a
table function and a python one serving tables, attached like remote servers."""

import shutil
import subprocess
import sys
from collections.abc import Iterator
from datetime import date
from pathlib import Path

import pytest

from tests.vgi_support import require_vgi, skip_or_fail
from trilogy import Dialects, Environment, Executor
from trilogy.dialect.config import DuckDBConfig
from trilogy.dialect.table_function import MissingSourceArgumentException

ROOT = Path(__file__).parent
WEATHER_CRATE = ROOT / "weather_worker"
WEATHER_BINARY = (
    WEATHER_CRATE
    / "target"
    / "release"
    / f"trilogy-test-weather-worker{'.exe' if sys.platform == 'win32' else ''}"
)
QUERIES = sorted(
    p.name
    for p in ROOT.glob("query_*.preql")
    if p.name != "query_weather_missing.preql"
)


def build_weather_worker() -> None:
    if shutil.which("cargo") is None:
        skip_or_fail("cargo not available")
    build = subprocess.run(
        ["cargo", "build", "--release", "--quiet"],
        cwd=WEATHER_CRATE,
        capture_output=True,
        check=False,
    )
    if build.returncode != 0:
        skip_or_fail(f"weather worker build failed: {build.stderr.decode()[-1000:]}")


def serve(argv: list[str]) -> tuple[subprocess.Popen, int]:
    """Start a worker on a free port; both SDKs print `PORT:<n>` once bound."""
    worker = subprocess.Popen(
        [*argv, "--http"],
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL,
        text=True,
    )
    assert worker.stdout is not None
    line = worker.stdout.readline()
    if not line.startswith("PORT:"):
        worker.kill()
        raise RuntimeError(f"{argv[0]} did not report a port: {line!r}")
    return worker, int(line.removeprefix("PORT:"))


@pytest.fixture(scope="module")
def executor() -> Iterator[Executor]:
    require_vgi()
    try:
        import falcon  # noqa: F401
    except ImportError:
        skip_or_fail("vgi-python[http] is needed to serve the stations worker")
    build_weather_worker()
    workers = {
        "stations": serve([sys.executable, str(ROOT / "stations_worker.py")]),
        "weather": serve([str(WEATHER_BINARY)]),
    }
    ex = Dialects.DUCK_DB.default_executor(
        environment=Environment(working_path=ROOT),
        conf=DuckDBConfig(python_datasource_transport="vgi"),
    )
    try:
        for name, (_, port) in workers.items():
            ex.execute_raw_sql(f"ATTACH 'http://127.0.0.1:{port}' AS {name} (TYPE vgi)")
        yield ex
    finally:
        ex.close()
        for worker, _ in workers.values():
            worker.kill()
            worker.wait()


def run(executor: Executor, name: str) -> list[dict]:
    executor.environment = Environment(working_path=ROOT)
    results = executor.execute_text((ROOT / name).read_text(encoding="utf-8"))
    return [dict(row._mapping) for row in results[-1].fetchall()]


def plan(name: str) -> None:
    Dialects.DUCK_DB.default_executor(
        environment=Environment(working_path=ROOT)
    ).generate_sql((ROOT / name).read_text(encoding="utf-8"))


@pytest.mark.parametrize("name", QUERIES)
def test_query_plans(name: str):
    plan(name)


def test_weather_requires_coordinates():
    with pytest.raises(
        MissingSourceArgumentException, match="where weather.longitude = <literal>"
    ):
        plan("query_weather_missing.preql")


def test_table_address(executor: Executor):
    assert run(executor, "query_departures.preql") == [
        {
            "stations_station_name": "Tester Alpha",
            "departures": 3,
            "total_delay": 15,
        },
        {
            "stations_station_name": "Tester Gamma",
            "departures": 2,
            "total_delay": 8,
        },
        {"stations_station_name": "Tester Beta", "departures": 1, "total_delay": 2},
    ]


def test_query_datasource(executor: Executor):
    assert run(executor, "query_regions.preql") == [
        {"regions_regional_region": "north", "station_count": 2}
    ]


def test_table_function_arguments(executor: Executor):
    assert run(executor, "query_weather.preql") == [
        {
            "weather_forecast_date": date(2030, 1, day),
            "weather_temp_max": 10.0 + offset,
            "weather_temp_min": 20.0 - offset,
        }
        for offset, day in enumerate([1, 2, 3])
    ]


def test_cross_server(executor: Executor):
    assert run(executor, "query_cross_server.preql") == [
        {"station_count": 3, "departures": 6, "tester_a_high": 12.0}
    ]
