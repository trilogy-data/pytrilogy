from pathlib import Path

import pytest

from trilogy import Dialects, Environment, Executor
from trilogy.dialect.config import DuckDBConfig
from trilogy.dialect.table_function import MissingSourceArgumentException

ROOT = Path(__file__).parent


@pytest.fixture(scope="module")
def executor():
    pytest.importorskip("haybarn")
    ex = Dialects.DUCK_DB.default_executor(
        environment=Environment(working_path=ROOT),
        conf=DuckDBConfig(python_datasource_transport="vgi"),
    )
    try:
        ex.execute_raw_sql((ROOT / "setup.sql").read_text(encoding="utf-8"))
    except Exception as e:
        ex.close()
        pytest.skip(f"public VGI servers unreachable: {e}")
    yield ex
    ex.close()


def run(executor: Executor, name: str) -> list[dict]:
    executor.environment = Environment(working_path=ROOT)
    results = executor.execute_text((ROOT / name).read_text(encoding="utf-8"))
    return [dict(row._mapping) for row in results[-1].fetchall()]


@pytest.mark.parametrize(
    "name",
    ["query_quakes.preql", "query_trains.preql", "query_weather.preql"],
)
def test_server_query(executor: Executor, name: str):
    assert run(executor, name)


def test_places_null_filter(executor: Executor):
    rows = run(executor, "query_places.preql")
    assert rows
    assert all(row["places_category"] is not None for row in rows)


def test_cross_server(executor: Executor):
    [row] = run(executor, "query_cross_server.preql")
    assert row["quakes"] > 0
    assert row["stations"] > 0
    assert row["amsterdam_week_high"] is not None
    assert row["amsterdam_places"] > 0


def test_weather_requires_coordinates(executor: Executor):
    with pytest.raises(
        MissingSourceArgumentException, match="where weather.longitude = <literal>"
    ):
        run(executor, "query_weather_missing.preql")
