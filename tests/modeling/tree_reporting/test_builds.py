from pathlib import Path

import pytest

from tests.modeling._row_compare import values_match
from tests.modeling.tree_reporting import harness
from tests.modeling.tree_reporting.generate_fixtures import (
    EXPECTED_DBH,
    EXPECTED_SURVIVORS,
)

SUMMARIES = [*harness.COUNT_SUMMARIES, "tree_rollup_city"]


def build(entrypoint: str, target: str, output: Path, stem: str) -> list[dict]:
    executor = harness.build_executor(entrypoint, output, [stem])
    executor.update_datasource(executor.environment.datasources[target])
    return harness.execute_records(
        f"SELECT * FROM read_parquet('{(output / f'{stem}_v2.parquet').as_posix()}')"
    )


def fixture_records(stem: str) -> list[dict]:
    return harness.execute_records(
        f"SELECT * FROM read_parquet('{(harness.FIXTURES / f'{stem}_v2.parquet').as_posix()}')"
    )


def ordered(rows: list[dict]) -> list[list]:
    return sorted(([row[k] for k in sorted(row)] for row in rows), key=repr)


@pytest.mark.parametrize("city", harness.CITY_BUILDS)
def test_city_build_plans(city: str, tmp_path: Path) -> None:
    entrypoint, target = harness.CITY_BUILDS[city]
    executor = harness.build_executor(
        entrypoint, tmp_path, [f"{city.lower()}_tree_info"]
    )
    assert executor.update_datasource(
        executor.environment.datasources[target], dry_run=True
    )


@pytest.mark.parametrize("city", harness.CITY_BUILDS)
def test_city_build_publishes_cluster_survivors(city: str, tmp_path: Path) -> None:
    entrypoint, target = harness.CITY_BUILDS[city]
    rows = build(entrypoint, target, tmp_path, f"{city.lower()}_tree_info")
    ids = [row["tree_id"] for row in rows]
    assert len(ids) == len(set(ids))
    assert set(ids) == EXPECTED_SURVIVORS[city]
    assert all(
        row["tree_id"] == row["cluster_id"] and row["reported_missing"] is False
        for row in rows
    )
    dbh = {row["tree_id"]: row["diameter_at_breast_height"] for row in rows}
    assert all(dbh[k] == v for k, v in EXPECTED_DBH.items() if k in dbh)
    if city == "USBOS":
        assert {
            row["tree_id"]: row["height_m"]
            for row in rows
            if row["height_m"] is not None
        } == {"bos-m1": 14.0}


@pytest.mark.parametrize("name", SUMMARIES)
def test_summary_build_matches_fixture(name: str, tmp_path: Path) -> None:
    built = ordered(build("tree_rollups", name, tmp_path, name))
    expected = ordered(fixture_records(name))
    assert len(built) == len(expected)
    for left, right in zip(built, expected):
        assert all(values_match(a, b) for a, b in zip(left, right)), (left, right)


def test_native_range_build_matches_fixture(tmp_path: Path) -> None:
    name = "species_native_range"
    assert ordered(build("tree_rollups", name, tmp_path, name)) == ordered(
        fixture_records(name)
    )
