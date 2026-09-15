"""A lookup keyed on a concept derived from row properties (`cell <- f(lat, lon)`)
must join onto a partitioned union of `complete where` sources the same way it
joins onto a single source: compute the cell on the union rows, LEFT JOIN the
lookup on it."""

import math

import pytest

from trilogy import Dialects, Environment

CELL_A = math.floor(-73.2120 / 0.00003) * 100000000 + math.floor(44.4760 / 0.00002)

MODEL = f"""
key city enum<string>['USBTV'];
key source enum<string>['MUNICIPAL', 'OSM'];
key tree_id string;
property tree_id.latitude float;
property tree_id.longitude float;

auto cell <- cast(floor(longitude / 0.00003) as bigint) * 100000000
           + cast(floor(latitude / 0.00002) as bigint);

property cell.label string;
root datasource lookup (cell: cell, label: label)
grain (cell)
query '''SELECT * FROM (VALUES ({CELL_A}::BIGINT, 'road')) AS t(cell, label)''';
"""

ROW_A = "('a', 'USBTV', 'MUNICIPAL', 44.4760::DOUBLE, -73.2120::DOUBLE)"
ROW_B = "('b', 'USBTV', 'OSM', 44.4770::DOUBLE, -73.2130::DOUBLE)"
COLS = "tree_id, city, source, latitude, longitude"

ONE_SOURCE = f"""
root datasource trees (
    tree_id: tree_id, city: city, source: source, latitude: ?latitude, longitude: ?longitude
)
grain (tree_id)
query '''SELECT * FROM (VALUES {ROW_A}, {ROW_B}) AS t({COLS})''';
"""

PARTITIONED_UNION = f"""
root partial datasource municipal (
    tree_id: tree_id, city: city, source: source, latitude: ?latitude, longitude: ?longitude
)
grain (tree_id)
complete where city = 'USBTV' and source = 'MUNICIPAL'
query '''SELECT * FROM (VALUES {ROW_A}) AS t({COLS})''';

root partial datasource osm (
    tree_id: tree_id, city: city, source: source, latitude: ?latitude, longitude: ?longitude
)
grain (tree_id)
complete where city = 'USBTV' and source = 'OSM'
query '''SELECT * FROM (VALUES {ROW_B}) AS t({COLS})''';
"""


def _executor(sources: str):
    env = Environment()
    env.parse(MODEL + sources)
    return Dialects.DUCK_DB.default_executor(environment=env)


@pytest.mark.parametrize(
    "sources", [ONE_SOURCE, PARTITIONED_UNION], ids=["single", "union"]
)
def test_derived_cell_computed_on_rows(sources: str):
    executor = _executor(sources)
    rows = executor.execute_text(
        "select tree_id, cell where city = 'USBTV' order by tree_id asc;"
    )[-1].fetchall()
    assert rows[0] == ("a", CELL_A)
    assert rows[1][0] == "b"


@pytest.mark.parametrize(
    "sources", [ONE_SOURCE, PARTITIONED_UNION], ids=["single", "union"]
)
def test_lookup_joins_on_derived_cell(sources: str):
    executor = _executor(sources)
    query = "select tree_id, label where city = 'USBTV' order by tree_id asc;"
    sql = executor.generate_sql(query)[-1]
    assert "LEFT OUTER JOIN" in sql, sql
    assert "ON 1=1" not in sql, sql
    rows = executor.execute_text(query)[-1].fetchall()
    assert rows == [("a", "road"), ("b", None)]


@pytest.mark.parametrize(
    "sources", [ONE_SOURCE, PARTITIONED_UNION], ids=["single", "union"]
)
def test_lookup_joins_on_derived_cell_with_cell_selected(sources: str):
    executor = _executor(sources)
    query = "select tree_id, cell, label where city = 'USBTV' order by tree_id asc;"
    rows = executor.execute_text(query)[-1].fetchall()
    assert rows == [("a", CELL_A, "road"), ("b", rows[1][1], None)]
