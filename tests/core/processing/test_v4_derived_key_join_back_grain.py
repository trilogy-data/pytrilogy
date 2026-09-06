"""An aggregate keyed on a concept derived from the row (`cluster_id <-
coalesce(min(...) by cell, tree_id)`) joined back onto the rows that produced
it is 1:1 by construction, so the FINAL merge must not regroup."""

from decimal import Decimal

from trilogy import Dialects, Environment
from trilogy.core.processing.v4_helper.group_graph import _lineage_pinned_grain

_MODEL = """
key tree_id string;
property tree_id.city string;
property tree_id.raw_latitude float;
property tree_id.raw_longitude float;
property tree_id.source_label string;
property city.cell_lat_deg float;
property city.cell_lon_deg float;

datasource dedup_cells (
    city: city,
    cell_lat_deg: cell_lat_deg,
    cell_lon_deg: cell_lon_deg,
)
grain (city)
query '''
SELECT * FROM (VALUES
    ('USBOS', 0.00008983, 0.00012155),
    ('USSFO', 0.00008983, 0.00011361)
) AS t(city, cell_lat_deg, cell_lon_deg)
''';

datasource trees (
    tree_id: tree_id,
    city: city,
    source_label: source_label,
    raw_latitude: raw_latitude,
    raw_longitude: raw_longitude,
)
grain (tree_id)
query '''
SELECT * FROM (VALUES
    ('t1', 'USBOS', 'MUNI', 42.3601, -71.0589),
    ('t2', 'USBOS', 'OSM',  42.3601, -71.0589),
    ('t3', 'USBOS', 'MUNI', 42.3700, -71.0600)
) AS t(tree_id, city, source_label, raw_latitude, raw_longitude)
''';

auto cell_a <- concat(
    cast(floor(raw_longitude / cell_lon_deg) as string), ':',
    cast(floor(raw_latitude / cell_lat_deg) as string)
);
auto anchor_a <- min(tree_id ? source_label = 'MUNI') by cell_a;
auto cluster_id <- coalesce(anchor_a, tree_id);
auto merged_lat <- max(raw_latitude) by cluster_id;
"""


def _executor():
    env = Environment()
    env.parse(_MODEL)
    return Dialects.DUCK_DB.default_executor(environment=env)


def test_derived_key_basic_pins_its_row_grain():
    env = Environment()
    env.parse(_MODEL)
    build = env.materialize_for_select()
    pinned = _lineage_pinned_grain({"local.cluster_id"}, build)
    assert "local.tree_id" in pinned
    assert "local.cell_a" in pinned


def test_join_back_on_derived_key_has_no_final_group():
    executor = _executor()
    query = "select tree_id, cluster_id, merged_lat order by tree_id asc;"
    sql = executor.generate_sql(query)[-1]
    final_select = sql[sql.rfind("\nSELECT\n") :]
    assert "GROUP BY" not in final_select, sql
    assert sql.count("GROUP BY") == 2, sql
    rows = executor.execute_text(query)[-1].fetchall()
    assert rows == [
        ("t1", "t1", Decimal("42.3601")),
        ("t2", "t1", Decimal("42.3601")),
        ("t3", "t3", Decimal("42.3700")),
    ]


def test_join_back_without_row_key_still_dedupes():
    executor = _executor()
    query = "select cluster_id, merged_lat, raw_latitude order by cluster_id asc;"
    rows = executor.execute_text(query)[-1].fetchall()
    assert rows == [
        ("t1", Decimal("42.3601"), Decimal("42.3601")),
        ("t3", Decimal("42.3700"), Decimal("42.3700")),
    ]


_PARTITION_MODEL = """
key tree_id string;
key city string;
property tree_id.raw_latitude float;
property tree_id.raw_longitude float;
key source_label enum<string>['MUNI','OSM'];
property city.cell_lat_deg float;
property city.cell_lon_deg float;

datasource dedup_cells (
    city: city,
    cell_lat_deg: cell_lat_deg,
    cell_lon_deg: cell_lon_deg,
)
grain (city)
query '''
SELECT * FROM (VALUES
    ('USBOS', 0.00008983, 0.00012155),
    ('USSFO', 0.00008983, 0.00011361)
) AS t(city, cell_lat_deg, cell_lon_deg)
''';

root partial datasource trees_muni (
    tree_id: tree_id,
    city: city,
    source_label: source_label,
    raw_latitude: raw_latitude,
    raw_longitude: raw_longitude,
)
grain (tree_id)
complete where source_label = 'MUNI'
query '''
SELECT * FROM (VALUES
    ('t1', 'USBOS', 'MUNI', 42.3601, -71.0589),
    ('t3', 'USBOS', 'MUNI', 42.3700, -71.0600)
) AS t(tree_id, city, source_label, raw_latitude, raw_longitude)
''';

root partial datasource trees_osm (
    tree_id: tree_id,
    city: city,
    source_label: source_label,
    raw_latitude: raw_latitude,
    raw_longitude: raw_longitude,
)
grain (tree_id)
complete where source_label = 'OSM'
query '''
SELECT * FROM (VALUES
    ('t2', 'USBOS', 'OSM', 42.3601, -71.0589),
    ('t4', 'USSFO', 'OSM', 37.7700, -122.4100)
) AS t(tree_id, city, source_label, raw_latitude, raw_longitude)
''';

auto cell_a <- concat(
    cast(floor(raw_longitude / cell_lon_deg) as string), ':',
    cast(floor(raw_latitude / cell_lat_deg) as string)
);
"""


def test_key_filter_determined_by_grain_has_no_group():
    env = Environment()
    env.parse(_PARTITION_MODEL)
    executor = Dialects.DUCK_DB.default_executor(environment=env)
    query = "select tree_id, cell_a where city = 'USBOS' order by tree_id asc;"
    sql = executor.generate_sql(query)[-1]
    assert "GROUP BY" not in sql, sql
    rows = executor.execute_text(query)[-1].fetchall()
    assert [row[0] for row in rows] == ["t1", "t2", "t3"]


_REDUNDANT_GRAIN_MODEL = """
key tree_id string;
key city string;
key species string;
property tree_id.raw_latitude float;
property tree_id.raw_longitude float;
property tree_id.source_label string;
property tree_id.raw_species string;
property city.cell_lat_deg float;
property city.cell_lon_deg float;

datasource dedup_cells (
    city: city,
    cell_lat_deg: cell_lat_deg,
    cell_lon_deg: cell_lon_deg,
)
grain (city)
address dedup_cells;

datasource trees (
    tree_id: tree_id,
    city: city,
    source_label: source_label,
    raw_latitude: raw_latitude,
    raw_longitude: raw_longitude,
    raw_species: raw_species,
)
grain (tree_id)
address trees;

auto cell_a <- concat(
    cast(floor(raw_longitude / cell_lon_deg) as string), ':',
    cast(floor(raw_latitude / cell_lat_deg) as string)
);
auto anchor_a <- min(tree_id ? source_label = 'MUNI') by cell_a;
auto cluster_id <- coalesce(anchor_a, tree_id);
auto merged_species <- max(raw_species) by cluster_id;
merge merged_species into species;

datasource merged_trees (
    tree_id: tree_id,
    cluster_id: cluster_id,
    species: species,
)
grain (tree_id, cluster_id)
address merged_trees;
"""


def test_group_check_uses_fd_closure_over_upstream_grain():
    from trilogy.core.processing.discovery_utility import check_if_group_required

    env = Environment()
    env.parse(_REDUNDANT_GRAIN_MODEL)
    build = env.materialize_for_select()
    parent = build.datasources["merged_trees"]
    assert set(parent.grain.components) == {"local.tree_id", "local.cluster_id"}
    # Raw keys reach past the grain (the cells), so only FD closure sees that
    # tree_id determines cluster_id.
    assert not build.concepts["local.cluster_id"].keys <= set(parent.grain.components)
    downstream = [build.concepts["local.tree_id"], build.concepts["local.species"]]
    result = check_if_group_required(downstream, [parent], build)
    assert result.required is False
