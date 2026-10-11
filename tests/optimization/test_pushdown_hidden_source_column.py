"""Lock: predicate pushdown never filters on a column its source hides.

`hide_unused_concepts` hid `city` in a CTE nothing read it from, and
pushdown still counted the consumer's `source_map` entry for it as
materialized: `city = 'USSFO'` landed on a WHERE over a column the source no
longer rendered ("does not have a column named city"). The phase that drops
the pushed copy then re-armed the push, so the plan never settled and the pass
cap froze it in the broken state.
"""

import logging

import duckdb

from trilogy import Dialects
from trilogy.core.models.environment import Environment

MODEL = """
key tree_id string;
key species string;
key city enum<string>['USSFO'];
property tree_id.raw_species string;
property tree_id.raw_latitude float;
property tree_id.source_label string;
auto source_class <- source_label;
property city.cell_lat_deg float;
root datasource dedup_cells (
    city: city,
    cell_lat_deg: cell_lat_deg,
)
grain (city)
query '''SELECT 'USSFO' AS city, 0.00008983 AS cell_lat_deg''';
auto cell_a <- raw_latitude / cell_lat_deg;
def any_anchor(cell) -> min(tree_id ? source_class != 'osm') by cell;
auto anchor_a <- @any_anchor(cell_a);
auto row_key <- concat(tree_id, '#');
auto self_if_not_osm <- @any_anchor(row_key);
auto cluster_id <- coalesce(
    self_if_not_osm,
    anchor_a,
    tree_id
);
auto species_key <- raw_species;
auto merged_species <- max(species_key) by cluster_id;
merge merged_species into species;
key ussfo_source enum<string>['SF_OPENDATA', 'COMMUNITY_USSFO', 'OSM_USSFO', 'SATELLITE_USSFO'];
auto ussfo_source_label <- concat(ussfo_source, '');
merge ussfo_source_label into source_label;
root partial datasource ussfo_osm_tree_info (
    tree_id: tree_id,
    city: city,
    data_source: ussfo_source,
    species: ?raw_species,
    latitude: ?raw_latitude,
)
grain (tree_id)
complete where city = 'USSFO' and ussfo_source = 'OSM_USSFO'
query '''SELECT * FROM (VALUES
    ('osm-2', 'USSFO', 'OSM_USSFO', NULL, 37.790012)
) AS t(tree_id, city, data_source, species, latitude)''';
root partial datasource sf_raw_tree_info (
    tree_id: tree_id,
    city: city,
    data_source: ussfo_source,
    species: raw_species,
    latitude: ?raw_latitude,
)
grain (tree_id)
complete where city = 'USSFO' and ussfo_source = 'SF_OPENDATA'
query '''SELECT * FROM (VALUES
    ('sf-2', 'USSFO', 'SF_OPENDATA', 'Unknown', 37.78)
) AS t(tree_id, city, data_source, species, latitude)''';
root partial datasource sf_community_tree_info (
    tree_id: tree_id,
    city: city,
    data_source: ussfo_source,
    species: raw_species,
    latitude: ?raw_latitude,
)
grain (tree_id)
complete where city = 'USSFO' and ussfo_source = 'COMMUNITY_USSFO'
query '''SELECT * FROM (VALUES
    ('community-y', 'USSFO', 'COMMUNITY_USSFO', 'Unknown', 37.79)
) AS t(tree_id, city, data_source, species, latitude)'''
where city = 'USSFO';
root partial datasource ussfo_satellite_tree_info (
    tree_id: tree_id,
    city: city,
    data_source: ussfo_source,
    species: raw_species,
    latitude: ?raw_latitude,
)
grain (tree_id)
complete where city = 'USSFO' and ussfo_source = 'SATELLITE_USSFO'
query '''SELECT * FROM (VALUES
    ('sat-e', 'USSFO', 'SATELLITE_USSFO', 'Pinus radiata', 37.790012)
) AS t(tree_id, city, data_source, species, latitude)'''
where city = 'USSFO';
"""


def test_pushdown_skips_column_hidden_by_its_source(caplog):
    env = Environment()
    env.parse(MODEL)
    executor = Dialects.DUCK_DB.default_executor(environment=env)
    with caplog.at_level(logging.WARNING):
        sql = executor.generate_sql(
            "select tree_id, species, cluster_id "
            "where city = 'USSFO' and tree_id = cluster_id;"
        )[-1]
    assert "plan still changing" not in caplog.text
    rows = sorted(duckdb.connect().execute(sql).fetchall())
    assert rows == [
        ("community-y", "Unknown", "community-y"),
        ("osm-2", None, "osm-2"),
        ("sat-e", "Pinus radiata", "sat-e"),
        ("sf-2", "Unknown", "sf-2"),
    ]
