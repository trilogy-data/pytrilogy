"""Lock: a value is never grain-pinned to a merged key computed from it.

`species` is merged from `max(raw_species) by cluster_id`, so it is a function
of `cluster_id`. A partial target persisting both, beside a keyless update
column only ever held apart from the rows, pinned the NULL-absorbing
`cluster_id` coalesce to the target's other keys, `species` included: the
pinned lineage read the very aggregate grouped by it, and planning failed with
"a circular dependency between group nodes". `_reads` saw no lineage on the
merge-demoted `species` and missed the dependency.
"""

from pathlib import Path

from trilogy import Dialects
from trilogy.core.models.environment import Environment

MODEL = """
key tree_id string;
key city enum<string>['X', 'Y'];
key xx_source enum<string>['A', 'B'];
key species string;
property tree_id.raw_lat float;
property tree_id.raw_species string;
property tree_id.source_label string;
property tree_id.latitude float;
auto xx_source_label <- concat(xx_source, '');
merge xx_source_label into source_label;
auto source_class <- case when source_label = 'A' then 'municipal' else 'osm' end;

auto cell <- cast(floor(raw_lat / 0.001) as bigint);
auto row_key <- concat(tree_id, '#');
auto anchor <- min(tree_id ? source_class = 'municipal') by cell;
auto self_if_municipal <- min(tree_id ? source_class = 'municipal') by row_key;
auto cluster_id <- coalesce(self_if_municipal, anchor, tree_id);
auto merged_species <- max(raw_species) by cluster_id;
auto merged_latitude <- max(raw_lat ? source_class = 'municipal') by cluster_id;
merge merged_species into species;
merge merged_latitude into latitude;

property <*>.a_updated datetime;
property <*>.b_updated datetime;
auto updated_through <- greatest(a_updated, b_updated);
datasource a_time (updated: a_updated)
query '''SELECT TIMESTAMP '2026-01-01' AS updated''';
datasource b_time (updated: b_updated)
query '''SELECT TIMESTAMP '2026-01-02' AS updated''';

root partial datasource part_a (
    tree_id: tree_id, city: city, data_source: xx_source, lat: ?raw_lat, sp: ?raw_species
)
grain (tree_id)
complete where city = 'X' and xx_source = 'A'
query '''SELECT 'a1' AS tree_id, 'X' AS city, 'A' AS data_source, 1.0 AS lat, 'oak' AS sp''';

root partial datasource part_b (
    tree_id: tree_id, city: city, data_source: xx_source, lat: ?raw_lat, sp: ?raw_species
)
grain (tree_id)
complete where city = 'X' and xx_source = 'B'
query '''
SELECT 'b1' AS tree_id, 'X' AS city, 'B' AS data_source, 1.0001 AS lat, 'Oak tree' AS sp
UNION ALL
SELECT 'b2', 'X', 'B', 5.0, 'elm'
''';

partial datasource target (
    tree_id, city, data_source: xx_source, species, ?latitude, cluster_id, updated_through
)
grain (tree_id)
complete where city = 'X'
file `./target.parquet`
where tree_id = cluster_id;
"""


def test_pruned_cluster_target_refreshes(tmp_path: Path):
    env = Environment(working_path=tmp_path)
    env.parse(MODEL)
    engine = Dialects.DUCK_DB.default_executor(environment=env)
    engine.update_datasource(env.datasources["target"])
    rows = engine.execute_raw_sql(
        f"SELECT tree_id, data_source, species, latitude, cluster_id "
        f"FROM read_parquet('{(tmp_path / 'target.parquet').as_posix()}') "
        "ORDER BY tree_id"
    ).fetchall()
    assert rows == [("a1", "A", "oak", 1.0, "a1"), ("b2", "B", "elm", None, "b2")]
