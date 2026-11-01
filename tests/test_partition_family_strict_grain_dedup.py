from trilogy import Dialects, Environment
from trilogy.core.models.build import BuildUnionDatasource

# The shape of tree_reporting's cross-source dedup (#836): partitions split by
# a source key, so the union's grain is (tree_id, src), clustered through an
# aggregate by a derived row key and pruned to cluster survivors.
MODEL = """
key city enum<string>['A', 'B'];
key tree_id string;
key species string;
property tree_id.raw_species string;
property tree_id.raw_rank int;
property tree_id.source_label string;
key src enum<string>['MUNI_A', 'OSM_A'];
auto src_label <- concat(src, '');
merge src_label into source_label;
auto row_key <- concat(tree_id, '#');
auto self_if_municipal <- min(tree_id ? source_label = 'MUNI_A') by row_key;
auto cluster_id <- coalesce(self_if_municipal, tree_id);
key modified_tree_id string;
property modified_tree_id.modified_species string;
root datasource tree_modifications (tree_id: modified_tree_id, species: modified_species)
grain (modified_tree_id)
query '''SELECT 'x' tree_id, 'S' species''';
merge modified_tree_id into ~tree_id;
auto cluster_top_rank <- max(raw_rank) by cluster_id;
auto merged_species <- coalesce(
    modified_species,
    (max(raw_species) by cluster_id),
    case when cluster_top_rank = 0 then (max(raw_species) by cluster_id) else null end
);
merge merged_species into species;
root partial datasource muni_rows (tree_id: tree_id, city: city, species: raw_species, rank: raw_rank, data_source: src)
grain (tree_id)
complete where city = 'A' and src = 'MUNI_A'
query '''SELECT 'm' tree_id, 'A' city, 'S' species, 3 rank, 'MUNI_A' data_source''';
root partial datasource osm_rows (tree_id: tree_id, city: city, species: raw_species, rank: raw_rank, data_source: src)
grain (tree_id)
complete where city = 'A' and src = 'OSM_A'
query '''SELECT 'a' tree_id, 'A' city, 'S' species, 1 rank, 'OSM_A' data_source''';
partial datasource out (tree_id, city, species, cluster_id, data_source: src)
grain (tree_id)
complete where city = 'A'
address out_t
where tree_id = cluster_id;
"""


def build(model: str) -> list[tuple]:
    env = Environment()
    env.parse(model)
    executor = Dialects.DUCK_DB.default_executor(environment=env)
    executor.execute_raw_sql(
        executor.update_datasource(env.datasources["out"], dry_run=True)
    )
    return sorted(executor.execute_raw_sql("select * from out_t").fetchall())


def test_strict_partition_grain_dedup_builds():
    assert build(MODEL) == [
        ("a", "A", "S", "a", "OSM_A"),
        ("m", "A", "S", "m", "MUNI_A"),
    ]


def test_source_as_tree_property_builds_the_same_rows():
    model = MODEL.replace(
        "key src enum<string>['MUNI_A', 'OSM_A'];",
        "property tree_id.src enum<string>['MUNI_A', 'OSM_A'];",
    )
    assert build(model) == build(MODEL)


def test_discriminator_pinned_alike_in_every_arm_leaves_union_grain():
    env = Environment()
    env.parse(MODEL)
    build_env = env.materialize_for_select()
    union = BuildUnionDatasource(
        children=[build_env.datasources["muni_rows"], build_env.datasources["osm_rows"]]
    )
    assert union.bound_discriminators == {"local.src"}
    assert set(union.grain.components) == {"local.tree_id", "local.src"}
