"""Lock (#786): a persist's own target is not part of its statement's domain.

The build's WHERE carries the target's `complete where`, so `statement_graph`
promoted the (unpublished, unreadable) target to a whole-domain holder. The pin
then read `reported_missing` as stored beside `tree_id` and dropped the lookup
join feeding `modified_missing`: `Missing source reference`.
"""

from trilogy import Dialects
from trilogy.core.domain_graph import DomainGraph, assemble_full_graph
from trilogy.core.enums import DatasourceState
from trilogy.core.grain_pin import statement_graph
from trilogy.core.models.environment import Environment

MODEL = """
key tree_id string;
key city string;
key src enum<string>['MUNI', 'SAT'];
property tree_id.raw_pos int;
property city.cell_size int;
auto cell <- raw_pos / cell_size;
auto anchor <- min(tree_id ? src = 'MUNI') by cell;
auto cluster_id <- coalesce(anchor, tree_id);

root datasource cells (city: city, cell_size: cell_size) grain (city)
query '''SELECT 'X' AS city, 1 AS cell_size''';
root partial datasource muni (tree_id: tree_id, city: city, src: src, pos: raw_pos)
grain (tree_id)
complete where src = 'MUNI'
query '''SELECT * FROM (VALUES ('m-1', 'X', 'MUNI', 1), ('m-2', 'X', 'MUNI', 3))
AS t(tree_id, city, src, pos)''';
root partial datasource sat (tree_id: tree_id, city: city, src: src, pos: raw_pos)
grain (tree_id)
complete where src = 'SAT'
query '''SELECT * FROM (VALUES ('s-1', 'X', 'SAT', 1), ('s-2', 'X', 'SAT', 2))
AS t(tree_id, city, src, pos)''';

key modified_tree_id string;
property modified_tree_id.modified_missing bool;
root datasource mods (tree_id: modified_tree_id, missing: modified_missing)
grain (modified_tree_id)
query '''SELECT * FROM (VALUES ('m-2', true), ('elsewhere', true))
AS t(tree_id, missing)''';
merge modified_tree_id into ~tree_id;
auto reported_missing <- coalesce(modified_missing, false);

partial datasource published (tree_id, city, cluster_id, reported_missing)
grain (tree_id)
complete where city = 'X'
address published_trees
where tree_id = cluster_id and reported_missing = false;
"""


def _env() -> Environment:
    env = Environment()
    env.parse(MODEL)
    return env


def test_partial_target_build_keeps_merged_lookup():
    env = _env()
    executor = Dialects.DUCK_DB.default_executor(environment=env)
    executor.update_datasource(env.datasources["published"])
    rows = executor.execute_raw_sql(
        "select tree_id, cluster_id, reported_missing from published_trees"
        " order by tree_id"
    ).fetchall()
    assert rows == [("m-1", "m-1", False), ("s-2", "s-2", False)]


def test_statement_graph_skips_unpublished_datasource():
    env = _env()
    where = env.datasources["published"].non_partial_for
    full = assemble_full_graph(env, DomainGraph())

    def promoted() -> bool:
        graph = statement_graph(full, where, env)
        return any(
            b.datasource == "published" and b.complete for b in graph.binding_edges
        )

    assert promoted()
    env.datasources["published"].status = DatasourceState.UNPUBLISHED
    assert not promoted()
