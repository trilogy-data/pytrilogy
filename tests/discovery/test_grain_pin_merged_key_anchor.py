"""Lock: a merge-demoted key anchors a grain pin on its origin's rows.

`merge merged_species into species` leaves `species` a lineage-less key whose
value is `max(raw_species) by cluster_id`. The pin read it as an entity of its
own, so a target persisting `species` beside the pruned `cluster_id` pinned
the gate's WHERE-scope `cluster_id` coalesce to `species`, a value computed
from that very gate. The planner then rebuilt the cluster lineage over the
already-pruned rows and joined the two copies on the merged value, silently
dropping every anchor row whose copies disagreed.
"""

import re

import duckdb
import pytest

from trilogy import Dialects
from trilogy.core.models.environment import Environment

ANCHOR = {
    "inline": "auto anchor <- min(tree_id ? src = 'MUNI') by cell;",
    "def": (
        "def municipal_anchor(c) -> min(tree_id ? src = 'MUNI') by c;\n"
        "auto anchor <- @municipal_anchor(cell);"
    ),
}

MODEL = """
key tree_id string;
key species string;
key city string;
key src enum<string>['MUNI', 'SAT'];
property tree_id.raw_species string;
property tree_id.raw_pos int;
property city.cell_size int;
property <*>.updated_at datetime;
auto cell <- raw_pos / cell_size;
{anchor}
auto cluster_id <- coalesce(anchor, tree_id);
auto merged_species <- substring((max(raw_species) by cluster_id), 1, 200);
merge merged_species into species;
auto published_through <- greatest(updated_at, updated_at);

root datasource update_time (t: updated_at)
query '''SELECT TIMESTAMP '2026-01-01' AS t''';
root datasource cells (city: city, cell_size: cell_size) grain (city)
query '''SELECT 'X' AS city, 1 AS cell_size''';
root partial datasource muni (
    tree_id: tree_id, city: city, src: src, pos: raw_pos, species: raw_species
)
grain (tree_id)
complete where src = 'MUNI'
query '''SELECT * FROM (VALUES ('m-1', 'X', 'MUNI', 1, 'Unknown'), ('m-2', 'X', 'MUNI', 3, 'Quercus'))
AS t(tree_id, city, src, pos, species)''';
root partial datasource sat (
    tree_id: tree_id, city: city, src: src, pos: raw_pos, species: raw_species
)
grain (tree_id)
complete where src = 'SAT'
query '''SELECT * FROM (VALUES ('s-1', 'X', 'SAT', 1, 'Zelkova'), ('s-2', 'X', 'SAT', 2, 'Pinus'))
AS t(tree_id, city, src, pos, species)''';

partial datasource published ({columns})
grain (tree_id)
complete where city = 'X'
address published_trees
where tree_id = cluster_id
freshness by published_through;
"""


COLUMNS = {
    "with_city": "tree_id, city, species, cluster_id, published_through",
    "without_city": "tree_id, species, cluster_id, published_through",
}


def _build_rows(anchor: str, columns: str) -> list[tuple]:
    env = Environment()
    env.parse(MODEL.format(anchor=ANCHOR[anchor], columns=COLUMNS[columns]))
    executor = Dialects.DUCK_DB.default_executor(environment=env)
    sql = executor.update_datasource(env.datasources["published"], dry_run=True)
    assert sql is not None
    # a `<*>` constant is beside every row: nothing pins to it
    assert "__preql_internal_all_rows" not in sql
    query = re.sub(r'^.*?INSERT INTO "[^"]*"\s*', "", sql, count=1, flags=re.DOTALL)
    cursor = duckdb.connect().execute(query)
    columns = [d[0] for d in cursor.description]
    return sorted(
        (row["tree_id"], row["species"])
        for row in (dict(zip(columns, r)) for r in cursor.fetchall())
    )


@pytest.mark.parametrize("columns", sorted(COLUMNS))
@pytest.mark.parametrize("anchor", sorted(ANCHOR))
def test_pruned_target_keeps_every_cluster_anchor(anchor: str, columns: str):
    # `m-1` absorbs `s-1`: the datasource `where` only chooses which rows are
    # written, so the persisted species is the whole cluster's
    assert _build_rows(anchor, columns) == [
        ("m-1", "Zelkova"),
        ("m-2", "Quercus"),
        ("s-2", "Pinus"),
    ]
