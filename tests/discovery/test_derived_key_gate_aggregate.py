"""Lock: a target gate on an aggregate keyed by a row-level derivation plans.

`where count(tree_id) by genus > 1`, with `genus` a `case` over the partial
`raw_species`, joins the gate's feeder back onto the rows on `genus`, a column
no scan binds. A ROOT host widened its scan by the key and planned to nothing
("Could not resolve connections"); with a grouped output beside it the FINAL
merge never widened its row stream by `genus` and joined the feeder keyless;
beside a keyless update column the select pins `genus`, so only the row
stream computing it can carry it. The `by raw_species` control always planned.
"""

from pathlib import Path

import pytest

from trilogy import Dialects
from trilogy.core.models.environment import Environment

SOURCES = """
key tree_id string;
key city enum<string>['X', 'Y'];
key xx_source enum<string>['A', 'B'];
property tree_id.raw_species string;
property tree_id.raw_lat float;
auto genus <- case
    when strpos(raw_species, ' ') > 0
        then substring(raw_species, 1, strpos(raw_species, ' ') - 1)
    else raw_species
end;
auto x_genus <- count(tree_id) by genus;
auto x_species <- count(tree_id) by raw_species;
auto cell <- cast(floor(raw_lat / 0.001) as bigint);
auto anchor <- min(tree_id) by cell;

property <*>.a_updated datetime;
property <*>.b_updated datetime;
auto updated_through <- greatest(a_updated, b_updated);
datasource a_time (updated: a_updated)
query '''SELECT TIMESTAMP '2026-01-01' AS updated''';
datasource b_time (updated: b_updated)
query '''SELECT TIMESTAMP '2026-01-02' AS updated''';

root partial datasource part_a (
    tree_id: tree_id, city: city, data_source: xx_source, sp: ?raw_species, lat: ?raw_lat
)
grain (tree_id)
complete where city = 'X' and xx_source = 'A'
query '''SELECT 'a1' AS tree_id, 'X' AS city, 'A' AS data_source, 'Acer rubrum' AS sp, 1.0 AS lat''';

root partial datasource part_b (
    tree_id: tree_id, city: city, data_source: xx_source, sp: ?raw_species, lat: ?raw_lat
)
grain (tree_id)
complete where city = 'X' and xx_source = 'B'
query '''
SELECT 'b1' AS tree_id, 'X' AS city, 'B' AS data_source, 'Acer saccharum' AS sp, 1.0001 AS lat
UNION ALL
SELECT 'b2', 'X', 'B', 'Quercus', 5.0
''';
"""

ACER = [("a1", "Acer rubrum"), ("b1", "Acer saccharum")]
ALL = ACER + [("b2", "Quercus")]


def _refresh(tmp_path: Path, columns: str, gate: str) -> list[tuple]:
    env = Environment(working_path=tmp_path)
    env.parse(SOURCES + f"""
partial datasource target ({columns})
grain (tree_id)
complete where city = 'X'
file `./target.parquet`
where {gate};
""")
    engine = Dialects.DUCK_DB.default_executor(environment=env)
    engine.update_datasource(env.datasources["target"])
    path = (tmp_path / "target.parquet").as_posix()
    return engine.execute_raw_sql(
        f"SELECT tree_id, raw_species FROM read_parquet('{path}') ORDER BY tree_id"
    ).fetchall()


@pytest.mark.parametrize(
    "columns",
    [
        "tree_id, city, data_source: xx_source, raw_species",
        "tree_id, city, data_source: xx_source, raw_species, anchor",
        "tree_id, city, data_source: xx_source, raw_species, anchor, updated_through",
    ],
)
@pytest.mark.parametrize(
    "gate, expected", [("x_genus > 1", ACER), ("x_species > 0", ALL)]
)
def test_gate_on_aggregate_by_derived_key(tmp_path: Path, columns, gate, expected):
    assert _refresh(tmp_path, columns, gate) == expected
