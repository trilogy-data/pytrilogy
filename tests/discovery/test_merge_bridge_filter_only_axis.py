"""Lock: the merged-unnest bridge survives when the derived concept is only FILTERED on.

`test_merge_derived_bridge` covers the shape where the far-side concept is
SELECTed (or aggregated). The row-level filter path lost the axis separately:
a WHERE atom whose lineage roots (`ecoregion_id`, `biome`) constrain a
non-grouping output (`latitude`) split those roots into the private root_d1
scan, which then had no join key back to the tree rows -- the FINAL merge
degraded to `INNER JOIN ... ON 1=1` (silent fan-out pre-0.3.321, the keyless-
join planner-bug exception after).

Upstream sf_tree_reporting: the dot map is `SELECT latitude, longitude` with a
nativeness filter, so every tree in the city came back as native.

The split is withheld only when it would strand the condition scan. The
opposite shape -- a condition whose roots ARE the SELECT's own scan -- must
keep its split; `tests/engine/demo/test_demo_duckdb_import.py::
test_demo_merge_rowset_e2e` fans out 8 rows to 88 if that is broken too.
"""

import pytest

from trilogy import Dialects
from trilogy.core.models.environment import Environment

_MODEL = """
key tree_id string;
key city string;
key species string;
property tree_id.latitude float;
property tree_id.longitude float;

key ecoregion_id int;
property ecoregion_id.biome string;

property species.native_ecoregions list<int>;
property species.tree_form string;

auto tree_eco_region <- unnest(native_ecoregions);
merge tree_eco_region into ecoregion_id;

datasource trees (
    tree_id: tree_id,
    city: city,
    species: species,
    latitude: latitude,
    longitude: longitude,
)
grain (tree_id)
query '''
select 't1' tree_id, 'CITY_A' city, 'A' species, 37.7 latitude, -122.4 longitude
union all select 't2', 'CITY_A', 'B', 37.8, -122.5
union all select 't3', 'CITY_A', 'A', 37.9, -122.6
union all select 't4', 'CITY_B', 'A', 40.7, -74.0
''';

datasource enrichment (
    species: species,
    native_ecoregions: native_ecoregions,
    tree_form: tree_form,
)
grain (species)
query '''
select 'A' species, [423, 424] native_ecoregions, 'broadleaf' tree_form
union all select 'B', [661], 'conifer'
''';

datasource ecoregions (
    ecoregion_id: ecoregion_id,
    biome: biome,
)
grain (ecoregion_id)
query '''
select 423 ecoregion_id, 'Mediterranean Forests, Woodlands & Scrub' biome
union all select 424, 'Temperate Conifer Forests'
union all select 661, 'Temperate Broadleaf & Mixed Forests'
''';

constant active_ecoregion <- 423;
constant active_biome <- 'Mediterranean Forests, Woodlands & Scrub';

property native_locality_bucket <- CASE
    WHEN ecoregion_id = active_ecoregion THEN 'Native'
    WHEN biome = active_biome THEN 'Same biome, non-native'
    ELSE 'Non-Native'
end::string;
"""

# Species A is native to 423; only its trees in CITY_A qualify.
_NATIVE_POINTS = [(37.7, -122.4), (37.9, -122.6)]

_ROW_CASES = [
    pytest.param(
        """SELECT latitude, longitude
        WHERE city = 'CITY_A' and native_locality_bucket = 'Native';""",
        id="bucket_filter_alone",
    ),
    pytest.param(
        """SELECT latitude, longitude
        WHERE city = 'CITY_A' and species = 'A'
        and native_locality_bucket = 'Native';""",
        id="bucket_and_species_filter",
    ),
    pytest.param(
        """SELECT latitude, longitude
        WHERE city = 'CITY_A' and tree_form = 'broadleaf'
        and native_locality_bucket = 'Native';""",
        id="bucket_and_tree_form_filter",
    ),
]


def _engine():
    engine = Dialects.DUCK_DB.default_executor(environment=Environment())
    engine.parse_text(_MODEL)
    return engine


@pytest.mark.parametrize("query", _ROW_CASES)
def test_filter_only_bridge_rows(query: str):
    rows = _engine().execute_text(query)[-1].fetchall()
    assert sorted((float(r[0]), float(r[1])) for r in rows) == _NATIVE_POINTS


@pytest.mark.parametrize("query", _ROW_CASES)
def test_filter_only_bridge_joins_keyed(query: str):
    sql = _engine().generate_sql(query)[-1]
    assert "1=1" not in sql, sql
    assert "enrichment" in sql, sql


def test_filter_only_bridge_selected_concept_rows():
    rows = _engine().execute_text("""SELECT native_locality_bucket, latitude, longitude
            WHERE city = 'CITY_A' and species = 'A'
            and native_locality_bucket = 'Native';""")[-1].fetchall()
    assert sorted((r[0], float(r[1]), float(r[2])) for r in rows) == [
        ("Native", 37.7, -122.4),
        ("Native", 37.9, -122.6),
    ]


def test_filter_only_bridge_aggregate_rows():
    rows = _engine().execute_text("""SELECT city, count(tree_id) as tree_count
            WHERE native_locality_bucket = 'Native';""")[-1].fetchall()
    assert sorted((r[0], int(r[1])) for r in rows) == [("CITY_A", 2), ("CITY_B", 1)]
