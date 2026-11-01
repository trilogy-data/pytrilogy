import pytest

from trilogy import Dialects, Environment

# #835: a list aggregate over a merged unnest, grouped by the list's owner,
# must not FULL JOIN in the dimension members no owner references.
MODEL = """
key species string;
key ecoregion_id int;
property ecoregion_id.biome string;
property ecoregion_id.realm string;
root datasource eco (ecoregion_id, ?biome, ?realm) grain (ecoregion_id)
query '''select 1 as ecoregion_id, 'forest' as biome, 'n' as realm
union all select 2, 'forest', 'n' union all select 3, 'desert', 'p'
union all select 4, 'tundra', 'p' ''';
property species.native_ecoregions list<int>;
datasource enrich (species: SPECIES, native_ecoregions: ?native_ecoregions) grain (species)
query '''select 'Acer rubrum' as species, [1,2] as native_ecoregions
union all select 'Acer saccharum', [1] union all select 'Opuntia ficus', [3]''';
auto native_ecoregion <- unnest(native_ecoregions);
merge native_ecoregion into ecoregion_id;
auto native_biomes <- array_sort(array_distinct(array_agg(biome) by species));
auto native_realms <- array_sort(array_distinct(array_agg(realm) by species));
"""

TARGET = """
datasource snr (species, ?native_ecoregions, ?native_biomes, ?native_realms)
grain (species)
address snr_t;
"""

TREES = """
key tree_id int;
datasource trees (tree_id, species) grain (tree_id)
query '''select 1 tree_id, 'Acer rubrum' species union all select 2, 'Opuntia ficus'
union all select 3, 'Acer saccharum' ''';
"""

EXPECTED = [
    ("Acer rubrum", [1, 2], ["forest"], ["n"]),
    ("Acer saccharum", [1], ["forest"], ["n"]),
    ("Opuntia ficus", [3], ["desert"], ["p"]),
]


def executor(spelling: str, extra: str = ""):
    env = Environment()
    env.parse(MODEL.replace("SPECIES", spelling) + extra)
    return env, Dialects.DUCK_DB.default_executor(environment=env)


QUERY = "SELECT species, array_sort(array_distinct(array_agg(biome))) as b ORDER BY species;"


def test_list_aggregate_over_merged_unnest_has_no_null_owner():
    _, ex = executor("species")
    rows = ex.execute_text(QUERY)[-1].fetchall()
    assert rows == [(s, b) for s, _, b, _ in EXPECTED]


@pytest.mark.xfail(
    strict=True,
    reason="since <=0.3.434: a second species-grain table turns the lookup into FULL JOIN on 1=1",
)
def test_list_aggregate_beside_second_species_source():
    _, ex = executor("species", TARGET)
    rows = ex.execute_text(QUERY)[-1].fetchall()
    assert rows == [(s, b) for s, _, b, _ in EXPECTED]


def test_native_range_build_over_partial_enrichment():
    env, ex = executor("~species", TARGET + TREES)
    ex.execute_raw_sql(ex.update_datasource(env.datasources["snr"], dry_run=True))
    assert sorted(ex.execute_raw_sql("select * from snr_t").fetchall()) == EXPECTED
