from trilogy import Dialects, Environment

MODEL = """
key species string;
key ecoregion_id int;
property ecoregion_id.biome string;
property species.native_ecoregions list<int>;

datasource eco (ecoregion_id, biome) grain (ecoregion_id)
query '''select 1 as ecoregion_id, 'forest' as biome union all select 2, 'forest'
union all select 3, 'desert' union all select 4, 'tundra' ''';

key tree_id int;
property tree_id.tree_species string;
merge tree_species into species;

datasource trees (tree_id, tree_species) grain (tree_id)
query '''select 1 as tree_id, 'Acer rubrum' as tree_species
union all select 2, 'Opuntia ficus' union all select 3, 'Malus'
union all select 4, 'Unknown' ''';

datasource enrich (~species, ?native_ecoregions) grain (species)
query '''select 'Acer rubrum' as species, [1,2] as native_ecoregions
union all select 'Opuntia ficus', [3]
union all select 'Malus', []::int[]
union all select 'Unknown', null::int[]''';

auto native_ecoregion <- unnest(native_ecoregions);
merge native_ecoregion into ecoregion_id;
auto native_biomes <- array_sort(array_distinct(array_agg(biome) by species));
"""


def test_owner_with_no_unnested_rows_keeps_its_row():
    executor = Dialects.DUCK_DB.default_executor(environment=Environment())
    executor.parse_text(MODEL)
    rows = executor.execute_text(
        "select species, native_ecoregions, native_biomes order by species asc;"
    )[-1].fetchall()
    assert [(r[0], r[2]) for r in rows] == [
        ("Acer rubrum", ["forest"]),
        ("Malus", None),
        ("Opuntia ficus", ["desert"]),
        ("Unknown", None),
    ]
