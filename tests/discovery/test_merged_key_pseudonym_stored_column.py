"""Lock: a key a datasource stores is read from it, never re-derived through a
derivation merged into it (`merge merged_species into species`).

`published` stores `tree_id`, `city` and `species`; `raw_a` holds the raw
species the merge origin derives from. Any query over those three concepts and
a species-keyed dimension must plan from `published` alone, whatever the origin
is, whether `raw_a` is a partial partition, whichever member of the merged
class sorts first, and however the dimension is reached.
"""

import pytest

from trilogy import Dialects

ORIGINS = {
    "alias": "{raw}",
    "scalar": "trim({raw})",
    "aggregate_in_scalar": "substring((max({raw}) by cluster_id), 1, 200)",
    # `modified_species` has no datasource: entering the derivation is fatal
    "unbound_root": "coalesce(modified_species, {raw})",
}

DIMENSIONS = {
    "direct": """
property species.native bool;
datasource enrichment (species: species, native: native)
grain (species)
query '''select 'Acer rubrum' as species, true as native
union all select 'Quercus alba' as species, false as native''';
auto bucket <- case when native then 'native' else 'other' end;
""",
    # an unnest connector merged into the dimension key bridges species to it
    "unnest_connector": """
key ecoregion_id int;
property ecoregion_id.biome string;
property species.native_ecoregions list<int>;
auto tree_eco_region <- unnest(native_ecoregions);
merge tree_eco_region into ecoregion_id;
datasource enrichment (species: species, native_ecoregions: native_ecoregions)
grain (species)
query '''select 'Acer rubrum' as species, [1] as native_ecoregions
union all select 'Quercus alba' as species, [2] as native_ecoregions''';
datasource ecoregions (ecoregion_id: ecoregion_id, biome: biome)
grain (ecoregion_id)
query '''select 1 as ecoregion_id, 'forest' as biome
union all select 2 as ecoregion_id, 'desert' as biome''';
auto bucket <- case when biome = 'forest' then 'native' else 'other' end;
""",
}

# `a`/`b` trees in city A; `c` in city B
TREES = """select 'a' as tree_id, 'A' as city, 'Acer rubrum' as species
union all select 'b' as tree_id, 'A' as city, 'Quercus alba' as species
union all select 'c' as tree_id, 'B' as city, 'Acer rubrum' as species"""


def model(origin: str, partial: bool, raw: str, dimension: str) -> str:
    raw_kind = "root partial datasource" if partial else "root datasource"
    complete = "complete where city = 'A'" if partial else ""
    return f"""
key tree_id string;
key species string;
property tree_id.city string;
property tree_id.{raw} string;
property tree_id.cluster_id string;
property tree_id.modified_species string;

datasource published (tree_id: tree_id, city: city, species: species)
grain (tree_id)
query '''{TREES}''';

{raw_kind} raw_a (tree_id: tree_id, city: city, species: {raw}, cluster_id: cluster_id)
grain (tree_id)
{complete}
query '''select tree_id, city, species, tree_id as cluster_id from ({TREES}) where city = 'A' ''';

{DIMENSIONS[dimension]}
auto merged_species <- {ORIGINS[origin].format(raw=raw)};
merge merged_species into species;
"""


QUERIES = {
    "select bucket, count(tree_id) -> n where city = 'A' order by bucket asc;": [
        ("native", 1),
        ("other", 1),
    ],
    "select bucket, count(tree_id) -> n order by bucket asc;": [
        ("native", 2),
        ("other", 1),
    ],
    "select species, count(tree_id) -> n where city = 'A' order by species asc;": [
        ("Acer rubrum", 1),
        ("Quercus alba", 1),
    ],
}


@pytest.mark.parametrize("dimension", sorted(DIMENSIONS))
# `raw_species` sorts before `species`, `zraw_species` after
@pytest.mark.parametrize("raw", ["raw_species", "zraw_species"])
@pytest.mark.parametrize("partial", [True, False], ids=["partial", "full"])
@pytest.mark.parametrize("origin", sorted(ORIGINS))
@pytest.mark.parametrize("query", sorted(QUERIES))
def test_stored_key_read_over_merged_pseudonym(
    query: str, origin: str, partial: bool, raw: str, dimension: str
):
    executor = Dialects.DUCK_DB.default_executor()
    executor.parse_text(model(origin, partial, raw, dimension))
    sql = executor.generate_sql(query)[-1]
    assert "raw_a" not in sql, sql
    assert executor.execute_text(query)[-1].fetchall() == QUERIES[query]
