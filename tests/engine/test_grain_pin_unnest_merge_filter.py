"""A WHERE over a CASE ... ELSE read through an `unnest` + `merge` axis, in a
scope a partial binding can pad, is pinned to the select's rows (`grain_pin`).
The pinned value joins those rows on its anchor; without it the filter was
cross-joined (`on 1=1`) and every row came back (issue #808). A WHERE over the
row stream alone still reads the `complete where` binding it implies, for the
rows and for the scan the pinned value is evaluated over."""

import pytest

from trilogy import Dialects

MODEL = """
key tree_id string;
key species string;
key city string;
property tree_id.latitude float;
property tree_id.raw_height float;
key ecoregion_id int;
property ecoregion_id.biome string;
property species.native_ecoregions list<int>;
auto tree_eco_region <- unnest(native_ecoregions);
merge tree_eco_region into ecoregion_id;

partial datasource trees_a (tree_id: tree_id, city: city, species: species, latitude: latitude)
grain (tree_id)
complete where city = 'A'
query '''select 't1' as tree_id, 'A' as city, 'oak' as species, 1.0::double as latitude
union all select 't2', 'A', 'pine', 2.0
union all select 't3', 'A', 'elm', 3.0''';

datasource trees (tree_id: tree_id, city: city, species: species, latitude: latitude)
grain (tree_id)
query '''select 't1' as tree_id, 'A' as city, 'oak' as species, 1.0::double as latitude
union all select 't2', 'A', 'pine', 2.0
union all select 't3', 'A', 'elm', 3.0
union all select 'b1', 'B', 'elm', 9.0''';

root datasource enrichment (species: species, native_ecoregions: ?native_ecoregions)
grain (species)
query '''select 'oak' as species, [1, 2] as native_ecoregions
union all select 'pine', [2]
union all select 'elm', [3]''';

root datasource eco (ecoregion_id: ecoregion_id, biome: ?biome)
grain (ecoregion_id)
query '''select 1 as ecoregion_id, 'forest' as biome
union all select 2, 'desert'
union all select 3, 'tundra' ''';

partial datasource heights (tree_id: tree_id, raw_height: raw_height)
grain (tree_id)
query '''select 't1' as tree_id, 5.0 as raw_height''';

auto bucket <- case when ecoregion_id = 1 then 'Native' when biome = 'desert' then 'Same' else 'Other' end;
"""


@pytest.mark.parametrize(
    "query, expected",
    [
        (
            "select latitude where city = 'A' and bucket = 'Other' order by latitude;",
            [(3.0,)],
        ),
        (
            "select tree_id where city = 'A' and bucket = 'Other' order by tree_id;",
            [("t3",)],
        ),
        (
            "select latitude where bucket = 'Other' order by latitude;",
            [(3.0,), (9.0,)],
        ),
        (
            "select latitude where city = 'A' and bucket = 'Native' order by latitude;",
            [(1.0,)],
        ),
        (
            "select tree_id where city = 'A' and bucket = 'Same' order by tree_id;",
            [("t1",), ("t2",)],
        ),
    ],
)
def test_pinned_filter_joins_the_select_rows(query: str, expected: list[tuple]):
    executor = Dialects.DUCK_DB.default_executor()
    executor.parse_text(MODEL)
    sql = executor.generate_sql(query)[-1]
    assert "on 1=1" not in sql, sql
    assert [tuple(r) for r in executor.execute_raw_sql(sql).fetchall()] == expected


@pytest.mark.parametrize(
    "query, expected",
    [
        (
            "select latitude where city = 'A' and bucket = 'Other' order by latitude;",
            [(3.0,)],
        ),
        (
            "select tree_id where city = 'A' and bucket = 'Other' order by tree_id;",
            [("t3",)],
        ),
        (
            "select latitude where city = 'A' and bucket = 'Native' order by latitude;",
            [(1.0,)],
        ),
        (
            "select tree_id where city = 'A' and bucket = 'Same' order by tree_id;",
            [("t1",), ("t2",)],
        ),
        (
            "select tree_id, native_ecoregions where city = 'A' order by tree_id;",
            [("t1", [1, 2]), ("t2", [2]), ("t3", [3])],
        ),
    ],
)
def test_filter_reads_the_complete_where_source(query: str, expected: list[tuple]):
    executor = Dialects.DUCK_DB.default_executor()
    executor.parse_text(MODEL)
    sql = executor.generate_sql(query)[-1]
    assert '"trees_a"' in sql, sql
    assert 'as "trees"' not in sql, sql
    assert [tuple(r) for r in executor.execute_raw_sql(sql).fetchall()] == expected


MERGED_SPECIES = """
property tree_id.raw_species string;
auto merged_species <- coalesce(raw_species, 'x');
merge merged_species into species;

datasource raw (tree_id: tree_id, raw_species: raw_species)
grain (tree_id)
query '''select 't1' as tree_id, 'oak' as raw_species
union all select 't2', 'pine'
union all select 't3', 'elm'
union all select 'b1', 'elm' ''';
"""


@pytest.mark.parametrize("extra", ["", MERGED_SPECIES], ids=["species", "merged"])
@pytest.mark.parametrize(
    "query, expected",
    [
        (
            "select upper(species) as s, native_ecoregions where city = 'A' and bucket = 'Other';",
            [("ELM", [3])],
        ),
        (
            "select upper(species) as s, native_ecoregions where city = 'A' and bucket = 'Same' order by s;",
            [("OAK", [1, 2]), ("PINE", [2])],
        ),
        (
            "select upper(species) as s where city = 'A' and bucket = 'Same' order by s;",
            [("OAK",), ("PINE",)],
        ),
    ],
)
def test_pinned_filter_beside_species_grain_outputs(
    query: str, expected: list[tuple], extra: str
):
    """A merged unnest axis does not split `city` (a key bound beside the rows,
    on no side of the merge) off the row stream: sourced alone, it met the
    pinned value on nothing."""
    executor = Dialects.DUCK_DB.default_executor()
    executor.parse_text(MODEL + extra)
    sql = executor.generate_sql(query)[-1]
    assert "on 1=1" not in sql, sql
    assert [tuple(r) for r in executor.execute_raw_sql(sql).fetchall()] == expected


PADDED_SPECIES = (
    MODEL.replace(
        "union all select 't3', 'A', 'elm', 3.0'''",
        "union all select 't3', 'A', 'elm', 3.0\nunion all select 't4', 'A', 'ash', 4.0'''",
    )
    .replace(
        "union all select 'b1', 'B', 'elm', 9.0'''",
        "union all select 'b1', 'B', 'elm', 9.0\nunion all select 't4', 'A', 'ash', 4.0'''",
    )
    .replace(
        "union all select 'elm', [3]'''",
        "union all select 'elm', [3]\nunion all select 'ash', []::int[]'''",
    )
)


@pytest.mark.parametrize(
    "query, expected",
    [
        (
            "select species, bucket where city = 'A' order by species, bucket;",
            [
                ("ash", "Other"),
                ("elm", "Other"),
                ("oak", "Native"),
                ("oak", "Same"),
                ("pine", "Same"),
            ],
        ),
        (
            "select latitude where city = 'A' and bucket = 'Other' order by latitude;",
            [(3.0,), (4.0,)],
        ),
        (
            "select upper(species) as s where city = 'A' and bucket = 'Other' order by s;",
            [("ASH",), ("ELM",)],
        ),
        (
            "select species, native_ecoregions where city = 'A' and bucket = 'Other' order by species;",
            [("ash", []), ("elm", [3])],
        ),
        (
            "select species where city = 'A' and bucket = 'Same' order by species;",
            [("oak",), ("pine",)],
        ),
    ],
)
def test_pinned_filter_on_padded_species(query: str, expected: list[tuple]):
    """`ash` has no native ecoregion: under the padding `trees_a` brings, its
    pinned bucket is the ELSE branch, and the WHERE reads the value the
    projection shows."""
    executor = Dialects.DUCK_DB.default_executor()
    executor.parse_text(PADDED_SPECIES)
    sql = executor.generate_sql(query)[-1]
    assert [tuple(r) for r in executor.execute_raw_sql(sql).fetchall()] == expected
