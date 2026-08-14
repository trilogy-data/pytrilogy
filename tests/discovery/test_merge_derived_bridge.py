"""Lock: a bridge datasource reachable only through a merged DERIVED key is kept.

`trees` binds (tree_id, city, species); `tree_enrichment` binds species ->
(`native_ecoregions list<int>`, `primary_ecoregion`); a derived concept is
merged into `ecoregion_id`, the grain of `ecoregion_info`. The only path from a
tree to an ecoregion attribute runs trees -> enrichment (species) -> the merged
key -> ecoregion.

v4 pruned the bridge entirely and cross-joined `ecoregion_info` to `trees` ON
1=1, so every tree paired with every ecoregion and the classifying CASE fanned
out (upstream sf_tree_reporting "native locality bucket" regression, 0.3.316+).
Two independent causes, one per parametrization:

- every kind: address coverage vetoed planning the connector the source search
  had already CHOSEN as a join hop — the merged key's surviving address is bound
  by the dimension scan and its input keys by the fact scan, so every address
  looked covered while the two scans shared no column.
- unnest only: parse gives the row-multiplying family no grain, so the concept
  self-grains and the merge rewrites that onto the merged class, leaving the
  connector candidate no axis to advertise. It binds its `keys` instead.

Executed row assertions, not just SQL shape: the cross join produced valid SQL.
"""

import pytest

from trilogy import Dialects
from trilogy.core.models.environment import Environment

_MODEL = """
key tree_id string;
key species string;
key city string;
key ecoregion_id int;

property ecoregion_id.realm string;
property ecoregion_id.biome string;

property species.native_ecoregions list<int>;
property species.primary_ecoregion int;

{merge}

datasource trees (
    tree_id: tree_id,
    city: city,
    species: species,
)
grain (tree_id)
query '''
select 't1' tree_id, 'USBOS' city, 'A' species
union all select 't2' tree_id, 'USBOS' city, 'A' species
union all select 't3' tree_id, 'USBOS' city, 'B' species
''';

datasource tree_enrichment (
    species: species,
    native_ecoregions: native_ecoregions,
    primary_ecoregion: primary_ecoregion,
)
grain (species)
query '''
select 'A' species, [339] native_ecoregions, 339 primary_ecoregion
union all select 'B' species, [500] native_ecoregions, 500 primary_ecoregion
''';

datasource ecoregion_info (
    ecoregion_id: ecoregion_id,
    realm: realm,
    biome: biome,
)
grain (ecoregion_id)
query '''
select 339 ecoregion_id, 'nearctic' realm, 'Temperate Broadleaf & Mixed Forests' biome
union all select 500 ecoregion_id, 'palearctic' realm, 'Mediterranean Forests, Woodlands & Scrub' biome
union all select 600 ecoregion_id, 'nearctic' realm, 'Deserts & Xeric Shrublands' biome
''';

constant active_city_ecoregion <- 339;
constant active_city_biome <- 'Temperate Broadleaf & Mixed Forests';
constant active_city_realm <- 'nearctic';

property native_locality_bucket <- CASE
    WHEN ecoregion_id = active_city_ecoregion THEN 'Native'
    WHEN biome = active_city_biome THEN 'Same biome, non-native'
    WHEN realm = active_city_realm THEN 'Native Region, Different Biome'
    ELSE 'Non-Native, Different Biome'
END::string;
"""

_QUERY = """
WHERE city = 'USBOS'
SELECT
    native_locality_bucket,
    count(tree_id) as tree_count
ORDER BY tree_count desc;
"""

_UNNEST = (
    "auto merged_key <- unnest(native_ecoregions);\nmerge merged_key into ecoregion_id;"
)
_AGGREGATE = "auto merged_key <- max(primary_ecoregion) by species;\nmerge merged_key into ecoregion_id;"
_WINDOW = "auto merged_key <- lag primary_ecoregion order by species asc;\nmerge merged_key into ecoregion_id;"

# A: 339 (native), B: 500 (palearctic / mediterranean, matches no branch).
_NATIVE_A = [("Native", 2), ("Non-Native, Different Biome", 1)]
# lag shifts A's 339 onto B, leaving A unmatched -- the mirror image, which a
# cross join (every bucket, count 3) and a species-blind join both fail.
_NATIVE_B = [("Native", 1), ("Non-Native, Different Biome", 2)]

_CASES = [
    pytest.param(_UNNEST, _NATIVE_A, id="unnest"),
    pytest.param(_AGGREGATE, _NATIVE_A, id="aggregate"),
    pytest.param(_WINDOW, _NATIVE_B, id="window"),
]


def _engine(merge: str):
    engine = Dialects.DUCK_DB.default_executor(environment=Environment())
    engine.parse_text(_MODEL.format(merge=merge))
    return engine


@pytest.mark.parametrize("merge,expected", _CASES)
def test_merged_derived_bridge_rows(merge: str, expected: list[tuple[str, int]]):
    rows = _engine(merge).execute_text(_QUERY)[-1].fetchall()
    assert sorted((r[0], int(r[1])) for r in rows) == sorted(expected)


@pytest.mark.parametrize("merge,expected", _CASES)
def test_merged_derived_bridge_sql_shape(merge: str, expected: list[tuple[str, int]]):
    sql = _engine(merge).generate_sql(_QUERY)[-1]
    assert "1=1" not in sql, sql
    assert "tree_enrichment" in sql, sql


def test_merged_unnest_bridge_row_projection():
    rows = (
        _engine(_UNNEST)
        .execute_text("SELECT tree_id, native_locality_bucket ORDER BY tree_id asc;")[
            -1
        ]
        .fetchall()
    )
    # The trailing NULL-tree row is baseline semantics (verified on 0.3.315):
    # the merge is symmetric, so an ecoregion no species is native to (600)
    # surfaces with no tree attached.
    assert [(r[0], r[1]) for r in rows] == [
        ("t1", "Native"),
        ("t2", "Native"),
        ("t3", "Non-Native, Different Biome"),
        (None, "Native Region, Different Biome"),
    ]
