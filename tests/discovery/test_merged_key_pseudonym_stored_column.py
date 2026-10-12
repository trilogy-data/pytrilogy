"""Lock: a key a datasource stores is read from it, never re-derived through a
derivation merged into it (`merge merged_species into species`).

`published` stores `tree_id`, `city` and `species`; `raw_a` holds the raw
species the merge origin derives from. Any query over those three concepts and
a species-keyed dimension must plan from `published` alone, whatever the origin
is, whether `raw_a` is a partial partition, whichever member of the merged
class sorts first, and however the dimension is reached.
"""

import pytest

from trilogy import Dialects, Environment
from trilogy.core.env_processor import generate_graph
from trilogy.core.exceptions import DisconnectedConceptsException
from trilogy.core.processing.concept_strategies_v4 import V4History
from trilogy.core.processing.nodes import SelectNode, StrategyNode
from trilogy.core.processing.v4_helper.concept_graph import (
    computed_origin_relation_members,
)
from trilogy.core.processing.v4_helper.source_planning import (
    SourceRequest,
    _network_source,
    plan_source,
)

ORIGINS = {
    "alias": "{raw}",
    "scalar": "trim({raw})",
    "aggregate": "max({raw}) by cluster_id",
    "aggregate_in_scalar": "substring((max({raw}) by cluster_id), 1, 200)",
    "aggregate_over_unbound": "max(coalesce(modified_species, {raw})) by cluster_id",
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


# how `published` stores the key: completely, for city A only, or not at all
STORED = {
    "complete": f"""datasource published (tree_id: tree_id, city: city, species: species)
grain (tree_id)
query '''{TREES}''';""",
    "partial": f"""partial datasource published (tree_id: tree_id, city: city, species: species)
grain (tree_id)
complete where city = 'A'
query '''select * from ({TREES}) where city = 'A' ''';""",
    "none": "",
}


def model(
    origin: str,
    partial: bool,
    raw: str,
    dimension: str,
    stored: str = "complete",
    raw_name: str = "raw_a",
) -> str:
    raw_kind = "root partial datasource" if partial else "root datasource"
    complete = "complete where city = 'A'" if partial else ""
    raw_filter = "where city = 'A'" if partial else ""
    return f"""
key tree_id string;
key species string;
property tree_id.city string;
property tree_id.{raw} string;
property tree_id.cluster_id string;
property tree_id.modified_species string;

{STORED[stored]}

{raw_kind} {raw_name} (tree_id: tree_id, city: city, species: {raw}, cluster_id: cluster_id)
grain (tree_id)
{complete}
query '''select tree_id, city, species, tree_id as cluster_id from ({TREES}) {raw_filter} ''';

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


def source_request(
    origin: str, raw: str, dimension: str, outputs: list[str]
) -> SourceRequest:
    env = Environment()
    env.parse(model(origin, True, raw, dimension))
    build_env = env.materialize_for_select()
    return SourceRequest(
        outputs=[build_env.concepts[f"local.{name}"] for name in outputs],
        environment=build_env,
        graph=generate_graph(build_env),
        history=V4History(base_environment=env),
    )


def scanned(node: StrategyNode) -> tuple[set[str], set[str]]:
    datasources: set[str] = set()
    outputs: set[str] = set()
    stack = [node]
    while stack:
        current = stack.pop()
        stack.extend(current.parents)
        outputs |= {concept.address for concept in current.output_concepts}
        if isinstance(current, SelectNode):
            datasources.add(current.datasource.name)
    return datasources, outputs


@pytest.mark.parametrize("raw", ["raw_species", "zraw_species"])
@pytest.mark.parametrize(
    "dimension, outputs",
    [
        ("direct", ["native", "species", "tree_id"]),
        ("unnest_connector", ["biome", "ecoregion_id", "tree_id"]),
    ],
)
def test_bridge_never_carries_the_class_representative_spelling(
    raw: str, dimension: str, outputs: list[str]
):
    decision = _network_source(source_request("alias", raw, dimension, outputs))
    assert decision is not None and decision.bridge is not None
    carried = {concept.address for concept in decision.bridge.concepts}
    assert f"local.{raw}" not in carried
    assert "local.species" in carried


@pytest.mark.parametrize(
    "origin",
    ["aggregate", "aggregate_in_scalar", "aggregate_over_unbound", "unbound_root"],
)
def test_derived_connector_skips_merge_origin_of_a_scanned_key(origin: str):
    node = plan_source(
        source_request(
            origin,
            "raw_species",
            "unnest_connector",
            ["biome", "ecoregion_id", "tree_id"],
        )
    )
    assert node is not None
    datasources, outputs = scanned(node)
    assert datasources == {"ecoregions", "enrichment", "published"}
    assert "local.merged_species" not in outputs


@pytest.mark.parametrize("partial", [True, False], ids=["partial", "full"])
@pytest.mark.parametrize("origin", ["aggregate", "aggregate_over_unbound"])
def test_stored_aggregate_merge_is_not_a_relation_axis(origin: str, partial: bool):
    env = Environment()
    env.parse(model(origin, partial, "raw_species", "unnest_connector"))
    members = computed_origin_relation_members(env.materialize_for_select())
    assert "local.merged_species" not in members
    # the unnest relates species to ecoregion_id, which no table stores
    assert {"local.ecoregion_id", "local.tree_eco_region"} <= members


def rows(text: str, query: str, forbid: str | None = "raw_a") -> list[tuple]:
    executor = Dialects.DUCK_DB.default_executor()
    executor.parse_text(text)
    sql = executor.generate_sql(query)[-1]
    if forbid is not None:
        assert forbid not in sql, sql
    return executor.execute_text(query)[-1].fetchall()


DERIVABLE = ["alias", "scalar", "aggregate", "aggregate_in_scalar"]
UNBOUND = ["aggregate_over_unbound", "unbound_root"]


# `published` holds city A only and `raw_a` every city: the key for city B
# rows comes from the derivation, never from a null-extended `published`.
@pytest.mark.parametrize("origin", DERIVABLE)
@pytest.mark.parametrize(
    "query, expected",
    [
        (
            "select species, count(tree_id) -> n order by species asc;",
            [("Acer rubrum", 2), ("Quercus alba", 1)],
        ),
        (
            "select bucket, count(tree_id) -> n order by bucket asc;",
            [("native", 2), ("other", 1)],
        ),
        (
            "select species, count(tree_id) -> n where city = 'A' order by species asc;",
            [("Acer rubrum", 1), ("Quercus alba", 1)],
        ),
    ],
)
def test_partial_stored_key_completes_from_the_derivation(
    origin: str, query: str, expected: list[tuple]
):
    text = model(origin, False, "raw_species", "direct", stored="partial")
    assert rows(text, query, forbid=None) == expected


# nothing stores the key: the derivation is the only source
@pytest.mark.parametrize("origin", DERIVABLE)
@pytest.mark.parametrize(
    "query, expected",
    [
        (
            "select species, count(tree_id) -> n order by species asc;",
            [("Acer rubrum", 2), ("Quercus alba", 1)],
        ),
        (
            "select bucket, count(tree_id) -> n order by bucket asc;",
            [("native", 2), ("other", 1)],
        ),
    ],
)
def test_unstored_key_derives(origin: str, query: str, expected: list[tuple]):
    text = model(origin, False, "raw_species", "direct", stored="none")
    assert rows(text, query, forbid=None) == expected


@pytest.mark.parametrize("origin", UNBOUND)
def test_unstored_key_over_an_unbound_root_is_unresolvable(origin: str):
    text = model(origin, False, "raw_species", "direct", stored="none")
    with pytest.raises(DisconnectedConceptsException):
        rows(text, "select species, count(tree_id) -> n;", forbid=None)


# the stored key under its other names and beside the raw column it derives
# from: still read from `published`, and the raw partition joins on `tree_id`
@pytest.mark.parametrize("partial", [True, False], ids=["partial", "full"])
@pytest.mark.parametrize("origin", sorted(ORIGINS))
@pytest.mark.parametrize(
    "query, expected, reads_raw",
    [
        (
            "select merged_species, count(tree_id) -> n order by merged_species asc;",
            [("Acer rubrum", 2), ("Quercus alba", 1)],
            False,
        ),
        (
            "select tree_id where merged_species = 'Acer rubrum' order by tree_id asc;",
            [("a",), ("c",)],
            False,
        ),
        (
            "select tree_id, raw_species, species where city = 'A' order by tree_id asc;",
            [
                ("a", "Acer rubrum", "Acer rubrum"),
                ("b", "Quercus alba", "Quercus alba"),
            ],
            True,
        ),
        (
            "select cluster_id, species where city = 'A' order by cluster_id asc;",
            [("a", "Acer rubrum"), ("b", "Quercus alba")],
            True,
        ),
    ],
)
def test_stored_key_by_other_names_and_beside_its_raw_column(
    partial: bool, origin: str, query: str, expected: list[tuple], reads_raw: bool
):
    text = model(origin, partial, "raw_species", "direct")
    # a bare rename makes the raw column itself a spelling of the key, so a
    # complete raw partition is as good a source as `published`
    forbid = None if reads_raw or (origin == "alias" and not partial) else "raw_a"
    assert rows(text, query, forbid=forbid) == expected


# the raw partition sorts before `published`: the stored column still wins
@pytest.mark.parametrize("origin", ["scalar", "aggregate_in_scalar"])
def test_stored_column_beats_an_earlier_sorting_derivation(origin: str):
    text = model(origin, False, "raw_species", "direct", raw_name="a_raw")
    assert rows(
        text, "select tree_id, species order by tree_id asc;", forbid="a_raw"
    ) == [
        ("a", "Acer rubrum"),
        ("b", "Quercus alba"),
        ("c", "Acer rubrum"),
    ]
