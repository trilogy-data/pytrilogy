"""Lock (#758): an aggregate `by species` over a species -> many-ecoregions axis
answers at its authored grain, whatever else the model merges.

Two planner errors fanned these answers out:
- two unrelated global `merge`s in one namespace were read as legs of one
  composite relation, so a global `count(tree_id)` widened its input grain
  with the ecoregion axis and counted tree x ecoregion rows;
- an unnest output was keyed by its parent's keys alone, so `species`
  "determined" `ecoregion_id` and `min(ecoregion_id) by species` was grouped
  at (species, ecoregion_id) beside that axis. Its keys are now the parent's
  keys plus a virtual position key, which is never sourced.

Each case is checked against an oracle computed in Python."""

from collections import defaultdict

import pytest

from trilogy import Dialects
from trilogy.core.models.author import Grain
from trilogy.core.models.environment import Environment

TREES = [(1, "a", 10.0), (2, "a", 20.0), (3, "b", 5.0), (4, "c", 7.0), (5, "b", 1.0)]
NATIVE = {"a": [1, 2, 3], "b": [2, 3], "c": [3]}

BASE = """
key tree_id int;
key species string;
property tree_id.dbh float;
key ecoregion_id int;
property ecoregion_id.biome string;
datasource trees (tree_id, species, dbh) grain (tree_id)
query '''select 1 tree_id, 'a' species, 10.0 dbh union all select 2, 'a', 20.0
union all select 3, 'b', 5.0 union all select 4, 'c', 7.0 union all select 5, 'b', 1.0 ''';
datasource eco (ecoregion_id, biome) grain (ecoregion_id)
query '''select 1 ecoregion_id, 'x' biome union all select 2, 'y' union all select 3, 'y' ''';
auto rank_v <- min(ecoregion_id) by species;
"""

UNNEST = """
property species.native_ecoregions list<int>;
auto tree_eco_region <- unnest(native_ecoregions);
merge tree_eco_region into {partial}ecoregion_id;
datasource enrich (species, native_ecoregions) grain (species)
query '''select 'a' species, [1, 2, 3] native_ecoregions union all select 'b', [2, 3]
union all select 'c', [3] ''';
"""

AXES = {
    "unnest_merge": UNNEST.format(partial=""),
    "unnest_partial_merge": UNNEST.format(partial="~"),
    "bridge": """
datasource bridge (species, ecoregion_id) grain (species, ecoregion_id)
query '''select 'a' species, 1 ecoregion_id union all select 'a', 2 union all select 'a', 3
union all select 'b', 2 union all select 'b', 3 union all select 'c', 3 ''';
""",
}

UNRELATED_MERGES = """
key modified_tree_id int;
datasource mods (tree_id: modified_tree_id) grain (modified_tree_id)
query '''select 1 tree_id''';
merge modified_tree_id into ~tree_id;
key alt_species string;
datasource alts (species: alt_species) grain (alt_species) query '''select 'a' species''';
merge alt_species into ~species;
"""

METRICS = {
    "count": ("count(tree_id)", lambda rows: len(rows)),
    "count_f": (
        "count(tree_id ? rank_v = 1)",
        lambda rows: sum(r[3] == 1 for r in rows),
    ),
    "sum_f": (
        "sum(dbh ? rank_v = 1)",
        lambda rows: sum(r[2] for r in rows if r[3] == 1) or None,
    ),
}


def _oracle(metric: str, group: str | None) -> list[tuple]:
    rank = {s: min(es) for s, es in NATIVE.items()}
    groups: dict[tuple, list[tuple]] = defaultdict(list)
    for tree_id, species, dbh in TREES:
        if group == "ecoregion_id":
            keys = [(e,) for e in NATIVE[species]]
        elif group == "species":
            keys = [(species,)]
        elif group == "rank_v":
            keys = [(rank[species],)]
        else:
            keys = [()]
        for key in keys:
            groups[key].append((tree_id, species, dbh, rank[species]))
    fn = METRICS[metric][1]
    return sorted(((*k, fn(rows)) for k, rows in groups.items()), key=str)


def _normalize(value):
    return (
        float(value)
        if isinstance(value, (int, float)) or hasattr(value, "is_nan")
        else value
    )


@pytest.mark.parametrize("group", [None, "species", "rank_v", "ecoregion_id"])
@pytest.mark.parametrize("metric", list(METRICS))
@pytest.mark.parametrize("unrelated", [False, True])
@pytest.mark.parametrize("axis", list(AXES))
def test_species_aggregate_over_ecoregion_axis(axis, unrelated, metric, group):
    model = BASE + AXES[axis] + (UNRELATED_MERGES if unrelated else "")
    columns = ([group] if group else []) + [f"{METRICS[metric][0]} as m"]
    engine = Dialects.DUCK_DB.default_executor(environment=Environment())
    engine.parse_text(model)
    rows = engine.execute_text(f"select {', '.join(columns)};")[-1].fetchall()
    got = sorted((tuple(_normalize(v) for v in r) for r in rows), key=str)
    want = [tuple(_normalize(v) for v in r) for r in _oracle(metric, group)]
    assert got == want


def test_unnest_output_keyed_by_parent_and_position():
    env = Environment()
    env.parse(
        BASE + AXES["unnest_partial_merge"] + "auto shifted <- tree_eco_region + 1;"
    )
    element = env.concepts["tree_eco_region"]
    ordinal = env.concepts["__unnest_ordinal_tree_eco_region"]
    assert element.keys == {"local.species", ordinal.address}
    assert ordinal.is_internal
    assert env.concepts["shifted"].keys == {element.address}


@pytest.mark.parametrize("axis", ["unnest_merge", "unnest_partial_merge"])
def test_element_beside_by_aggregate_keeps_element_grain(axis):
    model = (
        BASE
        + AXES[axis]
        + "auto gap <- tree_eco_region - (min(tree_eco_region) by species);"
    )
    engine = Dialects.DUCK_DB.default_executor(environment=Environment())
    engine.parse_text(model)
    rows = engine.execute_text("select species, tree_eco_region, gap;")[-1].fetchall()
    want = sorted((s, e, e - min(es)) for s, es in NATIVE.items() for e in es)
    assert sorted(tuple(r) for r in rows) == want


def test_element_beside_by_aggregate_select_grain_is_element():
    env = Environment()
    env.parse(
        BASE
        + AXES["unnest_partial_merge"]
        + "auto gap <- tree_eco_region - (min(tree_eco_region) by species);"
    )
    grain = Grain(components={"local.species", "local.tree_eco_region"})
    _, select_grain, keys = env.concepts["gap"].get_select_grain_and_keys(grain, env)
    assert select_grain.components == {"local.species", "local.tree_eco_region"}
    assert keys == {"local.species", "local.tree_eco_region"}
