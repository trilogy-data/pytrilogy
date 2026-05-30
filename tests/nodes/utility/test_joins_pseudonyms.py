"""Targeted coverage for pseudonym handling in join_resolution.

A pseudonym is a concept under another name. For the join algorithm it must be
treated as equivalent to the canonical column (same join key), while partial /
nullable status is preserved across the equivalence class.
"""

import dataclasses

import pytest

from trilogy import Environment, parse
from trilogy.core import graph as nx
from trilogy.core.enums import JoinType, Modifier
from trilogy.core.processing.join_resolution import (
    add_node_join_concept,
    get_modifiers,
    get_node_joins,
    resolve_instantiated_concept,
)


def _merged_env():
    """Two imports of the same model, merged on a property -> two datasources
    that expose the merged column under distinct (pseudonym) addresses."""
    base = Environment()
    imp = Environment()
    imp.parse("""
key uid int;
property uid.uname string;
datasource people (uid:uid, uname:uname) grain(uid)
query '''select 1 uid, 'a' uname''';
""")
    base.add_import("p1", imp)
    base.add_import("p2", imp)
    base.parse("merge p1.uname into p2.uname;")
    return base.materialize_for_select()


# --- resolve_instantiated_concept ------------------------------------------


def test_resolve_instantiated_concept_direct():
    build = _merged_env()
    ds = build.datasources["p2.people"]
    concept = build.concepts["p2.uname"]
    assert resolve_instantiated_concept(concept, ds).address == "p2.uname"


def test_resolve_instantiated_concept_via_pseudonym():
    # p1.uname's address was rewritten to p2.uname by the merge but it still
    # carries the pseudonym; it must resolve to p2's own instance.
    build = _merged_env()
    ds_p2 = build.datasources["p2.people"]
    p1_uname = build.concepts["p1.uname"]
    assert resolve_instantiated_concept(p1_uname, ds_p2).address == "p2.uname"


def test_resolve_instantiated_concept_reverse_pseudonym():
    # The queried concept declares no pseudonyms, but an output concept declares
    # *it* as a pseudonym (the rename-basics case behind q14). Must still resolve.
    build = _merged_env()
    ds_p2 = build.datasources["p2.people"]
    origin = build.alias_origin_lookup["p1.uname"]  # address p1.uname
    stripped = dataclasses.replace(origin, pseudonyms=set())
    assert "p1.uname" not in [c.address for c in ds_p2.output_concepts]
    assert resolve_instantiated_concept(stripped, ds_p2).address == "p2.uname"


def test_resolve_instantiated_concept_not_found_raises():
    build = _merged_env()
    ds_p2 = build.datasources["p2.people"]
    with pytest.raises(SyntaxError):
        resolve_instantiated_concept(build.concepts["p1.uid"], ds_p2)


# --- add_node_join_concept --------------------------------------------------


def _build_join_graph(build, ds_identifiers):
    graph = nx.Graph()
    concept_map = {}
    for ident in ds_identifiers:
        graph.add_node(f"ds~{ident}")
    for ident in ds_identifiers:
        ds = build.datasources[ident]
        for concept in ds.output_concepts:
            add_node_join_concept(graph, concept, concept_map, f"ds~{ident}", build)
    return graph


@pytest.mark.parametrize(
    "order", [["p1.people", "p2.people"], ["p2.people", "p1.people"]]
)
def test_add_node_join_concept_connects_shared_pseudonym(order):
    """Regardless of processing order, the datasource processed second must be
    connected to a pseudonym node already registered by the first — otherwise
    find_all_connecting_concepts misses the link and the join cross-products."""
    build = _merged_env()
    graph = _build_join_graph(build, order)
    shared = set(graph.neighbors("ds~p1.people")) & set(graph.neighbors("ds~p2.people"))
    # Both the canonical and the pseudonym key node connect the two sources.
    assert "c~p1.uname" in shared
    assert "c~p2.uname" in shared


# --- get_modifiers ----------------------------------------------------------


def _two_sided_nullable_env():
    """Both datasources carry the join key as a nullable column."""
    env, _ = parse("""
key a_id int;
key shared int;
key b_id int;
datasource a_src (a_id:a_id, shared:?shared) grain(a_id) address a_src;
datasource b_src (b_id:b_id, shared:?shared) grain(b_id) address b_src;
""")
    return env.materialize_for_select()


def test_get_modifiers_both_sides_nullable():
    """Null-safe equality is only meaningful when a NULL key must match a NULL
    key -> stamp NULLABLE only when both sides are nullable."""
    build = _two_sided_nullable_env()
    a = build.datasources["a_src"]
    b = build.datasources["b_src"]
    shared = build.concepts["shared"]
    assert get_modifiers(shared, a, b) == [Modifier.NULLABLE]


def test_get_modifiers_one_side_nullable_stays_plain():
    """A nullable key against a non-null key (e.g. a dimension PK) must NOT get
    the null-safe wrapper -- it is equivalent to ``=`` and only costs perf."""
    env, _ = parse("""
key a_id int;
key shared int;
key b_id int;
datasource a_src (a_id:a_id, shared:shared) grain(a_id) address a_src;
datasource b_src (b_id:b_id, shared:?shared) grain(b_id) address b_src;
""")
    build = env.materialize_for_select()
    a = build.datasources["a_src"]  # shared non-null
    b = build.datasources["b_src"]  # shared nullable
    shared = build.concepts["shared"]
    assert get_modifiers(shared, a, b) == []
    assert get_modifiers(shared, b, a) == []


def test_get_modifiers_nullable_via_pseudonym():
    """Both-sides-nullable must be detected even when one side carries the key
    under an equivalent (pseudonym) address -- a raw-address check misses it."""
    build = _two_sided_nullable_env()
    a = build.datasources["a_src"]
    b = build.datasources["b_src"]
    # b carries the key under a pseudonym; the join references the alias address.
    nullable_col = next(c for c in b.nullable_concepts if c.address == "local.shared")
    nullable_col.pseudonyms.add("local.alias_shared")
    join_concept = dataclasses.replace(
        build.concepts["shared"],
        name="alias_shared",
        canonical_name="alias_shared",
        pseudonyms={"local.shared"},
    )
    assert join_concept.address == "local.alias_shared"
    assert get_modifiers(join_concept, a, b) == [Modifier.NULLABLE]


# --- get_node_joins end-to-end ---------------------------------------------


def test_get_node_joins_merge_partial_preserved():
    """A partial column on one side of a pseudonym merge must drive an outer
    join (preserve the complete side) and stamp PARTIAL on the pair."""
    env = Environment()
    env.parse("""
key a_id int;
property a_id.a_name string;
key b_id int;
property b_id.b_name string;
datasource a_src (a_id:a_id, a_name:~a_name) grain(a_id)
    query '''select 1 a_id, 'x' a_name''';
datasource b_src (b_id:b_id, b_name:b_name) grain(b_id)
    query '''select 1 b_id, 'x' b_name''';
merge a_id.a_name into b_id.b_name;
""")
    build = env.materialize_for_select()
    joins = get_node_joins(
        [build.datasources["a_src"], build.datasources["b_src"]], environment=build
    )
    assert len(joins) == 1
    join = joins[0]
    # a_src is partial on the shared key -> must not INNER-join it away.
    assert join.join_type in (
        JoinType.LEFT_OUTER,
        JoinType.RIGHT_OUTER,
        JoinType.FULL,
    )
    # The shared key is present as a join pair.
    assert join.concept_pairs


def test_get_node_joins_merge_nullable_drives_outer_join():
    """A nullable join key on a merge pseudonym must propagate to the join
    scorer so the join is widened to an outer join (preserving the NULL-key
    rows) rather than silently INNER-joining them away."""
    env = Environment()
    env.parse("""
key a_id int;
property a_id.a_name string;
key b_id int;
property b_id.b_name string;
datasource a_src (a_id:a_id, a_name:?a_name) grain(a_id)
    query '''select 1 a_id, 'x' a_name''';
datasource b_src (b_id:b_id, b_name:b_name) grain(b_id)
    query '''select 1 b_id, 'x' b_name''';
merge a_id.a_name into b_id.b_name;
""")
    build = env.materialize_for_select()
    joins = get_node_joins(
        [build.datasources["a_src"], build.datasources["b_src"]], environment=build
    )
    assert len(joins) == 1
    join = joins[0]
    assert join.concept_pairs
    assert join.join_type in (
        JoinType.LEFT_OUTER,
        JoinType.RIGHT_OUTER,
        JoinType.FULL,
    )
