"""Targeted coverage for pseudonym handling in join_resolution.

A pseudonym is a concept under another name. The join graph keys every
equivalence class to one canonical address, so pseudonyms join exactly like the
column they alias, while partial / nullable status is preserved across the class.
"""

import dataclasses

from trilogy import Environment, parse
from trilogy.core.enums import JoinType, Modifier
from trilogy.core.processing.join_resolution import (
    build_canonical_address_map,
    get_modifiers,
    get_node_joins,
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


# --- build_canonical_address_map -------------------------------------------


def test_canonical_map_collapses_merge_pseudonyms():
    build = _merged_env()
    dses = [build.datasources["p1.people"], build.datasources["p2.people"]]
    canonical = build_canonical_address_map(dses, build)
    # p1.uname and p2.uname are the same column -> one canonical representative.
    assert canonical["p1.uname"] == canonical["p2.uname"]


def test_canonical_map_collapses_one_way_alias():
    """A one-directional pseudonym (rowset rename, q14-style) must still collapse
    via transitive closure even though only one side declares the link."""
    env, _ = parse("""
key channel int;
key alt_channel int;
datasource base (channel:channel) grain(channel) address base;
datasource alt (alt_channel:alt_channel) grain(alt_channel) address alt;
""")
    build = env.materialize_for_select()
    base_c = build.datasources["base"]
    alt_c = build.datasources["alt"]
    # Inject a one-way pseudonym: alt_channel declares channel as an alias.
    alt_concept = next(
        c for c in alt_c.output_concepts if c.address == "local.alt_channel"
    )
    alt_concept.pseudonyms.add("local.channel")
    canonical = build_canonical_address_map([base_c, alt_c], build)
    assert canonical["local.alt_channel"] == canonical["local.channel"]


# --- get_node_joins: pseudonym equivalence ---------------------------------


def test_get_node_joins_merge_joins_on_canonical_key():
    """Two datasources sharing only a pseudonym key must join on that key, not
    collapse to a 1=1 cross product (q23 regression)."""
    build = _merged_env()
    joins = get_node_joins(
        [build.datasources["p1.people"], build.datasources["p2.people"]],
        environment=build,
    )
    assert len(joins) == 1
    join = joins[0]
    # Real join key (not an empty/cross-product join).
    assert join.concept_pairs
    pair = join.concept_pairs[0]
    # Left and right resolve to each datasource's own instance of the column.
    assert pair.left.address in {"p1.uname", "p2.uname"}
    assert pair.right.address in {"p1.uname", "p2.uname"}


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
    assert get_modifiers(shared, shared, a, b) == [Modifier.NULLABLE]


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
    assert get_modifiers(shared, shared, a, b) == []
    assert get_modifiers(shared, shared, b, a) == []


def test_get_modifiers_nullable_via_pseudonym():
    """Both-sides-nullable must be detected even when one side carries the key
    under an equivalent (pseudonym) address -- a raw-address check misses it."""
    build = _two_sided_nullable_env()
    a = build.datasources["a_src"]
    b = build.datasources["b_src"]
    # b carries the key under a pseudonym; the join references the alias address.
    nullable_col = next(c for c in b.nullable_concepts if c.address == "local.shared")
    nullable_col.pseudonyms.add("local.alias_shared")
    alias_concept = dataclasses.replace(
        build.concepts["shared"],
        name="alias_shared",
        canonical_name="alias_shared",
        pseudonyms={"local.shared"},
    )
    a_concept = build.concepts["shared"]
    assert alias_concept.address == "local.alias_shared"
    assert get_modifiers(a_concept, alias_concept, a, b) == [Modifier.NULLABLE]


# --- get_node_joins end-to-end: partial / nullable preservation ------------


def test_get_node_joins_merge_partial_preserved():
    """A partial column on one side of a pseudonym merge must drive an outer
    join (preserve the complete side)."""
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
    assert join.join_type in (
        JoinType.LEFT_OUTER,
        JoinType.RIGHT_OUTER,
        JoinType.FULL,
    )
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
