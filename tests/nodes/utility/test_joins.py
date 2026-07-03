from collections import defaultdict

import pytest

from trilogy import parse
from trilogy.core.enums import JoinType, Modifier
from trilogy.core.models.build import BuildFunction, BuildGrain
from trilogy.core.models.execute import (
    CTE,
    BaseJoin,
    ConceptPair,
    CTEConceptPair,
    Join,
    QueryDatasource,
)
from trilogy.core.processing.join_resolution import (
    compute_outer_null_status,
    get_join_type,
    prune_outer_join_pairs,
    reduce_concept_pairs,
)
from trilogy.dialect.base import BaseDialect
from trilogy.dialect.common import render_join, render_join_concept


def test_render_join_concept():
    env, _ = parse("""key x int;
        key y int;
    
datasource x_source (
    y:y,
    y+1:x)
    grain(x)
address x_source;

        
        """)
    x = BaseDialect()
    env = env.materialize_for_select()
    concept = env.concepts["x"]
    ds = env.datasources["x_source"]
    node = CTE.from_datasource(ds)
    # folded-in datasource: column resolves to the raw expression
    col = node.consumer_column(concept)
    rendered = render_join_concept(
        name=node.name,
        node=node,
        col=col,
        render_expr=x.render_expr,
        quote_character=x.QUOTE_CHARACTER,
        concept=concept,
        use_map=defaultdict(set),
    )

    assert rendered == "`x_source`.`y` + 1"

    env, _ = parse("""key x int;
        key y int;
    
datasource x_source (
    y:y,
    raw('''y + 1'''):x)
    grain(x)
address x_source;

        
        """)
    x = BaseDialect()
    env = env.materialize_for_select()
    concept = env.concepts["x"]
    ds = env.datasources["x_source"]
    node = CTE.from_datasource(ds)
    col = node.consumer_column(concept)
    rendered = render_join_concept(
        name=node.name,
        node=node,
        col=col,
        render_expr=x.render_expr,
        quote_character=x.QUOTE_CHARACTER,
        concept=concept,
        use_map=defaultdict(set),
    )

    assert rendered == "y + 1"


def test_consumer_column_derived_pseudonym_fallback():
    """A cross-namespace ``merge ... into ~key`` collapses both sides onto one
    canonical concept. The requested concept (``p_last_name``) is not a raw
    column of the far datasource, but the side-appropriate derivation survives
    as a pseudonym output (``r_last_name <- split(full_name)``). ``consumer_column``
    must return that derivation's lineage so the merge key renders the local
    split instead of asserting on the absent raw column."""
    env, _ = parse("""
key rid int;
property rid.full_name string;
key pid int;
property pid.p_last_name string;

datasource rich (rid:rid, name:full_name) grain(rid) address rich;
datasource passengers (pid:pid, last_name:p_last_name) grain(pid) address passengers;

auto r_last_name <- split(full_name, ',')[-1];
    """)
    env = env.materialize_for_select()
    rich_ds = env.datasources["rich"]
    r_last = env.concepts["r_last_name"]
    p_last = env.concepts["p_last_name"]

    # Simulate the merge: the rich-side derivation rides as an output pseudonym
    # of the canonical key.
    node = CTE.from_datasource(rich_ds)
    node.output_columns = node.output_columns + [r_last]
    p_last.pseudonyms.add(r_last.address)

    col = node.consumer_column(p_last)
    assert isinstance(col, BuildFunction)
    assert col is r_last.lineage

    rendered = render_join_concept(
        name=node.name,
        node=node,
        col=col,
        render_expr=BaseDialect().render_expr,
        quote_character=BaseDialect().QUOTE_CHARACTER,
        concept=p_last,
        use_map=defaultdict(set),
    )
    assert rendered == "split(`rich`.`name`, ',')[-1]"


def test_consumer_column_absent_concept_asserts():
    """A concept that is neither a raw column nor a derived pseudonym output is a
    planning bug — ``consumer_column`` must still fail loudly rather than emit a
    bogus reference."""
    env, _ = parse("""
key rid int;
property rid.full_name string;
key other int;

datasource rich (rid:rid, name:full_name) grain(rid) address rich;
datasource elsewhere (id:other) grain(other) address elsewhere;
    """)
    env = env.materialize_for_select()
    node = CTE.from_datasource(env.datasources["rich"])
    with pytest.raises(AssertionError):
        node.consumer_column(env.concepts["other"])


def test_reduce_concept_pair():
    # Parse environment with datasources that have overlapping keys
    env, _ = parse("""
key a int;
key b int;
key c int;

datasource root_ds (
    a
) grain(a)
address foo
;

datasource join_ds_1 (
    a,
    b
) grain(a, b)
address bar;

datasource join_ds_2 (
    a:a,
    b:b,
    c:c
) grain(b)
address baz;
        """)

    env = env.materialize_for_select()

    # Get concepts
    concept_a = env.concepts["a"]
    concept_b = env.concepts["b"]
    concept_c = env.concepts["c"]

    # Get datasources
    root_ds = env.datasources["root_ds"]
    join_ds_1 = env.datasources["join_ds_1"]
    join_ds_2 = env.datasources["join_ds_2"]

    # Create QueryDatasources
    root_qds = QueryDatasource(
        input_concepts=[concept_a],
        output_concepts=[concept_a],
        datasources=[root_ds],
        grain=BuildGrain(components=set([concept_a.address])),
        joins=[],
        source_map={concept_a.address: {root_ds}},
    )

    join_qds_1 = QueryDatasource(
        input_concepts=[concept_a, concept_b],
        output_concepts=[concept_a, concept_b],
        datasources=[join_ds_1],
        grain=BuildGrain(components=set([concept_a.address, concept_b.address])),
        joins=[],
        source_map={
            concept_a.address: {join_ds_1},
            concept_b.address: {join_ds_1},
        },
    )

    join_qds_2 = QueryDatasource(
        input_concepts=[concept_a, concept_b, concept_c],
        output_concepts=[concept_a, concept_b, concept_c],
        datasources=[join_ds_2],
        grain=BuildGrain(
            components=set([concept_b.address])
        ),  # Key point: grain is just b, making it a PK
        joins=[],
        source_map={
            concept_a.address: {join_ds_2},
            concept_b.address: {join_ds_2},
            concept_c.address: {join_ds_2},
        },
    )

    # Create concept pairs representing multiple join conditions
    # This represents a join where we have both 'a' and 'b' as join keys,
    # but since join_qds_2 has grain of just 'b', the 'a' join is redundant
    test_pairs = [
        ConceptPair(left=concept_a, right=concept_a, existing_datasource=join_qds_1),
        ConceptPair(left=concept_b, right=concept_b, existing_datasource=join_qds_1),
    ]

    # Test the reduction - should keep only the primary key join (b)
    reduced_pairs = reduce_concept_pairs(test_pairs, join_qds_2)

    # Assert that only the primary key concept pair remains
    for x in reduced_pairs:
        print(repr(x))
    assert len(reduced_pairs) == 1
    assert reduced_pairs[0].left == concept_b
    assert reduced_pairs[0].right == concept_b

    # Test case with no reduction needed - single concept pair
    single_pair = [
        ConceptPair(left=concept_a, right=concept_a, existing_datasource=root_qds)
    ]

    reduced_single = reduce_concept_pairs(single_pair, join_qds_1)
    assert len(reduced_single) == 1
    assert reduced_single[0].left == concept_a

    # Test case with multiple non-redundant pairs (different concepts)
    different_pairs = [
        ConceptPair(
            left=concept_a,
            right=concept_b,  # Different concepts, should not be reduced
            existing_datasource=join_qds_1,
        ),
        ConceptPair(left=concept_b, right=concept_c, existing_datasource=join_qds_2),
    ]

    reduced_different = reduce_concept_pairs(different_pairs, join_qds_1)
    assert (
        len(reduced_different) == 2
    )  # Should keep both since they're different concepts


def _fd_test_sources():
    env, _ = parse("""
key a int;
key b int;
key x int;

datasource dim (a, b) grain (a)
address dim_tbl;

datasource f1 (a, b, x) grain (x)
address f1_tbl;
        """)
    build_env = env.materialize_for_select()
    a = build_env.concepts["a"]
    b = build_env.concepts["b"]
    x = build_env.concepts["x"]
    dim = build_env.datasources["dim"]
    f1 = build_env.datasources["f1"]
    left_qds = QueryDatasource(
        input_concepts=[a, b],
        output_concepts=[a, b],
        datasources=[dim],
        grain=BuildGrain(components={a.address}),
        joins=[],
        source_map={a.address: {dim}, b.address: {dim}},
    )
    # right grain {a, x} is NOT a subset of the join keys, so the existing
    # grain restriction stays out of the way and the FD prune is what decides
    right_qds = QueryDatasource(
        input_concepts=[a, b, x],
        output_concepts=[a, b, x],
        datasources=[f1],
        grain=BuildGrain(components={a.address, x.address}),
        joins=[],
        source_map={a.address: {f1}, b.address: {f1}, x.address: {f1}},
    )
    return build_env, a, b, left_qds, right_qds


def test_reduce_concept_pairs_fd_through_binding():
    """`dim` binds a and b completely at grain (a), so a → b holds globally
    (the domain graph's complete-binding rule) — the b pair is redundant once
    a is joined. Without the graph the reduction cannot see through the
    binding and keeps both (the pre-graph behavior, pinned here too)."""
    build_env, a, b, left_qds, right_qds = _fd_test_sources()
    pairs = [
        ConceptPair(left=a, right=a, existing_datasource=left_qds),
        ConceptPair(left=b, right=b, existing_datasource=left_qds),
    ]
    without_graph = reduce_concept_pairs(list(pairs), right_qds)
    assert len(without_graph) == 2
    with_graph = reduce_concept_pairs(
        list(pairs), right_qds, domain_graph=build_env.domain_graph
    )
    assert len(with_graph) == 1, with_graph
    assert with_graph[0].left == a


def test_reduce_concept_pairs_fd_never_prunes_grain_pair():
    build_env, a, b, left_qds, right_qds = _fd_test_sources()
    x = build_env.concepts["x"]
    right_qds.grain = BuildGrain(components={b.address, x.address})
    pairs = [
        ConceptPair(left=a, right=a, existing_datasource=left_qds),
        ConceptPair(left=b, right=b, existing_datasource=left_qds),
    ]
    reduced = reduce_concept_pairs(
        list(pairs), right_qds, domain_graph=build_env.domain_graph
    )
    assert any(p.right == b for p in reduced), reduced


def test_reduce_concept_pairs_fd_transitive():
    """Pure transitive closure: A → B, B → C prunes the c pair when a is
    joined, even though no single declaration relates a to c."""
    from trilogy.core.domain_graph import DomainGraph, FDEdge

    build_env, a, b, left_qds, right_qds = _fd_test_sources()
    c = build_env.concepts["x"]
    graph = DomainGraph(
        fd_edges=[
            FDEdge(determinants=frozenset({a.address}), dependent=b.address),
            FDEdge(determinants=frozenset({b.address}), dependent=c.address),
        ]
    )
    right_qds.grain = BuildGrain(components={a.address})
    pairs = [
        ConceptPair(left=a, right=a, existing_datasource=left_qds),
        ConceptPair(left=c, right=c, existing_datasource=left_qds),
    ]
    reduced = reduce_concept_pairs(list(pairs), right_qds, domain_graph=graph)
    assert len(reduced) == 1
    assert reduced[0].left == a


def test_reduce_concept_pairs_fd_mutual_keeps_one():
    """Mutually-dependent keys (a → b and b → a) keep exactly one pair —
    greedy over the surviving determinant set, never both pruned."""
    from trilogy.core.domain_graph import DomainGraph, FDEdge

    build_env, a, b, left_qds, right_qds = _fd_test_sources()
    graph = DomainGraph(
        fd_edges=[
            FDEdge(determinants=frozenset({a.address}), dependent=b.address),
            FDEdge(determinants=frozenset({b.address}), dependent=a.address),
        ]
    )
    pairs = [
        ConceptPair(left=a, right=a, existing_datasource=left_qds),
        ConceptPair(left=b, right=b, existing_datasource=left_qds),
    ]
    reduced = reduce_concept_pairs(list(pairs), right_qds, domain_graph=graph)
    assert len(reduced) == 1, reduced


@pytest.mark.parametrize(
    "left_partial,left_nullable,right_partial,right_nullable,expected",
    [
        # Rendering is preserving-by-default (docs/subset_union_join_design.md):
        # a partial side declares a SUBSET domain and renders FULL — the
        # narrowing pass (UpgradeOuterFromKeySetEquivalence) restores direction
        # only when the superset side provably carries the key's full domain.
        (False, False, False, False, JoinType.INNER),
        (True, False, False, False, JoinType.FULL),
        # left is nullable, not partial — preserve left's NULL-key rows;
        # the non-nullable right has nothing to null-safely match them.
        (False, True, False, False, JoinType.LEFT_OUTER),
        (False, False, True, False, JoinType.FULL),
        # right is nullable, left is not — mirror of the case above.
        (False, False, False, True, JoinType.RIGHT_OUTER),
        (True, False, True, False, JoinType.FULL),
        # both nullable: the null-safe equality (get_modifiers) pairs the NULL
        # groups, so INNER is the narrowed EQUAL form.
        (False, True, False, True, JoinType.INNER),
        # any partial side renders preserving, nullable or not — subset speaks
        # to VALUES, NULL is not a value, and the two never interact here.
        (True, True, False, False, JoinType.FULL),
        (False, False, True, True, JoinType.FULL),
        (True, True, True, True, JoinType.FULL),
    ],
)
def test_get_join_type_all_combinations(
    left_partial, left_nullable, right_partial, right_nullable, expected
):
    """Test all combinations of partial/nullable states"""
    left = "table_a"
    right = "table_b"
    partials = {}
    nullables = {}
    all_connecting_keys = {"key1"}

    if left_partial:
        partials[left] = ["key1"]
    if left_nullable:
        nullables[left] = ["key1"]
    if right_partial:
        partials[right] = ["key1"]
    if right_nullable:
        nullables[right] = ["key1"]

    result = get_join_type(left, right, partials, nullables, all_connecting_keys)
    assert result == expected


def test_get_join_type_no_matching_keys():
    """Test when tables exist in partials/nullables but no connecting keys match"""
    left = "table_a"
    right = "table_b"
    partials = {"table_a": ["other_key"], "table_b": ["another_key"]}
    nullables = {"table_a": ["different_key"]}
    all_connecting_keys = {"key1", "key2"}

    result = get_join_type(left, right, partials, nullables, all_connecting_keys)
    assert result == JoinType.INNER


def test_get_join_type_empty_connecting_keys():
    """Test with empty set of connecting keys"""
    left = "table_a"
    right = "table_b"
    partials = {"table_a": ["key1"], "table_b": ["key2"]}
    nullables = {"table_a": ["key3"]}
    all_connecting_keys = set()

    result = get_join_type(left, right, partials, nullables, all_connecting_keys)
    assert result == JoinType.INNER


def test_get_join_type_multiple_connecting_keys():
    """ANY partial key among multiple connecting keys makes the relation a
    declared subset — rendered preserving."""
    left = "table_a"
    right = "table_b"
    partials = {"table_b": ["key1"]}  # Only one of three keys is partial
    nullables = {}
    all_connecting_keys = {"key1", "key2", "key3"}

    result = get_join_type(left, right, partials, nullables, all_connecting_keys)
    assert result == JoinType.FULL


def test_render_join_coalesce():
    """When multiple CTEConceptPairs share the same right concept,
    render_join should produce a COALESCE on the left values."""
    env, _ = parse("""
key shared_id int;
key fact1_id int;
key fact2_id int;

datasource dim (id:shared_id) grain(shared_id) address dim_table;
datasource fact1 (id:fact1_id, sid:shared_id) grain(fact1_id) address fact1_table;
datasource fact2 (id:fact2_id, sid:shared_id) grain(fact2_id) address fact2_table;
    """)
    dialect = BaseDialect()
    env = env.materialize_for_select()
    shared = env.concepts["shared_id"]
    f1_id = env.concepts["fact1_id"]
    f2_id = env.concepts["fact2_id"]
    ds_dim = env.datasources["dim"]
    ds_f1 = env.datasources["fact1"]
    ds_f2 = env.datasources["fact2"]

    def make_cte(name, concepts, ds):
        qds = QueryDatasource(
            input_concepts=concepts,
            output_concepts=concepts,
            datasources=[ds],
            grain=BuildGrain(),
            joins=[],
            source_map={c.address: {ds} for c in concepts},
        )
        return CTE(
            name=name,
            output_columns=concepts,
            grain=BuildGrain(),
            source=qds,
            source_map={c.address: [ds.identifier] for c in concepts},
        )

    cte_f1 = make_cte("fact1_cte", [f1_id, shared], ds_f1)
    cte_f2 = make_cte("fact2_cte", [f2_id, shared], ds_f2)
    cte_dim = make_cte("dim_cte", [shared], ds_dim)

    # Two pairs with same right concept but different left CTEs
    join = Join(
        right_cte=cte_dim,
        jointype=JoinType.LEFT_OUTER,
        joinkey_pairs=[
            CTEConceptPair(
                left=shared,
                right=shared,
                existing_datasource=ds_f1,
                cte=cte_f1,
            ),
            CTEConceptPair(
                left=shared,
                right=shared,
                existing_datasource=ds_f2,
                cte=cte_f2,
            ),
        ],
    )

    def null_wrapper(left: str, right: str, modifiers: list[Modifier]) -> str:
        return f"{left} = {right}"

    result = render_join(
        join=join,
        quote_character=dialect.QUOTE_CHARACTER,
        render_expr_func=dialect.render_expr,
        cte=cte_f1,
        use_map=defaultdict(set),
        null_wrapper=null_wrapper,
    )
    assert result is not None
    assert "coalesce(" in result
    assert "fact1_cte" in result
    assert "fact2_cte" in result
    assert "dim_cte" in result


def test_reduce_concept_pairs_multi_partial():
    """reduce_concept_pairs dedup behavior:
    - Different left concepts for same right: always keep both (distinct join keys)
    - Same left concept from different datasources: keep both only when PARTIAL
    """
    env, _ = parse("""
key shared_id int;
key fact1_id int;
property fact1_id.f1_shared int;
key fact2_id int;
property fact2_id.f2_shared int;

datasource dim (id:shared_id) grain(shared_id) address dim_table;
datasource fact1 (id:fact1_id, sid:f1_shared) grain(fact1_id) address fact1_table;
datasource fact2 (id:fact2_id, sid:f2_shared) grain(fact2_id) address fact2_table;
    """)
    env = env.materialize_for_select()
    shared = env.concepts["shared_id"]
    f1_shared = env.concepts["f1_shared"]
    f2_shared = env.concepts["f2_shared"]
    ds_f1 = env.datasources["fact1"]
    ds_f2 = env.datasources["fact2"]
    ds_dim = env.datasources["dim"]

    # Different left concepts for same right -> keep both (distinct join keys)
    pairs = [
        ConceptPair(left=f1_shared, right=shared, existing_datasource=ds_f1),
        ConceptPair(left=f2_shared, right=shared, existing_datasource=ds_f2),
    ]
    result = reduce_concept_pairs(pairs, ds_dim)
    assert len(result) == 2

    # Same left concept and same existing_datasource -> deduplicate
    pairs_same = [
        ConceptPair(left=f1_shared, right=shared, existing_datasource=ds_f1),
        ConceptPair(left=f1_shared, right=shared, existing_datasource=ds_f1),
    ]
    result_same = reduce_concept_pairs(pairs_same, ds_dim)
    assert len(result_same) == 1

    # Same left concept, different datasources, WITH PARTIAL -> keep both
    pairs_partial = [
        ConceptPair(
            left=shared,
            right=shared,
            existing_datasource=ds_f1,
            modifiers=[Modifier.PARTIAL],
        ),
        ConceptPair(
            left=shared,
            right=shared,
            existing_datasource=ds_f2,
            modifiers=[Modifier.PARTIAL],
        ),
    ]
    result_partial = reduce_concept_pairs(pairs_partial, ds_dim)
    assert len(result_partial) == 2

    # Same left concept, different datasources, NO PARTIAL -> deduplicate
    pairs_no_partial = [
        ConceptPair(left=shared, right=shared, existing_datasource=ds_f1),
        ConceptPair(left=shared, right=shared, existing_datasource=ds_f2),
    ]
    result_no_partial = reduce_concept_pairs(pairs_no_partial, ds_dim)
    assert len(result_no_partial) == 1


def test_reduce_concept_pairs_outer_join_keeps_dups():
    """For OUTER joins, ``reduce_concept_pairs`` must keep duplicate
    ``(right, left.address)`` pairs from different datasources — the JOIN ON
    renderer (or downstream prune) needs all candidates to decide which side
    is preserved."""
    env, _ = parse("""
key shared_id int;
key fact1_id int;
key fact2_id int;

datasource dim (id:shared_id) grain(shared_id) address dim_table;
datasource fact1 (id:fact1_id, sid:shared_id) grain(fact1_id) address fact1_table;
datasource fact2 (id:fact2_id, sid:shared_id) grain(fact2_id) address fact2_table;
    """)
    env = env.materialize_for_select()
    shared = env.concepts["shared_id"]
    ds_f1 = env.datasources["fact1"]
    ds_f2 = env.datasources["fact2"]
    ds_dim = env.datasources["dim"]

    pairs = [
        ConceptPair(left=shared, right=shared, existing_datasource=ds_f1),
        ConceptPair(left=shared, right=shared, existing_datasource=ds_f2),
    ]
    # INNER (default) deduplicates because neither pair is partial.
    inner_result = reduce_concept_pairs(pairs, ds_dim, JoinType.INNER)
    assert len(inner_result) == 1

    # LEFT OUTER preserves both pairs — one of the lefts may be the NULL-able
    # side of an upstream outer join, and a later pass must be free to pick
    # the preserved one.
    for outer in (JoinType.LEFT_OUTER, JoinType.RIGHT_OUTER, JoinType.FULL):
        outer_result = reduce_concept_pairs(list(pairs), ds_dim, outer)
        assert len(outer_result) == 2, f"{outer.name} should keep both pairs"


def test_compute_outer_null_status():
    """Each datasource's null-status counts how many upstream joins place it
    on the NULL-able side. A score of 0 means it's preserved everywhere."""
    env, _ = parse("""
key id int;
datasource a (id:id) grain(id) address a;
datasource b (id:id) grain(id) address b;
datasource c (id:id) grain(id) address c;
    """)
    env = env.materialize_for_select()
    ds_a = env.datasources["a"]
    ds_b = env.datasources["b"]
    ds_c = env.datasources["c"]

    # A LEFT OUTER B  →  B is NULL-able (score 1), A preserved (score 0).
    joins = [
        BaseJoin(
            left_datasource=ds_a,
            right_datasource=ds_b,
            join_type=JoinType.LEFT_OUTER,
            concepts=[],
        )
    ]
    status = compute_outer_null_status(joins)
    assert status.get(ds_a.identifier, 0) == 0
    assert status[ds_b.identifier] == 1

    # A RIGHT OUTER B  →  A is NULL-able, B preserved.
    joins = [
        BaseJoin(
            left_datasource=ds_a,
            right_datasource=ds_b,
            join_type=JoinType.RIGHT_OUTER,
            concepts=[],
        )
    ]
    status = compute_outer_null_status(joins)
    assert status[ds_a.identifier] == 1
    assert status.get(ds_b.identifier, 0) == 0

    # FULL → both NULL-able.
    joins = [
        BaseJoin(
            left_datasource=ds_a,
            right_datasource=ds_b,
            join_type=JoinType.FULL,
            concepts=[],
        )
    ]
    status = compute_outer_null_status(joins)
    assert status[ds_a.identifier] == 1
    assert status[ds_b.identifier] == 1

    # INNER → no contribution.
    joins = [
        BaseJoin(
            left_datasource=ds_a,
            right_datasource=ds_b,
            join_type=JoinType.INNER,
            concepts=[],
        )
    ]
    status = compute_outer_null_status(joins)
    assert status == {}

    # Stacked outer joins accumulate. A LEFT B, then (A merged) LEFT C →
    # B and C each get score 1.
    joins = [
        BaseJoin(
            left_datasource=ds_a,
            right_datasource=ds_b,
            join_type=JoinType.LEFT_OUTER,
            concepts=[],
        ),
        BaseJoin(
            left_datasource=None,  # multi-left after first join
            right_datasource=ds_c,
            join_type=JoinType.LEFT_OUTER,
            concepts=[],
        ),
    ]
    status = compute_outer_null_status(joins)
    assert status.get(ds_a.identifier, 0) == 0
    assert status[ds_b.identifier] == 1
    assert status[ds_c.identifier] == 1


def test_prune_outer_join_pairs_drops_nullable_side():
    """When a LEFT/RIGHT OUTER join has multi-left pairs sharing the same
    ``(right_addr, left_addr)``, prune to keep the most-preserved side. The
    NULL-able side's pair would render ``coalesce(preserved, nullable) =
    right`` — always equal to ``preserved``, so pure SQL bloat."""
    env, _ = parse("""
key shared_id int;
key fact1_id int;
key fact2_id int;

datasource dim (id:shared_id) grain(shared_id) address dim_table;
datasource fact1 (id:fact1_id, sid:shared_id) grain(fact1_id) address fact1_table;
datasource fact2 (id:fact2_id, sid:shared_id) grain(fact2_id) address fact2_table;
    """)
    env = env.materialize_for_select()
    shared = env.concepts["shared_id"]
    ds_dim = env.datasources["dim"]
    ds_f1 = env.datasources["fact1"]
    ds_f2 = env.datasources["fact2"]

    # f1 LEFT OUTER f2 → f2 is NULL-able. Then dim LEFT JOIN with multi-left
    # candidates {f1, f2} sharing shared_id. The pair from f2 should be pruned.
    upstream_join = BaseJoin(
        left_datasource=ds_f1,
        right_datasource=ds_f2,
        join_type=JoinType.LEFT_OUTER,
        concepts=[],
    )
    multi_left_join = BaseJoin(
        left_datasource=None,
        right_datasource=ds_dim,
        join_type=JoinType.LEFT_OUTER,
        concept_pairs=[
            ConceptPair(left=shared, right=shared, existing_datasource=ds_f1),
            ConceptPair(left=shared, right=shared, existing_datasource=ds_f2),
        ],
    )
    joins = [upstream_join, multi_left_join]
    null_status = compute_outer_null_status(joins)
    assert null_status[ds_f2.identifier] == 1
    prune_outer_join_pairs(joins, null_status)
    # Only the preserved (f1) pair survives.
    assert len(multi_left_join.concept_pairs) == 1
    assert (
        multi_left_join.concept_pairs[0].existing_datasource.identifier
        == ds_f1.identifier
    )

    # FULL join is left untouched: both sides may be NULL → keep all pairs so
    # the renderer COALESCEs.
    full_join = BaseJoin(
        left_datasource=None,
        right_datasource=ds_dim,
        join_type=JoinType.FULL,
        concept_pairs=[
            ConceptPair(left=shared, right=shared, existing_datasource=ds_f1),
            ConceptPair(left=shared, right=shared, existing_datasource=ds_f2),
        ],
    )
    prune_outer_join_pairs([full_join], null_status)
    assert len(full_join.concept_pairs) == 2

    # Distinct (right, left_addr) groups are independent: pruning one doesn't
    # touch the other.
    other_concept = env.concepts["fact1_id"]
    other_concept_f2_alias = env.concepts["fact2_id"]
    # Two distinct join keys on the same outer join: shared_id (multi-left)
    # and a distinct key from each side. Only the multi-left group dedups.
    multi_left_with_distinct = BaseJoin(
        left_datasource=None,
        right_datasource=ds_dim,
        join_type=JoinType.LEFT_OUTER,
        concept_pairs=[
            ConceptPair(left=shared, right=shared, existing_datasource=ds_f1),
            ConceptPair(left=shared, right=shared, existing_datasource=ds_f2),
            ConceptPair(left=other_concept, right=shared, existing_datasource=ds_f1),
            ConceptPair(
                left=other_concept_f2_alias,
                right=shared,
                existing_datasource=ds_f2,
            ),
        ],
    )
    prune_outer_join_pairs([upstream_join, multi_left_with_distinct], null_status)
    # shared/shared group → 1 pair (preserved). Distinct-left groups → unchanged.
    addresses = sorted(
        (p.left.address, p.existing_datasource.identifier)
        for p in multi_left_with_distinct.concept_pairs
    )
    assert (shared.address, ds_f1.identifier) in addresses
    assert (shared.address, ds_f2.identifier) not in addresses
    assert (other_concept.address, ds_f1.identifier) in addresses
    assert (other_concept_f2_alias.address, ds_f2.identifier) in addresses
