from collections import defaultdict

import pytest

from trilogy import parse
from trilogy.core.enums import JoinType, Modifier
from trilogy.core.models.build import BuildGrain
from trilogy.core.models.execute import CTE, CTEConceptPair, Join, QueryDatasource
from trilogy.core.processing.utility import (
    ConceptPair,
    get_join_type,
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
    y_concept = env.concepts["y"]
    ds = env.datasources["x_source"]
    rendered = render_join_concept(
        name="test",
        cte=CTE(
            name="test",
            output_columns=[concept, y_concept],
            grain=BuildGrain(),
            source=QueryDatasource(
                input_concepts=[concept, y_concept],
                output_concepts=[concept, y_concept],
                datasources=[ds],
                grain=BuildGrain(),
                joins=[],
                source_map={concept.address: {ds}, y_concept.address: {ds}},
            ),
            source_map={
                concept.address: [ds.identifier],
                y_concept.address: [ds.identifier],
            },
        ),
        render_expr=x.render_expr,
        quote_character=x.QUOTE_CHARACTER,
        inlined_ctes=["test"],
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
    y_concept = env.concepts["y"]
    ds = env.datasources["x_source"]
    rendered = render_join_concept(
        name="test",
        cte=CTE(
            name="test",
            output_columns=[concept, y_concept],
            grain=BuildGrain(),
            source=QueryDatasource(
                input_concepts=[concept, y_concept],
                output_concepts=[concept, y_concept],
                datasources=[ds],
                grain=BuildGrain(),
                joins=[],
                source_map={concept.address: {ds}, y_concept.address: {ds}},
            ),
            source_map={
                concept.address: [ds.identifier],
                y_concept.address: [ds.identifier],
            },
        ),
        render_expr=x.render_expr,
        quote_character=x.QUOTE_CHARACTER,
        inlined_ctes=["test"],
        concept=concept,
        use_map=defaultdict(set),
    )

    assert rendered == "y + 1"


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


@pytest.mark.parametrize(
    "left_partial,left_nullable,right_partial,right_nullable,expected",
    [
        (False, False, False, False, JoinType.INNER),
        (True, False, False, False, JoinType.RIGHT_OUTER),
        (False, True, False, False, JoinType.RIGHT_OUTER),
        (False, False, True, False, JoinType.LEFT_OUTER),
        (False, False, False, True, JoinType.LEFT_OUTER),
        (True, False, True, False, JoinType.FULL),
        (False, True, False, True, JoinType.FULL),
        (True, True, False, False, JoinType.RIGHT_OUTER),
        (False, False, True, True, JoinType.LEFT_OUTER),
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
    """Test that ANY partial/nullable key among multiple connecting keys triggers incomplete"""
    left = "table_a"
    right = "table_b"
    partials = {"table_b": ["key1"]}  # Only one of three keys is partial
    nullables = {}
    all_connecting_keys = {"key1", "key2", "key3"}

    result = get_join_type(left, right, partials, nullables, all_connecting_keys)
    assert result == JoinType.LEFT_OUTER


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
    """reduce_concept_pairs should only keep multiple pairs for the same right
    key when they have PARTIAL modifiers (FULL JOIN semantics need COALESCE)."""
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

    # Different left concepts, NOT partial -> deduplicate
    pairs = [
        ConceptPair(left=f1_shared, right=shared, existing_datasource=ds_f1),
        ConceptPair(left=f2_shared, right=shared, existing_datasource=ds_f2),
    ]
    result = reduce_concept_pairs(pairs, ds_dim)
    assert len(result) == 1

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
