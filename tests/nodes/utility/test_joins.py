from collections import defaultdict

from trilogy import parse
from trilogy.core.models.build import BuildGrain
from trilogy.core.models.execute import CTE, QueryDatasource
from trilogy.core.processing.utility import ConceptPair, reduce_concept_pairs
from trilogy.dialect.base import BaseDialect
from trilogy.dialect.common import render_join_concept


def test_render_join_concept():
    env, _ = parse(
        """key x int;
        key y int;
    
datasource x_source (
    y:y,
    y+1:x)
    grain(x)
address x_source;

        
        """
    )
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

    env, _ = parse(
        """key x int;
        key y int;
    
datasource x_source (
    y:y,
    raw('''y + 1'''):x)
    grain(x)
address x_source;

        
        """
    )
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
    env, _ = parse(
        """
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
        """
    )

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
