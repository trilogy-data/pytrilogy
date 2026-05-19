from trilogy.core.enums import ComparisonOperator, JoinType, SourceType
from trilogy.core.models.build import BuildComparison, BuildConcept, BuildGrain
from trilogy.core.models.execute import (
    CTE,
    BaseJoin,
    ConceptPair,
    CTEConceptPair,
    Join,
    QueryDatasource,
    UnionCTE,
)
from trilogy.core.optimizations.union_dim_pushdown import (
    UnionDimPushdown,
    _DimDescriptor,
)


def _branch_cte(name: str, datasource, columns: list[BuildConcept]) -> CTE:
    return CTE(
        name=name,
        source=QueryDatasource(
            input_concepts=columns,
            output_concepts=columns,
            datasources=[datasource],
            grain=BuildGrain(),
            joins=[],
            source_map={c.address: {datasource} for c in columns},
            base_datasource=datasource,
        ),
        output_columns=list(columns),
        parent_ctes=[],
        grain=BuildGrain(),
        source_map={c.address: [datasource.name] for c in columns},
        existence_source_map={},
    )


def _union_cte(
    name: str,
    branches: list[CTE],
    columns: list[BuildConcept],
) -> UnionCTE:
    return UnionCTE(
        name=name,
        source=QueryDatasource(
            input_concepts=columns,
            output_concepts=columns,
            datasources=[b.source for b in branches],
            grain=BuildGrain(),
            joins=[],
            source_map={c.address: {b.source for b in branches} for c in columns},
            source_type=SourceType.UNION,
        ),
        parent_ctes=list(branches),
        internal_ctes=list(branches),
        output_columns=list(columns),
        grain=BuildGrain(),
    )


def _dim_consumer(
    union: UnionCTE,
    dim: CTE,
    left_key: BuildConcept,
    right_key: BuildConcept,
    dim_value: BuildConcept,
    condition: BuildComparison | None = None,
) -> CTE:
    pair = ConceptPair(
        left=left_key,
        right=right_key,
        existing_datasource=union.source,
    )
    cte_pair = CTEConceptPair(
        left=left_key,
        right=right_key,
        existing_datasource=union.source,
        cte=union,
    )
    return CTE(
        name="consumer",
        source=QueryDatasource(
            input_concepts=[left_key, dim_value],
            output_concepts=[left_key, dim_value],
            datasources=[union.source, dim.source],
            grain=BuildGrain(),
            joins=[
                BaseJoin(
                    right_datasource=dim.source,
                    join_type=JoinType.INNER,
                    concept_pairs=[pair],
                )
            ],
            source_map={
                left_key.address: {union.source},
                right_key.address: {union.source, dim.source},
                dim_value.address: {dim.source},
            },
        ),
        output_columns=[left_key],
        parent_ctes=[union, dim],
        condition=condition,
        grain=BuildGrain(),
        source_map={
            left_key.address: [union.name],
            right_key.address: [union.name, dim.name],
            dim_value.address: [dim.name],
        },
        existence_source_map={},
        joins=[
            Join(
                right_cte=dim,
                jointype=JoinType.INNER,
                left_cte=union,
                joinkey_pairs=[cte_pair],
            )
        ],
    )


def test_union_dim_pushdown_preflights_all_branches_before_mutating(
    test_environment,
):
    env = test_environment.materialize_for_select()
    products = env.datasources["products"]
    category = env.datasources["category"]
    product_id = env.concepts["product_id"]
    category_id = env.concepts["category_id"]
    category_name = env.concepts["category_name"]

    valid = _branch_cte("valid", products, [product_id, category_id])
    missing_fk = _branch_cte("missing_fk", products, [product_id])
    union = _union_cte("unioned", [valid, missing_fk], [product_id, category_id])
    dim = CTE.from_datasource(category)
    dim.name = "category_dim"
    atom = BuildComparison(
        left=category_name,
        right="special",
        operator=ComparisonOperator.EQ,
    )
    consumer = _dim_consumer(union, dim, category_id, category_id, category_name, atom)
    descriptor = _DimDescriptor(
        dim_qds=dim.source,
        join_qds_id=dim.source.identifier,
        key_pairs=[
            ConceptPair(
                left=category_id,
                right=category_id,
                existing_datasource=valid.source,
            )
        ],
        dim_concepts=[category_id, category_name],
        where_atoms=[atom],
        strip_safe=True,
    )

    assert UnionDimPushdown()._apply(union, [consumer], descriptor) is False
    assert valid.source.joins == []
    assert valid.joins == []
    assert valid.condition is None
    assert category_name.address not in valid.source_map


def test_union_dim_pushdown_strips_direct_consumer_when_safe(test_environment):
    env = test_environment.materialize_for_select()
    products = env.datasources["products"]
    category = env.datasources["category"]
    product_id = env.concepts["product_id"]
    category_id = env.concepts["category_id"]
    category_name = env.concepts["category_name"]

    branch1 = _branch_cte("branch1", products, [product_id, category_id])
    branch2 = _branch_cte("branch2", products, [product_id, category_id])
    union = _union_cte("unioned", [branch1, branch2], [product_id, category_id])
    dim = CTE.from_datasource(category)
    dim.name = "category_dim"
    atom = BuildComparison(
        left=category_name,
        right="special",
        operator=ComparisonOperator.EQ,
    )
    consumer = _dim_consumer(union, dim, category_id, category_id, category_name, atom)

    optimized, _ = UnionDimPushdown().optimize(union, {union.name: [consumer]})

    assert optimized is True
    assert all(branch.source.joins for branch in [branch1, branch2])
    assert all(branch.condition == atom for branch in [branch1, branch2])
    assert category_name.address in {c.address for c in union.output_columns}
    assert consumer.condition is None
    assert consumer.source.joins == []
    assert consumer.joins == []


def test_union_dim_pushdown_filter_only_requires_a_filter(test_environment):
    env = test_environment.materialize_for_select()
    products = env.datasources["products"]
    category = env.datasources["category"]
    product_id = env.concepts["product_id"]
    category_id = env.concepts["category_id"]
    category_name = env.concepts["category_name"]

    branch1 = _branch_cte("branch1", products, [product_id, category_id])
    branch2 = _branch_cte("branch2", products, [product_id, category_id])
    union = _union_cte("unioned", [branch1, branch2], [product_id, category_id])
    dim = CTE.from_datasource(category)
    dim.name = "category_dim"
    consumer = _dim_consumer(union, dim, category_id, category_id, category_name)
    descriptor = _DimDescriptor(
        dim_qds=dim.source,
        join_qds_id=dim.source.identifier,
        key_pairs=[
            ConceptPair(
                left=category_id,
                right=category_id,
                existing_datasource=branch1.source,
            )
        ],
        dim_concepts=[category_id, category_name],
        where_atoms=[],
        strip_safe=False,
    )

    assert UnionDimPushdown()._apply(union, [consumer], descriptor) is False
    assert branch1.source.joins == []
    assert branch2.source.joins == []


def test_union_dim_pushdown_uses_branch_binding_key_for_dim_source(
    test_environment,
):
    env = test_environment.materialize_for_select()
    products = env.datasources["products"]
    category = env.datasources["category"]
    product_id = env.concepts["product_id"]
    category_id = env.concepts["category_id"]
    category_name = env.concepts["category_name"]

    branch = _branch_cte("branch", products, [product_id, category_id])
    dim = CTE.from_datasource(category)
    dim.name = "category_dim"
    descriptor = _DimDescriptor(
        dim_qds=dim.source,
        join_qds_id=dim.source.identifier,
        key_pairs=[
            ConceptPair(
                left=category_id,
                right=category_id,
                existing_datasource=branch.source,
            )
        ],
        dim_concepts=[category_id, category_name],
        where_atoms=[],
        strip_safe=True,
    )

    assert UnionDimPushdown()._push_into_branch(branch, dim, descriptor) is True
    assert branch.source_map[category_name.address] == [branch.source_key_for(dim)]
    assert branch.source_map[category_name.address] == [dim.name]


def test_union_dim_pushdown_uses_inlined_binding_key_for_dim_source(
    test_environment,
):
    env = test_environment.materialize_for_select()
    products = env.datasources["products"]
    category = env.datasources["category"]
    product_id = env.concepts["product_id"]
    category_id = env.concepts["category_id"]
    category_name = env.concepts["category_name"]

    branch = _branch_cte("branch", products, [product_id, category_id])
    union = _union_cte("unioned", [branch], [product_id, category_id])
    dim = CTE.from_datasource(category)
    dim.name = "category_dim"
    consumer = _dim_consumer(union, dim, category_id, category_id, category_name)
    assert consumer.inline_parent_datasource(dim) is True
    descriptor = _DimDescriptor(
        dim_qds=dim.source,
        join_qds_id=dim.source.identifier,
        key_pairs=[
            ConceptPair(
                left=category_id,
                right=category_id,
                existing_datasource=branch.source,
            )
        ],
        dim_concepts=[category_id, category_name],
        where_atoms=[],
        strip_safe=True,
    )

    assert (
        UnionDimPushdown()._push_into_branch(branch, dim, descriptor, consumer) is True
    )
    assert branch.source_map[category_name.address] == [branch.source_key_for(dim)]
    assert branch.source_map[category_name.address] == [category.safe_identifier]
    assert branch.parent_ctes == []
    assert branch.inlined_parents == [dim]
