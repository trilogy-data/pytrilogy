from trilogy.core.enums import ComparisonOperator, FunctionType, JoinType, SourceType
from trilogy.core.models.build import (
    BuildColumnAssignment,
    BuildComparison,
    BuildConcept,
    BuildDatasource,
    BuildFunction,
    BuildGrain,
    BuildSubselectComparison,
)
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
    _add_render_dependencies,
    _base_datasource,
    _datasource_matches_raw_id,
    _DimDescriptor,
    _find_dim_cte_for_qds,
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


def test_union_dim_pushdown_plain_target_moves_dim_and_strips(test_environment):
    """_apply_plain pushes a shared dim into the consumers' common parent CTE
    (the dedup) and strips it from each consumer, so the parent becomes a
    direct dim-joining consumer of the union below it."""
    env = test_environment.materialize_for_select()
    products = env.datasources["products"]
    category = env.datasources["category"]
    product_id = env.concepts["product_id"]
    category_id = env.concepts["category_id"]
    category_name = env.concepts["category_name"]

    target = _branch_cte("dedup", products, [product_id, category_id])
    dim = CTE.from_datasource(category)
    dim.name = "category_dim"
    atom = BuildComparison(
        left=category_name,
        right="special",
        operator=ComparisonOperator.EQ,
    )
    consumer = _dim_consumer(target, dim, category_id, category_id, category_name, atom)
    descriptor = _DimDescriptor(
        dim_qds=dim.source,
        join_qds_id=dim.source.identifier,
        key_pairs=[
            ConceptPair(
                left=category_id,
                right=category_id,
                existing_datasource=target.source,
            )
        ],
        dim_concepts=[category_id, category_name],
        where_atoms=[atom],
        strip_safe=True,
    )

    assert UnionDimPushdown()._apply_plain(target, [consumer], descriptor) is True
    assert target.source.joins
    assert target.condition == atom
    assert category_name.address in {c.address for c in target.output_columns}
    assert consumer.source.joins == []
    assert consumer.joins == []
    assert consumer.condition is None


def test_union_dim_pushdown_plain_target_requires_strippable_filter(test_environment):
    """_apply_plain is MOVE-only: without a filter (or when not strip-safe) it
    declines, leaving the dim join on the consumers."""
    env = test_environment.materialize_for_select()
    products = env.datasources["products"]
    category = env.datasources["category"]
    product_id = env.concepts["product_id"]
    category_id = env.concepts["category_id"]
    category_name = env.concepts["category_name"]

    target = _branch_cte("dedup", products, [product_id, category_id])
    dim = CTE.from_datasource(category)
    dim.name = "category_dim"
    consumer = _dim_consumer(target, dim, category_id, category_id, category_name)
    descriptor = _DimDescriptor(
        dim_qds=dim.source,
        join_qds_id=dim.source.identifier,
        key_pairs=[
            ConceptPair(
                left=category_id,
                right=category_id,
                existing_datasource=target.source,
            )
        ],
        dim_concepts=[category_id, category_name],
        where_atoms=[],
        strip_safe=True,
    )

    assert UnionDimPushdown()._apply_plain(target, [consumer], descriptor) is False
    assert target.source.joins == []


def test_union_dim_pushdown_coarsens_dead_fk_grain(test_environment):
    """After a dim is moved into a pure dedup, its FK (no longer read by the
    pure-dedup consumer) is dropped from the dedup's grain/output."""
    env = test_environment.materialize_for_select()
    products = env.datasources["products"]
    category = env.datasources["category"]
    product_id = env.concepts["product_id"]
    category_id = env.concepts["category_id"]
    category_name = env.concepts["category_name"]

    # target: pure dedup grained on (product_id, category_id), with the coarser
    # category_name (the exposed dim attr) also output.
    target = _branch_cte("dedup", products, [product_id, category_id, category_name])
    target.group_to_grain = True
    target.grain = BuildGrain(components={product_id.address, category_id.address})
    # consumer: pure dedup that reads only product_id (never the FK category_id)
    consumer = _branch_cte("consumer", products, [product_id])
    consumer.group_to_grain = True
    consumer.grain = BuildGrain(components={product_id.address})
    consumer.source_map[category_id.address] = ["dedup"]
    descriptor = _DimDescriptor(
        dim_qds=category.source if hasattr(category, "source") else category,
        join_qds_id="category",
        key_pairs=[
            ConceptPair(
                left=category_id,
                right=category_id,
                existing_datasource=target.source,
            )
        ],
        dim_concepts=[category_id, category_name],
        where_atoms=[],
        strip_safe=True,
    )

    assert UnionDimPushdown()._coarsen_dead_fk_grain(target, descriptor, [consumer])
    assert category_id.address not in target.grain.components
    assert category_id.address not in {c.address for c in target.output_columns}
    assert category_id.address not in consumer.source_map


def test_union_dim_pushdown_coarsen_keeps_fk_when_consumer_reads_it(test_environment):
    """The FK stays when a consumer still renders it (would change results)."""
    env = test_environment.materialize_for_select()
    products = env.datasources["products"]
    product_id = env.concepts["product_id"]
    category_id = env.concepts["category_id"]
    category_name = env.concepts["category_name"]

    target = _branch_cte("dedup", products, [product_id, category_id, category_name])
    target.group_to_grain = True
    target.grain = BuildGrain(components={product_id.address, category_id.address})
    # consumer groups on the FK -> it is rendered, must not be dropped
    consumer = _branch_cte("consumer", products, [product_id, category_id])
    consumer.group_to_grain = True
    consumer.grain = BuildGrain(components={product_id.address, category_id.address})
    descriptor = _DimDescriptor(
        dim_qds=products,
        join_qds_id="category",
        key_pairs=[
            ConceptPair(
                left=category_id,
                right=category_id,
                existing_datasource=target.source,
            )
        ],
        dim_concepts=[category_id, category_name],
        where_atoms=[],
        strip_safe=True,
    )

    assert not UnionDimPushdown()._coarsen_dead_fk_grain(target, descriptor, [consumer])
    assert category_id.address in target.grain.components


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


def test_union_dim_pushdown_resolves_inlined_dim_by_raw_datasource_id(
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

    assert _find_dim_cte_for_qds(consumer, category.identifier) is dim


def test_union_dim_pushdown_finds_dim_from_join_when_not_parent(test_environment):
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
    consumer.parent_ctes = [union]

    assert _find_dim_cte_for_qds(consumer, dim.source.identifier) is dim


def test_union_dim_pushdown_rejects_ambiguous_raw_datasource_binding(
    test_environment,
):
    env = test_environment.materialize_for_select()
    products = env.datasources["products"]
    category = env.datasources["category"]
    product_id = env.concepts["product_id"]
    category_id = env.concepts["category_id"]

    branch = _branch_cte("branch", products, [product_id, category_id])
    union = _union_cte("unioned", [branch], [product_id, category_id])
    first_dim = CTE.from_datasource(category)
    first_dim.name = "category_dim_one"
    second_dim = CTE.from_datasource(category)
    second_dim.name = "category_dim_two"
    consumer = _dim_consumer(
        union,
        first_dim,
        category_id,
        category_id,
        env.concepts["category_name"],
    )
    consumer.parent_ctes = [union]
    consumer.inlined_parents = [first_dim, second_dim]
    consumer.joins.append(
        Join(
            right_cte=second_dim,
            jointype=JoinType.INNER,
            left_cte=union,
            joinkey_pairs=[
                CTEConceptPair(
                    left=category_id,
                    right=category_id,
                    existing_datasource=union.source,
                    cte=union,
                )
            ],
        )
    )

    assert _find_dim_cte_for_qds(consumer, category.identifier) is None


def test_union_dim_pushdown_helper_paths_for_raw_datasources(test_environment):
    env = test_environment.materialize_for_select()
    category = env.datasources["category"]
    category_id = env.concepts["category_id"]
    category_name = env.concepts["category_name"]

    raw = BuildDatasource(
        name="raw_category",
        columns=[BuildColumnAssignment(alias="category_id", concept=category_id)],
        address="raw_category_addr",
        grain=BuildGrain(),
    )
    wrapped = QueryDatasource.from_datasource(raw)
    dependency = BuildFunction(
        operator=FunctionType.ADD,
        arguments=[category_id, 1],
        output_data_type=category_name.datatype,
        output_purpose=category_name.purpose,
        arg_count=2,
    )
    derived = BuildConcept(
        name="derived_category_name",
        canonical_name="derived_category_name",
        datatype=category_name.datatype,
        purpose=category_name.purpose,
        build_is_aggregate=False,
        grain=BuildGrain(),
        lineage=dependency,
    )
    concepts = _add_render_dependencies([derived], raw)

    assert _base_datasource(raw) is None
    assert _base_datasource(wrapped) is raw
    assert _datasource_matches_raw_id(raw, raw.identifier) is True
    assert category_id in concepts
    assert (
        _find_dim_cte_for_qds(
            _dim_consumer(
                _union_cte(
                    "unioned",
                    [_branch_cte("branch", category, [category_id])],
                    [category_id],
                ),
                CTE.from_datasource(category),
                category_id,
                category_id,
                category_name,
            ),
            raw.identifier,
        )
        is None
    )


def test_union_dim_pushdown_propagates_existence_dependencies(test_environment):
    env = test_environment.materialize_for_select()
    products = env.datasources["products"]
    category = env.datasources["category"]
    product_id = env.concepts["product_id"]
    category_id = env.concepts["category_id"]
    category_name = env.concepts["category_name"]

    branch = _branch_cte("branch", products, [product_id, category_id])
    source_consumer = _branch_cte("consumer", products, [product_id, category_id])
    source_consumer.existence_source_map = {category_name.address: ["category_parent"]}
    category_parent = CTE.from_datasource(category)
    category_parent.name = "category_parent"
    source_consumer.parent_ctes = [category_parent]
    atom = BuildSubselectComparison(
        left=product_id,
        right=category_name,
        operator=ComparisonOperator.IN,
    )

    UnionDimPushdown()._propagate_existence_sources(branch, source_consumer, atom)

    assert branch.existence_source_map[category_name.address] == ["category_parent"]
    assert branch.parent_ctes == [category_parent]


def test_dim_cte_exposes_rejects_filter_derivative_of_dim(test_environment):
    """A CTE built over the dim's base datasource but exposing only a derived
    column (not the dim's own concepts) must not be accepted as the dim join
    target — q02's ``relevent_week_seq`` GROUP over ``date_dim`` is the live
    case that otherwise rendered an INVALID_ALIAS into the union branches."""
    env = test_environment.materialize_for_select()
    category = env.datasources["category"]
    category_id = env.concepts["category_id"]
    category_name = env.concepts["category_name"]
    category_name_length = env.concepts["category_name_length"]

    descriptor = _DimDescriptor(
        dim_qds=category,
        join_qds_id=category.identifier,
        key_pairs=[
            ConceptPair(
                left=category_id,
                right=category_id,
                existing_datasource=category,
            )
        ],
        dim_concepts=[category_id, category_name],
        where_atoms=[],
        strip_safe=True,
    )
    rule = UnionDimPushdown()

    exposing = _branch_cte("dim_full", category, [category_id, category_name])
    assert rule._dim_cte_exposes(exposing, descriptor) is True

    derivative = _branch_cte("dim_derived", category, [category_name_length])
    assert rule._dim_cte_exposes(derivative, descriptor) is False
