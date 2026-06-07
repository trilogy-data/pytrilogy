from dataclasses import replace

from trilogy import parse
from trilogy.core.enums import (
    BooleanOperator,
    ComparisonOperator,
    FunctionType,
    JoinType,
    Purpose,
    SourceType,
)
from trilogy.core.models.build import (
    BuildComparison,
    BuildConcept,
    BuildConditional,
    BuildFunction,
    BuildGrain,
    BuildSubselectComparison,
    BuildWhereClause,
)
from trilogy.core.models.core import (
    DataType,
)
from trilogy.core.models.environment import Environment
from trilogy.core.models.execute import (
    CTE,
    CTEConceptPair,
    InstantiatedUnnestJoin,
    Join,
    QueryDatasource,
    UnionCTE,
)
from trilogy.core.optimization import (
    PredicatePushdown,
    PredicatePushdownRemove,
    _grains_equivalent,
    canonicalize_graph,
    filter_irrelevant_ctes,
)
from trilogy.core.optimizations.predicate_pushdown import (
    _consumer_outer_joins_union,
    _parent_covers_condition,
    _parent_nullable_in_cte,
    is_child_of,
)
from trilogy.core.processing.condition_utility import decompose_condition


def _simple_cte(
    name: str,
    columns,
    *,
    grain: BuildGrain | None = None,
    condition=None,
    group_to_grain: bool = False,
    source_map: dict[str, list[str]] | None = None,
    parent_ctes=None,
    joins=None,
) -> CTE:
    local_grain = grain or BuildGrain()
    return CTE(
        name=name,
        source=QueryDatasource(
            input_concepts=columns,
            output_concepts=columns,
            datasources=[],
            grain=local_grain,
            joins=[],
            source_map={c.address: set() for c in columns},
        ),
        output_columns=columns,
        source_map=source_map or {c.address: [name] for c in columns},
        grain=local_grain,
        condition=condition,
        group_to_grain=group_to_grain,
        parent_ctes=parent_ctes or [],
        joins=joins or [],
    )


def test_canonicalize_graph_dedupes_live_references_and_realigns_union_branches():
    a = BuildConcept(
        name="a",
        canonical_name="a",
        datatype=DataType.INTEGER,
        purpose=Purpose.KEY,
        build_is_aggregate=False,
        grain=BuildGrain(),
    )
    b = BuildConcept(
        name="b",
        canonical_name="b",
        datatype=DataType.INTEGER,
        purpose=Purpose.PROPERTY,
        build_is_aggregate=False,
        grain=BuildGrain(),
    )
    live_parent = _simple_cte("parent", [a])
    stale_parent = _simple_cte("parent", [a])
    child = _simple_cte(
        "child",
        [a],
        parent_ctes=[stale_parent, stale_parent],
        source_map={a.address: [stale_parent.name]},
    )
    pair = CTEConceptPair(
        left=a,
        right=a,
        existing_datasource=child.source,
        cte=stale_parent,
    )
    unnest_join = InstantiatedUnnestJoin(object_to_unnest=a)
    child.joins = [
        unnest_join,
        Join(
            right_cte=stale_parent,
            left_cte=stale_parent,
            jointype=JoinType.INNER,
            joinkey_pairs=[pair],
        ),
    ]
    short_branch = _simple_cte(
        "short_branch",
        [a],
        source_map={a.address: ["short_branch"], b.address: ["short_branch"]},
    )
    missing_branch = _simple_cte("missing_branch", [a])
    full_branch = _simple_cte("full_branch", [a, b])
    union = UnionCTE(
        name="unioned",
        source=QueryDatasource(
            input_concepts=[a, b],
            output_concepts=[a, b],
            datasources=[
                missing_branch.source,
                short_branch.source,
                full_branch.source,
            ],
            grain=BuildGrain(),
            joins=[],
            source_map={
                a.address: {
                    missing_branch.source,
                    short_branch.source,
                    full_branch.source,
                },
                b.address: {short_branch.source, full_branch.source},
            },
            source_type=SourceType.UNION,
        ),
        parent_ctes=[],
        internal_ctes=[missing_branch, short_branch, full_branch],
        output_columns=[a, b],
        grain=BuildGrain(),
    )

    canonicalize_graph([live_parent, child, union])

    assert child.parent_ctes == [live_parent]
    assert child.joins[0] is unnest_join
    assert child.joins[1].right_cte is live_parent
    assert child.joins[1].left_cte is live_parent
    assert pair.cte is live_parent
    assert missing_branch.output_columns == [a]
    assert short_branch.output_columns == full_branch.output_columns
    assert short_branch.hidden_concepts == full_branch.hidden_concepts


def test_filter_irrelevant_ctes_keeps_union_branch_parents_but_not_branches():
    a = BuildConcept(
        name="a",
        canonical_name="a",
        datatype=DataType.INTEGER,
        purpose=Purpose.KEY,
        build_is_aggregate=False,
        grain=BuildGrain(),
    )
    parent = _simple_cte("parent", [a])
    branch = _simple_cte("branch", [a], parent_ctes=[parent])
    union = UnionCTE(
        name="unioned",
        source=QueryDatasource(
            input_concepts=[a],
            output_concepts=[a],
            datasources=[branch.source],
            grain=BuildGrain(),
            joins=[],
            source_map={a.address: {branch.source}},
            source_type=SourceType.UNION,
        ),
        parent_ctes=[parent],
        internal_ctes=[branch],
        output_columns=[a],
        grain=BuildGrain(),
    )
    root = _simple_cte("root", [a], parent_ctes=[union])
    unused = _simple_cte("unused", [a])

    filtered = filter_irrelevant_ctes([parent, branch, union, root, unused], root)

    assert [cte.name for cte in filtered] == ["parent", "unioned", "root"]


def test_is_child_function():
    condition = BuildConditional(
        left=BuildComparison(left=1, right=2, operator=ComparisonOperator.EQ),
        right=BuildComparison(left=3, right=4, operator=ComparisonOperator.EQ),
        operator=BooleanOperator.AND,
    )
    assert (
        is_child_of(
            BuildComparison(left=1, right=2, operator=ComparisonOperator.EQ), condition
        )
        is True
    )
    assert (
        is_child_of(
            BuildComparison(left=3, right=4, operator=ComparisonOperator.EQ), condition
        )
        is True
    )
    assert (
        is_child_of(
            BuildComparison(left=1, right=2, operator=ComparisonOperator.EQ),
            condition.left,
        )
        is True
    )
    assert (
        is_child_of(
            BuildComparison(left=3, right=4, operator=ComparisonOperator.EQ),
            condition.right,
        )
        is True
    )
    assert (
        is_child_of(
            BuildComparison(left=1, right=2, operator=ComparisonOperator.EQ),
            condition.right,
        )
        is False
    )
    assert (
        is_child_of(
            BuildComparison(left=3, right=4, operator=ComparisonOperator.EQ),
            condition.left,
        )
        is False
    )


def test_parent_nullable_detects_right_outer_join_pair_cte():
    key = BuildConcept(
        name="k",
        canonical_name="k",
        datatype=DataType.INTEGER,
        purpose=Purpose.KEY,
        build_is_aggregate=False,
        grain=BuildGrain(),
    )
    parent = _simple_cte("parent", [key])
    right = _simple_cte("right", [key])
    child = _simple_cte("child", [key])
    child.joins = [
        Join(
            right_cte=right,
            jointype=JoinType.RIGHT_OUTER,
            joinkey_pairs=[
                CTEConceptPair(
                    left=key,
                    right=key,
                    existing_datasource=child.source,
                    cte=parent,
                )
            ],
        )
    ]

    assert _parent_nullable_in_cte(child, parent.name) is True


def test_predicate_pushdown_remove_drops_existence_only_parent():
    row = BuildConcept(
        name="row_id",
        canonical_name="row_id",
        datatype=DataType.INTEGER,
        purpose=Purpose.KEY,
        build_is_aggregate=False,
        grain=BuildGrain(),
    )
    exists = BuildConcept(
        name="exists_id",
        canonical_name="exists_id",
        datatype=DataType.INTEGER,
        purpose=Purpose.KEY,
        build_is_aggregate=False,
        grain=BuildGrain(),
    )
    condition = BuildSubselectComparison(
        left=row,
        right=exists,
        operator=ComparisonOperator.IN,
    )
    filtered_parent = _simple_cte("filtered_parent", [row], condition=condition)
    existence_parent = _simple_cte("existence_parent", [exists])
    consumer = _simple_cte(
        "consumer",
        [row, exists],
        condition=condition,
        parent_ctes=[filtered_parent, existence_parent],
        source_map={
            row.address: [filtered_parent.name],
            exists.address: [existence_parent.name],
        },
    )

    optimized, _ = PredicatePushdownRemove().optimize(consumer, {})

    assert optimized is True
    assert consumer.condition is None
    assert consumer.parent_ctes == [filtered_parent]


def test_predicate_pushdown_union_branch_propagates_existence_dependency():
    row = BuildConcept(
        name="row_id",
        canonical_name="row_id",
        datatype=DataType.INTEGER,
        purpose=Purpose.KEY,
        build_is_aggregate=False,
        grain=BuildGrain(),
    )
    exists = BuildConcept(
        name="exists_id",
        canonical_name="exists_id",
        datatype=DataType.INTEGER,
        purpose=Purpose.KEY,
        build_is_aggregate=False,
        grain=BuildGrain(),
    )
    branch = _simple_cte("branch", [row])
    union = UnionCTE(
        name="unioned",
        source=QueryDatasource(
            input_concepts=[row],
            output_concepts=[row],
            datasources=[branch.source],
            grain=BuildGrain(),
            joins=[],
            source_map={row.address: {branch.source}},
            source_type=SourceType.UNION,
        ),
        parent_ctes=[],
        internal_ctes=[branch],
        output_columns=[row],
        grain=BuildGrain(),
    )
    existence_parent = _simple_cte("existence_parent", [exists])
    condition = BuildSubselectComparison(
        left=row,
        right=exists,
        operator=ComparisonOperator.IN,
    )
    consumer = _simple_cte(
        "consumer",
        [row, exists],
        condition=condition,
        parent_ctes=[union, existence_parent],
        source_map={
            row.address: [union.name],
            exists.address: [existence_parent.name],
        },
    )

    optimized = PredicatePushdown()._push_into_union_branches(
        consumer,
        union,
        condition,
        {union.name: [consumer]},
    )

    assert optimized is True
    assert branch.condition == condition
    assert branch.source_map[exists.address] == [existence_parent.name]
    assert branch.parent_ctes == [existence_parent]
    assert union.parent_ctes == [existence_parent]


def test_predicate_pushdown_parent_propagates_existence_dependency():
    row = BuildConcept(
        name="row_id",
        canonical_name="row_id",
        datatype=DataType.INTEGER,
        purpose=Purpose.KEY,
        build_is_aggregate=False,
        grain=BuildGrain(),
    )
    exists = BuildConcept(
        name="exists_id",
        canonical_name="exists_id",
        datatype=DataType.INTEGER,
        purpose=Purpose.KEY,
        build_is_aggregate=False,
        grain=BuildGrain(),
    )
    condition = BuildSubselectComparison(
        left=row,
        right=exists,
        operator=ComparisonOperator.IN,
    )
    parent = _simple_cte("parent", [row])
    existence_parent = _simple_cte("existence_parent", [exists])
    consumer = _simple_cte(
        "consumer",
        [row, exists],
        condition=condition,
        parent_ctes=[parent, existence_parent],
        source_map={
            row.address: [parent.name],
            exists.address: [existence_parent.name],
        },
    )

    optimized = PredicatePushdown()._check_parent(
        consumer,
        parent,
        condition,
        {parent.name: [consumer]},
    )

    assert optimized is True
    assert parent.condition == condition
    assert parent.source_map[exists.address] == [existence_parent.name]
    assert parent.parent_ctes == [existence_parent]


def test_grains_equivalent_rejects_empty_grain_fallback(test_environment):
    build_env = test_environment.materialize_for_select()
    product_id = build_env.concepts["product_id"]
    child = _simple_cte(
        "child",
        [product_id],
        grain=BuildGrain(components={product_id.address}),
    )
    parent = _simple_cte("parent", [product_id], grain=BuildGrain())

    assert not _grains_equivalent(child, parent)


def test_parent_nullable_ignores_non_join_entries(test_environment):
    build_env = test_environment.materialize_for_select()
    product_id = build_env.concepts["product_id"]
    cte = _simple_cte("child", [product_id], joins=[object()])

    assert not _parent_nullable_in_cte(cte, "parent")


def test_having_pushdown_guard_branches():
    env = Environment()
    env.parse(
        """
key sale_id int;
auto sale_count <- count(sale_id);
""",
        persist=True,
    )
    build_env = env.materialize_for_select()
    sale_count = build_env.concepts["sale_count"]
    condition = BuildComparison(
        left=sale_count,
        right=1,
        operator=ComparisonOperator.GT,
    )
    rule = PredicatePushdown(having_alias=True)
    group_parent = _simple_cte(
        "grouped",
        [sale_count],
        condition=None,
        group_to_grain=True,
    )
    consumer = _simple_cte("consumer", [sale_count], condition=condition)
    plain_parent = _simple_cte(
        "plain",
        [sale_count],
        condition=None,
        group_to_grain=False,
    )

    assert not rule._push_having_into_group_parent(
        consumer, plain_parent, condition, {}
    )
    assert not rule._push_having_into_group_parent(consumer, group_parent, None, {})
    assert not rule._push_having_into_group_parent(
        consumer,
        group_parent,
        BuildSubselectComparison(
            left=sale_count,
            right=sale_count,
            operator=ComparisonOperator.IN,
        ),
        {},
    )
    assert not rule._push_having_into_group_parent(
        consumer,
        group_parent,
        BuildComparison(left=1, right=2, operator=ComparisonOperator.EQ),
        {},
    )
    group_parent.condition = condition
    assert not rule._push_having_into_group_parent(
        consumer, group_parent, condition, {}
    )
    group_parent.condition = None
    assert not rule._push_having_into_group_parent(
        consumer, group_parent, condition, {}
    )
    assert not rule._push_having_into_group_parent(
        consumer, group_parent, condition, {group_parent.name: [object()]}
    )
    unfiltered_child = _simple_cte(
        "unfiltered_child",
        [sale_count],
        condition=None,
        source_map={sale_count.address: [group_parent.name]},
    )
    assert not rule._push_having_into_group_parent(
        consumer, group_parent, condition, {group_parent.name: [unfiltered_child]}
    )


def test_having_pushup_keeps_consumer_filter_across_full_join():
    env = Environment()
    env.parse(
        """
key order_id int;
key customer_id int;
auto qty_per_order <- sum(order_id);
""",
        persist=True,
    )
    build_env = env.materialize_for_select()
    qty_per_order = build_env.concepts["qty_per_order"]
    customer_id = build_env.concepts["customer_id"]
    condition = BuildComparison(
        left=qty_per_order,
        right=300,
        operator=ComparisonOperator.GT,
    )
    rule = PredicatePushdown(having_alias=True)
    group_parent = _simple_cte(
        "grouped",
        [qty_per_order],
        condition=None,
        group_to_grain=True,
    )
    customer_parent = _simple_cte("customers", [customer_id])
    consumer = _simple_cte(
        "consumer",
        [qty_per_order, customer_id],
        condition=condition,
        source_map={
            qty_per_order.address: [group_parent.name],
            customer_id.address: [customer_parent.name],
        },
        parent_ctes=[group_parent, customer_parent],
    )
    consumer.joins = [
        Join(
            right_cte=customer_parent,
            jointype=JoinType.FULL,
            joinkey_pairs=[
                CTEConceptPair(
                    left=customer_id,
                    right=customer_id,
                    existing_datasource=consumer.source,
                    cte=customer_parent,
                )
            ],
        )
    ]

    assert rule._push_having_into_group_parent(
        consumer,
        group_parent,
        condition,
        {group_parent.name: [consumer]},
    )
    assert is_child_of(condition, group_parent.condition)
    assert is_child_of(condition, consumer.condition)


def test_child_of_complex():
    #   monitor."customer_count" > 10 and monitor."store_sales_date_year" = 2001 and monitor."store_sales_date_month_of_year" = 1 and monitor."store_sales_item_current_price" > 1.2 * monitor."_virtual_6264207893106521"
    env, _ = parse("""
key customer_count int;
key year int;    
                   key avg_price float;
                   key current_price float;               
""")
    env = env.materialize_for_select()
    comp = BuildConditional(
        left=BuildConditional(
            left=BuildComparison(
                left=env.concepts["customer_count"],
                right=10,
                operator=ComparisonOperator.GT,
            ),
            right=BuildComparison(
                left=env.concepts["year"], right=2001, operator=ComparisonOperator.EQ
            ),
            operator=BooleanOperator.AND,
        ),
        right=BuildComparison(
            left=env.concepts["current_price"],
            right=BuildFunction(
                operator=FunctionType.MULTIPLY,
                output_purpose=Purpose.PROPERTY,
                output_data_type=DataType.FLOAT,
                arguments=[1.2, env.concepts["avg_price"]],
                arg_count=2,
            ),
            operator=ComparisonOperator.GT,
        ),
        operator=BooleanOperator.AND,
    )

    assert is_child_of(comp, comp) is True


def test_decomposition_function():
    condition = BuildConditional(
        left=BuildComparison(left=1, right=2, operator=ComparisonOperator.EQ),
        right=BuildComparison(left=3, right=4, operator=ComparisonOperator.EQ),
        operator=BooleanOperator.AND,
    )
    decomposed = decompose_condition(condition)
    assert decomposed == [
        BuildComparison(left=1, right=2, operator=ComparisonOperator.EQ),
        BuildComparison(left=3, right=4, operator=ComparisonOperator.EQ),
    ]


def test_basic_pushdown(test_environment: Environment, test_environment_graph):
    test_environment = test_environment.materialize_for_select()
    datasource = list(test_environment.datasources.values())[0]
    outputs = [c.concept for c in datasource.columns]
    cte_source_map = {outputs[0].address: [datasource.name]}
    parent = CTE(
        name="parent",
        source=QueryDatasource(
            input_concepts=[outputs[0]],
            output_concepts=[outputs[0]],
            datasources=[datasource],
            grain=BuildGrain(),
            joins=[],
            source_map={outputs[0].address: {datasource}},
        ),
        output_columns=[],
        parent_ctes=[],
        grain=BuildGrain(),
        source_map=cte_source_map,
        existence_source_map={},
    )

    cte2 = CTE(
        name="test",
        source=QueryDatasource(
            input_concepts=[outputs[0]],
            output_concepts=[outputs[0]],
            datasources=[parent.source],
            grain=BuildGrain(),
            joins=[],
            source_map={outputs[0].address: {datasource}},
        ),
        output_columns=[],
        parent_ctes=[parent],
        condition=BuildComparison(
            left=outputs[0], right=outputs[0], operator=ComparisonOperator.EQ
        ),
        grain=BuildGrain(),
        source_map=cte_source_map,
        existence_source_map={},
    )
    inverse_map = {"parent": [cte2]}
    rule = PredicatePushdown()
    rule2 = PredicatePushdownRemove()
    assert rule.optimize(cte2, inverse_map)[0] is True
    assert rule.optimize(cte2, inverse_map)[0] is False
    assert rule2.optimize(cte2, inverse_map)[0] is True
    assert (
        cte2.condition is None
    ), f"{cte2.condition}, {parent.condition}, {is_child_of(cte2.condition, parent.condition)}"


def test_invalid_pushdown(test_environment: Environment, test_environment_graph):
    test_environment = test_environment.materialize_for_select()
    datasource = list(test_environment.datasources.values())[0]
    outputs = [c.concept for c in datasource.columns]
    cte_source_map = {outputs[0].address: [datasource.name]}
    parent = CTE(
        name="parent",
        source=QueryDatasource(
            input_concepts=[outputs[0]],
            output_concepts=[outputs[0]],
            datasources=[datasource],
            grain=BuildGrain(),
            joins=[],
            source_map={outputs[0].address: {datasource}},
        ),
        output_columns=[],
        grain=BuildGrain(),
        source_map=cte_source_map,
    )
    cte1 = CTE(
        name="test1",
        source=QueryDatasource(
            input_concepts=[outputs[0]],
            output_concepts=[outputs[0]],
            datasources=[datasource],
            grain=BuildGrain(),
            joins=[],
            source_map={outputs[0].address: {datasource}},
        ),
        output_columns=[],
        parent_ctes=[parent],
        grain=BuildGrain(),
        source_map=cte_source_map,
        existence_source_map={},
    )

    cte2 = CTE(
        name="test2",
        source=QueryDatasource(
            input_concepts=[outputs[0]],
            output_concepts=[outputs[0]],
            datasources=[datasource],
            grain=BuildGrain(),
            joins=[],
            source_map={outputs[0].address: {datasource}},
        ),
        output_columns=[],
        parent_ctes=[parent],
        condition=BuildComparison(
            left=outputs[0], right=outputs[0], operator=ComparisonOperator.EQ
        ),
        grain=BuildGrain(),
        source_map=cte_source_map,
        existence_source_map={},
    )

    inverse_map = {"parent": [cte1, cte2]}
    rule = PredicatePushdown()
    # we cannot push down as not all children have the same filter
    assert rule.optimize(cte1, inverse_map)[0] is False
    assert cte1.condition is None
    assert cte2.condition is not None


def test_invalid_aggregate_pushdown(
    test_environment: Environment, test_environment_graph
):
    test_environment = test_environment.materialize_for_select()
    datasource = list(test_environment.datasources.values())[0]
    outputs = [c.concept for c in datasource.columns]
    cte_source_map = {outputs[0].address: [datasource.name]}
    parent = CTE(
        name="parent",
        source=QueryDatasource(
            input_concepts=[outputs[0]],
            output_concepts=[outputs[0]],
            datasources=[datasource],
            grain=BuildGrain(),
            joins=[],
            source_map={outputs[0].address: {datasource}},
        ),
        output_columns=[],
        grain=BuildGrain(),
        source_map=cte_source_map,
    )

    cte2 = CTE(
        name="test2",
        source=QueryDatasource(
            input_concepts=[outputs[0]],
            output_concepts=[outputs[0]],
            datasources=[datasource],
            grain=BuildGrain(),
            joins=[],
            source_map={outputs[0].address: {datasource}},
        ),
        output_columns=[],
        parent_ctes=[parent],
        condition=BuildComparison(
            left=BuildFunction(
                operator=FunctionType.COUNT,
                arguments=[outputs[0]],
                output_data_type=DataType.INTEGER,
                output_purpose=Purpose.METRIC,
            ),
            right=12,
            operator=ComparisonOperator.EQ,
        ),
        grain=BuildGrain(),
        source_map=cte_source_map,
        existence_source_map={},
    )

    inverse_map = {"parent": [cte2]}
    # we cannot push down as the condition is on an aggregate
    rule = PredicatePushdown()
    assert rule.optimize(cte2, inverse_map)[0] is False
    assert parent.condition is None
    assert cte2.condition is not None


def test_decomposition_pushdown(test_environment: Environment, test_environment_graph):
    test_environment = test_environment.materialize_for_select()
    category_ds = test_environment.datasources["category"]
    products = test_environment.datasources["products"]
    product_id = test_environment.concepts["product_id"]
    category_name = test_environment.concepts["category_name"]
    category_id = test_environment.concepts["category_id"]
    parent1 = CTE(
        name="products",
        source=QueryDatasource(
            input_concepts=[product_id, category_id],
            output_concepts=[product_id, category_id],
            datasources=[products],
            grain=BuildGrain(),
            joins=[],
            source_map={
                product_id.address: {products},
                category_id.address: {products},
            },
        ),
        output_columns=[],
        condition=BuildComparison(
            left=product_id, right=1, operator=ComparisonOperator.EQ
        ),
        grain=BuildGrain(),
        source_map={
            product_id.address: [products.name],
            category_id.address: [products.name],
        },
    )
    parent2 = CTE(
        name="parent2",
        source=QueryDatasource(
            input_concepts=[category_id, category_name],
            output_concepts=[category_id, category_name],
            datasources=[category_ds],
            grain=BuildGrain(),
            joins=[],
            source_map={
                category_id.address: {category_ds},
                category_name.address: {category_ds},
            },
        ),
        output_columns=[],
        grain=BuildGrain(),
        source_map={
            category_id.address: [category_ds.name],
            category_name.address: [category_ds.name],
        },
    )
    cte1 = CTE(
        name="test1",
        source=QueryDatasource(
            input_concepts=[product_id, category_id, category_name],
            output_concepts=[product_id, category_name],
            datasources=[parent2.source, parent1.source],
            grain=BuildGrain(),
            joins=[],
            source_map={
                product_id.address: {parent1.source},
                category_name.address: {parent2.source},
                category_id.address: {parent1.source, parent2.source},
            },
        ),
        output_columns=[],
        parent_ctes=[parent1, parent2],
        condition=BuildConditional(
            left=BuildComparison(
                left=product_id, right=product_id, operator=ComparisonOperator.EQ
            ),
            right=BuildComparison(
                left=category_name, right=category_name, operator=ComparisonOperator.EQ
            ),
            operator=BooleanOperator.AND,
        ),
        grain=BuildGrain(),
        source_map={
            product_id.address: [parent1.name],
            category_name.address: [parent2.name],
            category_id.address: [parent1.name, parent2.name],
        },
    )

    inverse_map = {"parent1": [cte1], "parent2": [cte1]}

    assert parent2.condition is None
    rule = PredicatePushdown()
    # two to pushup, then last will fail
    assert rule.optimize(cte1, inverse_map)[0] is True
    assert rule.optimize(cte1, inverse_map)[0] is False
    assert parent1.condition == BuildConditional(
        left=BuildComparison(left=product_id, right=1, operator=ComparisonOperator.EQ),
        right=BuildComparison(
            left=product_id, right=product_id, operator=ComparisonOperator.EQ
        ),
        operator=BooleanOperator.AND,
    )
    assert isinstance(parent2.condition, BuildComparison)
    assert parent2.condition.left == category_name
    assert parent2.condition.right == category_name
    assert parent2.condition.operator == ComparisonOperator.EQ
    assert str(parent2.condition) == str(
        BuildComparison(
            left=category_name, right=category_name, operator=ComparisonOperator.EQ
        )
    )
    # we cannot safely remove this condition
    # as not all parents have both
    assert cte1.condition is not None


def _make_branch_cte(name: str, datasource, product_id, category_id) -> CTE:
    return CTE(
        name=name,
        source=QueryDatasource(
            input_concepts=[product_id, category_id],
            output_concepts=[product_id, category_id],
            datasources=[datasource],
            grain=BuildGrain(),
            joins=[],
            source_map={
                product_id.address: {datasource},
                category_id.address: {datasource},
            },
            base_datasource=datasource,
        ),
        output_columns=[product_id, category_id],
        parent_ctes=[],
        grain=BuildGrain(),
        source_map={
            product_id.address: [datasource.name],
            category_id.address: [datasource.name],
        },
        existence_source_map={},
    )


def _make_union(
    name: str, branch1: CTE, branch2: CTE, product_id, category_id
) -> UnionCTE:
    return UnionCTE(
        name=name,
        source=QueryDatasource(
            input_concepts=[product_id, category_id],
            output_concepts=[product_id, category_id],
            datasources=[branch1.source, branch2.source],
            grain=BuildGrain(),
            joins=[],
            source_map={
                product_id.address: {branch1.source, branch2.source},
                category_id.address: {branch1.source, branch2.source},
            },
            source_type=SourceType.UNION,
        ),
        parent_ctes=[branch1, branch2],
        internal_ctes=[branch1, branch2],
        output_columns=[product_id, category_id],
        grain=BuildGrain(),
    )


def _make_union_consumer(
    name: str,
    union: UnionCTE,
    product_id: BuildConcept,
    category_id: BuildConcept,
    condition: BuildComparison,
) -> CTE:
    return CTE(
        name=name,
        source=QueryDatasource(
            input_concepts=[product_id, category_id],
            output_concepts=[product_id, category_id],
            datasources=[union.source],
            grain=BuildGrain(),
            joins=[],
            source_map={
                product_id.address: {union.source},
                category_id.address: {union.source},
            },
        ),
        output_columns=[product_id, category_id],
        parent_ctes=[union],
        condition=condition,
        grain=BuildGrain(),
        source_map={
            product_id.address: [union.name],
            category_id.address: [union.name],
        },
        existence_source_map={},
    )


def test_union_branch_pushdown(test_environment, test_environment_graph):
    test_environment = test_environment.materialize_for_select()
    products = test_environment.datasources["products"]
    product_id = test_environment.concepts["product_id"]
    category_id = test_environment.concepts["category_id"]

    branch1 = _make_branch_cte("branch1", products, product_id, category_id)
    branch2 = _make_branch_cte("branch2", products, product_id, category_id)
    union = _make_union("union_cte", branch1, branch2, product_id, category_id)

    consumer_condition = BuildComparison(
        left=product_id, right=1, operator=ComparisonOperator.EQ
    )
    consumer = CTE(
        name="consumer",
        source=QueryDatasource(
            input_concepts=[product_id, category_id],
            output_concepts=[product_id, category_id],
            datasources=[union.source],
            grain=BuildGrain(),
            joins=[],
            source_map={
                product_id.address: {union.source},
                category_id.address: {union.source},
            },
        ),
        output_columns=[product_id, category_id],
        parent_ctes=[union],
        condition=consumer_condition,
        grain=BuildGrain(),
        source_map={
            product_id.address: [union.name],
            category_id.address: [union.name],
        },
        existence_source_map={},
    )

    inverse_map = {"union_cte": [consumer]}
    rule = PredicatePushdown()
    assert rule.optimize(consumer, inverse_map)[0] is True
    assert branch1.condition is not None
    assert is_child_of(consumer_condition, branch1.condition)
    assert branch2.condition is not None
    assert is_child_of(consumer_condition, branch2.condition)

    # second time around it's idempotent
    fresh_rule = PredicatePushdown()
    assert fresh_rule.optimize(consumer, inverse_map)[0] is False


def test_union_branch_prune_requires_all_consumers_to_share_condition(
    test_environment, test_environment_graph
):
    test_environment = test_environment.materialize_for_select()
    products = test_environment.datasources["products"]
    product_id = test_environment.concepts["product_id"]
    category_id = test_environment.concepts["category_id"]

    product_one = BuildComparison(
        left=product_id, right=1, operator=ComparisonOperator.EQ
    )
    product_two = BuildComparison(
        left=product_id, right=2, operator=ComparisonOperator.EQ
    )
    branch1_ds = replace(products, name="products_one")
    branch1_ds.non_partial_for = BuildWhereClause(conditional=product_one)
    branch2_ds = replace(products, name="products_two")
    branch2_ds.non_partial_for = BuildWhereClause(conditional=product_two)
    branch1 = _make_branch_cte("branch1", branch1_ds, product_id, category_id)
    branch2 = _make_branch_cte("branch2", branch2_ds, product_id, category_id)
    union = _make_union("union_cte", branch1, branch2, product_id, category_id)
    consumer_one = _make_union_consumer(
        "consumer_one", union, product_id, category_id, product_one
    )
    consumer_two = _make_union_consumer(
        "consumer_two", union, product_id, category_id, product_two
    )

    inverse_map = {"union_cte": [consumer_one, consumer_two]}

    assert PredicatePushdown().optimize(consumer_one, inverse_map)[0] is False
    assert union.internal_ctes == [branch1, branch2]


def test_union_branch_prune_single_consumer(test_environment, test_environment_graph):
    test_environment = test_environment.materialize_for_select()
    products = test_environment.datasources["products"]
    product_id = test_environment.concepts["product_id"]
    category_id = test_environment.concepts["category_id"]

    product_one = BuildComparison(
        left=product_id, right=1, operator=ComparisonOperator.EQ
    )
    product_two = BuildComparison(
        left=product_id, right=2, operator=ComparisonOperator.EQ
    )
    branch1_ds = replace(products, name="products_one")
    branch1_ds.non_partial_for = BuildWhereClause(conditional=product_one)
    branch2_ds = replace(products, name="products_two")
    branch2_ds.non_partial_for = BuildWhereClause(conditional=product_two)
    branch1 = _make_branch_cte("branch1", branch1_ds, product_id, category_id)
    branch2 = _make_branch_cte("branch2", branch2_ds, product_id, category_id)
    union = _make_union("union_cte", branch1, branch2, product_id, category_id)
    consumer = _make_union_consumer(
        "consumer", union, product_id, category_id, product_one
    )

    inverse_map = {"union_cte": [consumer]}

    assert PredicatePushdown().optimize(consumer, inverse_map)[0] is True
    assert union.internal_ctes == [branch1]


def test_union_branch_pushdown_remove_strips_consumer(
    test_environment, test_environment_graph
):
    test_environment = test_environment.materialize_for_select()
    products = test_environment.datasources["products"]
    product_id = test_environment.concepts["product_id"]
    category_id = test_environment.concepts["category_id"]

    branch1 = _make_branch_cte("branch1", products, product_id, category_id)
    branch2 = _make_branch_cte("branch2", products, product_id, category_id)
    union = _make_union("union_cte", branch1, branch2, product_id, category_id)

    consumer = CTE(
        name="consumer",
        source=QueryDatasource(
            input_concepts=[product_id, category_id],
            output_concepts=[product_id, category_id],
            datasources=[union.source],
            grain=BuildGrain(),
            joins=[],
            source_map={
                product_id.address: {union.source},
                category_id.address: {union.source},
            },
        ),
        output_columns=[product_id, category_id],
        parent_ctes=[union],
        condition=BuildComparison(
            left=product_id, right=1, operator=ComparisonOperator.EQ
        ),
        grain=BuildGrain(),
        source_map={
            product_id.address: [union.name],
            category_id.address: [union.name],
        },
        existence_source_map={},
    )
    inverse_map = {"union_cte": [consumer]}
    push = PredicatePushdown()
    assert push.optimize(consumer, inverse_map)[0] is True
    remove = PredicatePushdownRemove()
    assert remove.optimize(consumer, inverse_map)[0] is True
    assert consumer.condition is None


def test_union_branch_pushdown_skips_concept_not_on_union(
    test_environment, test_environment_graph
):
    test_environment = test_environment.materialize_for_select()
    products = test_environment.datasources["products"]
    category_ds = test_environment.datasources["category"]
    product_id = test_environment.concepts["product_id"]
    category_id = test_environment.concepts["category_id"]
    category_name = test_environment.concepts["category_name"]

    branch1 = _make_branch_cte("branch1", products, product_id, category_id)
    branch2 = _make_branch_cte("branch2", products, product_id, category_id)
    union = _make_union("union_cte", branch1, branch2, product_id, category_id)

    # consumer joins union to category_ds and filters on category_name —
    # category_name isn't on the union outputs so the atom must stay put.
    consumer = CTE(
        name="consumer",
        source=QueryDatasource(
            input_concepts=[product_id, category_id, category_name],
            output_concepts=[product_id, category_name],
            datasources=[union.source, category_ds],
            grain=BuildGrain(),
            joins=[],
            source_map={
                product_id.address: {union.source},
                category_id.address: {union.source, category_ds},
                category_name.address: {category_ds},
            },
        ),
        output_columns=[product_id, category_name],
        parent_ctes=[union],
        condition=BuildComparison(
            left=category_name, right="x", operator=ComparisonOperator.EQ
        ),
        grain=BuildGrain(),
        source_map={
            product_id.address: [union.name],
            category_id.address: [union.name, category_ds.name],
            category_name.address: [category_ds.name],
        },
        existence_source_map={},
    )
    inverse_map = {"union_cte": [consumer]}
    rule = PredicatePushdown()
    assert rule.optimize(consumer, inverse_map)[0] is False
    assert branch1.condition is None
    assert branch2.condition is None
    assert consumer.condition is not None


def test_union_branch_pushdown_skips_existence_on_union_output(
    test_environment, test_environment_graph
):
    """Existence args referencing a union-output concept can't be pushed —
    a branch can't refer to its own union-level output as an external IN target.
    """
    test_environment = test_environment.materialize_for_select()
    products = test_environment.datasources["products"]
    product_id = test_environment.concepts["product_id"]
    category_id = test_environment.concepts["category_id"]

    branch1 = _make_branch_cte("branch1", products, product_id, category_id)
    branch2 = _make_branch_cte("branch2", products, product_id, category_id)
    union = _make_union("union_cte", branch1, branch2, product_id, category_id)

    # IN-subselect whose target is category_id (a union output).
    consumer_condition = BuildSubselectComparison(
        left=product_id,
        right=category_id,
        operator=ComparisonOperator.IN,
    )
    consumer = CTE(
        name="consumer",
        source=QueryDatasource(
            input_concepts=[product_id, category_id],
            output_concepts=[product_id],
            datasources=[union.source],
            grain=BuildGrain(),
            joins=[],
            source_map={
                product_id.address: {union.source},
                category_id.address: {union.source},
            },
        ),
        output_columns=[product_id],
        parent_ctes=[union],
        condition=consumer_condition,
        grain=BuildGrain(),
        source_map={
            product_id.address: [union.name],
            category_id.address: [union.name],
        },
        existence_source_map={},
    )
    inverse_map = {"union_cte": [consumer]}
    rule = PredicatePushdown()
    assert rule.optimize(consumer, inverse_map)[0] is False
    assert branch1.condition is None
    assert branch2.condition is None


def test_consumer_outer_joins_union_helper(test_environment, test_environment_graph):
    """``_consumer_outer_joins_union`` returns True when ``union`` sits on the
    nullable side of any consumer outer-join, False otherwise. This drives the
    bail-out in ``_push_into_union_branches``."""
    test_environment = test_environment.materialize_for_select()
    products = test_environment.datasources["products"]
    category_ds = test_environment.datasources["category"]
    product_id = test_environment.concepts["product_id"]
    category_id = test_environment.concepts["category_id"]

    branch1 = _make_branch_cte("branch1", products, product_id, category_id)
    branch2 = _make_branch_cte("branch2", products, product_id, category_id)
    union = _make_union("union_cte", branch1, branch2, product_id, category_id)
    other_cte = _make_branch_cte("other", category_ds, category_id, category_id)

    def _consumer(joins: list[Join]) -> CTE:
        return CTE(
            name="c",
            source=QueryDatasource(
                input_concepts=[product_id, category_id],
                output_concepts=[product_id, category_id],
                datasources=[union.source],
                grain=BuildGrain(),
                joins=[],
                source_map={
                    product_id.address: {union.source},
                    category_id.address: {union.source},
                },
            ),
            output_columns=[product_id, category_id],
            parent_ctes=[union, other_cte],
            grain=BuildGrain(),
            source_map={},
            existence_source_map={},
            joins=joins,
        )

    # No outer joins → safe to push.
    assert _consumer_outer_joins_union(_consumer([]), union) is False

    # INNER join referencing the union → still safe.
    inner = Join(right_cte=union, jointype=JoinType.INNER, left_cte=other_cte)
    assert _consumer_outer_joins_union(_consumer([inner]), union) is False

    # Union on RIGHT of LEFT_OUTER → nullable, must bail.
    left_outer = Join(right_cte=union, jointype=JoinType.LEFT_OUTER, left_cte=other_cte)
    assert _consumer_outer_joins_union(_consumer([left_outer]), union) is True

    # Union on LEFT of RIGHT_OUTER → nullable, must bail.
    right_outer = Join(
        right_cte=other_cte, jointype=JoinType.RIGHT_OUTER, left_cte=union
    )
    assert _consumer_outer_joins_union(_consumer([right_outer]), union) is True

    # FULL on either side → always nullable.
    full_left = Join(right_cte=other_cte, jointype=JoinType.FULL, left_cte=union)
    assert _consumer_outer_joins_union(_consumer([full_left]), union) is True
    full_right = Join(right_cte=union, jointype=JoinType.FULL, left_cte=other_cte)
    assert _consumer_outer_joins_union(_consumer([full_right]), union) is True

    # LEFT_OUTER but the union is on the LEFT side → preserved, safe.
    left_outer_safe = Join(
        right_cte=other_cte, jointype=JoinType.LEFT_OUTER, left_cte=union
    )
    assert _consumer_outer_joins_union(_consumer([left_outer_safe]), union) is False

    # Non-CTE consumer → conservative bail.
    assert _consumer_outer_joins_union(union, union) is True


def test_parent_covers_condition_union(test_environment, test_environment_graph):
    """``_parent_covers_condition`` for a UnionCTE: True iff every branch's
    condition implies the consumer atom; False if any branch's condition is
    None or doesn't cover."""
    test_environment = test_environment.materialize_for_select()
    products = test_environment.datasources["products"]
    product_id = test_environment.concepts["product_id"]
    category_id = test_environment.concepts["category_id"]

    atom = BuildComparison(left=product_id, right=1, operator=ComparisonOperator.EQ)
    other_atom = BuildComparison(
        left=product_id, right=2, operator=ComparisonOperator.EQ
    )

    # Both branches contain the atom → covered.
    b1 = _make_branch_cte("b1", products, product_id, category_id)
    b2 = _make_branch_cte("b2", products, product_id, category_id)
    b1.condition = atom
    b2.condition = atom
    union_covered = _make_union("u_covered", b1, b2, product_id, category_id)
    assert _parent_covers_condition(union_covered, atom) is True

    # One branch lacks the atom → not covered.
    b1b = _make_branch_cte("b1b", products, product_id, category_id)
    b2b = _make_branch_cte("b2b", products, product_id, category_id)
    b1b.condition = atom
    b2b.condition = other_atom
    union_partial = _make_union("u_partial", b1b, b2b, product_id, category_id)
    assert _parent_covers_condition(union_partial, atom) is False

    # Empty internal_ctes → vacuously not covered (guard against pushing into
    # a union that has no branches to cover the atom).
    union_empty = _make_union(
        "u_empty",
        _make_branch_cte("be1", products, product_id, category_id),
        _make_branch_cte("be2", products, product_id, category_id),
        product_id,
        category_id,
    )
    union_empty.internal_ctes = []
    assert _parent_covers_condition(union_empty, atom) is False

    # Plain CTE delegates to ``is_child_of`` against the parent's condition.
    plain = _make_branch_cte("plain", products, product_id, category_id)
    plain.condition = atom
    assert _parent_covers_condition(plain, atom) is True
    plain.condition = other_atom
    assert _parent_covers_condition(plain, atom) is False


def test_union_branch_pushdown_skips_when_consumer_outer_joins_union(
    test_environment, test_environment_graph
):
    """End-to-end: the optimizer must bail when the consumer references the
    union on the nullable side of an outer join."""
    test_environment = test_environment.materialize_for_select()
    products = test_environment.datasources["products"]
    category_ds = test_environment.datasources["category"]
    product_id = test_environment.concepts["product_id"]
    category_id = test_environment.concepts["category_id"]

    branch1 = _make_branch_cte("branch1", products, product_id, category_id)
    branch2 = _make_branch_cte("branch2", products, product_id, category_id)
    union = _make_union("union_cte", branch1, branch2, product_id, category_id)
    sibling = _make_branch_cte("sibling", category_ds, category_id, category_id)

    consumer_condition = BuildComparison(
        left=product_id, right=1, operator=ComparisonOperator.EQ
    )
    consumer = CTE(
        name="consumer",
        source=QueryDatasource(
            input_concepts=[product_id, category_id],
            output_concepts=[product_id, category_id],
            datasources=[union.source],
            grain=BuildGrain(),
            joins=[],
            source_map={
                product_id.address: {union.source},
                category_id.address: {union.source},
            },
        ),
        output_columns=[product_id, category_id],
        parent_ctes=[union, sibling],
        condition=consumer_condition,
        grain=BuildGrain(),
        source_map={
            product_id.address: [union.name],
            category_id.address: [union.name],
        },
        existence_source_map={},
        joins=[
            Join(right_cte=union, jointype=JoinType.LEFT_OUTER, left_cte=sibling),
        ],
    )

    inverse_map = {"union_cte": [consumer]}
    assert PredicatePushdown().optimize(consumer, inverse_map)[0] is False
    assert branch1.condition is None
    assert branch2.condition is None
    assert consumer.condition is consumer_condition
