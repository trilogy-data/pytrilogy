from trilogy import parse, Environment
from trilogy.core.enums import (
    BooleanOperator,
    ComparisonOperator,
    FunctionType,
    Purpose,
)
from trilogy.core.execute_models import (
    CTE,
    BoundComparison,
    BoundConditional,
    DataType,
    BoundEnvironment,
    BoundFunction,
    BoundGrain,
    QueryDatasource,
)
from trilogy.core.optimization import (
    PredicatePushdown,
    PredicatePushdownRemove,
)
from trilogy.core.optimizations.predicate_pushdown import (
    is_child_of,
)
from trilogy.core.processing.utility import decompose_condition


def test_is_child_function():
    condition = BoundConditional(
        left=BoundComparison(left=1, right=2, operator=ComparisonOperator.EQ),
        right=BoundComparison(left=3, right=4, operator=ComparisonOperator.EQ),
        operator=BooleanOperator.AND,
    )
    assert (
        is_child_of(
            BoundComparison(left=1, right=2, operator=ComparisonOperator.EQ), condition
        )
        is True
    )
    assert (
        is_child_of(
            BoundComparison(left=3, right=4, operator=ComparisonOperator.EQ), condition
        )
        is True
    )
    assert (
        is_child_of(
            BoundComparison(left=1, right=2, operator=ComparisonOperator.EQ), condition.left
        )
        is True
    )
    assert (
        is_child_of(
            BoundComparison(left=3, right=4, operator=ComparisonOperator.EQ), condition.right
        )
        is True
    )
    assert (
        is_child_of(
            BoundComparison(left=1, right=2, operator=ComparisonOperator.EQ), condition.right
        )
        is False
    )
    assert (
        is_child_of(
            BoundComparison(left=3, right=4, operator=ComparisonOperator.EQ), condition.left
        )
        is False
    )


def test_child_of_complex():
    #   monitor."customer_count" > 10 and monitor."store_sales_date_year" = 2001 and monitor."store_sales_date_month_of_year" = 1 and monitor."store_sales_item_current_price" > 1.2 * monitor."_virtual_6264207893106521"
    env, _ = parse(
        """
key customer_count int;
key year int;    
                   key avg_price float;
                   key current_price float;               
"""
    )
    comp = BoundConditional(
        left=BoundConditional(
            left=BoundComparison(
                left=env.concepts["customer_count"],
                right=10,
                operator=ComparisonOperator.GT,
            ),
            right=BoundComparison(
                left=env.concepts["year"], right=2001, operator=ComparisonOperator.EQ
            ),
            operator=BooleanOperator.AND,
        ),
        right=BoundComparison(
            left=env.concepts["current_price"],
            right=BoundFunction(
                operator=FunctionType.MULTIPLY,
                output_purpose=Purpose.PROPERTY,
                output_datatype=DataType.FLOAT,
                arguments=[1.2, env.concepts["avg_price"]],
                arg_count=2,
            ),
            operator=ComparisonOperator.GT,
        ),
        operator=BooleanOperator.AND,
    )

    assert is_child_of(comp, comp) is True


def test_decomposition_function():
    condition = BoundConditional(
        left=BoundComparison(left=1, right=2, operator=ComparisonOperator.EQ),
        right=BoundComparison(left=3, right=4, operator=ComparisonOperator.EQ),
        operator=BooleanOperator.AND,
    )
    decomposed = decompose_condition(condition)
    assert decomposed == [
        BoundComparison(left=1, right=2, operator=ComparisonOperator.EQ),
        BoundComparison(left=3, right=4, operator=ComparisonOperator.EQ),
    ]


def test_basic_pushdown(test_environment: Environment, test_environment_graph):
    test_environment = test_environment.instantiate()
    datasource = list(test_environment.datasources.values())[0]
    outputs = [c.concept for c in datasource.columns]
    cte_source_map = {outputs[0].address: [datasource.name]}
    parent = CTE(
        name="parent",
        source=QueryDatasource(
            input_concepts=[outputs[0]],
            output_concepts=[outputs[0]],
            datasources=[datasource],
            grain=BoundGrain(),
            joins=[],
            source_map={outputs[0].address: {datasource}},
        ),
        output_columns=[],
        parent_ctes=[],
        grain=BoundGrain(),
        source_map=cte_source_map,
        existence_source_map={},
    )

    cte2 = CTE(
        name="test",
        source=QueryDatasource(
            input_concepts=[outputs[0]],
            output_concepts=[outputs[0]],
            datasources=[parent.source],
            grain=BoundGrain(),
            joins=[],
            source_map={outputs[0].address: {datasource}},
        ),
        output_columns=[],
        parent_ctes=[parent],
        condition=BoundComparison(
            left=outputs[0], right=outputs[0], operator=ComparisonOperator.EQ
        ),
        grain=BoundGrain(),
        source_map=cte_source_map,
        existence_source_map={},
    )
    inverse_map = {"parent": [cte2]}
    rule = PredicatePushdown()
    rule2 = PredicatePushdownRemove()
    assert rule.optimize(cte2, inverse_map) is True
    assert rule.optimize(cte2, inverse_map) is False
    assert rule2.optimize(cte2, inverse_map) is True
    assert (
        cte2.condition is None
    ), f"{cte2.condition}, {parent.condition}, {is_child_of(cte2.condition, parent.condition)}"


def test_invalid_pushdown(test_environment: BoundEnvironment, test_environment_graph):
    test_environment = test_environment.instantiate()
    datasource = list(test_environment.datasources.values())[0]
    outputs = [c.concept for c in datasource.columns]
    cte_source_map = {outputs[0].address: [datasource.name]}
    parent = CTE(
        name="parent",
        source=QueryDatasource(
            input_concepts=[outputs[0]],
            output_concepts=[outputs[0]],
            datasources=[datasource],
            grain=BoundGrain(),
            joins=[],
            source_map={outputs[0].address: {datasource}},
        ),
        output_columns=[],
        grain=BoundGrain(),
        source_map=cte_source_map,
    )
    cte1 = CTE(
        name="test1",
        source=QueryDatasource(
            input_concepts=[outputs[0]],
            output_concepts=[outputs[0]],
            datasources=[datasource],
            grain=BoundGrain(),
            joins=[],
            source_map={outputs[0].address: {datasource}},
        ),
        output_columns=[],
        parent_ctes=[parent],
        grain=BoundGrain(),
        source_map=cte_source_map,
        existence_source_map={},
    )

    cte2 = CTE(
        name="test2",
        source=QueryDatasource(
            input_concepts=[outputs[0]],
            output_concepts=[outputs[0]],
            datasources=[datasource],
            grain=BoundGrain(),
            joins=[],
            source_map={outputs[0].address: {datasource}},
        ),
        output_columns=[],
        parent_ctes=[parent],
        condition=BoundComparison(
            left=outputs[0], right=outputs[0], operator=ComparisonOperator.EQ
        ),
        grain=BoundGrain(),
        source_map=cte_source_map,
        existence_source_map={},
    )

    inverse_map = {"parent": [cte1, cte2]}
    rule = PredicatePushdown()
    # we cannot push down as not all children have the same filter
    assert rule.optimize(cte1, inverse_map) is False
    assert cte1.condition is None
    assert cte2.condition is not None


def test_invalid_aggregate_pushdown(
    test_environment: Environment, test_environment_graph
):
    test_environment = test_environment.instantiate()
    datasource = list(test_environment.datasources.values())[0]
    outputs = [c.concept for c in datasource.columns]
    cte_source_map = {outputs[0].address: [datasource.name]}
    parent = CTE(
        name="parent",
        source=QueryDatasource(
            input_concepts=[outputs[0]],
            output_concepts=[outputs[0]],
            datasources=[datasource],
            grain=BoundGrain(),
            joins=[],
            source_map={outputs[0].address: {datasource}},
        ),
        output_columns=[],
        grain=BoundGrain(),
        source_map=cte_source_map,
    )

    cte2 = CTE(
        name="test2",
        source=QueryDatasource(
            input_concepts=[outputs[0]],
            output_concepts=[outputs[0]],
            datasources=[datasource],
            grain=BoundGrain(),
            joins=[],
            source_map={outputs[0].address: {datasource}},
        ),
        output_columns=[],
        parent_ctes=[parent],
        condition=BoundComparison(
            left=BoundFunction(
                operator=FunctionType.COUNT,
                arguments=[outputs[0]],
                output_datatype=DataType.INTEGER,
                output_purpose=Purpose.METRIC,
            ),
            right=12,
            operator=ComparisonOperator.EQ,
        ),
        grain=BoundGrain(),
        source_map=cte_source_map,
        existence_source_map={},
    )

    inverse_map = {"parent": [cte2]}
    # we cannot push down as the condition is on an aggregate
    rule = PredicatePushdown()
    assert rule.optimize(cte2, inverse_map) is False
    assert parent.condition is None
    assert cte2.condition is not None


def test_decomposition_pushdown(test_environment: Environment, test_environment_graph):
    test_environment = test_environment.instantiate()
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
            grain=BoundGrain(),
            joins=[],
            source_map={
                product_id.address: {products},
                category_id.address: {products},
            },
        ),
        output_columns=[],
        condition=BoundComparison(left=product_id, right=1, operator=ComparisonOperator.EQ),
        grain=BoundGrain(),
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
            grain=BoundGrain(),
            joins=[],
            source_map={
                category_id.address: {category_ds},
                category_name.address: {category_ds},
            },
        ),
        output_columns=[],
        grain=BoundGrain(),
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
            grain=BoundGrain(),
            joins=[],
            source_map={
                product_id.address: {parent1.source},
                category_name.address: {parent2.source},
                category_id.address: {parent1.source, parent2.source},
            },
        ),
        output_columns=[],
        parent_ctes=[parent1, parent2],
        condition=BoundConditional(
            left=BoundComparison(
                left=product_id, right=product_id, operator=ComparisonOperator.EQ
            ),
            right=BoundComparison(
                left=category_name, right=category_name, operator=ComparisonOperator.EQ
            ),
            operator=BooleanOperator.AND,
        ),
        grain=BoundGrain(),
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
    assert rule.optimize(cte1, inverse_map) is True
    assert rule.optimize(cte1, inverse_map) is False
    assert parent1.condition == BoundConditional(
        left=BoundComparison(left=product_id, right=1, operator=ComparisonOperator.EQ),
        right=BoundComparison(
            left=product_id, right=product_id, operator=ComparisonOperator.EQ
        ),
        operator=BooleanOperator.AND,
    )
    assert isinstance(parent2.condition, BoundComparison)
    assert parent2.condition.left == category_name
    assert parent2.condition.right == category_name
    assert parent2.condition.operator == ComparisonOperator.EQ
    assert str(parent2.condition) == str(
        BoundComparison(
            left=category_name, right=category_name, operator=ComparisonOperator.EQ
        )
    )
    # we cannot safely remove this condition
    # as not all parents have both
    assert cte1.condition is not None
