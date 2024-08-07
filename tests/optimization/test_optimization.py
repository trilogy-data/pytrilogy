from trilogy.core.optimization import (
    PredicatePushdown,
)
from trilogy.core.optimizations.predicate_pushdown import (
    decompose_condition,
    is_child_of,
)
from trilogy.core.models import (
    CTE,
    QueryDatasource,
    Conditional,
    Environment,
    Grain,
    Comparison,
)
from trilogy.core.enums import BooleanOperator, ComparisonOperator


def test_is_child_function():
    condition = Conditional(
        left=Comparison(left=1, right=2, operator=ComparisonOperator.EQ),
        right=Comparison(left=3, right=4, operator=ComparisonOperator.EQ),
        operator=BooleanOperator.AND,
    )
    assert (
        is_child_of(
            Comparison(left=1, right=2, operator=ComparisonOperator.EQ), condition
        )
        is True
    )
    assert (
        is_child_of(
            Comparison(left=3, right=4, operator=ComparisonOperator.EQ), condition
        )
        is True
    )
    assert (
        is_child_of(
            Comparison(left=1, right=2, operator=ComparisonOperator.EQ), condition.left
        )
        is True
    )
    assert (
        is_child_of(
            Comparison(left=3, right=4, operator=ComparisonOperator.EQ), condition.right
        )
        is True
    )
    assert (
        is_child_of(
            Comparison(left=1, right=2, operator=ComparisonOperator.EQ), condition.right
        )
        is False
    )
    assert (
        is_child_of(
            Comparison(left=3, right=4, operator=ComparisonOperator.EQ), condition.left
        )
        is False
    )


def test_decomposition_function():
    condition = Conditional(
        left=Comparison(left=1, right=2, operator=ComparisonOperator.EQ),
        right=Comparison(left=3, right=4, operator=ComparisonOperator.EQ),
        operator=BooleanOperator.AND,
    )
    decomposed = decompose_condition(condition)
    assert decomposed == [
        Comparison(left=1, right=2, operator=ComparisonOperator.EQ),
        Comparison(left=3, right=4, operator=ComparisonOperator.EQ),
    ]


def test_basic_pushdown(test_environment: Environment, test_environment_graph):
    datasource = list(test_environment.datasources.values())[0]
    outputs = [c.concept for c in datasource.columns]
    cte_source_map = {outputs[0].address: [datasource.name]}
    parent = CTE(
        name="parent",
        source=QueryDatasource(
            input_concepts=[outputs[0]],
            output_concepts=[outputs[0]],
            datasources=[datasource],
            grain=Grain(),
            joins=[],
            source_map={outputs[0].address: {datasource}},
        ),
        output_columns=[],
        parent_ctes=[],
        grain=Grain(),
        source_map=cte_source_map,
        existence_source_map={},
    )

    cte2 = CTE(
        name="test",
        source=QueryDatasource(
            input_concepts=[outputs[0]],
            output_concepts=[outputs[0]],
            datasources=[parent.source],
            grain=Grain(),
            joins=[],
            source_map={outputs[0].address: {datasource}},
        ),
        output_columns=[],
        parent_ctes=[parent],
        condition=Comparison(
            left=outputs[0], right=outputs[0], operator=ComparisonOperator.EQ
        ),
        grain=Grain(),
        source_map=cte_source_map,
        existence_source_map={},
    )
    inverse_map = {"parent": [cte2]}
    rule = PredicatePushdown()
    assert rule.optimize(cte2, inverse_map) is True
    assert (
        cte2.condition is None
    ), f"{cte2.condition}, {parent.condition}, {is_child_of(cte2.condition, parent.condition)}"


def test_invalid_pushdown(test_environment: Environment, test_environment_graph):
    datasource = list(test_environment.datasources.values())[0]
    outputs = [c.concept for c in datasource.columns]
    cte_source_map = {outputs[0].address: [datasource.name]}
    parent = CTE(
        name="parent",
        source=QueryDatasource(
            input_concepts=[outputs[0]],
            output_concepts=[outputs[0]],
            datasources=[datasource],
            grain=Grain(),
            joins=[],
            source_map={outputs[0].address: {datasource}},
        ),
        output_columns=[],
        grain=Grain(),
        source_map=cte_source_map,
    )
    cte1 = CTE(
        name="test1",
        source=QueryDatasource(
            input_concepts=[outputs[0]],
            output_concepts=[outputs[0]],
            datasources=[datasource],
            grain=Grain(),
            joins=[],
            source_map={outputs[0].address: {datasource}},
        ),
        output_columns=[],
        parent_ctes=[parent],
        grain=Grain(),
        source_map=cte_source_map,
        existence_source_map={},
    )

    cte2 = CTE(
        name="test2",
        source=QueryDatasource(
            input_concepts=[outputs[0]],
            output_concepts=[outputs[0]],
            datasources=[datasource],
            grain=Grain(),
            joins=[],
            source_map={outputs[0].address: {datasource}},
        ),
        output_columns=[],
        parent_ctes=[parent],
        condition=Comparison(
            left=outputs[0], right=outputs[0], operator=ComparisonOperator.EQ
        ),
        grain=Grain(),
        source_map=cte_source_map,
        existence_source_map={},
    )

    inverse_map = {"parent": [cte1, cte2]}
    rule = PredicatePushdown()
    assert rule.optimize(cte1, inverse_map) is False
    assert cte1.condition is None
    assert cte2.condition is not None


def test_decomposition_pushdown(test_environment: Environment, test_environment_graph):
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
            grain=Grain(),
            joins=[],
            source_map={
                product_id.address: {products},
                category_id.address: {products},
            },
        ),
        output_columns=[],
        condition=Comparison(left=product_id, right=1, operator=ComparisonOperator.EQ),
        grain=Grain(),
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
            grain=Grain(),
            joins=[],
            source_map={
                category_id.address: {category_ds},
                category_name.address: {category_ds},
            },
        ),
        output_columns=[],
        grain=Grain(),
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
            grain=Grain(),
            joins=[],
            source_map={
                product_id.address: {parent1.source},
                category_name.address: {parent2.source},
                category_id.address: {parent1.source, parent2.source},
            },
        ),
        output_columns=[],
        parent_ctes=[parent1, parent2],
        condition=Conditional(
            left=Comparison(
                left=product_id, right=product_id, operator=ComparisonOperator.EQ
            ),
            right=Comparison(
                left=category_name, right=category_name, operator=ComparisonOperator.EQ
            ),
            operator=BooleanOperator.AND,
        ),
        grain=Grain(),
        source_map={
            product_id.address: [parent1.name],
            category_name.address: [parent2.name],
            category_id.address: [parent1.name, parent2.name],
        },
    )

    inverse_map = {"parent1": [cte1], "parent2": [cte1]}

    assert parent2.condition is None
    rule = PredicatePushdown()
    assert rule.optimize(cte1, inverse_map) is True

    assert parent1.condition == Conditional(
        left=Comparison(left=product_id, right=1, operator=ComparisonOperator.EQ),
        right=Comparison(
            left=product_id, right=product_id, operator=ComparisonOperator.EQ
        ),
        operator=BooleanOperator.AND,
    )
    assert isinstance(parent2.condition, Comparison)
    assert parent2.condition.left == category_name
    assert parent2.condition.right == category_name
    assert parent2.condition.operator == ComparisonOperator.EQ
    assert str(parent2.condition) == str(
        Comparison(
            left=category_name, right=category_name, operator=ComparisonOperator.EQ
        )
    )
    # we cannot safely remove this condition
    # as not all parents have both
    assert cte1.condition is not None
