from trilogy.core.optimization import (
    PredicatePushdown,
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
    datasource = list(test_environment.datasources.values())[0]
    outputs = [c.concept for c in datasource.columns]
    concept_0 = outputs[0]
    concept_1 = outputs[1]
    cte_source_map = {output.address: [datasource.name] for output in outputs}
    parent1 = CTE(
        name="parent1",
        source=QueryDatasource(
            input_concepts=[concept_0],
            output_concepts=[concept_0],
            datasources=[datasource],
            grain=Grain(),
            joins=[],
            source_map={outputs[0].address: {datasource}},
        ),
        output_columns=[],
        condition=Comparison(left=outputs[0], right=1, operator=ComparisonOperator.EQ),
        grain=Grain(),
        source_map={outputs[0].address: [datasource.name]},
    )
    parent2 = CTE(
        name="parent2",
        source=QueryDatasource(
            input_concepts=[concept_1],
            output_concepts=[concept_1],
            datasources=[datasource],
            grain=Grain(),
            joins=[],
            source_map={concept_1.address: {datasource}},
        ),
        output_columns=[],
        grain=Grain(),
        source_map={concept_1.address: [datasource.name]},
    )
    cte1 = CTE(
        name="test1",
        source=QueryDatasource(
            input_concepts=[outputs[0], outputs[1]],
            output_concepts=[outputs[0], concept_1],
            datasources=[datasource],
            grain=Grain(),
            joins=[],
            source_map={
                outputs[0].address: {datasource},
                concept_1.address: {datasource},
            },
        ),
        output_columns=[],
        parent_ctes=[parent1, parent2],
        condition=Conditional(
            left=Comparison(
                left=outputs[0], right=outputs[0], operator=ComparisonOperator.EQ
            ),
            right=Comparison(
                left=concept_1, right=concept_1, operator=ComparisonOperator.EQ
            ),
            operator=BooleanOperator.AND,
        ),
        grain=Grain(),
        source_map=cte_source_map,
    )

    inverse_map = {"parent1": [cte1], "parent2": [cte1]}

    assert parent2.condition is None
    rule = PredicatePushdown()
    assert rule.optimize(cte1, inverse_map) is True

    assert parent1.condition == Conditional(
        left=Comparison(left=outputs[0], right=1, operator=ComparisonOperator.EQ),
        right=Comparison(
            left=outputs[0], right=outputs[0], operator=ComparisonOperator.EQ
        ),
        operator=BooleanOperator.AND,
    )
    assert isinstance(parent2.condition, Comparison)
    assert parent2.condition.left == concept_1
    assert parent2.condition.right == concept_1
    assert parent2.condition.operator == ComparisonOperator.EQ
    assert str(parent2.condition) == str(
        Comparison(left=outputs[1], right=outputs[1], operator=ComparisonOperator.EQ)
    )
    # we cannot safely remove this condition
    # as not all parents have both
    assert cte1.condition is not None
