from preql.core.enums import BooleanOperator
from preql.core.models import CTE, Grain, QueryDatasource, Conditional


def test_cte_merge(test_environment, test_environment_graph):
    datasource = list(test_environment.datasources.values())[0]
    outputs = [c.concept for c in datasource.columns]
    output_map = {
        c.address: {
            datasource,
        }
        for c in outputs
    }
    a = CTE(
        name="test",
        output_columns=[outputs[0]],
        grain=Grain(),
        source=QueryDatasource(
            input_concepts=[outputs[0]],
            output_concepts=[outputs[0]],
            datasources=[datasource],
            grain=Grain(),
            joins=[],
            source_map={outputs[0].address: {datasource}},
        ),
        source_map={c.address: datasource.identifier for c in outputs},
    )
    b = CTE(
        name="testb",
        output_columns=outputs,
        grain=Grain(),
        source=QueryDatasource(
            input_concepts=outputs,
            output_concepts=outputs,
            datasources=[datasource],
            grain=Grain(),
            joins=[],
            source_map=output_map,
        ),
        source_map={c.address: datasource.identifier for c in outputs},
    )

    merged = a + b
    assert merged.output_columns == outputs


def test_concept(test_environment, test_environment_graph):
    test_concept = list(test_environment.concepts.values())[0]
    new = test_concept.with_namespace("test")
    assert new.namespace == "test"


def test_conditional(test_environment, test_environment_graph):
    test_concept = list(test_environment.concepts.values())[-1]

    condition_a = Conditional(
        left=test_concept, right=test_concept, operator=BooleanOperator.AND
    )
    condition_b = Conditional(
        left=test_concept, right=test_concept, operator=BooleanOperator.AND
    )

    merged = condition_a + condition_b
    assert merged.left == condition_a
    assert merged.right == condition_b
    assert merged.operator == BooleanOperator.AND


def test_grain(test_environment):
    grains = [Grain()] * 3
    sum(grains)
    oid = test_environment.concepts["order_id"]
    pid = test_environment.concepts["product_id"]
    sid = test_environment.concepts["store_id"]

    x = Grain(components=[oid, pid])
    y = Grain(components=[pid, sid])
    z = Grain(components=[sid])

    assert x.intersection(y) == Grain(components=[pid])
    assert x.union(y) == Grain(components=[oid, pid, sid])

    assert z.isdisjoint(x)
    assert z.issubset(y)
