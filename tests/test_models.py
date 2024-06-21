from preql.core.enums import BooleanOperator, Purpose, JoinType
from preql.core.models import (
    CTE,
    Grain,
    QueryDatasource,
    Conditional,
    SelectStatement,
    Environment,
    Address,
    UndefinedConcept,
    BaseJoin,
)


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
    assert (
        new.namespace == ("test" + "." + test_concept.namespace)
        if test_concept.namespace != "local"
        else "test"
    )


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
    cid = test_environment.concepts["category_id"]
    cname = test_environment.concepts["category_name"]

    x = Grain(components=[oid, pid])
    y = Grain(components=[pid, cid])
    z = Grain(components=[cid])
    z2 = Grain(components=[cid, cname])

    assert x.intersection(y) == Grain(components=[pid])
    assert x.union(y) == Grain(components=[oid, pid, cid])

    assert z.isdisjoint(x)
    assert z.issubset(y)

    assert z2 == z, f"Property should be removed from grain ({z.set}) vs {z2.set}"


def test_select(test_environment: Environment):
    oid = test_environment.concepts["order_id"]
    pid = test_environment.concepts["product_id"]
    cid = test_environment.concepts["category_id"]
    cname = test_environment.concepts["category_name"]
    x = SelectStatement(selection=[oid, pid, cid, cname])
    ds = x.to_datasource(
        test_environment.namespace, "test", address=Address(location="test")
    )

    assert ds.grain == Grain(components=[oid, pid, cid])


def test_undefined(test_environment: Environment):
    x = UndefinedConcept(
        name="test",
        datatype="int",
        purpose=Purpose.CONSTANT,
        grain=Grain(),
        namespace="test",
        environment=test_environment.concepts,
    )

    y = x.with_select_grain(Grain(components=[test_environment.concepts["order_id"]]))

    assert y.grain == Grain(components=[test_environment.concepts["order_id"]])

    z = x.with_default_grain()

    assert z.grain == Grain()


def test_base_join(test_environment: Environment):
    exc: SyntaxError | None = None
    try:
        BaseJoin(
            left_datasource=test_environment.datasources["revenue"],
            right_datasource=test_environment.datasources["revenue"],
            concepts=[test_environment.concepts["product_id"]],
            join_type=JoinType.RIGHT_OUTER,
        )
    except Exception as exc2:
        exc = exc2
        pass
    assert isinstance(exc, SyntaxError)

    x = BaseJoin(
        left_datasource=test_environment.datasources["revenue"],
        right_datasource=test_environment.datasources["products"],
        concepts=[
            test_environment.concepts["product_id"],
            test_environment.concepts["category_name"],
        ],
        join_type=JoinType.RIGHT_OUTER,
        filter_to_mutual=True,
    )

    assert x.concepts == [test_environment.concepts["product_id"]]

    exc3: SyntaxError | None = None
    try:
        x = BaseJoin(
            left_datasource=test_environment.datasources["revenue"],
            right_datasource=test_environment.datasources["category"],
            concepts=[
                test_environment.concepts["product_id"],
                test_environment.concepts["category_name"],
            ],
            join_type=JoinType.RIGHT_OUTER,
            filter_to_mutual=True,
        )
    except Exception as exc4:
        exc3 = exc4
        pass
    assert isinstance(exc3, SyntaxError)
