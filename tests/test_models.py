from trilogy.core.enums import BooleanOperator, Purpose, JoinType, ComparisonOperator
from trilogy.core.models import (
    CTE,
    Grain,
    QueryDatasource,
    Conditional,
    SelectStatement,
    Environment,
    Address,
    UndefinedConcept,
    BaseJoin,
    Comparison,
    Join,
    CTEConceptPair,
    Concept,
    AggregateWrapper,
    RowsetItem,
    TupleWrapper,
    DataType,
)
from trilogy import parse
from copy import deepcopy


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
        source_map={c.address: [datasource.identifier] for c in outputs},
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
        source_map={c.address: [datasource.identifier] for c in outputs},
    )

    merged = a + b
    assert merged.output_columns == outputs

    assert "Target: Grain<Abstract>." in merged.comment


def test_concept(test_environment, test_environment_graph):
    test_concept = list(test_environment.concepts.values())[0]
    new = test_concept.with_namespace("test")
    assert (
        new.namespace == ("test" + "." + test_concept.namespace)
        if test_concept.namespace != "local"
        else "test"
    )


def test_concept_filter(test_environment, test_environment_graph):
    test_concept: Concept = list(test_environment.concepts.values())[0]
    new = test_concept.with_filter(
        Comparison(left=1, right=2, operator=ComparisonOperator.EQ)
    )
    new2 = test_concept.with_filter(
        Comparison(left=1, right=2, operator=ComparisonOperator.EQ)
    )

    assert new.name == new2.name != test_concept.name

    new3 = new.with_filter(Comparison(left=1, right=2, operator=ComparisonOperator.EQ))
    assert new3 == new


def test_conditional(test_environment, test_environment_graph):
    test_concept = list(test_environment.concepts.values())[-1]

    condition_a = Conditional(
        left=test_concept, right=test_concept, operator=BooleanOperator.AND
    )
    condition_b = Conditional(
        left=test_concept, right=test_concept, operator=BooleanOperator.AND
    )
    merged = condition_a + condition_b
    assert merged == condition_a

    test_concept_two = [
        x
        for x in test_environment.concepts.values()
        if x.address != test_concept.address
    ].pop()
    condition_c = Conditional(
        left=test_concept, right=test_concept_two, operator=BooleanOperator.AND
    )
    merged_two = condition_a + condition_c
    assert merged_two.left == condition_a, f"{str(merged_two.left), str(condition_a)}"
    assert merged_two.right == condition_c
    assert merged_two.operator == BooleanOperator.AND


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

    y = x.with_select_context(Grain(components=[test_environment.concepts["order_id"]]))

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
            # test_environment.concepts["category_name"],
        ],
        join_type=JoinType.RIGHT_OUTER,
    )

    assert x.concepts == [test_environment.concepts["product_id"]]


def test_comparison():
    try:
        Comparison(left=1, right="abc", operator=ComparisonOperator.EQ)
    except Exception as exc:
        assert isinstance(exc, SyntaxError)


def test_join(test_environment: Environment):
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
        source_map={c.address: [datasource.identifier] for c in outputs},
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
        source_map={c.address: [datasource.identifier] for c in outputs},
    )
    test = Join(
        left_cte=a,
        right_cte=b,
        joinkey_pairs=[
            CTEConceptPair(left=x, right=x, existing_datasource=a.source, cte=a)
            for x in outputs
        ],
        jointype=JoinType.RIGHT_OUTER,
    )

    assert (
        str(test)
        == "right outer join testb on test.local.product_id=local.product_id,test.local.category_id=local.category_id"
    ), str(test)


def test_concept_address_in_check():
    target = Concept(
        name="test",
        datatype="int",
        purpose=Purpose.CONSTANT,
        grain=Grain(),
        environment={},
    )
    assert target.address == "local.test"
    x = [target]

    assert "local.test" in x


def test_rowset_with_filter_derivation():
    env, statements = parse(
        """
key x int;

datasource test (
x:x
)
grain (x)
address test
;

with greater_than_ten as
select x
where 
x >10;

auto avg_greater_ten <- avg(greater_than_ten.x) by *;

select avg_greater_ten;

"""
    )

    lineage = env.concepts["avg_greater_ten"].lineage
    assert isinstance(lineage, AggregateWrapper)
    assert isinstance(lineage.function.concept_arguments[0].lineage, RowsetItem)


def test_tuple_clone():
    x = TupleWrapper([1, 2, 3], type=DataType.INTEGER)
    y = deepcopy(x)
    assert y == x
