from preql.parsing.render import render_query, render_environment, Renderer
from preql.core.models import (
    OrderBy,
    Ordering,
    OrderItem,
    Select,
    WhereClause,
    Conditional,
    Comparison,
    Persist,
    Address,
    SelectItem,
    ConceptDeclaration,
    ListWrapper,
    Function,
    Purpose,
    DataType,
)
from preql import Environment
from preql.core.enums import ComparisonOperator, BooleanOperator, Modifier, FunctionType


def test_basic_query(test_environment):
    query = Select(
        selection=[test_environment.concepts["order_id"]],
        where_clause=None,
        order_by=OrderBy(
            items=[
                OrderItem(
                    expr=test_environment.concepts["order_id"],
                    order=Ordering.ASCENDING,
                )
            ]
        ),
    )

    string_query = render_query(query)
    assert (
        string_query
        == """SELECT
    order_id,
ORDER BY
    order_id asc
;"""
    )


def test_full_query(test_environment):
    query = Select(
        selection=[test_environment.concepts["order_id"]],
        where_clause=WhereClause(
            conditional=Conditional(
                left=Comparison(
                    left=test_environment.concepts["order_id"],
                    right=123,
                    operator=ComparisonOperator.EQ,
                ),
                right=Comparison(
                    left=test_environment.concepts["order_id"],
                    right=456,
                    operator=ComparisonOperator.EQ,
                ),
                operator=BooleanOperator.OR,
            ),
        ),
        order_by=OrderBy(
            items=[
                OrderItem(
                    expr=test_environment.concepts["order_id"],
                    order=Ordering.ASCENDING,
                )
            ]
        ),
    )

    string_query = render_query(query)
    assert (
        string_query
        == """SELECT
    order_id,
WHERE
    (order_id = 123 or order_id = 456)
ORDER BY
    order_id asc
;"""
    )


def test_environment_rendering(test_environment):
    rendered = render_environment(test_environment)

    assert "address tblRevenue" in rendered


def test_persist(test_environment: Environment):
    select = Select(
        selection=[test_environment.concepts["order_id"]],
        where_clause=None,
        order_by=OrderBy(
            items=[
                OrderItem(
                    expr=test_environment.concepts["order_id"],
                    order=Ordering.ASCENDING,
                )
            ]
        ),
    )
    query = Persist(
        select=select,
        datasource=select.to_datasource(
            namespace=test_environment.namespace,
            identifier="test",
            address=Address(location="tbl_test"),
        ),
    )

    string_query = render_query(query)
    assert (
        string_query
        == """PERSIST test INTO tbl_test FROM SELECT
    order_id,
ORDER BY
    order_id asc
;"""
    )


def test_render_select_item(test_environment: Environment):
    test = Renderer().to_string(
        SelectItem(
            content=test_environment.concepts["order_id"], modifiers=[Modifier.HIDDEN]
        )
    )

    assert test == "--order_id"


def test_render_concept_declaration(test_environment: Environment):
    test = Renderer().to_string(
        ConceptDeclaration(concept=test_environment.concepts["order_id"])
    )

    assert test == "key local.order_id int;"


def test_render_list_wrapper(test_environment: Environment):
    test = Renderer().to_string(ListWrapper([1, 2, 3, 4]))

    assert test == "[1, 2, 3, 4]"


def test_render_constant(test_environment: Environment):
    test = Renderer().to_string(
        Function(
            arguments=[[1, 2, 3, 4]],
            operator=FunctionType.CONSTANT,
            output_purpose=Purpose.CONSTANT,
            output_datatype=DataType.ARRAY,
        )
    )

    assert test == "[1, 2, 3, 4]"
