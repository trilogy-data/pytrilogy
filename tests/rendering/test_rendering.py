from preql.parsing.render import render_query
from preql.core.models import (
    OrderBy,
    Ordering,
    OrderItem,
    Select,
    WhereClause,
    Conditional,
    Comparison,
)
from preql.core.enums import ComparisonOperator, BooleanOperator


def test_basic_query(test_environment):
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
            [
                OrderItem(
                    expr=test_environment.concepts["order_id"],
                    order=Ordering.ASCENDING,
                )
            ]
        ),
    )

    string_query = render_query(query)
    print(string_query)
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
