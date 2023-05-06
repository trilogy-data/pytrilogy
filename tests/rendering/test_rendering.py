from preql.parsing.render import render_query
from preql.core.models import OrderBy, Ordering, OrderItem, Select


def test_basic_query(test_environment):
    query = Select(
        selection=[test_environment.concepts["order_id"]],
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
ORDER BY
    order_id asc
;"""
    )
