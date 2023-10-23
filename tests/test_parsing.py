from preql.core.enums import DataType, Purpose
from preql.core.models import Parenthetical

from preql.parsing.parse_engine import (
    argument_to_purpose,
    function_args_to_output_purpose,
    arg_to_datatype,
    parse_text,
)
from preql.dialect.base import BaseDialect


def test_in():
    _, parsed = parse_text(
        "const order_id <- 3; SELECT order_id  WHERE order_id IN (1,2,3);"
    )
    query = parsed[-1]
    right = query.where_clause.conditional.right
    assert isinstance(
        right,
        Parenthetical,
    ), type(right)
    assert right.content[0] == 1
    rendered = BaseDialect().render_expr(right)
    assert rendered.strip() == "( 1,2,3 )".strip()


def test_not_in():
    _, parsed = parse_text(
        "const order_id <- 4; SELECT order_id  WHERE order_id NOT IN (1,2,3);"
    )
    query = parsed[-1]
    right = query.where_clause.conditional.right
    assert isinstance(
        right,
        Parenthetical,
    ), type(right)
    assert right.content[0] == 1
    rendered = BaseDialect().render_expr(right)
    assert rendered.strip() == "( 1,2,3 )".strip()


def test_sort():
    _, parsed = parse_text(
        "const order_id <- 4; SELECT order_id  ORDER BY order_id desc;"
    )

    _, parsed = parse_text(
        "const order_id <- 4; SELECT order_id  ORDER BY order_id DESC;"
    )
    _, parsed = parse_text(
        "const order_id <- 4; SELECT order_id  ORDER BY order_id DesC;"
    )


def test_arg_to_datatype():
    assert arg_to_datatype(1.00) == DataType.FLOAT
    assert arg_to_datatype("test") == DataType.STRING


def test_argument_to_purpose(test_environment):
    assert argument_to_purpose(1.00) == Purpose.CONSTANT
    assert argument_to_purpose("test") == Purpose.CONSTANT
    assert argument_to_purpose(test_environment.concepts["order_id"]) == Purpose.KEY
    assert (
        function_args_to_output_purpose(
            [
                "test",
                1.00,
            ]
        )
        == Purpose.CONSTANT
    )
    assert (
        function_args_to_output_purpose(
            ["test", 1.00, test_environment.concepts["order_id"]]
        )
        == Purpose.PROPERTY
    )
