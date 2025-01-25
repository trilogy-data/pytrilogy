from trilogy.core.exceptions import InvalidSyntaxException
from trilogy.core.models.core import (
    NumericType,
)
from trilogy.parsing.parse_engine import (
    parse_text,
)


def test_numeric():
    env, _ = parse_text(
        "const order_id numeric(12,2); auto rounded <- cast(order_id as numeric(15,2));"
    )
    assert env.concepts["order_id"].datatype == NumericType(precision=12, scale=2)


def test_cast_error():
    found = False
    try:
        env, _ = parse_text(
            """
    const x <- 1;
    const y <- 'fun';

    select 
        x
    where 
        x = y;

    """
        )
    except InvalidSyntaxException as e:
        assert "Cannot compare DataType.INTEGER and DataType.STRING" in str(e)
        found = True
    if not found:
        assert False, "Expected InvalidSyntaxException not raised"


def test_is_error():
    found = False
    try:
        env, _ = parse_text(
            """
    const x <- 1;
    const y <- 'fun';

    select 
        x
    where 
        x is [1,2];

    """
        )
    except InvalidSyntaxException as e:
        assert "Cannot use is with non-null or boolean value [1, 2]" in str(e)
        found = True
    if not found:
        assert False, "Expected InvalidSyntaxException not raised"
