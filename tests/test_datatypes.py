from trilogy.core.models import (
    NumericType,
)
from trilogy.parsing.parse_engine import (
    parse_text,
)


def test_numeric():
    env, _ = parse_text(
        "const order_id numeric(12,2); const rounded <- cast(order_id as numeric(15,2));"
    )
    assert env.concepts["order_id"].datatype == NumericType(precision=12, scale=2)
