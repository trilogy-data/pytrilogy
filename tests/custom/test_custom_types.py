from trilogy.core.models import Parenthetical
from trilogy.parsing.parse_engine import (
    parse_text,
)
from trilogy.dialect.base import BaseDialect
import pytest


# not implemented yet
@pytest.mark.xfail
def test_custom_type():
    _, parsed = parse_text(
        """def PositiveInteger int;
        add validator PositiveInteger -> x>0;

datasource test (
    positive:PositiveInteger
)

validate test;

    """
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
