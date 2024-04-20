from preql.core.enums import DataType, Purpose
from preql.core.models import Parenthetical, ProcessedQuery, ShowStatement, Select
from preql.core.functions import argument_to_purpose, function_args_to_output_purpose
from preql.parsing.parse_engine import (
    arg_to_datatype,
    parse_text,
)
from preql.constants import MagicConstants
from preql.dialect.base import BaseDialect


def test_custom_Type():
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

