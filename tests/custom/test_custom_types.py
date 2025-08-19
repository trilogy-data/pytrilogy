from pytest import raises

from trilogy import Dialects
from trilogy.parsing.parse_engine import InvalidSyntaxException, parse_text


def test_custom_type():
    env, parsed = parse_text(
        """type positive int;

        # add validator PositiveInteger -> x>0;

key field int::positive;

datasource test (
    field
)
grain (field)
query '''
select 1 as field union all select 2''';


def add_positive_numbers(x: int::positive, y: int::positive) -> x + y;

select @add_positive_numbers(field, 2::int::positive) as fun;


    """
    )
    dialects = Dialects.DUCK_DB.default_executor(environment=env, hooks=[])

    sql = dialects.generate_sql(parsed[-1])[0]
    assert '"test"."field" + cast(2 as int)' in sql, sql

    with raises(InvalidSyntaxException, match="expected traits \['positive'\]"):
        sql = dialects.parse_text(
            """

    select @add_positive_numbers(1, -2) as fun;
    """
        )[0]
