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


def test_any_type_custom_type():
    env, parsed = parse_text(
        """type identifier any;

        key field int::identifier;

        datasource test (
            field
        )
        grain (field)
        query '''
        select 1 as field union all select 2''';


        def add_identifiers(x: int::identifier, y: int::identifier) -> x + y;

        select @add_identifiers(field, 2::int::identifier) as fun;


        """
    )
    dialects = Dialects.DUCK_DB.default_executor(environment=env, hooks=[])

    sql = dialects.generate_sql(parsed[-1])[0]
    assert '"test"."field" + cast(2 as int)' in sql, sql

    with raises(InvalidSyntaxException, match="expected traits \['identifier'\]"):
        sql = dialects.parse_text(
            """

    select @add_identifiers(1, -2) as fun;
    """
        )[0]


def test_multi_type_custom_type():
    env, parsed = parse_text(
        """type identifier int | string;

        key field int::identifier;

        datasource test (
            field
        )
        grain (field)
        query '''
        select 1 as field union all select 2''';


        def add_identifiers(x: int::identifier, y: int::identifier) -> x::string || '.' || y::string;

        select @add_identifiers(field, 2::int::identifier) as fun;


        """
    )
    dialects = Dialects.DUCK_DB.default_executor(environment=env, hooks=[])

    sql = dialects.generate_sql(parsed[-1])[0]
    assert "cast(cast(2 as int) as string)" in sql, sql


def test_identifier():
    env, parse = parse_text(
        """
import std.semantic;

key field int::flag;

datasource test (
    field
)
grain (field)
query '''
select 1 as field union all select 0''';

                            select field::string as string_field,
                            field +1 as no_field;

select sum(field) as fun;
"""
    )
    dialects = Dialects.DUCK_DB.default_executor(environment=env, hooks=[])

    dialects.generate_sql(parse[-1])[0]
    assert "flag" not in env.concepts["fun"].datatype.traits
    assert "flag" in env.concepts["string_field"].datatype.traits
    assert "flag" not in env.concepts["no_field"].datatype.traits
