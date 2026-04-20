from pytest import raises

from trilogy import Dialects
from trilogy.core.models.author import TraitDataType
from trilogy.core.models.core import DataType
from trilogy.parsing.parse_engine_v2 import parse_text
from trilogy.parsing.v2.model import HydrationError


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

    with raises(TypeError, match="expected traits \\['positive'\\]"):
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

    with raises(TypeError, match="expected traits \\['identifier'\\]"):
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


def test_bare_trait_cast_infers_base_type():
    env, _ = parse_text(
        """
type percent float;
key amount float;
key total float;
auto ratio <- (amount / total)::percent;
auto explicit_ratio <- (amount / total)::float::percent;
auto via_cast <- cast(amount / total as percent);
"""
    )
    for name in ("local.ratio", "local.explicit_ratio", "local.via_cast"):
        dtype = env.concepts[name].datatype
        assert isinstance(dtype, TraitDataType), f"{name}: {dtype}"
        assert dtype.type == DataType.FLOAT
        assert dtype.traits == ["percent"]


def test_bare_trait_cast_unknown_name_raises():
    with raises(HydrationError, match="Unknown cast target 'not_a_trait'"):
        parse_text(
            """
key amount float;
auto bad <- amount::not_a_trait;
"""
        )


def test_bare_trait_cast_trait_upstream():
    env, _ = parse_text(
        """
type percent float;
key amount float::percent;
auto doubled <- amount::percent;
"""
    )
    dtype = env.concepts["local.doubled"].datatype
    assert isinstance(dtype, TraitDataType)
    assert dtype.type == DataType.FLOAT
    assert dtype.traits == ["percent"]


def test_bare_trait_cast_trait_upstream_multi_base():
    env, _ = parse_text(
        """
type flag int;
type identifier int | string;
key raw int::flag;
auto as_id <- raw::identifier;
"""
    )
    dtype = env.concepts["local.as_id"].datatype
    assert isinstance(dtype, TraitDataType)
    assert dtype.type == DataType.INTEGER
    assert "identifier" in dtype.traits


def test_bare_trait_cast_multi_base_picks_compatible():
    env, _ = parse_text(
        """
type identifier int | string;
key raw_int int;
key raw_str string;
auto as_int <- raw_int::identifier;
auto as_str <- raw_str::identifier;
"""
    )
    int_dtype = env.concepts["local.as_int"].datatype
    assert isinstance(int_dtype, TraitDataType)
    assert int_dtype.type == DataType.INTEGER
    assert int_dtype.traits == ["identifier"]
    str_dtype = env.concepts["local.as_str"].datatype
    assert isinstance(str_dtype, TraitDataType)
    assert str_dtype.type == DataType.STRING
    assert str_dtype.traits == ["identifier"]


def test_bare_trait_cast_multi_base_no_match_rejects():
    with raises(HydrationError, match="Cannot cast .* directly to trait"):
        parse_text(
            """
type identifier int | string;
key raw float;
auto bad <- raw::identifier;
"""
        )


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
