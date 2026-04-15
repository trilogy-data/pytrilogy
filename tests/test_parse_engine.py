from pytest import raises

from trilogy.core.exceptions import InvalidSyntaxException, UndefinedConceptException
from trilogy.core.models.environment import Environment
from trilogy.parsing.parse_engine_v2 import ERROR_CODES

TEXT = """
const a <- 1;

select
    a,
    b
;
"""


def test_parser():
    env = Environment()
    with raises(UndefinedConceptException):
        env.parse(TEXT)


def test_parse_datatype_in_datasource():
    env = Environment()
    test_text = """
key x int;
property x.timestamp timestamp;

datasource funky (
    x: x,
    timestamp:timestamp)
address fun;

"""
    env.parse(test_text)


def test_from_error():
    env = Environment()
    TEXT2 = """
    const a <- 1;

    select
        a,
    FROM a
    ;
    """
    with raises(InvalidSyntaxException) as e:
        env.parse(TEXT2)
    assert ERROR_CODES[101] in str(e.value), e.value


def test_order_by_missing_defaults_to_asc():
    env = Environment()
    TEXT2 = """
const a <- 1;

select
    a,
order by a
;
    """
    _, queries = env.parse(TEXT2)
    assert queries[-1].order_by.items[0].order.value == "asc"

    env = Environment()
    TEXT2 = """
const a <- 1;

select
    a,
order by a;
    """
    _, queries = env.parse(TEXT2)
    assert queries[-1].order_by.items[0].order.value == "asc"


def test_alias_error():
    env = Environment()
    TEXT2 = """
    const a <- 1;

    select
        a+2 fun,
    ;
    """
    with raises(InvalidSyntaxException) as e:
        env.parse(TEXT2)
    assert ERROR_CODES[201] in str(e.value), e.value


def test_semicolon_error():
    env = Environment()
    TEXT2 = """
    const a <- 1;

    select
        a+2 as fun,

    """
    with raises(InvalidSyntaxException) as e:
        env.parse(TEXT2)
    assert ERROR_CODES[202] in str(e.value), e.value


def test_duplicate_error():
    env = Environment()
    TEXT2 = """
    const a <- 1;

    select
        a as fun,
        a as fun,
    ;
    """
    with raises(InvalidSyntaxException) as e:
        env.parse(TEXT2)
    assert "Duplicate select output for local.fun" in str(e.value), e.value

    TEXT2 = """
    const a <- 1;

    select
        a+1 as fun2,
        a+1 as fun2,
    ;
    """
    with raises(InvalidSyntaxException) as e:
        env.parse(TEXT2)
    assert "Duplicate select output for local.fun" in str(e.value), e.value
