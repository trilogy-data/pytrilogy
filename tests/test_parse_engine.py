from pytest import raises

from trilogy.core.exceptions import UndefinedConceptException
from trilogy.core.models.environment import Environment
from trilogy.parsing.parse_engine import (
    ERROR_CODES,
    PARSER,
    InvalidSyntaxException,
    ParseToObjects,
    unpack_visit_error,
)

TEXT = """
const a <- 1;

select
    a,
    b
;
"""


def test_parser():
    env = Environment()
    x = ParseToObjects(environment=env)
    x.set_text(TEXT)

    failed = False
    try:
        tokens = PARSER.parse(TEXT)
        x.transform(tokens)
        x.run_second_parse_pass()
    except Exception as e:
        failed = True
        with raises(UndefinedConceptException):
            unpack_visit_error(e)
    assert failed


TEXT2 = """
const a <- 1;

select
    a,
FROM a
;
"""


def test_parse_datatype_in_datasource():
    env = Environment()
    x = ParseToObjects(environment=env)
    test_text = """
key x int;
property x.timestamp timestamp;

datasource funky (
    x: x,
    timestamp:timestamp)
address fun;

"""
    x.set_text(test_text)

    tokens = PARSER.parse(test_text)
    x.transform(tokens)
    x.run_second_parse_pass()


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


def test_error_order_by_missing():
    env = Environment()
    TEXT2 = """
const a <- 1;

select
    a,
order by a
;
    """
    with raises(InvalidSyntaxException) as e:
        env.parse(TEXT2)
    assert ERROR_CODES[210] in str(e.value)

    env = Environment()
    TEXT2 = """
const a <- 1;

select
    a,
order by a;
    """
    with raises(InvalidSyntaxException) as e:
        env.parse(TEXT2)
    assert ERROR_CODES[210] in str(e.value)


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
