from pytest import raises

from trilogy.core.exceptions import UndefinedConceptException
from trilogy.core.models.environment import Environment
from trilogy.parsing.parse_engine import (
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


TEXT2 = """
const a <- 1;

select
    a,
FROM a
;
"""


def test_from_error():
    env = Environment()

    with raises(InvalidSyntaxException):
        env.parse(TEXT2)
