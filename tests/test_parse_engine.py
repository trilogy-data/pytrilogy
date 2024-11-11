from trilogy.parsing.parse_engine import ParseToObjects, PARSER, unpack_visit_error
from trilogy import Environment
from trilogy.core.exceptions import UndefinedConceptException
from pytest import raises

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
    x.environment.concepts.fail_on_missing = False
    x.set_text(TEXT)
    tokens = PARSER.parse(TEXT)
    x.transform(tokens)
    failed = False
    try:
        x.hydrate_missing()
    except Exception as e:
        failed = True
        with raises(UndefinedConceptException):
            unpack_visit_error(e)
    assert failed
