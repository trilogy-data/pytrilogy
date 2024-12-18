from trilogy import Environment
from pathlib import Path

from trilogy.parsing.parse_engine import ParseToObjects, PARSER
from pathlib import Path


parent = Path(__file__).parent

PARSED = {}
TOKENS = {}
TEXT_LOOKUP = {}


def infinite_parsing():
    text = open(parent / "order.preql").read()
    tokens = PARSER.parse(text)
    parser = ParseToObjects(Environment(working_path = parent), tokens=TOKENS, text_lookup=TEXT_LOOKUP, parsed=PARSED)
    parser.set_text(text)
    # disable fail on missing to allow for circular dependencies
    parser.prepare_parse()
    parser.transform(PARSER.parse(text))
    # this will reset fail on missing
    try:
        parser.hydrate_missing()
    except Exception as e:
        pass


def test_parsing_recursion():
    parsed = Environment().from_file(Path(__file__).parent / "customer.preql")
    parsed = Environment().from_file(Path(__file__).parent / "order.preql")