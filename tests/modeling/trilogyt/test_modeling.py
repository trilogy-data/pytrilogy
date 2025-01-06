from pathlib import Path

from trilogy.core.models_environment import Environment
from trilogy.parsing.parse_engine import PARSER, ParseToObjects

parent = Path(__file__).parent

PARSED = {}
TOKENS = {}
TEXT_LOOKUP = {}


def infinite_parsing():
    text = open(parent / "order.preql").read()
    parser = ParseToObjects(
        Environment(working_path=parent),
        tokens=TOKENS,
        text_lookup=TEXT_LOOKUP,
        parsed=PARSED,
    )
    parser.set_text(text)
    # disable fail on missing to allow for circular dependencies
    parser.prepare_parse()
    parser.transform(PARSER.parse(text))
    # this will reset fail on missing
    try:
        parser.hydrate_missing()
    except Exception:
        pass


def test_infinite_parsing():
    infinite_parsing()


def test_parsing_recursion():
    Environment().from_file(Path(__file__).parent / "customer.preql")
    Environment().from_file(Path(__file__).parent / "order.preql")
