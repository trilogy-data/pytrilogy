from preql.parsing.parse_engine import parse_text
from preql.core.models import Environment


def parse(input:str)-> tuple[Environment, list]:
    return parse_text(input)