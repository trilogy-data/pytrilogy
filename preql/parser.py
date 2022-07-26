from preql.core.models import Environment
from preql.parsing.parse_engine import parse_text
from typing import Optional


def parse(
    input: str, environment: Optional[Environment] = None
) -> tuple[Environment, list]:
    return parse_text(input, environment=environment)
