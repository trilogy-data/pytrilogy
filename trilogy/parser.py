from typing import Optional

from trilogy.core.models.environment import Environment
from trilogy.parsing.parse_engine import parse_text


def parse(
    input: str, environment: Optional[Environment] = None
) -> tuple[Environment, list]:
    return parse_text(input, environment=environment)
