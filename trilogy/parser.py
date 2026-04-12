from typing import Optional

from trilogy.constants import CONFIG, ParserVersion
from trilogy.core.models.environment import Environment


def parse(
    input: str, environment: Optional[Environment] = None
) -> tuple[Environment, list]:
    return parse_text(input, environment=environment)


def parse_text(
    input: str, environment: Optional[Environment] = None
) -> tuple[Environment, list]:
    if CONFIG.parser_version == ParserVersion.V2:
        from trilogy.parsing.parse_engine_v2 import parse_text as parse_text_v2

        return parse_text_v2(input, environment=environment)

    from trilogy.parsing.parse_engine import parse_text as parse_text_v1

    return parse_text_v1(input, environment=environment)
