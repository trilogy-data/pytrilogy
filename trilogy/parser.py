from pathlib import Path
from typing import Optional

from trilogy.constants import Parsing
from trilogy.core.models.environment import Environment
from trilogy.parsing.parse_engine_v2 import parse_text as _parse_text


def parse(
    input: str, environment: Optional[Environment] = None
) -> tuple[Environment, list]:
    return parse_text(input, environment=environment)


def parse_text(
    input: str,
    environment: Optional[Environment] = None,
    root: Path | None = None,
    parse_config: Parsing | None = None,
) -> tuple[Environment, list]:
    return _parse_text(
        input,
        environment=environment,
        root=root,
        parse_config=parse_config,
    )
