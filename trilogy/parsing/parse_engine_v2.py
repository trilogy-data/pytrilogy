from __future__ import annotations

from datetime import datetime
from logging import getLogger
from pathlib import Path
from typing import Any

from trilogy.constants import CONFIG, ParserBackend, Parsing
from trilogy.core.exceptions import InvalidSyntaxException
from trilogy.core.models.environment import Environment
from trilogy.parsing.v2.errors import ERROR_CODES
from trilogy.parsing.v2.hydration import HydrationContext, NativeHydrator
from trilogy.parsing.v2.lark_backend import parse_lark
from trilogy.parsing.v2.pest_backend import parse_pest
from trilogy.parsing.v2.syntax import (
    SyntaxDocument,
    SyntaxNode,
    SyntaxToken,
)

perf_logger = getLogger("trilogy.parse.performance")

MAX_PARSE_DEPTH = 10

__all__ = [
    "ERROR_CODES",
    "SyntaxDocument",
    "SyntaxNode",
    "SyntaxToken",
    "TopLevelStatementParser",
    "parse_syntax",
    "parse_text",
    "parse_text_raw",
]


def parse_syntax(text: str) -> SyntaxDocument:
    if CONFIG.parser_backend == ParserBackend.PEST:
        return parse_pest(text)
    return parse_lark(text)


def parse_text_raw(text: str, environment: Environment | None = None) -> None:
    _ = environment
    parse_syntax(text)


class TopLevelStatementParser:
    def __init__(
        self,
        environment: Environment,
        parse_address: str | None = None,
        token_address: Path | str | None = None,
        parsed: dict[str, Any] | None = None,
        tokens: dict[Path | str, SyntaxDocument] | None = None,
        text_lookup: dict[Path | str, str] | None = None,
        environment_lookup: dict[str, Environment] | None = None,
        import_keys: list[str] | None = None,
        parse_config: Parsing | None = None,
        max_parse_depth: int = MAX_PARSE_DEPTH,
    ) -> None:
        _ = parsed, tokens
        self.hydrator = NativeHydrator(
            HydrationContext(
                environment=environment,
                parse_address=parse_address or "root",
                token_address=token_address or "root",
                parse_config=parse_config,
                max_parse_depth=max_parse_depth,
            )
        )
        if import_keys:
            self.hydrator.import_keys = import_keys
        if environment_lookup:
            self.hydrator.parsed_environments = environment_lookup
        if text_lookup:
            self.hydrator.text_lookup = text_lookup

    @property
    def environment(self) -> Environment:
        return self.hydrator.environment

    def parse(self, document: SyntaxDocument) -> list[Any]:
        return self.hydrator.parse(document)


def parse_text(
    text: str,
    environment: Environment | None = None,
    root: Path | None = None,
    parse_config: Parsing | None = None,
) -> tuple[Environment, list[Any]]:
    environment = environment or (
        Environment(working_path=root) if root else Environment()
    )
    parser = TopLevelStatementParser(
        environment=environment,
        import_keys=["root"],
        parse_config=parse_config or CONFIG.parsing,
    )
    start = datetime.now()

    try:
        document = parse_syntax(text)
        output = parser.parse(document)
        environment.concepts.fail_on_missing = True
        end = datetime.now()
        perf_logger.debug(
            f"Parse time: {end - start} for {len(text)} characters, {len(output)} objects"
        )
    except SyntaxError as e:
        raise InvalidSyntaxException(str(e)).with_traceback(e.__traceback__)

    return environment, output
