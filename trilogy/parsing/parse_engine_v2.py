from __future__ import annotations

from datetime import datetime
from logging import getLogger
from os.path import dirname, join
from pathlib import Path
from re import IGNORECASE
from typing import Any

from lark import Lark, Token
from lark.exceptions import (
    UnexpectedCharacters,
    UnexpectedEOF,
    UnexpectedInput,
    UnexpectedToken,
)

from trilogy.constants import CONFIG, Parsing
from trilogy.core.exceptions import InvalidSyntaxException
from trilogy.core.models.environment import Environment
from trilogy.parsing.v2.hydration import HydrationContext, NativeHydrator
from trilogy.parsing.v2.syntax import (
    SyntaxDocument,
    SyntaxNode,
    SyntaxToken,
    syntax_document_from_parser,
)

perf_logger = getLogger("trilogy.parse.performance")

MAX_PARSE_DEPTH = 10

ERROR_CODES: dict[int, str] = {
    101: "Using FROM keyword? Trilogy does not have a FROM clause (Datasource resolution is automatic).",
    201: 'Missing alias? Alias must be specified with "AS" - e.g. `SELECT x+1 AS y`',
    202: "Missing closing semicolon? Statements must be terminated with a semicolon `;`.",
    210: "Missing order direction? Order by must be explicit about direction - specify `asc` or `desc`.",
}

DEFAULT_ERROR_SPAN: int = 30

__all__ = [
    "PARSER",
    "SyntaxDocument",
    "SyntaxNode",
    "SyntaxToken",
    "TopLevelStatementParser",
    "parse_syntax",
    "parse_text",
    "parse_text_raw",
]


_PARSER: Lark | None = None


def _get_parser() -> Lark:
    global _PARSER
    if _PARSER is None:
        with open(join(dirname(__file__), "trilogy.lark"), "r") as f:
            _PARSER = Lark(
                f.read(),
                start="start",
                propagate_positions=True,
                g_regex_flags=IGNORECASE,
                parser="lalr",
                cache=True,
            )
    return _PARSER


class _LazyParser:
    def __getattr__(self, name: str) -> Any:
        return getattr(_get_parser(), name)


PARSER = _LazyParser()


def parse_syntax(text: str) -> SyntaxDocument:
    return syntax_document_from_parser(text=text, tree=_get_parser().parse(text))


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


def _create_syntax_error(
    code: int,
    pos: int,
    text: str,
) -> InvalidSyntaxException:
    return InvalidSyntaxException(
        f"Syntax [{code}]: "
        + ERROR_CODES[code]
        + "\nLocation:\n"
        + inject_context_maker(pos, text.replace("\n", " "), DEFAULT_ERROR_SPAN)
    )


def _create_generic_syntax_error(
    message: str,
    pos: int,
    text: str,
) -> InvalidSyntaxException:
    return InvalidSyntaxException(
        message
        + "\nLocation:\n"
        + inject_context_maker(pos, text.replace("\n", " "), DEFAULT_ERROR_SPAN)
    )


def _handle_unexpected_token(e: UnexpectedToken, text: str) -> None:
    pos = e.pos_in_stream or 0
    if e.interactive_parser.lexer_thread.state:
        last_token = e.interactive_parser.lexer_thread.state.last_token
    else:
        last_token = None

    parsed_tokens = [x.value for x in e.token_history if x] if e.token_history else []
    if parsed_tokens == ["FROM"]:
        raise _create_syntax_error(101, pos, text)

    if last_token and e.token.type == "$END":
        try:
            e.interactive_parser.feed_token(Token("_TERMINATOR", ";"))
            state = e.interactive_parser.lexer_thread.state
            if state and state.last_token:
                new_pos = state.last_token.end_pos or pos
            else:
                new_pos = pos
            raise _create_syntax_error(202, new_pos, text)
        except UnexpectedToken:
            pass

    try:
        e.interactive_parser.feed_token(Token("AS", "AS"))
        state = e.interactive_parser.lexer_thread.state
        if state and state.last_token:
            new_pos = state.last_token.end_pos or pos
        else:
            new_pos = pos
        e.interactive_parser.feed_token(Token("IDENTIFIER", e.token.value))
        raise _create_syntax_error(201, new_pos, text)
    except UnexpectedToken:
        pass

    raise _create_generic_syntax_error(str(e), pos, text)


def inject_context_maker(pos: int, text: str, span: int = 40) -> str:
    start = max(pos - span, 0)
    end = pos + span
    before = text[start:pos].rsplit("\n", 1)[-1]
    after = text[pos:end].split("\n", 1)[0]
    rcap = ""
    if after and not after[-1].isspace() and not (end > len(text)):
        rcap = "..."
    lcap = ""
    if start > 0 and not before[0].isspace():
        lcap = "..."
    lpad = " "
    rpad = " "
    if before.endswith(" "):
        lpad = ""
    if after.startswith(" "):
        rpad = ""
    return f"{lcap}{before}{lpad}???{rpad}{after}{rcap}"


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
        environment.concepts.fail_on_missing = False
        output = parser.parse(document)
        environment.concepts.fail_on_missing = True
        end = datetime.now()
        perf_logger.debug(
            f"Parse time: {end - start} for {len(text)} characters, {len(output)} objects"
        )
    except UnexpectedToken as e:
        _handle_unexpected_token(e, text)
    except (UnexpectedCharacters, UnexpectedEOF, UnexpectedInput) as e:
        raise _create_generic_syntax_error(str(e), e.pos_in_stream or 0, text)
    except TypeError as e:
        raise InvalidSyntaxException(str(e)) from e

    return environment, output
