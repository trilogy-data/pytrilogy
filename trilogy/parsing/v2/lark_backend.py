from __future__ import annotations

from os.path import dirname, join
from re import IGNORECASE
from typing import TYPE_CHECKING, Any

from trilogy.parsing.v2.errors import create_generic_syntax_error, create_syntax_error
from trilogy.parsing.v2.syntax import SyntaxDocument, syntax_document_from_parser
from trilogy.utility import safe_open

if TYPE_CHECKING:
    from lark import Lark
    from lark.exceptions import UnexpectedToken

_PARSER: "Lark | None" = None


def _grammar_path() -> str:
    return join(dirname(dirname(__file__)), "trilogy.lark")


def _get_parser() -> "Lark":
    global _PARSER
    if _PARSER is None:
        from lark import Lark

        with safe_open(_grammar_path()) as f:
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


def parse_lark(text: str) -> SyntaxDocument:
    from lark.exceptions import (
        UnexpectedCharacters,
        UnexpectedEOF,
        UnexpectedInput,
        UnexpectedToken,
    )

    try:
        return syntax_document_from_parser(text=text, tree=_get_parser().parse(text))
    except UnexpectedToken as e:
        _handle_unexpected_token(e, text)
        raise  # unreachable — _handle_unexpected_token always raises
    except (UnexpectedCharacters, UnexpectedEOF, UnexpectedInput) as e:
        raise create_generic_syntax_error(str(e), e.pos_in_stream or 0, text) from e


def _handle_unexpected_token(e: "UnexpectedToken", text: str) -> None:
    from lark import Token
    from lark.exceptions import UnexpectedToken as _UnexpectedToken

    pos = e.pos_in_stream or 0
    if e.interactive_parser.lexer_thread.state:
        last_token = e.interactive_parser.lexer_thread.state.last_token
    else:
        last_token = None

    parsed_tokens = [x.value for x in e.token_history if x] if e.token_history else []
    if parsed_tokens == ["FROM"]:
        raise create_syntax_error(101, pos, text)

    if last_token and e.token.type == "$END":
        try:
            e.interactive_parser.feed_token(Token("_TERMINATOR", ";"))
            state = e.interactive_parser.lexer_thread.state
            if state and state.last_token:
                new_pos = state.last_token.end_pos or pos
            else:
                new_pos = pos
            raise create_syntax_error(202, new_pos, text)
        except _UnexpectedToken:
            pass

    try:
        e.interactive_parser.feed_token(Token("AS", "AS"))
        state = e.interactive_parser.lexer_thread.state
        if state and state.last_token:
            new_pos = state.last_token.end_pos or pos
        else:
            new_pos = pos
        e.interactive_parser.feed_token(Token("IDENTIFIER", e.token.value))
        raise create_syntax_error(201, new_pos, text)
    except _UnexpectedToken:
        pass

    raise create_generic_syntax_error(str(e), pos, text)
