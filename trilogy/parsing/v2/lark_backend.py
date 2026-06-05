from __future__ import annotations

import re
from os.path import dirname, join
from re import IGNORECASE
from typing import TYPE_CHECKING, Any

from trilogy.parsing.v2.errors import (
    create_generic_syntax_error,
    create_syntax_error,
    detect_clause_after_join,
    detect_definition_after_clause,
    detect_group_by,
    detect_subselect,
)
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


_BY_KEYWORD_RE = re.compile(r"\bby\b", IGNORECASE)


def _lark_parses(text: str) -> bool:
    try:
        _get_parser().parse(text)
        return True
    except Exception:
        return False


def _detect_unparenthesized_by_expr_lark(text: str, pos: int) -> int | None:
    """Lark counterpart of the pest probe: scans back for `by` and reparses
    with `(...)` inserted around the BY expression."""
    head = text[:pos]
    last_by = None
    for m in _BY_KEYWORD_RE.finditer(head):
        last_by = m
    if last_by is None:
        return None
    by_end = last_by.end()
    if not text[by_end:pos].strip():
        return None
    candidates = [pos]
    tail = text[pos:]
    boundary = re.search(
        r"\b(as|select|where|having|order|group|limit)\b|;",
        tail,
        IGNORECASE,
    )
    if boundary is not None:
        end = pos + boundary.start()
        if end > pos:
            candidates.append(end)
    for end in candidates:
        probe = text[:by_end] + " (" + text[by_end:end].rstrip() + ")" + text[end:]
        if _lark_parses(probe):
            return last_by.start()
    return None


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

    # `__ANON_0` is Lark's auto-name for the inline "<-" literal — only used by
    # derivation/binding rules. If it appears in `expected`, the user is in a
    # derivation context (auto/property/metric/rowset NAME) but stopped before
    # the arrow + expression.
    if "__ANON_0" in e.expected:
        raise create_syntax_error(203, pos, text)

    by_pos = _detect_unparenthesized_by_expr_lark(text, pos)
    if by_pos is not None:
        raise create_syntax_error(211, by_pos, text)

    sub_pos = detect_subselect(text, pos)
    if sub_pos is not None:
        raise create_syntax_error(102, sub_pos, text)

    gb_pos = detect_group_by(text, pos)
    if gb_pos is not None:
        raise create_syntax_error(103, gb_pos, text)

    def_pos = detect_definition_after_clause(text, pos)
    if def_pos is not None:
        raise create_syntax_error(104, def_pos, text)

    join_pos = detect_clause_after_join(text, pos)
    if join_pos is not None:
        raise create_syntax_error(220, join_pos, text)

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
