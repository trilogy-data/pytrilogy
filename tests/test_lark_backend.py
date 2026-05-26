"""Coverage for parsing.v2.lark_backend — error paths in particular."""

from __future__ import annotations

import pytest

from trilogy.core.exceptions import InvalidSyntaxException
from trilogy.parsing.v2.lark_backend import (
    PARSER,
    _detect_unparenthesized_by_expr_lark,
    _lark_parses,
    parse_lark,
)


def test_parse_lark_round_trip_simple_select():
    doc = parse_lark("select 1+1->x;")
    assert doc is not None


def test_parse_lark_unexpected_eof_is_invalid_syntax():
    # No terminator and incomplete -> falls through to UnexpectedToken/EOF.
    with pytest.raises(InvalidSyntaxException):
        parse_lark("select 1+1")


def test_parse_lark_unexpected_chars_is_invalid_syntax():
    with pytest.raises(InvalidSyntaxException):
        parse_lark("@@@invalid characters@@@")


def test_parse_lark_missing_alias_error_201():
    """sum(x) without `as` triggers the alias-suggestion path (error 201)."""
    src = (
        "key id int;\n"
        "property id.x int;\n"
        "datasource y (id:id, x:x) grain(id) address y;\n"
        "select sum(x);\n"
    )
    with pytest.raises(InvalidSyntaxException, match="201"):
        parse_lark(src)


def test_parse_lark_missing_semicolon_error_202():
    src = (
        "key id int;\n"
        "property id.x int;\n"
        "datasource y (id:id, x:x) grain(id) address y;\n"
        "select x as q\n"
    )
    with pytest.raises(InvalidSyntaxException, match="202"):
        parse_lark(src)


def test_lark_parses_valid_returns_true():
    assert _lark_parses("select 1->x;") is True


def test_lark_parses_invalid_returns_false():
    assert _lark_parses("garbage @@ tokens") is False


def test_detect_by_returns_none_when_no_by_in_head():
    # No `by` keyword to the left of pos at all.
    assert _detect_unparenthesized_by_expr_lark("select x -> y", 8) is None


def test_detect_by_returns_none_when_by_has_no_expression():
    # `by ` followed immediately by pos = empty expression.
    text = "select x by "
    assert _detect_unparenthesized_by_expr_lark(text, len(text)) is None


def test_parser_proxy_delegates_attribute_lookup():
    # Forces the lazy-init code path: __getattr__ on the proxy triggers
    # _get_parser() and returns the underlying Lark attribute.
    tree = PARSER.parse("select 1->x;")
    assert tree is not None
