"""Coverage for parsing.v2.pest_backend error-diagnostic helpers."""

from __future__ import annotations

import pytest

from trilogy.core.exceptions import InvalidSyntaxException
from trilogy.parsing.v2.pest_backend import (
    _compute_line_starts,
    _detect_unparenthesized_by_expr,
    _pest_error_char_pos,
    _pest_parses,
    parse_pest,
)


def test_parse_pest_round_trip():
    doc = parse_pest("select 1+1->x;")
    assert doc is not None


def test_parse_pest_error_101_from_keyword():
    with pytest.raises(InvalidSyntaxException, match="101"):
        parse_pest("select id FROM x;")


def test_parse_pest_error_201_missing_alias():
    src = (
        "key id int;\n"
        "property id.x int;\n"
        "datasource y (id:id, x:x) grain(id) address y;\n"
        "select sum(x);\n"
    )
    with pytest.raises(InvalidSyntaxException, match="201"):
        parse_pest(src)


def test_parse_pest_error_202_missing_semicolon():
    with pytest.raises(InvalidSyntaxException, match="202"):
        parse_pest("select 1->x")


def test_parse_pest_generic_error_on_pure_garbage():
    with pytest.raises(InvalidSyntaxException):
        parse_pest("@@@@ not valid @@@@")


def test_pest_parses_valid_returns_true():
    assert _pest_parses("select 1->y;") is True


def test_pest_parses_invalid_returns_false():
    assert _pest_parses("@@@") is False


def test_pest_error_char_pos_missing_marker_returns_zero():
    assert _pest_error_char_pos("no position marker", "abc") == 0


def test_pest_error_char_pos_clamps_to_text_length():
    # line:col past the end of text → returns len(text)
    pos = _pest_error_char_pos("--> 99:99", "hi")
    assert pos == len("hi")


def test_pest_error_char_pos_first_line():
    pos = _pest_error_char_pos("--> 1:5", "select 1;")
    assert pos == 4


def test_pest_error_char_pos_later_line():
    text = "a\nb\nccc"
    pos = _pest_error_char_pos("--> 3:2", text)
    assert pos == 5  # line 3, col 2


def test_detect_by_returns_none_when_no_by_keyword():
    assert _detect_unparenthesized_by_expr("select x as y", 8) is None


def test_detect_by_returns_none_when_empty_expression():
    text = "select sum(x) by "
    assert _detect_unparenthesized_by_expr(text, len(text)) is None


def test_compute_line_starts_no_newlines():
    assert _compute_line_starts("hello world") == [0]


def test_compute_line_starts_with_newlines():
    assert _compute_line_starts("a\nbb\nccc") == [0, 2, 5]
