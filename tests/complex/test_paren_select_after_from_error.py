"""Both grammar backends should surface a friendly Syntax [228] when a
`chart`/`copy` statement's `from` is handed a parenthesized select — the
grammar wants a bare select statement, and the raw `expected select_statement`
error doesn't hint that the parens are the problem."""

from __future__ import annotations

import pytest

from trilogy.core.exceptions import InvalidSyntaxException
from trilogy.parsing.v2.errors import detect_paren_select_after_from
from trilogy.parsing.v2.lark_backend import parse_lark
from trilogy.parsing.v2.pest_backend import parse_pest

_MODEL = """key category string;
property category.value int;
datasource d (cat: category, val: value) grain (category) address t;
"""

_BAD = [
    _MODEL + """chart layer bar ( x_axis <- category, y_axis <- value )
from (
select category, value
);
""",
    _MODEL + """copy into csv 'out.csv' from (select category, value);
""",
]

_GOOD = [
    _MODEL + """chart layer bar ( x_axis <- category, y_axis <- value )
from select category, value;
""",
    _MODEL + """copy into csv 'out.csv' from select category, value;
""",
]


@pytest.mark.parametrize("backend", [parse_lark, parse_pest])
@pytest.mark.parametrize("body", _BAD)
def test_paren_select_after_from_friendly_error(backend, body):
    with pytest.raises(InvalidSyntaxException) as exc:
        backend(body)
    msg = str(exc.value)
    assert "Syntax [228]" in msg, msg
    assert "remove the parentheses" in msg, msg


@pytest.mark.parametrize("backend", [parse_lark, parse_pest])
@pytest.mark.parametrize("body", _GOOD)
def test_bare_select_after_from_parses(backend, body):
    backend(body)


def test_detector_finds_paren():
    text = "chart layer bar ( x_axis <- a, y_axis <- b ) from (select a, b);"
    pos = text.rindex("(")
    found = detect_paren_select_after_from(text, pos)
    assert found == pos


def test_detector_ignores_non_chart_statement():
    text = "auto x <- (select max(a) as m);"
    assert detect_paren_select_after_from(text, text.rindex("(")) is None
