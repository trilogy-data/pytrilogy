"""Both grammar backends (Lark, Pest) should surface a friendly Syntax [233]
when `by` is written as a clause after the select list.

Real failure mode (TPC-DS agent eval): agents write `select a, coalesce(sum(x),
0) as profit by *;` or `select count(k) as n, by *;`, treating `by *` as a
select-level clause. Trilogy has none: grouping is automatic, and a grain
pins to one aggregate (`sum(x) by * as total`)."""

from __future__ import annotations

import pytest

from trilogy.core.exceptions import InvalidSyntaxException
from trilogy.parsing.v2.errors import detect_select_level_by
from trilogy.parsing.v2.lark_backend import parse_lark
from trilogy.parsing.v2.pest_backend import parse_pest

_IMPORTS = "import x as x;\n"

_BAD = [
    "select x.a, coalesce(sum(x.val), 0) as profit by *;",
    "select count(x.id) as n, by *;",
    "select x.a, sum(x.val) as s by x.a;",
    "select x.a, sum(x.val) as s by x.a, x.b;",
    "select x.a, sum(x.val) as s\nby *\norder by x.a asc;",
    "where x.a > 1 select sum(x.val) as s by *;",
]

_GOOD = [
    "select x.a, sum(x.val) by * as s;",
    "select sum(x.val) by x.a as s, count(x.id) as n;",
    "select x.a, sum(x.val) as s by rollup (x.a);",
    "select x.a, sum(x.val) as s order by x.a asc;",
]


@pytest.mark.parametrize("backend", [parse_lark, parse_pest])
@pytest.mark.parametrize("body", _BAD)
def test_select_level_by_friendly_error(backend, body):
    text = _IMPORTS + body
    with pytest.raises(InvalidSyntaxException) as exc:
        backend(text)
    msg = str(exc.value)
    assert "Syntax [233]" in msg, msg
    assert "no select-level `by`" in msg, msg
    assert "??? by" in msg, msg


@pytest.mark.parametrize("backend", [parse_lark, parse_pest])
@pytest.mark.parametrize("body", _GOOD)
def test_aggregate_level_by_parses(backend, body):
    backend(_IMPORTS + body)


@pytest.mark.parametrize("body", _BAD)
def test_detector_points_at_by(body):
    pos = body.rindex("by")
    found = detect_select_level_by(body, pos)
    assert found is not None
    assert body[found : found + 2] == "by"


@pytest.mark.parametrize(
    "text",
    [
        "select x.a, sum(x.val) by * as s;",
        "select x.a, sum(x.val) as s by rollup (x.a);",
        "select x.a, sum(x.val by *) as s;",
        "auto r <- coalesce(sum(x.val), 0) by x.id;",
    ],
)
def test_detector_ignores_valid_and_sibling_shapes(text):
    assert detect_select_level_by(text, text.rindex("by")) is None
