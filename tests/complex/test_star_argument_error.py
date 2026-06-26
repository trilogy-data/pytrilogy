"""Both grammar backends (Lark, Pest) should surface a friendly Syntax [223]
when `*` is passed as a function argument — the SQL `count(*)` idiom, which
Trilogy doesn't support (there is no `*` row-marker).

Real failure mode (TPC-DS enriched eval, recurring): the agent writes
`count(*)` and gets a cryptic `expected sum_operator`, then loops. The fix is to
count a key field (`count(store_sales.id)` — counts are already distinct)."""

from __future__ import annotations

import pytest

from trilogy.core.exceptions import InvalidSyntaxException
from trilogy.parsing.v2.errors import detect_star_argument
from trilogy.parsing.v2.lark_backend import parse_lark
from trilogy.parsing.v2.pest_backend import parse_pest

_BAD = [
    "const x <- count(*);",
    "const x <- sum(*);",
    "select count(*) as c;",
    "select customer.id, count(*) as n;",
    "select customer.id, count( * ) as n;",
]

# Legitimate `*` (multiplication) and `by *` must still parse cleanly.
_GOOD = [
    "const x <- 2 * 3;",
    "select customer.id, sum(s.qty * s.price) as v;",
    "select customer.id, count(s.id) as n;",
    "select customer.id, count(s.id) by * as n;",
]


@pytest.mark.parametrize("backend", [parse_lark, parse_pest])
@pytest.mark.parametrize("body", _BAD)
def test_star_argument_friendly_error(backend, body):
    with pytest.raises(InvalidSyntaxException) as exc:
        backend(body)
    msg = str(exc.value)
    assert "Syntax [223]" in msg, msg
    assert "not a valid argument" in msg, msg


@pytest.mark.parametrize("backend", [parse_lark, parse_pest])
@pytest.mark.parametrize("body", _GOOD)
def test_legitimate_star_parses(backend, body):
    backend(body)


def test_detector_finds_star_argument():
    text = "select count(*) as c;"
    found = detect_star_argument(text, text.index("*"))
    assert found is not None
    assert text[found:].startswith("count(")


def test_detector_ignores_multiplication():
    text = "const x <- 2 * 3;"
    assert detect_star_argument(text, text.index("*")) is None
