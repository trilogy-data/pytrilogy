"""Both grammar backends (Lark, Pest) should surface a friendly Syntax [213]
when a `by <grain>` clause is attached to an expression that contains NO
aggregate at all — e.g. `item.current_price by item.id`.

Real failure mode (TPC-DS q06, enriched run 20260629-030015): the agent wants
each distinct item's price once at item grain and writes

    auto item_price_by_cat <- item.current_price by item.id, item.category;

`by` only attaches to an aggregate, so the parser emitted a raw grammar-token
list ("expected LOGICAL_OR, ...") that gave no hint the fix is to wrap the
expression in `group(...)`. The agent then looped for many turns. The valid
form is `group(item.current_price) by item.id, item.category` (one value per
grain) or a reduction such as `max(...) by ...`."""

from __future__ import annotations

import pytest

from trilogy.core.exceptions import InvalidSyntaxException
from trilogy.parsing.v2.errors import (
    detect_by_on_non_aggregate,
    detect_by_on_wrapped_aggregate,
)
from trilogy.parsing.v2.lark_backend import parse_lark
from trilogy.parsing.v2.pest_backend import parse_pest

_IMPORTS = "import x as x;\n"

# Offending statement bodies: `by` trails an expression with no aggregate.
_BAD = [
    "auto r <- x.current_price by x.id, x.category;",
    "auto r <- x.val by x.id;",
    "auto r <- (x.val + 1) by x.id;",
    "auto r <- coalesce(x.val, 0) by x.id;",
    "select x.val by x.id as r;",
]

# Valid forms that MUST still parse cleanly (no false positive).
_GOOD = [
    "auto r <- group(x.current_price) by x.id, x.category;",
    "select max(x.val) by x.id as r;",
    "auto r <- sum(x.val) by x.id;",
]


@pytest.mark.parametrize("backend", [parse_lark, parse_pest])
@pytest.mark.parametrize("body", _BAD)
def test_non_aggregate_by_friendly_error(backend, body):
    with pytest.raises(InvalidSyntaxException) as exc:
        backend(_IMPORTS + body)
    msg = str(exc.value)
    assert "Syntax [213]" in msg, msg
    assert "group(" in msg, msg


@pytest.mark.parametrize("backend", [parse_lark, parse_pest])
@pytest.mark.parametrize("body", _GOOD)
def test_aggregate_by_still_parses(backend, body):
    backend(_IMPORTS + body)


def test_detector_finds_bare_identifier_by():
    text = "auto r <- x.current_price by x.id, x.category;"
    found = detect_by_on_non_aggregate(text, text.index(" by ") + 1)
    assert found is not None
    assert text[found:].startswith("by ")


def test_detector_ignores_aggregate_by():
    text = "select sum(x.val) by x.id as r;"
    assert detect_by_on_non_aggregate(text, text.index(" by ") + 1) is None


def test_detector_ignores_wrapped_aggregate_by():
    # `coalesce(sum(x),0) by ...` has an aggregate -> [212], not [213].
    text = "auto r <- coalesce(sum(x.val), 0) by x.id;"
    pos = text.index(" by ") + 1
    assert detect_by_on_non_aggregate(text, pos) is None
    assert detect_by_on_wrapped_aggregate(text, pos) is not None


def test_detector_ignores_select_level_rollup():
    # `by rollup (...)` is a valid SELECT-level grouping clause, not a misplaced grain.
    text = "select x.val, sum(x.amt) as t by rollup (x.id) as r;"
    assert detect_by_on_non_aggregate(text, text.index(" by ") + 1) is None
