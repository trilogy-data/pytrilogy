"""Both grammar backends (Lark, Pest) should surface a friendly Syntax [212]
when a `by <grain>` clause is attached to an expression that WRAPS an aggregate
instead of to the aggregate itself.

Real failure mode (TPC-DS q05/q80, ~4M+ tokens of agent thrash): the agent wants
a NULL-safe grouped total and writes

    def rollup_agg(metric) -> coalesce(sum(metric), 0) by rollup a, b;

or `auto r <- coalesce(sum(x), 0) by rollup id;`. The `by` grain clause may only
follow a bare aggregate, so the parser choked on `by` with a generic message and
the agent looped rewriting it. The valid forms are to move the grain inside the
aggregate (`coalesce(sum(x) by id, 0)`) or to compute the grouped aggregate first
(`auto m <- sum(x) by id;` then `coalesce(m, 0)`)."""

from __future__ import annotations

import pytest

from trilogy.core.exceptions import InvalidSyntaxException
from trilogy.parsing.v2.errors import detect_by_on_wrapped_aggregate
from trilogy.parsing.v2.lark_backend import parse_lark
from trilogy.parsing.v2.pest_backend import parse_pest

_IMPORTS = "import x as x;\n"

# Each is the offending statement body; all wrap an aggregate then trail `by`.
_BAD = [
    "auto r <- coalesce(sum(x.val), 0) by rollup x.id;",
    "def rollup_agg(metric) -> coalesce(sum(metric), 0) by rollup x.a, x.b;",
    "select coalesce(sum(x.val), 0) by x.id as r;",
    "auto r <- round(avg(x.val), 2) by x.id;",
    "auto r <- coalesce(count(x.val), 0) by x.a, x.b;",
]

# Valid forms that MUST still parse cleanly (the suggested fixes + bare agg).
_GOOD = [
    "auto r <- coalesce(sum(x.val) by x.id, 0);",
    "auto m <- sum(x.val) by x.id;\nselect coalesce(m, 0) as r;",
    "select sum(x.val) by x.id as r;",
    "auto r <- coalesce(x.val, 0) by x.id;",  # no aggregate wrapped -> not [212]
]


@pytest.mark.parametrize("backend", [parse_lark, parse_pest])
@pytest.mark.parametrize("body", _BAD)
def test_wrapped_aggregate_by_friendly_error(backend, body):
    with pytest.raises(InvalidSyntaxException) as exc:
        backend(_IMPORTS + body)
    msg = str(exc.value)
    assert "Syntax [212]" in msg, msg
    assert "attach directly to an aggregate" in msg, msg


@pytest.mark.parametrize("backend", [parse_lark, parse_pest])
def test_grain_inside_aggregate_parses(backend):
    backend(_IMPORTS + "auto r <- coalesce(sum(x.val) by x.id, 0);")


@pytest.mark.parametrize("backend", [parse_lark, parse_pest])
def test_grouped_aggregate_then_wrap_parses(backend):
    backend(_IMPORTS + "auto m <- sum(x.val) by x.id;\nselect coalesce(m, 0) as r;")


@pytest.mark.parametrize("backend", [parse_lark, parse_pest])
def test_bare_aggregate_by_parses(backend):
    backend(_IMPORTS + "select sum(x.val) by x.id as r;")


def test_detector_finds_wrapped_by():
    text = "auto r <- coalesce(sum(x.val), 0) by rollup x.id;"
    pos = text.index(" by ") + 1
    found = detect_by_on_wrapped_aggregate(text, pos)
    assert found is not None
    assert text[found:].startswith("by ")


def test_detector_ignores_bare_aggregate_by():
    # `sum(x.val) by x.id` is the valid form — must not fire.
    text = "select sum(x.val) by x.id as r;"
    assert detect_by_on_wrapped_aggregate(text, text.index(" by ") + 1) is None


def test_detector_ignores_wrapper_without_aggregate():
    # No aggregate inside the wrapper -> a different error, not [212].
    text = "auto r <- coalesce(x.val, 0) by x.id;"
    assert detect_by_on_wrapped_aggregate(text, text.index(" by ") + 1) is None
