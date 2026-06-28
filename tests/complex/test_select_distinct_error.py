"""Both grammar backends (Lark, Pest) should surface a friendly Syntax [224]
when a SQL-style `SELECT DISTINCT` is used. Trilogy groups by the non-aggregate
select columns automatically, so distinctness is implicit — `distinct` has no
keyword and the bare word otherwise mis-reports as a missing-alias [201].

Real failure mode (TPC-DS enriched eval): the agent wrote
`select distinct s.channel, s.channel_dim_text_id` to explore distinct values and
got a cryptic `[201] Missing alias? ... distinct s.channel as distinct_s_channel`.
"""

from __future__ import annotations

import pytest

from trilogy.core.exceptions import InvalidSyntaxException
from trilogy.parsing.v2.errors import detect_select_distinct
from trilogy.parsing.v2.lark_backend import parse_lark
from trilogy.parsing.v2.pest_backend import parse_pest

_BAD = [
    "select distinct x.a, x.b limit 5;",
    "select distinct x.a as a;",
    "select  DISTINCT  x.a, x.b;",
]

# A concept whose name merely starts with `distinct` must still parse cleanly.
_GOOD = [
    "select x.a, x.b limit 5;",
    "select x.distinct_flag as f;",
]


@pytest.mark.parametrize("backend", [parse_lark, parse_pest])
@pytest.mark.parametrize("body", _BAD)
def test_select_distinct_friendly_error(backend, body):
    with pytest.raises(InvalidSyntaxException) as exc:
        backend(body)
    msg = str(exc.value)
    assert "Syntax [224]" in msg, msg
    assert "DISTINCT keyword" in msg, msg


@pytest.mark.parametrize("backend", [parse_lark, parse_pest])
@pytest.mark.parametrize("body", _GOOD)
def test_distinct_named_concept_parses(backend, body):
    backend(body)


def test_detector_finds_distinct():
    text = "select distinct x.a, x.b;"
    found = detect_select_distinct(text, text.index("x.a"))
    assert found is not None
    assert text[found:].startswith("distinct")


def test_detector_ignores_distinct_named_concept():
    text = "select x.distinct_flag as f;"
    assert detect_select_distinct(text, text.index("x.distinct_flag")) is None
