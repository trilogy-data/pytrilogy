"""Both grammar backends (Lark, Pest) should surface a friendly Syntax [103]
when a SQL-style `GROUP BY` clause appears.

Real failure mode: the agent writes `select a, count(b) group by a` out of SQL
habit. Trilogy groups automatically by the non-aggregated select fields, so the
bare parser error (`expected limit, order_by, having, ...`) didn't tell the agent
to just drop the clause."""

from __future__ import annotations

import pytest

from trilogy.core.exceptions import InvalidSyntaxException
from trilogy.parsing.v2.errors import detect_group_by
from trilogy.parsing.v2.lark_backend import parse_lark
from trilogy.parsing.v2.pest_backend import parse_pest

_IMPORTS = "import x as x;\n"


@pytest.mark.parametrize("backend", [parse_lark, parse_pest])
def test_group_by_friendly_error(backend):
    src = _IMPORTS + "select x.id, count(x.val) as cnt group by x.id;"
    with pytest.raises(InvalidSyntaxException) as exc:
        backend(src)
    msg = str(exc.value)
    assert "Syntax [103]" in msg, msg
    assert "no GROUP BY" in msg, msg


@pytest.mark.parametrize("backend", [parse_lark, parse_pest])
def test_group_by_does_not_fire_on_valid_query(backend):
    """Automatic grouping (no GROUP BY) is the valid form and must parse."""
    backend(_IMPORTS + "select x.id, count(x.val) as cnt;")


@pytest.mark.parametrize("backend", [parse_lark, parse_pest])
def test_group_function_does_not_fire_103(backend):
    """`group(x) by dim` (the group function) is not a GROUP BY clause."""
    # Should parse cleanly; the detector requires `group` followed by `by`.
    backend(_IMPORTS + "select x.id, group(x.val) by x.id as g;")


def test_detector_finds_group_by():
    text = "select x.id, count(x.val) as cnt group by x.id;"
    pos = text.index("group")
    found = detect_group_by(text, pos)
    assert found is not None
    assert text[found:].startswith("group by")


def test_detector_ignores_group_function():
    text = "select group(x.val) by x.id as g;"
    assert detect_group_by(text, text.index("group")) is None
