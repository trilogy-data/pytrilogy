"""Inline `(select ...)` subqueries are supported (desugared to an anonymous
rowset). Parenthesized `(with ... as ...)` CTEs are not, and must still surface
a friendly Syntax [102] pointing at the named-rowset alternative."""

from __future__ import annotations

import pytest

from trilogy.core.exceptions import InvalidSyntaxException
from trilogy.parsing.v2.errors import detect_subselect
from trilogy.parsing.v2.lark_backend import parse_lark
from trilogy.parsing.v2.pest_backend import parse_pest

_IMPORTS = "import x as x;\n"


@pytest.mark.parametrize("backend", [parse_lark, parse_pest])
def test_membership_subquery_parses(backend):
    """`x in (select ...)` is now a first-class inline subquery, not an error."""
    src = _IMPORTS + (
        "where x.id in (select x.id where x.state = 'TN')\n" "select x.id;"
    )
    backend(src)


@pytest.mark.parametrize("backend", [parse_lark, parse_pest])
def test_scalar_subquery_parses(backend):
    src = _IMPORTS + "where x.val > (select max(x.val) -> m)\nselect x.id;"
    backend(src)


@pytest.mark.parametrize("backend", [parse_lark, parse_pest])
def test_with_cte_subquery_friendly_error(backend):
    """A parenthesized `with` CTE is still unsupported -> Syntax [102]."""
    src = _IMPORTS + (
        "where x.id in (with foo as (select 1) select x.id from foo)\n" "select x.id;"
    )
    with pytest.raises(InvalidSyntaxException) as exc:
        backend(src)
    assert "Syntax [102]" in str(exc.value)


@pytest.mark.parametrize("backend", [parse_lark, parse_pest])
def test_top_level_select_does_not_fire_102(backend):
    """A bare top-level SELECT must not be flagged — it's the valid form."""
    backend(_IMPORTS + "select x.id;")


def test_detector_ignores_closed_parens():
    """If the `(with ...)` is fully closed before pos, the detector should
    not surface it — the open-paren counter handles that case."""
    text = "where (with a as (select 1) select 1) and "
    assert detect_subselect(text, len(text)) is None


def test_detector_reports_open_paren_position():
    text = "where x in (with foo as (select "
    pos = len(text)
    found = detect_subselect(text, pos)
    assert found is not None
    assert text[found] == "("


def test_detector_ignores_plain_select_subquery():
    """`(select ...)` is supported now — the detector must not flag it."""
    text = "where x > (select max(y) -> m "
    assert detect_subselect(text, len(text)) is None
