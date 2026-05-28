"""Both grammar backends (Lark, Pest) should surface a friendly Syntax [102]
when a SQL-style subquery (`(select ...)` / `(with ...)`) appears in scope.

Real failure mode: the agent reaches for `IN (SELECT ...)` to filter on a
related dimension. Trilogy auto-resolves dimension joins via dot-paths, so
the subselect is unnecessary — but the bare Lark/Pest error just complained
about an unexpected token, which doesn't tell the agent what to do."""

from __future__ import annotations

import pytest

from trilogy.core.exceptions import InvalidSyntaxException
from trilogy.parsing.v2.errors import detect_subselect
from trilogy.parsing.v2.lark_backend import parse_lark
from trilogy.parsing.v2.pest_backend import parse_pest

_IMPORTS = "import x as x;\n"


@pytest.mark.parametrize("backend", [parse_lark, parse_pest])
def test_subselect_in_in_clause_friendly_error(backend):
    src = _IMPORTS + (
        "where x.id in (select x.id where x.state = 'TN')\n" "select x.id;"
    )
    with pytest.raises(InvalidSyntaxException) as exc:
        backend(src)
    msg = str(exc.value)
    assert "Syntax [102]" in msg, msg
    assert "dot-path" in msg or "dotted path" in msg, msg


@pytest.mark.parametrize("backend", [parse_lark, parse_pest])
def test_scalar_subselect_friendly_error(backend):
    src = _IMPORTS + "where x.val > (select max(x.val))\nselect x.id;"
    with pytest.raises(InvalidSyntaxException) as exc:
        backend(src)
    assert "Syntax [102]" in str(exc.value)


@pytest.mark.parametrize("backend", [parse_lark, parse_pest])
def test_with_subquery_friendly_error(backend):
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
    """If the `(select ...)` is fully closed before pos, the detector should
    not surface it — the open-paren counter handles that case."""
    text = "where (select 1) and "
    assert detect_subselect(text, len(text)) is None


def test_detector_reports_open_paren_position():
    text = "where x in (select x.id where "
    pos = len(text)
    found = detect_subselect(text, pos)
    assert found is not None
    assert text[found] == "("
