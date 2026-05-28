"""Both grammar backends (Lark, Pest) should surface a friendly Syntax [203]
when a derivation keyword (`auto`/`metric`/`property`/`rowset`) is followed
by a name but no `<-` and expression.

Real failure mode: an agent stops mid-write_file (truncated tool-call output),
and the parse rejection used to mention only Lark's internal `__ANON_0` token
name, which is unactionable. The new error explicitly names `<-` and shows a
template."""

from __future__ import annotations

import pytest

from trilogy.core.exceptions import InvalidSyntaxException
from trilogy.parsing.v2.lark_backend import parse_lark
from trilogy.parsing.v2.pest_backend import parse_pest

_IMPORTS = "import x as x;\n"

_TRUNCATED = [
    "auto y ",
    "metric m ",
    "property p ",
    "rowset r ",
]


@pytest.mark.parametrize("tail", _TRUNCATED)
@pytest.mark.parametrize("backend", [parse_lark, parse_pest])
def test_derivation_missing_arrow_friendly_error(backend, tail):
    with pytest.raises(InvalidSyntaxException) as exc:
        backend(_IMPORTS + tail)
    msg = str(exc.value)
    assert "Syntax [203]" in msg, msg
    assert "<-" in msg, msg


@pytest.mark.parametrize("backend", [parse_lark, parse_pest])
def test_arrow_with_no_expression_does_not_fire_203(backend):
    """When the arrow IS present but the expression is missing, that's a
    different failure (expression-side) — we don't want to wrongly suggest
    the arrow is missing."""
    with pytest.raises(InvalidSyntaxException) as exc:
        backend(_IMPORTS + "auto y <-")
    assert "Syntax [203]" not in str(exc.value)


@pytest.mark.parametrize("backend", [parse_lark, parse_pest])
def test_valid_derivation_parses(backend):
    backend(_IMPORTS + "auto y <- count(x);")


@pytest.mark.parametrize("backend", [parse_lark, parse_pest])
def test_real_agent_truncation_repro(backend):
    """The exact shape that hit the tpch agent on q13: imports, then a
    derivation keyword + name with nothing after."""
    src = (
        "# comment\n"
        "import orders as orders;\n"
        "import customer as customer;\n"
        "\n"
        "auto orders_per_customer "
    )
    with pytest.raises(InvalidSyntaxException) as exc:
        backend(src)
    assert "Syntax [203]" in str(exc.value)
