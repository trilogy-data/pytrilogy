"""Both grammar backends (Lark, Pest) should surface a friendly Syntax [222]
when a named `union(...) -> (...)` (or `with NAME as ...` / `rowset NAME <- ...`)
definition is not terminated with a `;` before the consuming statement.

Real failure mode (TPC-DS q05 enriched eval): the agent wrote a TVF definition
without the trailing `;` and got an opaque "expected metadata, PURPOSE, or
data_type" pointing at the output signature, reading it as "my signature columns
need types" rather than "I forgot a semicolon"."""

from __future__ import annotations

import pytest

from trilogy.core.exceptions import InvalidSyntaxException
from trilogy.parsing.v2.errors import detect_missing_signature_semicolon
from trilogy.parsing.v2.lark_backend import parse_lark
from trilogy.parsing.v2.pest_backend import parse_pest

_UNTERMINATED = """with u as union(
 (select foo as channel, sum(bar) as np),
 (select foo as channel, sum(baz) as np)
) -> (channel, np)
select u.channel, sum(u.np) as s limit 10;"""

_TERMINATED = """with u as union(
 (select foo as channel, sum(bar) as np),
 (select foo as channel, sum(baz) as np)
) -> (channel, np);
select u.channel, sum(u.np) as s limit 10;"""


@pytest.mark.parametrize("backend", [parse_lark, parse_pest])
def test_missing_semicolon_friendly_error(backend):
    with pytest.raises(InvalidSyntaxException) as exc:
        backend(_UNTERMINATED)
    msg = str(exc.value)
    assert "Syntax [222]" in msg, msg
    assert "terminated with a semicolon" in msg, msg


@pytest.mark.parametrize("backend", [parse_lark, parse_pest])
def test_terminated_definition_parses(backend):
    backend(_TERMINATED)


def test_detector_finds_unterminated_signature():
    pos = _UNTERMINATED.index("select u.channel")
    found = detect_missing_signature_semicolon(_UNTERMINATED, pos)
    assert found is not None
    # The `;` belongs right after the signature's closing `)`.
    assert _UNTERMINATED[:found].rstrip().endswith("(channel, np)")


def test_detector_ignores_terminated_signature():
    pos = _TERMINATED.index(");")
    assert detect_missing_signature_semicolon(_TERMINATED, pos) is None
