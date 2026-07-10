"""Both grammar backends (Lark, Pest) should surface a friendly Syntax [105]
when a `<-`-connected derivation (`rowset`/`auto`/`metric`/`property`) is written
with the SQL `as` connector instead — e.g. `rowset base as select ...`. These
keywords bind their name with `<-`; a `rowset` may also use `with <name> as ...`.

Real failure mode (TPC-DS enriched eval): the agent wrote `rowset base as select
...` and got the cryptic catch-all `expected EOI, block, or show_statement` with
the caret at the statement start and no direction.
"""

from __future__ import annotations

import pytest

from trilogy.core.exceptions import InvalidSyntaxException
from trilogy.parsing.v2.errors import detect_derivation_as_connector
from trilogy.parsing.v2.lark_backend import parse_lark
from trilogy.parsing.v2.pest_backend import parse_pest

_BAD = [
    "rowset base as select 1 as x;",
    "auto total as sum(1);",
    "metric total as sum(1);",
    "property foo.bar as 1;",
]

_GOOD = [
    "rowset base <- select 1 as x;",
    "with base as select 1 as x;",
    "auto total <- 1 + 1;",
    "select 1 as total;",
]


@pytest.mark.parametrize("backend", [parse_lark, parse_pest])
@pytest.mark.parametrize("body", _BAD)
def test_derivation_as_connector_friendly_error(backend, body):
    with pytest.raises(InvalidSyntaxException) as exc:
        backend(body)
    msg = str(exc.value)
    assert "Syntax [105]" in msg, msg
    assert "`<-`" in msg, msg


@pytest.mark.parametrize("backend", [parse_lark, parse_pest])
@pytest.mark.parametrize("body", _GOOD)
def test_valid_derivation_forms_parse(backend, body):
    backend(body)


def test_detector_finds_as_connector():
    text = "rowset base as select 1 as x;"
    found = detect_derivation_as_connector(text, 0)
    assert found is not None
    assert text[found:].startswith("as")


def test_detector_ignores_arrow_form():
    text = "rowset base <- select 1 as x;"
    assert detect_derivation_as_connector(text, 0) is None
