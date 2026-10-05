"""Both grammar backends should surface a friendly Syntax [232] when a `call`
or `copy into` target is missing its backticks, instead of listing grammar
token names (`expected MULTILINE_STRING or FILE_PATH`)."""

from __future__ import annotations

import pytest

from trilogy.core.exceptions import InvalidSyntaxException
from trilogy.parsing.v2.errors import detect_unquoted_path_target
from trilogy.parsing.v2.lark_backend import parse_lark
from trilogy.parsing.v2.pest_backend import parse_pest

_BAD = [
    ("call ./s.py;", "`./s.py`"),
    ("call ./tool/s.py from select 1 -> x;", "`./tool/s.py`"),
    ("const x <- 1;\nCALL s.py;", "`s.py`"),
    ("copy into csv out.csv from select 1 -> x;", "`out.csv`"),
]

_GOOD = [
    "call `./s.py`;",
    "call `./s.py` from select 1 -> x;",
    "copy into csv `out.csv` from select 1 -> x;",
    "copy into csv 'out.csv' from select 1 -> x;",
]


@pytest.mark.parametrize("backend", [parse_lark, parse_pest])
@pytest.mark.parametrize("body, quoted", _BAD)
def test_unquoted_path_target_friendly_error(backend, body: str, quoted: str):
    with pytest.raises(InvalidSyntaxException) as exc:
        backend(body)
    msg = str(exc.value)
    assert "Syntax [232]" in msg, msg
    assert f"write {quoted}" in msg, msg


@pytest.mark.parametrize("backend", [parse_lark, parse_pest])
@pytest.mark.parametrize("body", _GOOD)
def test_quoted_path_target_parses(backend, body: str):
    backend(body)


def test_detector_ignores_other_statements():
    text = "auto call_count <- 1;\nselect call_count +;"
    assert detect_unquoted_path_target(text, text.index("+")) is None
