"""Both grammar backends (Lark, Pest) should surface a friendly Syntax [229]
when an import names a FILE path instead of a dotted module path. The raw error
is `expected IMPORT_DOT` — IMPORT_DOT being the *leading* relative-dot token,
so it reads as "add dots at the front" and sends the author the wrong way.

Real failure mode (TPC-DS enriched eval, 2026-08-06): the top failure class,
34% of all failed `trilogy` calls. The agent transliterated the path it saw
from `file list` (`import raw/store_sales as ss;`), got refused at write time,
then "fixed" it by dropping `raw/` entirely (`import store_sales as ss;`).
"""

from __future__ import annotations

import pytest

from trilogy.core.exceptions import InvalidSyntaxException
from trilogy.parsing.v2.errors import (
    detect_import_file_path,
    module_path_from_file_path,
)
from trilogy.parsing.v2.lark_backend import parse_lark
from trilogy.parsing.v2.pest_backend import parse_pest

_BAD = [
    "import raw/store_sales as ss;",
    "import raw/web_sales.preql as ws;",
    "import raw\\store_sales as ss;",
    "import raw/dim/item;",
    "from raw/store_sales import x;",
    "import raw.item as i;\nimport raw/store_sales as ss;",
]

_GOOD = [
    "import raw.store_sales as ss;",
    "import store_sales;",
    "import .raw.store_sales as ss;",
]


@pytest.mark.parametrize("backend", [parse_lark, parse_pest])
@pytest.mark.parametrize("body", _BAD)
def test_import_file_path_friendly_error(backend, body):
    with pytest.raises(InvalidSyntaxException) as exc:
        backend(body)
    msg = str(exc.value)
    assert "Syntax [229]" in msg, msg
    assert "MODULE PATH" in msg, msg


@pytest.mark.parametrize("backend", [parse_lark, parse_pest])
@pytest.mark.parametrize("body", _GOOD)
def test_dotted_import_parses(backend, body):
    backend(body)


def test_error_names_the_corrected_path():
    with pytest.raises(InvalidSyntaxException) as exc:
        parse_pest("import raw/web_sales.preql as ws;")
    assert "`import raw.web_sales as <alias>;`" in str(exc.value)


@pytest.mark.parametrize(
    "path,expected",
    [
        ("raw/store_sales", "raw.store_sales"),
        ("raw/web_sales.preql", "raw.web_sales"),
        ("raw\\store_sales", "raw.store_sales"),
        ("./store_sales.preql", "store_sales"),
        ("raw/dim/item", "raw.dim.item"),
    ],
)
def test_module_path_conversion(path, expected):
    text = f"import {path} as x;"
    pos = detect_import_file_path(text, text.index("import"))
    assert pos is not None
    assert module_path_from_file_path(text, pos) == expected


def test_detector_ignores_division_in_unrelated_statement():
    text = "import raw.item as i; select import_count / total as ratio;"
    assert detect_import_file_path(text, text.index("ratio")) is None
