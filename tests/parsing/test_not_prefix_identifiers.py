"""Identifiers with a keyword prefix (`note` vs `not`, `andy` vs `and`) must
lex as identifiers. The pest grammar's `fnot` lacked a keyword-boundary guard,
so `note` parsed as `not e` — an error when `e` is undefined, silently wrong
semantics when it exists."""

from __future__ import annotations

import pytest

from trilogy.parsing.v2.lark_backend import parse_lark
from trilogy.parsing.v2.pest_backend import parse_pest

_MODEL = """key id int;
property id.note string;
property id.andy string;
property id.origin string;
datasource t (id: id, note: note, andy: andy, origin: origin) grain (id) address tbl;
"""

_QUERIES = [
    "select note;",
    "select id, note;",
    "select id where note = 'x';",
    "select andy;",
    "select origin;",
    "select id where not (note = 'x');",
]


@pytest.mark.parametrize("backend", [parse_lark, parse_pest])
@pytest.mark.parametrize("query", _QUERIES)
def test_keyword_prefix_identifiers_parse(backend, query):
    backend(_MODEL + query)
