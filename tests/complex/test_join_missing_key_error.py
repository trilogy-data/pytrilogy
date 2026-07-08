"""A query-scoped join with a missing/malformed key expression (`union join
...`, `subset join a.id =`) surfaces the opaque grammar rule `expected
sum_operator`. The Syntax [225] path maps it to a join-condition message on both
grammar backends, and must not steal the adjacent alias/filter error paths."""

from __future__ import annotations

import pytest

from trilogy.core.exceptions import InvalidSyntaxException
from trilogy.parsing.v2.errors import detect_join_missing_key
from trilogy.parsing.v2.lark_backend import parse_lark
from trilogy.parsing.v2.pest_backend import parse_pest

_IMPORTS = "import a as a;\nimport b as b;\n"


@pytest.mark.parametrize("backend", [parse_lark, parse_pest])
@pytest.mark.parametrize(
    "query",
    [
        "select a.x as x union join ... limit 5;",
        "select a.x as x subset join a.id = limit 5;",
        "select a.x as x union join limit 5;",
    ],
)
def test_missing_join_key_reports_225(backend, query):
    with pytest.raises(InvalidSyntaxException) as exc:
        backend(_IMPORTS + query)
    assert "Syntax [225]" in str(exc.value), str(exc.value)


@pytest.mark.parametrize("backend", [parse_lark, parse_pest])
@pytest.mark.parametrize(
    "query,expected_code",
    [
        # a missing alias is still 201, not a join-key error
        ("select a.x, count(a.v) limit 5;", "Syntax [201]"),
        # a select-expression error near a downstream join is not 225
        ("select a.x + union join a.id = b.id limit 5;", "Syntax [201]"),
    ],
)
def test_adjacent_paths_not_stolen(backend, query, expected_code):
    with pytest.raises(InvalidSyntaxException) as exc:
        backend(_IMPORTS + query)
    assert expected_code in str(exc.value), str(exc.value)


@pytest.mark.parametrize("backend", [parse_lark, parse_pest])
def test_valid_join_still_parses(backend):
    backend(_IMPORTS + "select a.x as x union join a.id = b.id limit 5;")


def test_detect_helper_skips_when_key_present():
    # a `select` between the join and the failure means the key already parsed
    text = "select a.x union join a.id = b.id select bad;"
    assert detect_join_missing_key(text, len(text) - 1) is None
