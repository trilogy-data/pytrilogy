"""A query-scoped join with a missing/malformed key expression (`union join
...`, `subset join a.id =`) surfaces the opaque grammar rule `expected
sum_operator`. The Syntax [225] path maps it to a join-condition message on both
grammar backends, and must not steal the adjacent alias/filter error paths."""

from __future__ import annotations

import pytest

from trilogy.core.exceptions import InvalidSyntaxException
from trilogy.parsing.v2.errors import (
    detect_join_comma_group,
    detect_join_missing_key,
)
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
        # a missing `as` connector is still 201, not a join-key error
        ("select a.x, count(a.v) total limit 5;", "Syntax [201]"),
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


@pytest.mark.parametrize("backend", [parse_lark, parse_pest])
@pytest.mark.parametrize(
    "query",
    [
        # well-formed key, but the join sits BEFORE the `where`
        "subset join a.id = b.id where a.x select a.x as x;",
        # well-formed key, but the join is a standalone statement (no select)
        "subset join a.id = b.id;",
        "union join a.id = b.id;",
    ],
)
def test_misplaced_join_reports_226(backend, query):
    with pytest.raises(InvalidSyntaxException) as exc:
        backend(_IMPORTS + query)
    assert "Syntax [226]" in str(exc.value), str(exc.value)


@pytest.mark.parametrize("backend", [parse_lark, parse_pest])
def test_misplaced_226_does_not_steal_valid_trailing_join(backend):
    # a well-formed join in a VALID position with a downstream error stays put
    with pytest.raises(InvalidSyntaxException) as exc:
        backend(_IMPORTS + "select a.x + subset join a.id = b.id limit 5;")
    assert "Syntax [226]" not in str(exc.value), str(exc.value)


# A join written AFTER the select list leaves no `select` between itself and the
# failure, so `detect_join_missing_key`'s guard cannot fire; without the hoisted
# end-of-input probe a plain missing `;` reports as a malformed join key.
@pytest.mark.parametrize("backend", [parse_lark, parse_pest])
@pytest.mark.parametrize(
    "query",
    [
        "select a.x, count(a.v) as n1\nunion join a.id = b.id",
        "select a.x,\nunion join a.id = b.id\nunion join a.k = b.k",
        "select a.x,\nunion join a.id = b.id and a.k = b.k",
        "select a.x,\nsubset join a.id = b.id\nwhere a.v = 2001",
        "rowset r <- select a.x,\nsubset join a.id = b.id",
    ],
)
def test_unterminated_post_select_join_reports_202(backend, query):
    with pytest.raises(InvalidSyntaxException) as exc:
        backend(_IMPORTS + query)
    assert "Syntax [202]" in str(exc.value), str(exc.value)


@pytest.mark.parametrize("backend", [parse_lark, parse_pest])
def test_unterminated_join_with_bad_key_still_reports_225(backend):
    with pytest.raises(InvalidSyntaxException) as exc:
        backend(_IMPORTS + "select a.x as x union join ... limit 5")
    assert "Syntax [225]" in str(exc.value), str(exc.value)


@pytest.mark.parametrize("backend", [parse_lark, parse_pest])
@pytest.mark.parametrize(
    "query",
    [
        "select a.x,\nsubset join a.id = b.id,\n  a.k = b.k;",
        "subset join a.id = b.id,\n  a.k = b.k\nselect a.x;",
        "select a.x,\nunion join a.id = b.id, a.k = b.k\nlimit 5;",
    ],
)
def test_comma_between_join_groups_reports_230(backend, query):
    with pytest.raises(InvalidSyntaxException) as exc:
        backend(_IMPORTS + query)
    assert "Syntax [230]" in str(exc.value), str(exc.value)


@pytest.mark.parametrize("backend", [parse_lark, parse_pest])
def test_comma_inside_join_key_call_is_not_230(backend):
    backend(_IMPORTS + "select a.x,\nsubset join coalesce(a.id, 0) = b.id;")


@pytest.mark.parametrize("backend", [parse_lark, parse_pest])
def test_select_list_comma_after_join_is_not_230(backend):
    # the comma belongs to the select list of a LEGACY pre-select join
    with pytest.raises(InvalidSyntaxException) as exc:
        backend(_IMPORTS + "subset join a.id = b.id select a.x, , a.v;")
    assert "Syntax [230]" not in str(exc.value), str(exc.value)


def test_comma_helper_requires_comma_at_failure():
    text = "select a.x subset join a.id = b.id and a.k = b.k"
    assert detect_join_comma_group(text, len(text)) is None
