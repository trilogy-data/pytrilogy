"""The Syntax [201] "missing alias" error should propose a concrete rewrite
based on the actual unaliased expression (both grammar backends), so the agent
can copy `expr as name` instead of re-deriving it from a generic example."""

from __future__ import annotations

import pytest

from trilogy.core.exceptions import InvalidSyntaxException
from trilogy.parsing.v2.errors import _suggest_alias, _unaliased_select_expr
from trilogy.parsing.v2.lark_backend import parse_lark
from trilogy.parsing.v2.pest_backend import parse_pest

_IMPORTS = "import foo as foo;\nimport a as a;\n"


@pytest.mark.parametrize("backend", [parse_lark, parse_pest])
@pytest.mark.parametrize(
    "query,expected",
    [
        ("select foo.id, count(foo.val) limit 10;", "count(foo.val) as val_count"),
        ("select count(foo.id);", "count(foo.id) as id_count"),
        ("select grouping_id(a.x, a.y);", "grouping_id(a.x, a.y) as y_grouping_id"),
    ],
)
def test_missing_alias_suggests_concrete_rewrite(backend, query, expected):
    with pytest.raises(InvalidSyntaxException) as exc:
        backend(_IMPORTS + query)
    msg = str(exc.value)
    assert "Syntax [201]" in msg, msg
    assert f"`{expected}`" in msg, msg
    # the proposed rewrite must itself parse
    backend(_IMPORTS + query.replace(expected.split(" as ")[0], expected, 1))


def test_suggest_alias_helper():
    assert (
        _suggest_alias("count(store_sales.ext_sales_price)") == "ext_sales_price_count"
    )
    assert _suggest_alias("avg(x.y)") == "y_avg"
    assert _suggest_alias("x + 1") == "x_1"


def test_unaliased_expr_survives_commas_in_args():
    text = "select grouping_id(a.x, a.y) limit 5;"
    pos = text.index("limit")
    assert _unaliased_select_expr(text, pos) == "grouping_id(a.x, a.y)"
