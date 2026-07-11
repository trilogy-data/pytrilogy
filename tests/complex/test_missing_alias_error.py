"""Unaliased select expressions now parse (they become anonymous outputs named
via ``suggest_select_alias``). The Syntax [201] "missing alias" error remains
for the genuinely broken `expr name` form (a missing `as` connector) and still
proposes a concrete rewrite based on the actual expression."""

from __future__ import annotations

import pytest

from trilogy.core.exceptions import InvalidSyntaxException
from trilogy.parsing.v2.errors import (
    _unaliased_select_expr,
    suggest_select_alias,
)
from trilogy.parsing.v2.lark_backend import parse_lark
from trilogy.parsing.v2.pest_backend import parse_pest

_IMPORTS = "import foo as foo;\nimport a as a;\n"


@pytest.mark.parametrize("backend", [parse_lark, parse_pest])
@pytest.mark.parametrize(
    "query",
    [
        "select foo.id, count(foo.val) limit 10;",
        "select count(foo.id);",
        "select grouping_id(a.x, a.y);",
    ],
)
def test_unaliased_expressions_parse(backend, query):
    backend(_IMPORTS + query)


@pytest.mark.parametrize("backend", [parse_lark, parse_pest])
def test_missing_as_connector_still_errors(backend):
    with pytest.raises(InvalidSyntaxException) as exc:
        backend(_IMPORTS + "select count(foo.val) total limit 10;")
    msg = str(exc.value)
    assert "Syntax [201]" in msg, msg


def test_suggest_alias_helper():
    assert (
        suggest_select_alias("count(store_sales.ext_sales_price)")
        == "ext_sales_price_count"
    )
    assert suggest_select_alias("avg(x.y)") == "y_avg"
    assert suggest_select_alias("x + 1") == "x_1"
    # trailing `)` of a paren-by must not read as one function call
    assert suggest_select_alias("sum(val) by (name, id)") == "sum_val_by_name_id"
    assert suggest_select_alias("1 + x") == "value_1_x"
    assert suggest_select_alias("") == "value"


def test_unaliased_expr_survives_commas_in_args():
    text = "select grouping_id(a.x, a.y) limit 5;"
    pos = text.index("limit")
    assert _unaliased_select_expr(text, pos) == "grouping_id(a.x, a.y)"
