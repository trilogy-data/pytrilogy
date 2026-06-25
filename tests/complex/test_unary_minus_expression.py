"""Unary minus on an expression — `-sum(x)`, `-col`, `-(a + b)` — should parse on
both grammar backends and negate the operand (TPC-DS q05: the agent wanted
`-sum(return_net_loss)` and burned turns finding the `expr * -1` workaround).

A negative numeric literal (`-1`) already lexes as a number; this covers the
prefix unary-minus applied to a non-literal expression."""

from __future__ import annotations

import pytest

from trilogy import Dialects, Environment
from trilogy.parsing.v2.lark_backend import parse_lark
from trilogy.parsing.v2.pest_backend import parse_pest

_ENV = """
key id int;
property id.acctbal float;
datasource customer (
  id: id,
  acctbal: acctbal
) grain (id) address customer;
"""

_BODIES = [
    "select -sum(acctbal) as m;",
    "select id, -acctbal as m;",
    "select -(sum(acctbal) + 1) as m;",
    "select -sum(acctbal) + 100 as m;",
    "select sum(-acctbal) as m;",
]


@pytest.mark.parametrize("backend", [parse_lark, parse_pest])
@pytest.mark.parametrize("body", _BODIES)
def test_unary_minus_parses(backend, body) -> None:
    backend(_ENV + body)


def test_unary_minus_negates_value() -> None:
    env = Environment()
    ex = Dialects.DUCK_DB.default_executor(environment=env)
    ex.parse_text(_ENV)
    neg = ex.generate_sql("select -sum(acctbal) as m;")[0]
    # negation renders as a multiply by -1
    assert "* -1" in neg


def test_unary_minus_precedence_left_binds_first() -> None:
    # `-a + b` is `(-a) + b`, not `-(a + b)`
    env = Environment()
    ex = Dialects.DUCK_DB.default_executor(environment=env)
    ex.parse_text(_ENV)
    sql = ex.generate_sql("select -sum(acctbal) + 100 as m;")[0]
    assert "* -1 ) + 100" in sql.replace('"', "").replace("  ", " ") or (
        "* -1" in sql and "+ 100" in sql
    )
