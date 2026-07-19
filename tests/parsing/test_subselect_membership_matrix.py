"""Structural parse matrix for the subselect / membership surface, on BOTH
grammar backends.

Two live rules are exercised here:

* ``expr_tuple`` (``parsing/v2/rules/subselect_rules.py``) — builds the
  ``TupleWrapper`` for a parenthesized ``(a, b, ...)`` operand, including the
  heterogeneous-elements branch where the element datatypes cannot be merged
  and the wrapper falls back to ``UNKNOWN``.
* the membership half of ``comparison``
  (``parsing/v2/rules/expression_rules.py``) — the ``in`` / ``not in`` handler
  that lowers composite tuples to ``ROW_TUPLE`` (``rewrite_composite_membership``),
  reconciles a ``(select ...)`` RHS against the left side
  (``resolve_subquery_membership``), and admits a multi-output subquery only in
  the membership-RHS scope (``membership_subquery_scope``).

The parse output *shape* is asserted here; end-to-end execution correctness of
the same constructs lives in ``tests/engine/test_duckdb_subquery.py`` and
``tests/engine/test_duckdb_tuple_membership.py``.
"""

from __future__ import annotations

import pytest

from trilogy import parse
from trilogy.constants import CONFIG, ParserBackend
from trilogy.core.enums import ComparisonOperator, FunctionType
from trilogy.core.exceptions import InvalidSyntaxException
from trilogy.core.models.author import (
    Comparison,
    ConceptRef,
    Function,
    SubqueryItem,
    SubselectComparison,
)
from trilogy.core.models.core import DataType, TupleWrapper
from trilogy.parsing.v2.model import HydrationError

_MODEL = """
key id int;
property id.val int;
property id.cat int;
property id.nm string;
property id.dt date;
"""
_SETUP = _MODEL + "with rs as select id, val, cat, nm, dt where val >= 20;\n"


@pytest.fixture(params=[ParserBackend.PEST, ParserBackend.LARK])
def backend(request):
    prior = CONFIG.parser_backend
    CONFIG.parser_backend = request.param
    yield request.param
    CONFIG.parser_backend = prior


def _predicate(body: str):
    """The single top-level WHERE predicate of the final statement."""
    _, stmts = parse(_SETUP + body)
    return stmts[-1].where_clause.conditional


def _is_row_tuple(node) -> bool:
    return isinstance(node, Function) and node.operator == FunctionType.ROW_TUPLE


# --- expr_tuple: TupleWrapper construction & datatype reduction ----------------


@pytest.mark.parametrize(
    "elements,expected_type,expected_nullable,expected_len",
    [
        ("10, 20, 30", DataType.INTEGER, False, 3),
        ("10, null", DataType.INTEGER, True, 2),  # a NULL element -> nullable
        ("10,", DataType.INTEGER, False, 1),  # single-element trailing comma
    ],
)
def test_value_list_tuple_wrapper(
    backend, elements, expected_type, expected_nullable, expected_len
):
    # A scalar left with a parenthesized list keeps the RHS as a value-list
    # TupleWrapper (no ROW_TUPLE lowering); its merged element type is set here.
    right = _predicate(f"select id where val in ({elements});").right
    assert isinstance(right, TupleWrapper)
    assert right.type == expected_type
    assert right.nullable is expected_nullable
    assert len(right.val) == expected_len


def test_heterogeneous_composite_tuple_reduces_to_unknown(backend):
    # (nm, dt) mixes STRING and DATE, so reduce_tuple_element_datatypes raises
    # and expr_tuple falls back to UNKNOWN. As a composite membership left the
    # wrapper is then lowered to ROW_TUPLE, which is valid because positions are
    # compared independently against the same-typed RHS positions.
    pred = _predicate("select id where (nm, dt) in (rs.nm, rs.dt);")
    assert _is_row_tuple(pred.left)
    assert _is_row_tuple(pred.right)


def test_heterogeneous_value_list_rejected_by_type_validation(backend):
    # The same UNKNOWN wrapper as a scalar value list is NOT a row tuple, so
    # SubselectComparison's element-type check rejects comparing STRING to DATE.
    with pytest.raises(InvalidSyntaxException, match="Cannot compare"):
        _predicate("select id where nm in (nm, dt);")


# --- membership comparison shapes ---------------------------------------------


@pytest.mark.parametrize(
    "op_text,op", [("in", ComparisonOperator.IN), ("not in", ComparisonOperator.NOT_IN)]
)
@pytest.mark.parametrize(
    "predicate,left_check,right_check",
    [
        # scalar value-list membership: left stays a concept, right a TupleWrapper
        (
            "val {op} (10, 20)",
            lambda n: isinstance(n, ConceptRef),
            lambda n: isinstance(n, TupleWrapper),
        ),
        # composite tuple membership: both sides lowered to ROW_TUPLE
        (
            "(val, cat) {op} (rs.val, rs.cat)",
            _is_row_tuple,
            _is_row_tuple,
        ),
        # scalar subquery membership: right is a single-column SubqueryItem
        (
            "val {op} (select rs.val)",
            lambda n: isinstance(n, ConceptRef),
            lambda n: isinstance(n, SubqueryItem),
        ),
        # tuple subquery membership: left lowered to ROW_TUPLE, right SubqueryItem
        (
            "(val, cat) {op} (select rs.val, rs.cat)",
            _is_row_tuple,
            lambda n: isinstance(n, SubqueryItem),
        ),
    ],
)
def test_membership_yields_subselect_comparison(
    backend, op_text, op, predicate, left_check, right_check
):
    pred = _predicate(f"select id where {predicate.format(op=op_text)};")
    assert isinstance(pred, SubselectComparison)
    assert pred.operator == op
    assert left_check(pred.left)
    assert right_check(pred.right)


# --- membership_subquery_scope: multi-output only admissible as a membership RHS


@pytest.mark.parametrize(
    "predicate",
    [
        "(val, cat) in (select rs.val, rs.cat)",  # tuple-left admits it
        "(val, cat) not in (select rs.val, rs.cat)",
    ],
)
def test_multi_output_subquery_admitted_on_membership_rhs(backend, predicate):
    pred = _predicate(f"select id where {predicate};")
    assert isinstance(pred, SubselectComparison)
    assert isinstance(pred.right, SubqueryItem)
    assert len(pred.right.contents) == 2


@pytest.mark.parametrize(
    "body",
    [
        # scalar-comparison position (not a membership) rejects multi-output
        "select id where val > (select rs.val, rs.cat);",
        # plain projected scalar position rejects multi-output
        "select rs.val, (select rs.val, rs.cat) as bad;",
    ],
)
def test_multi_output_subquery_rejected_outside_membership(backend, body):
    with pytest.raises(HydrationError, match="exactly one"):
        parse(_SETUP + body)


# --- arity / shape error matrix ------------------------------------------------


@pytest.mark.parametrize(
    "predicate,exc,message",
    [
        # tuple left, non-tuple (single parenthesized) right
        (
            "(val, cat) in (rs.val)",
            InvalidSyntaxException,
            "can only be tested for membership",
        ),
        # two tuples of differing length
        (
            "(val, cat) in (rs.val, rs.cat, rs.id)",
            InvalidSyntaxException,
            "same number of elements",
        ),
        # tuple left vs subquery projecting a different arity
        (
            "(val, cat) in (select rs.val)",
            HydrationError,
            "left side has 2 fields but the subquery selects 1",
        ),
        # scalar left vs multi-column subquery
        (
            "val in (select rs.val, rs.cat)",
            HydrationError,
            "projects 2 columns",
        ),
    ],
)
def test_membership_arity_errors(backend, predicate, exc, message):
    with pytest.raises(exc, match=message):
        _predicate(f"select id where {predicate};")


# --- non-membership operators are untouched -----------------------------------


@pytest.mark.parametrize(
    "predicate,right_check",
    [
        # `>` with a single-output subquery stays a plain Comparison (not
        # rewritten to SubselectComparison), carrying the SubqueryItem RHS
        ("val > (select max(val) -> m)", lambda n: isinstance(n, SubqueryItem)),
        # ordinary scalar comparison is unaffected by any membership logic
        ("val = cat", lambda n: isinstance(n, ConceptRef)),
    ],
)
def test_non_membership_comparison_unchanged(backend, predicate, right_check):
    pred = _predicate(f"select id where {predicate};")
    assert isinstance(pred, Comparison)
    assert not isinstance(pred, SubselectComparison)
    assert right_check(pred.right)
