"""Unit coverage for `_collect_subselect_comparisons`, the condition-tree walk
behind `_grain_key_membership_redirect` (TPC-DS q64). The redirect only fires
when a membership is found, and a membership rarely sits at the root of a
HAVING condition -- it is wrapped by the AND/OR and parenthetical combinators
the predicate built around it. The recursion through left/right/content is the
load-bearing part; pin it directly rather than rely on a query happening to
nest one."""

from trilogy.core.enums import BooleanOperator, ComparisonOperator
from trilogy.core.models.build import (
    BuildComparison,
    BuildConditional,
    BuildParenthetical,
    BuildSubselectComparison,
)
from trilogy.dialect.base import _collect_subselect_comparisons


def _membership(left: str) -> BuildSubselectComparison:
    return BuildSubselectComparison(
        left=left, right="set", operator=ComparisonOperator.IN
    )


def test_bare_membership_returned():
    m = _membership("a")
    assert _collect_subselect_comparisons(m) == [m]


def test_plain_comparison_has_no_membership():
    cmp = BuildComparison(left="a", right=1, operator=ComparisonOperator.EQ)
    assert _collect_subselect_comparisons(cmp) == []


def test_descends_conditional_left_and_right():
    left = _membership("a")
    right = _membership("b")
    tree = BuildConditional(left=left, right=right, operator=BooleanOperator.AND)
    assert _collect_subselect_comparisons(tree) == [left, right]


def test_descends_parenthetical_content():
    m = _membership("a")
    assert _collect_subselect_comparisons(BuildParenthetical(content=m)) == [m]


def test_deeply_nested_membership_under_mixed_combinators():
    target = _membership("deep")
    tree = BuildConditional(
        left=BuildComparison(left="x", right=1, operator=ComparisonOperator.GT),
        right=BuildParenthetical(
            content=BuildConditional(
                left=BuildComparison(left="y", right=2, operator=ComparisonOperator.LT),
                right=target,
                operator=BooleanOperator.OR,
            )
        ),
        operator=BooleanOperator.AND,
    )
    assert _collect_subselect_comparisons(tree) == [target]


def test_non_condition_node_yields_nothing():
    assert _collect_subselect_comparisons("a literal") == []
    assert _collect_subselect_comparisons(42) == []
