"""An engine-appended atom must not be reassociated by SQL operator precedence.

Authored parentheses arrive as ``BuildParenthetical`` and render as parentheses,
but predicate pushdown AND-ing a HAVING atom onto a WHERE whose top level is an
OR chain builds a bare ``BuildConditional`` tree. ``render_expr`` emitted
``left op right`` with no parenthesization, so ``(A or B) and C`` rendered as
``A or B and C`` and reparsed as ``A or (B and C)`` — the appended atom applied
to a single OR arm and silently no-op'd on the rest.

Repro: evals/tpcds_agent/bug_q47_window_rowset_churn.md (bug A).
"""

import pytest

from trilogy import Dialects, Environment
from trilogy.core.enums import BooleanOperator, ComparisonOperator
from trilogy.core.models.build import BuildComparison, BuildConditional
from trilogy.dialect.base import _protect_conditional_child

_MODEL = """
key sk int;
property sk.year int;
property sk.month int;
property sk.amount float;

datasource sales (s_sk: sk, s_year: year, s_month: month, s_amt: amount)
grain (sk)
query '''
select 1 as s_sk, 1999 as s_year, 3 as s_month, 10.0 as s_amt union all
select 2, 1999, 4, 20.0 union all
select 3, 1998, 12, 30.0 union all
select 4, 1998, 11, 40.0 union all
select 5, 2000, 1, 50.0
''';
"""

_QUERY = """
where (year = 1999) or (year = 1998 and month = 12)
select
    year,
    month,
    sum(amount) as total
having year = 1998
order by year asc, month asc;
"""


@pytest.fixture(scope="module")
def executor():
    env = Environment()
    env.parse(_MODEL)
    return Dialects.DUCK_DB.default_executor(environment=env)


def _atom(value: int) -> BuildComparison:
    return BuildComparison(left=value, right=value, operator=ComparisonOperator.EQ)


def _conditional(operator: BooleanOperator) -> BuildConditional:
    return BuildConditional(left=_atom(1), operator=operator, right=_atom(2))


def test_or_child_of_and_is_parenthesized():
    assert (
        _protect_conditional_child(
            _conditional(BooleanOperator.OR), BooleanOperator.AND, "x"
        )
        == "(x)"
    )


@pytest.mark.parametrize(
    "child_operator,parent_operator",
    [
        (BooleanOperator.AND, BooleanOperator.OR),
        (BooleanOperator.AND, BooleanOperator.AND),
        (BooleanOperator.OR, BooleanOperator.OR),
    ],
)
def test_higher_precedence_children_are_left_bare(child_operator, parent_operator):
    assert (
        _protect_conditional_child(_conditional(child_operator), parent_operator, "x")
        == "x"
    )


def test_pushed_having_atom_binds_to_the_whole_or_chain(executor):
    sql = executor.generate_sql(_QUERY)[-1]
    where = next(
        line for line in sql.splitlines() if '"sales"."s_year" = 1999 or' in line
    )
    assert where.strip().startswith("("), sql


def test_pushed_having_atom_filters_every_or_arm(executor):
    assert [(r[0], r[1]) for r in executor.execute_text(_QUERY)[-1].fetchall()] == [
        (1998, 12)
    ]
