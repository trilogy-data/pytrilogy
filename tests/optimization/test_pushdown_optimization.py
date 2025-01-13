from pathlib import Path

from trilogy import Dialects, parse
from trilogy.core.enums import BooleanOperator, ComparisonOperator, Purpose
from trilogy.core.models.author import Comparison, Conditional, SubselectComparison
from trilogy.core.optimizations.predicate_pushdown import (
    is_child_of,
)
from trilogy.core.processing.utility import decompose_condition


def test_pushdown():
    with open(Path(__file__).parent / "pushdown.preql") as f:
        text = f.read()

    env, queries = parse(text)

    generated = Dialects.DUCK_DB.default_executor(environment=env).generate_sql(
        queries[-1]
    )[0]

    print(generated)
    test_str = """ = '2024-01-01' """.strip()
    assert generated.count(test_str) == 2


def test_pushdown_execution():
    from trilogy.hooks.query_debugger import DebuggingHook

    DebuggingHook()
    with open(Path(__file__).parent / "pushdown.preql") as f:
        text = f.read()

    env, queries = parse(text)
    executor = Dialects.DUCK_DB.default_executor(environment=env)
    assert env.concepts["ratio"].purpose == Purpose.PROPERTY

    for query in queries:
        results = executor.execute_query(query)

    final = results.fetchall()

    assert len(final) == 9  # number of unique values in "other_thing"


def test_child_of():
    with open(Path(__file__).parent / "pushdown.preql") as f:
        text = f.read()

    env, queries = parse(text)

    test = Conditional(
        left=SubselectComparison(
            left=env.concepts["uuid"], right="a", operator=ComparisonOperator.EQ
        ),
        right=Comparison(left=3, right=4, operator=ComparisonOperator.EQ),
        operator=BooleanOperator.AND,
    )

    test2 = Conditional(
        left=SubselectComparison(
            left=env.concepts["uuid"], right="a", operator=ComparisonOperator.EQ
        ),
        right=Comparison(left=3, right=4, operator=ComparisonOperator.EQ),
        operator=BooleanOperator.AND,
    )
    assert is_child_of(test, test2) is True

    children = decompose_condition(test)
    for child in children:
        assert is_child_of(child, test2) is True
