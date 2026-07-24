from pathlib import Path

from trilogy import Dialects, parse
from trilogy.core.enums import BooleanOperator, ComparisonOperator, Purpose
from trilogy.core.models.build import (
    BuildComparison,
    BuildConditional,
    BuildSubselectComparison,
)
from trilogy.core.optimizations.predicate_pushdown import (
    is_child_of,
)
from trilogy.core.processing.condition_utility import decompose_condition


def test_pushdown():
    with open(Path(__file__).parent / "pushdown.preql") as f:
        text = f.read()

    env, queries = parse(text)

    generated = Dialects.DUCK_DB.default_executor(environment=env).generate_sql(
        queries[-1]
    )[0]

    print(generated)
    test_str = """ = '2024-01-01' """.strip()
    assert generated.count(test_str) == 1


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

    env, _queries = parse(text)
    env = env.materialize_for_select()

    test = BuildConditional(
        left=BuildSubselectComparison(
            left=env.concepts["uuid"], right="a", operator=ComparisonOperator.EQ
        ),
        right=BuildComparison(left=3, right=4, operator=ComparisonOperator.EQ),
        operator=BooleanOperator.AND,
    )

    test2 = BuildConditional(
        left=BuildSubselectComparison(
            left=env.concepts["uuid"], right="a", operator=ComparisonOperator.EQ
        ),
        right=BuildComparison(left=3, right=4, operator=ComparisonOperator.EQ),
        operator=BooleanOperator.AND,
    )
    assert is_child_of(test, test2) is True

    children = decompose_condition(test)
    for child in children:
        assert is_child_of(child, test2) is True


DUAL_EXISTENCE = """
key cust_id int;
key txn_id int;
key channel string;

datasource f (
    tid: txn_id,
    cid: cust_id,
    ch: channel,
)
grain (txn_id)
address f_tbl;

auto a_buyers <- cust_id ? channel = 'A';
auto b_buyers <- cust_id ? channel = 'B';

RAW_SQL('''
CREATE TABLE f_tbl (tid INT, cid INT, ch VARCHAR);
INSERT INTO f_tbl VALUES
    (10, 1, 'A'), (11, 2, 'A'), (14, 1, 'A'),
    (12, 2, 'B'), (13, 3, 'B');
''');

WHERE cust_id in a_buyers and cust_id in b_buyers
SELECT cust_id;
"""


def test_dual_existence_filter_no_cycle():
    """Two existence (`in`) filters on the same concept must not make
    PredicatePushdown push each into the other's producer CTE (a cycle)."""
    env, queries = parse(DUAL_EXISTENCE)
    executor = Dialects.DUCK_DB.default_executor(environment=env)
    # compiles (no NetworkXUnfeasible cycle) and returns the intersection only
    results = None
    for query in queries:
        results = executor.execute_query(query)
    assert results is not None
    assert [row[0] for row in results.fetchall()] == [2]


SELF_REF_EXISTENCE = """
key cust_id int;
key txn_id int;
key channel string;

datasource f (
    tid: txn_id,
    cid: cust_id,
    ch: channel,
)
grain (txn_id)
address f_tbl;

auto a_buyers <- cust_id ? channel = 'A';

RAW_SQL('''
CREATE TABLE f_tbl (tid INT, cid INT, ch VARCHAR);
INSERT INTO f_tbl VALUES
    (10, 1, 'A'), (11, 2, 'A'), (14, 1, 'A'),
    (12, 2, 'B'), (13, 3, 'B');
''');

WHERE cust_id in a_buyers
SELECT cust_id;
"""


def test_self_referential_membership_filter():
    """A membership `cust_id in a_buyers` whose set `a_buyers` is derived from the
    same scan as the output must not be injected on the set's producer (self-ref
    -> dangling IN-RHS CTE). It routes to FINAL with the set as a subselect feeder,
    and the output dedups to the requested grain (one row per cust_id)."""
    env, queries = parse(SELF_REF_EXISTENCE)
    executor = Dialects.DUCK_DB.default_executor(environment=env)
    results = None
    for query in queries:
        results = executor.execute_query(query)
    assert results is not None
    assert sorted(row[0] for row in results.fetchall()) == [1, 2]
