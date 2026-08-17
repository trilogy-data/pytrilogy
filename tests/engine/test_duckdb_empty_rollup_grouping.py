"""`by rollup ()` on a select with no dimensions must not emit `ROLLUP ()`.

The inferred-key form fills its keys from the resolved select grain; a select
projecting only constants and aggregates has an empty grain, so the key list is
empty. A rollup over zero keys is one grouping set - the grand total - which is
what a keyless standard grouping already renders. Emitting the literal
`GROUP BY ROLLUP ()` is a parser error in DuckDB (and everywhere else).
See evals/tpcds_agent/bug_q05_empty_rollup_generated_sql.md (q05).
"""

import pytest

from trilogy import Dialects

FIXTURE = """
key id int;
property id.g1 string?;
property id.v int;

datasource t (id: id, g1: g1, v: v)
grain (id)
query '''
select 1 id, 'a' g1, 10 v
union all select 2, 'a', 20 v
union all select 3, 'b', 5 v
''';
"""

CASES = [
    ("select sum(v) as total by rollup ();", [(35,)]),
    (
        "select 'store sales' as part, coalesce(sum(v), 0) as total by rollup ();",
        [("store sales", 35)],
    ),
    (
        "select coalesce(sum(v), 0) as total where g1 = 'a' by rollup ();",
        [(30,)],
    ),
    (
        "select sum(v) as total by rollup () having total > 1;",
        [(35,)],
    ),
    ("select sum(v) as total by grouping sets (());", [(35,)]),
]


@pytest.fixture(scope="module")
def executor():
    exec_ = Dialects.DUCK_DB.default_executor()
    exec_.execute_text(FIXTURE)
    return exec_


@pytest.mark.parametrize("query,expected", CASES)
def test_keyless_rollup_renders_grand_total(executor, query, expected):
    sql = executor.generate_sql(query)[-1]
    assert "ROLLUP ()" not in sql
    assert "CUBE ()" not in sql
    assert "GROUPING SETS ()" not in sql
    assert executor.execute_text(query)[-1].fetchall() == expected


def test_keyed_rollup_still_rolls_up(executor):
    query = "select g1, sum(v) as total by rollup (g1) order by total desc;"
    assert "ROLLUP" in executor.generate_sql(query)[-1]
    assert executor.execute_text(query)[-1].fetchall() == [
        (None, 35),
        ("a", 30),
        ("b", 5),
    ]


def test_inferred_key_rollup_still_rolls_up(executor):
    query = "select g1, sum(v) as total by rollup () order by total desc;"
    assert "ROLLUP" in executor.generate_sql(query)[-1]
    assert executor.execute_text(query)[-1].fetchall() == [
        (None, 35),
        ("a", 30),
        ("b", 5),
    ]
