"""
Test join order resolution with partial keys.

When two fact tables each have partial keys that merge into shared dimension keys,
the join generation should preserve all rows from all fact tables - no INNER JOINs
that would discard non-matching rows from partial sources.
"""

from trilogy import Dialects
from trilogy.core.models.environment import Environment

SETUP = """
key customer_id int;
property customer_id.customer_name string;

datasource customers (
    id:customer_id,
    name:customer_name
)
grain (customer_id)
query '''
SELECT 1 as id, 'Alice' as name
UNION ALL SELECT 2 as id, 'Bob' as name
UNION ALL SELECT 3 as id, 'Charlie' as name
''';

key fact1_id int;
property fact1_id.fact1_customer_id int;
property fact1_id.fact1_value int;

key fact2_id int;
property fact2_id.fact2_customer_id int;
property fact2_id.fact2_value int;

datasource fact1 (
    id:fact1_id,
    customer_id:fact1_customer_id,
    value:fact1_value
)
grain (fact1_id)
query '''
SELECT 1 as id, 1 as customer_id, 10 as value
UNION ALL SELECT 2 as id, 1 as customer_id, 20 as value
UNION ALL SELECT 3 as id, 2 as customer_id, 30 as value
''';

datasource fact2 (
    id:fact2_id,
    customer_id:fact2_customer_id,
    value:fact2_value
)
grain (fact2_id)
query '''
SELECT 1 as id, 2 as customer_id, 15 as value
UNION ALL SELECT 2 as id, 3 as customer_id, 25 as value
UNION ALL SELECT 3 as id, 3 as customer_id, 35 as value
''';

merge fact1_customer_id into ~customer_id;
merge fact2_customer_id into ~customer_id;
"""


def test_double_partial_key_join_order():
    """Two facts with partial keys should not use INNER JOIN (would lose rows)."""
    env = Environment()
    env.parse(SETUP)

    executor = Dialects.DUCK_DB.default_executor(environment=env)

    sql = executor.generate_sql("SELECT customer_name, fact1_value, fact2_value;")
    compiled_sql = sql[-1]

    print("Generated SQL:")
    print(compiled_sql)

    assert "INNER JOIN" not in compiled_sql, (
        "INNER JOIN should not be used between partial-keyed sources. "
        f"Generated SQL:\n{compiled_sql}"
    )


def test_double_partial_key_results():
    """Test actual results with partial key joins."""
    env = Environment()
    env.parse(SETUP)

    executor = Dialects.DUCK_DB.default_executor(environment=env)

    results = list(
        executor.execute_text(
            "SELECT customer_name, count(fact1_id) -> fact1_count, count(fact2_id) -> fact2_count;"
        )[0].fetchall()
    )

    customer_names = set(r[0] for r in results)
    print(f"Results: {results}")
    print(f"Customer names: {customer_names}")

    # Alice (customer_id=1) is only in fact1, Charlie (customer_id=3) is only in fact2,
    # Bob (customer_id=2) is in both. All three should appear.
    assert customer_names == {
        "Alice",
        "Bob",
        "Charlie",
    }, f"Expected customers from both fact tables. Got: {customer_names}"
