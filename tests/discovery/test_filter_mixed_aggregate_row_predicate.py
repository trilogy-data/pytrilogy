from trilogy import Dialects, Environment

MODEL = """
key order_id int;
key customer_id int;
key product_id int;
property order_id.order_value float;
property product_id.product_name string;

datasource orders (
    order_id: order_id,
    customer_id: customer_id,
    product_id: product_id,
    order_value: order_value,
)
grain (order_id)
query '''
select 1 as order_id, 101 as customer_id, 201 as product_id, 25.99 as order_value
union all
select 2 as order_id, 102 as customer_id, 202 as product_id, 45.50 as order_value
union all
select 3 as order_id, 101 as customer_id, 202 as product_id, 33.25 as order_value
union all
select 4 as order_id, 103 as customer_id, 201 as product_id, 78.00 as order_value
union all
select 5 as order_id, 102 as customer_id, 203 as product_id, 12.75 as order_value
union all
select 6 as order_id, 101 as customer_id, 203 as product_id, 89.99 as order_value
union all
select 7 as order_id, 104 as customer_id, 202 as product_id, 156.80 as order_value
union all
select 8 as order_id, 103 as customer_id, 203 as product_id, 67.45 as order_value
''';

datasource products (
    product_id: product_id,
    name: product_name,
)
grain (product_id)
query '''
select 201 as product_id, 'Laptop' as name
union all
select 202 as product_id, 'Mouse' as name
union all
select 203 as product_id, 'Keyboard' as name
''';
"""


def setup():
    env = Environment()
    env.parse(MODEL)
    return Dialects.DUCK_DB.default_executor(environment=env)


def fetch(query: str) -> list[tuple]:
    exec = setup()
    return [tuple(r) for r in exec.execute_query(query).fetchall()]


# regression: a `?` filter mixing an aggregate-comparison with a row-level
# predicate collapsed into one grouped CTE that GROUPed BY the row-predicate
# column while selecting the ungrouped content key (DuckDB BinderException:
# `column "customer_id" must appear in the GROUP BY clause`).
def test_mixed_aggregate_and_row_predicate_filter():
    rows = fetch("""
select customer_id ? count(order_id) by customer_id > 1
                     and product_name = 'Mouse' as filtered;
""")
    assert sorted(r[0] for r in rows) == [101, 102], rows


def test_mixed_filter_matches_where_form():
    filtered = fetch("""
select customer_id ? count(order_id) by customer_id > 1
                     and product_name = 'Mouse' as filtered;
""")
    where_form = fetch("""
select customer_id
where count(order_id) by customer_id > 1 and product_name = 'Mouse';
""")
    assert sorted(r[0] for r in filtered) == sorted(r[0] for r in where_form)


def test_row_predicate_on_group_key_keeps_grouped_pushdown():
    rows = fetch("""
select customer_id ? count(order_id) by customer_id > 1
                     and customer_id != 102 as filtered;
""")
    assert sorted(r[0] for r in rows) == [101, 103], rows


def test_pure_aggregate_filter_unchanged():
    rows = fetch("""
select customer_id ? count(order_id) by customer_id > 1 as filtered;
""")
    assert sorted(r[0] for r in rows) == [101, 102, 103], rows


def test_two_aggregate_filter_unchanged():
    rows = fetch("""
select customer_id ? count(order_id) by customer_id > 1
                     and sum(order_value) by customer_id > 50 as filtered;
""")
    assert sorted(r[0] for r in rows) == [101, 102, 103], rows


# regression: the standard (non-pushdown) filter plan applied the predicate
# only as a per-row CASE, so a sole-output filtered concept leaked a NULL
# group for the rows that failed the filter.
def test_sole_output_filter_has_no_null_group():
    rows = fetch("""
select product_name ? count(order_id) by customer_id > 1 as filtered;
""")
    assert sorted(r[0] for r in rows) == ["Keyboard", "Laptop", "Mouse"], rows


def test_filter_with_optional_preserves_non_qualifying_rows():
    rows = fetch("""
select
    customer_id ? count(order_id) by customer_id > 1
                  and product_name = 'Mouse' as filtered,
    customer_id;
""")
    assert set(rows) == {
        (101, 101),
        (102, 102),
        (None, 101),
        (None, 102),
        (None, 103),
        (None, 104),
    }, rows
