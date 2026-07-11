from trilogy import Dialects, Environment

MODEL = """
key order_id int;
key customer_id int;
key product_id int;
property order_id.order_value float;
property product_id.product_name string;

auto customer_order_count <- count(order_id) by customer_id;

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

datasource customer_summary (
    customer_id: customer_id,
    customer_order_count: customer_order_count,
)
grain (customer_id)
query '''
select 101 as customer_id, 3 as customer_order_count
union all
select 102 as customer_id, 2 as customer_order_count
union all
select 103 as customer_id, 2 as customer_order_count
union all
select 104 as customer_id, 1 as customer_order_count
''';
"""

EXPECTED = [
    (101, "Keyboard", 3),
    (101, "Laptop", 3),
    (101, "Mouse", 3),
    (102, "Keyboard", 2),
    (102, "Mouse", 2),
    (103, "Keyboard", 2),
    (103, "Laptop", 2),
    (104, "Mouse", 1),
]


def fetch(query: str) -> list[tuple]:
    env = Environment()
    env.parse(MODEL)
    exec = Dialects.DUCK_DB.default_executor(environment=env)
    return sorted(tuple(r) for r in exec.execute_query(query).fetchall())


# regression: a datasource-materialized aggregate is promoted "as root instead
# of derived" by discovery, but the merge search purged all AGGREGATE nodes
# from its graph — so joining the metric to a dimension through a bridge key
# not in the select (product_id here) raised UnresolvableQueryException.
def test_materialized_aggregate_joins_cross_key_dimension():
    rows = fetch("select customer_id, product_name, customer_order_count;")
    assert rows == EXPECTED, rows


def test_inline_equivalent_of_materialized_aggregate():
    rows = fetch(
        "select customer_id, product_name, count(order_id) by customer_id -> cnt;"
    )
    assert rows == EXPECTED, rows


def test_materialized_aggregate_same_grain_still_direct():
    rows = fetch("select customer_id, customer_order_count;")
    assert rows == [(101, 3), (102, 2), (103, 2), (104, 1)], rows


# regression: the plan for this joins the summary through a rollup-carrier
# group CTE that re-renders the count as SUM(...); the predicate atom pushed
# into that CTE rendered as `WHERE sum(...)` (BinderException) instead of
# routing to HAVING.
def test_mixed_filter_over_materialized_aggregate():
    rows = fetch("""
select customer_id ? customer_order_count > 1
                     and product_name = 'Mouse' as filtered;
""")
    assert sorted(r[0] for r in rows) == [101, 102], rows


def test_materialized_where_form_matches_filter_form():
    where_form = fetch("""
select customer_id
where customer_order_count > 1 and product_name = 'Mouse';
""")
    assert sorted(r[0] for r in where_form) == [101, 102], where_form


# WHERE on a finer-grain dim can't be served by the materialized value —
# the aggregate must re-derive from rows post-filter, matching the derived
# path exactly (Mouse-only counts).
def test_materialized_aggregate_rescopes_under_where():
    rows = fetch("""
select customer_id, product_name, customer_order_count
where product_name = 'Mouse';
""")
    assert rows == [(101, "Mouse", 1), (102, "Mouse", 1), (104, "Mouse", 1)], rows
