from tests.helpers.rows import executor_for, sorted_rows

MODEL = """
key customer_id int;
property customer_id.name string?;
property customer_id.tier string?;
key order_id int;

root datasource customers (customer_id: customer_id, name: ?name, tier: ?tier)
grain (customer_id)
query '''select 1 as customer_id, 'ann' as name, 'gold' as tier
union all select 2, 'bob', null
union all select 3, 'cat', 'gold'
union all select 4, null, null''';

root datasource orders (order_id: order_id, customer_id: ~?customer_id)
grain (order_id)
query '''select 100 as order_id, 1 as customer_id
union all select 101, null
union all select 102, 2
union all select 103, 4''';

auto customer_label <- coalesce(name, 'unknown');
"""


def test_null_member_domain_under_a_filtered_passthrough():
    rows = sorted_rows(
        executor_for(MODEL),
        "select customer_label, count(order_id) as n where tier is null;",
    )
    assert rows == [("bob", 1), ("unknown", 2)]
