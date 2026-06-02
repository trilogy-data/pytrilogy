from pathlib import Path

from trilogy import Dialects, Environment

base = Path(__file__).parent


def test_studio_join_issue():
    env = Environment(working_path=base)
    env, queries = env.parse("""import orders as orders;

auto cancelled_orders <- filter orders.id where orders.status = 'Cancelled';
auto orders.id.cancelled_count <- count(cancelled_orders);

WHERE
    orders.created_at.year = 2020
SELECT
    orders.users.city,
    orders.id.cancelled_count / orders.id.count -> cancellation_rate,
    orders.id.cancelled_count,
    orders.id.count,
    orders.created_at.year,
HAVING
    orders.id.count>10
ORDER BY

    cancellation_rate desc;""")

    Dialects.DUCK_DB.default_executor(environment=env).generate_sql(queries[-1])


def test_wrapped_aggregate_with_dependent_by_grain():
    """A ``round(sum(x))``-style wrapped aggregate selected alongside keys plus
    properties that are functionally determined by those keys (an order date,
    a per-user status, a distribution-center name reached two hops out). The
    inner aggregate's by-grain lists all of them, but the CTE that produces it
    reduces to just the keys. The renderer must recognize the reduced grain as
    equivalent and collapse ``agg(x) -> x`` instead of emitting
    ``INVALID_REFERENCE_BUG`` (regression: exact grain-equality guard)."""
    env = Environment(working_path=base)
    env, queries = env.parse("""import order_items as order_item;

auto order_item.revenue <- order_item.orders.item_count * order_item.sale_price;
auto order_item.cost <- order_item.products.cost * order_item.orders.item_count;
auto order_item.total_cost <- round(sum(order_item.cost), 2);
auto order_item.total_revenue <- sum(order_item.revenue);

auto order_item.orders.users.id.first_dt <- min(order_item.orders.created_at.date) by order_item.orders.users.id;
auto order_item.orders.users.id.latest_dt <- max(order_item.orders.created_at.date) by order_item.orders.users.id;
auto order_item.customer_status <- case
    when order_item.orders.users.id.first_dt = order_item.orders.users.id.latest_dt then 'New'
    else 'Returning' end;

SELECT
    order_item.orders.created_at.date,
    order_item.orders.users.id,
    order_item.orders.id,
    order_item.products.id,
    order_item.inventory_items.id,
    order_item.products.distribution_centers.name,
    order_item.customer_status,
    order_item.total_cost,
    order_item.total_revenue,
;""")
    sql = Dialects.BIGQUERY.default_executor(environment=env).generate_sql(queries[-1])
    assert "INVALID_REFERENCE_BUG" not in sql, sql
