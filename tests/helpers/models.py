"""Models shared across test modules.

The customers twin is a materialization oracle: `CUSTOMERS_MATERIALIZED`
stores as columns what `CUSTOMERS_DERIVED` derives, so every query returns
the same rows on both. Customer 3 (cat) has no order: the `~customer_id`
region."""

_CUSTOMER_BASE = """
key customer_id int;
property customer_id.name string;
key order_id int;
property order_id.delivery_date date?;
property order_id.amount int;

root datasource customers (customer_id: customer_id, name: name)
grain (customer_id)
query '''
select 1 as customer_id, 'ann' as name union all
select 2, 'bob' union all
select 3, 'cat'
''';
"""

_ORDER_ROWS = """
select 100 as order_id, 1 as customer_id, date '2026-01-01' as delivery_date, 10 as amount union all
select 101, 1, null, 20 union all
select 102, 2, date '2026-01-02', 30
"""

CUSTOMERS_DERIVED = _CUSTOMER_BASE + f"""
root datasource orders (
    order_id: order_id, customer_id: ~customer_id,
    delivery_date: delivery_date, amount: amount,
)
grain (order_id)
query '''{_ORDER_ROWS}''';

auto status <- case when delivery_date is not null then 'delivered' else 'in-transit' end;
auto undelivered <- delivery_date is null;
auto amount_or_zero <- coalesce(amount, 0);
auto label <- concat(name, '-', status);
auto flag <- case when undelivered then 1 else 0 end;
auto order_seq <- row_number order_id over customer_id order by amount asc;
auto order_rank <- rank order_id by amount desc;
"""

CUSTOMERS_MATERIALIZED = _CUSTOMER_BASE + f"""
property order_id.status string;
property order_id.undelivered bool;
property order_id.amount_or_zero int;
property order_id.label string;
property order_id.flag int;
property order_id.order_seq int;
property order_id.order_rank int;

root datasource orders (
    order_id: order_id, customer_id: ~customer_id,
    delivery_date: delivery_date, amount: amount,
    status: status, undelivered: undelivered,
    amount_or_zero: amount_or_zero, label: label,
    flag: flag, order_seq: order_seq, order_rank: order_rank,
)
grain (order_id)
query '''
select o.*,
    case when o.delivery_date is not null then 'delivered' else 'in-transit' end as status,
    o.delivery_date is null as undelivered,
    coalesce(o.amount, 0) as amount_or_zero,
    concat(c.name, '-', case when o.delivery_date is not null then 'delivered' else 'in-transit' end) as label,
    case when o.delivery_date is null then 1 else 0 end as flag,
    row_number() over (partition by o.customer_id order by o.amount asc) as order_seq,
    rank() over (order by o.amount desc) as order_rank
from ({_ORDER_ROWS}) o
join (select 1 as customer_id, 'ann' as name union all select 2, 'bob') c
    on o.customer_id = c.customer_id
''';
"""

CUSTOMER_ACTIVITY = """
auto activity <- case when count(order_id) by customer_id > 0 then 'active' else 'dormant' end;
auto late_name <- filter name where status = 'in-transit';
auto undelivered_customer <- filter name where undelivered;
auto big_name <- filter name where count(order_id) by customer_id > 1;
auto double_amount <- amount * 2;
"""

# `key is null` does not witness absence: these shapes have a NULL key or an
# unbound property on a REAL row.
NULLABLE_FK = """
key customer_id int;
property customer_id.name string;
key order_id int;

root datasource customers (customer_id: customer_id, name: name)
grain (customer_id)
query '''select 1 as customer_id, 'ann' as name''';

root datasource orders (order_id: order_id, customer_id: ?customer_id)
grain (order_id)
query '''select 100 as order_id, 1 as customer_id union all select 101, null''';

auto customer_label <- coalesce(name, 'unknown');
"""


# `returns` binds its OWN grain keys `~`: a line with no return still has its
# (order, item) entity, from `lines`, so a derivation keyed on it evaluates.
PARTIAL_PROPERTY_SOURCE = """
key order_id int;
key item_id int;
properties <order_id, item_id> (qty int, ret_order int?);
auto is_returned <- ret_order is not null;

root datasource lines (o: order_id, i: item_id, q: qty)
grain (order_id, item_id)
query '''select 1 as o, 10 as i, 5 as q union all select 2, 10, 7''';

root datasource returns (o: ~order_id, i: ~item_id, ro: ret_order)
grain (order_id, item_id)
query '''select 1 as o, 10 as i, 1 as ro''';
"""

# two `~` families (users, products) off orders and items
TWO_FAMILIES = """
key user_id int;
property user_id.state string;
key product_id int;
property product_id.brand string;
key order_id int;
property order_id.amount int;
key item_id int;
property item_id.qty int;

auto total_qty <- sum(qty);

datasource users (user_id: user_id, state: state)
grain (user_id) address users;

datasource products (product_id: product_id, brand: brand)
grain (product_id) address products;

datasource orders (order_id: order_id, user_id: ~user_id, amount: amount)
grain (order_id) address orders;

datasource items (
    item_id: item_id,
    order_id: order_id,
    product_id: ~product_id,
    user_id: ~user_id,
    qty: qty,
)
grain (item_id) address items;
"""

# order lines partial on the user and the product
LINE_ITEMS = """
key line_id int;
key order_id int;
key user_id int;
key product_id int;
property line_id.sale_price float;
property user_id.state string;
property product_id.cost float;
property line_id.item_margin <- sale_price - cost;
auto revenue <- sum(sale_price);
auto margin <- sum(item_margin);

datasource order_items (
    lid: line_id,
    oid: order_id,
    uid: ~user_id,
    pid: ~product_id,
    price: sale_price,
)
grain (line_id)
query '''
select 1 lid, 10 oid, 1 uid, 1 pid, 5.0 price union all
select 2 lid, 10 oid, 1 uid, 2 pid, 7.0 price union all
select 3 lid, 11 oid, 2 uid, 1 pid, 3.0 price
''';

datasource users (uid: user_id, st: state)
grain (user_id)
query '''
select 1 uid, 'ca' st union all select 2 uid, 'ny' st union all select 3 uid, 'wa' st
''';

datasource products (pid: product_id, c: cost)
grain (product_id)
query '''
select 1 pid, 1.0 c union all select 2 pid, 2.0 c union all select 3 pid, 3.0 c
''';
"""

PRODUCT_ORDERS = """
key product_id int;
property product_id.name string;
key order_id int;
property order_id.quantity int;

root datasource products (product_id: product_id, name: name)
grain (product_id)
query '''
select 1 as product_id, 'apple' as name union all
select 2, 'bean' union all
select 3, 'corn' union all
select 4, 'date'
''';

root datasource orders (order_id: order_id, product_id: product_id, quantity: quantity)
grain (order_id)
query '''
select 100 as order_id, 1 as product_id, 5 as quantity union all
select 101, 1, 20 union all
select 102, 2, 8
''';

auto even_name <- filter name where product_id % 2 = 0;
auto n_orders <- count(order_id) by product_id;
auto popular_name <- filter name where n_orders > 1;
auto bulk_name <- filter name where quantity > 10;
"""
