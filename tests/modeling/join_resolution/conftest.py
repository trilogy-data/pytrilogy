from pytest import fixture

from trilogy import Environment
from trilogy.core.env_processor import generate_graph
from trilogy import parse, Dialects
from trilogy.hooks.query_debugger import DebuggingHook
from logging import INFO


@fixture(scope="session")
def test_environment():
    env = Environment()
    test_declaration = """
key order_id int;
key store_id int;
key product_id int;
key wh_id int;

property order_id.order_timestamp datetime;
property order_id.order_year int;
property store_id.store_name string;
property product_id.product_name string;
property <wh_id, product_id>.inv_qty int;

datasource orders (
    order_id:order_id,
    store_id:store_id,
    order_timestamp:order_timestamp,
    date_part(order_timestamp, year): order_year,
)
grain (order_id)
query '''
select 1 order_id, 1 store_id, 1 product_id, DATETIME  '1992-09-20 11:30:00.123456789' order_timestamp
union all
select 2, 1, 2, DATETIME   '1992-09-20 11:30:00.123456789'
union all
select 3, 2, 1, DATETIME   '1992-09-20 11:30:00.123456789'
union all
select 4, 2, 2, DATETIME   '1992-09-20 11:30:00.123456789'
union all
select 5, 3, 2, DATETIME   '1992-09-20 11:30:00.123456789'
''';

datasource order_products (
    order_id: order_id,
    product_id:product_id,
)
grain(order_id, product_id)
query '''
select 1 order_id, 1 product_id
union all
select 1, 2
union all
select 2, 2
union all
select 3, 1
union all
select 4, 2
union all
select 5, 2
''';

datasource stores (
    store_id:store_id,
    store_name:store_name,
)
grain (store_id)
query '''
select 1 store_id, 'store1' store_name
union all
select 2, 'store2'
union all
select 3, 'store3'
''';

datasource products (
    product_id:product_id,
    product_name:product_name,
)
grain (product_id)
query '''
select 1 product_id, 'product1' product_name
union all
select 2, 'product2'
''';

datasource inventory (
    wh_id:wh_id,
    product_id:product_id,
    inv_qty:inv_qty,
)
grain (wh_id, product_id)
query '''
select 1 wh_id, 1 product_id, 2 inv_qty
union all
select 2, 1, 2
union all
select 2, 2, 2
union all
select 3, 2, 2
''';

datasource join_store_warehouse (
    store_id:~store_id,
    wh_id:~wh_id,
)
grain (store_id, wh_id)
query '''
select 1 wh_id, 1 store_id
union all
select 2, 1
union all
select 3, 2
''';

# aggregate tests
property store_id.store_order_count <- count(order_id) by store_id;
auto store_order_count_2 <- count(order_id) by store_id;

# property tests
property store_id.upper_store_name <- upper(store_name);
auto upper_store_name_2 <- upper(store_name);

# filter tests

select
    product_id,
    upper(product_name) -> even_product_name
where
    (product_id % 2) = 0;

"""
    parse(test_declaration, environment=env)
    yield env


@fixture(scope="session")
def test_executor(test_environment: Environment):
    yield Dialects.DUCK_DB.default_executor(
        environment=test_environment, hooks=[DebuggingHook(level=INFO)]
    )


@fixture(scope="session")
def test_environment_graph(test_environment):
    yield generate_graph(test_environment)
