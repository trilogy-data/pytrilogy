"""A grand-total broadcast join used to pair on the `all_rows` marker, a key no
source_map entry attributes to the dim scan; folding the scan away left it
unrenderable (bare AssertionError at render time). A single-row side now pairs
on no key, so the scan inlines and the marker never reaches the join."""

from trilogy import Dialects, Environment

MODEL = """
key store_sk int;
property store_sk.store_name string;
datasource store (
    s_store_sk: store_sk,
    s_store_name: store_name,
)
grain (store_sk)
address store;

key address_sk int;
property address_sk.city string;
datasource customer_address (
    ca_address_sk: address_sk,
    ca_city: city,
)
grain (address_sk)
address customer_address;

key ticket_number int;
property ticket_number.net_profit float;
datasource store_sales (
    ss_ticket_number: ticket_number,
    ss_addr_sk: ~?address_sk,
    ss_store_sk: ~?store_sk,
    ss_net_profit: net_profit,
)
grain (ticket_number)
address store_sales;

auto bench <- avg(net_profit ? address_sk is null) by *;
"""


def test_broadcast_join_key_survives_inline():
    executor = Dialects.DUCK_DB.default_executor(environment=Environment())
    sql = executor.generate_sql(MODEL + "\nselect store_sk as d, bench as v;")[-1]
    assert "__preql_internal_all_rows" not in sql, sql
    join_line = next(line for line in sql.splitlines() if "INNER JOIN" in line)
    assert join_line.strip().endswith("on 1=1"), join_line
