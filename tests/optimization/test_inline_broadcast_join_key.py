"""A grand-total broadcast join's `all_rows` marker is a join key the dim scan
produces, but no source_map entry attributes it to that scan; folding the scan
away used to leave the key unrenderable (bare AssertionError at render time)."""

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
    # The scan stays a CTE, so the marker resolves through a qualified alias on
    # both legs rather than through a column the raw `store` table lacks.
    marker = '"__preql_internal_all_rows"'
    join_line = next(
        line for line in sql.splitlines() if "INNER JOIN" in line and marker in line
    )
    assert join_line.count(f".{marker}") == 2, join_line
    assert f'"store".{marker}' not in sql, sql
