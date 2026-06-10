from __future__ import annotations

from trilogy import Dialects
from trilogy.core.processing.node_generators.select_helpers.datasource_injection import (
    get_union_sources,
)

WORKING = """
key order_id int;
property order_id.order_date date;
property order_id.store_id int;
property order_id.web_id int;

datasource web_orders (order_id: ~order_id, web_id: web_id, order_date: order_date)
grain (order_id) complete where order_date <= cast('2024-01-01' as date)
query '''select 1 as order_id, 4 web_id, cast('2024-01-01' as date) as order_date'''
where order_date <= '2024-01-01'::date;

datasource store_orders (order_id: ~order_id, store_id: store_id, order_date: order_date)
grain (order_id) complete where order_date > cast('2024-01-01' as date)
query '''select 2 as order_id, 3 store_id, cast('2024-10-01' as date) as order_date'''
where order_date > '2024-01-01'::date;
"""

MINE = """
key customer_id int;
property customer_id.region string;
property customer_id.revenue float;
auto total_revenue <- sum(revenue);

datasource east_summary (customer_id: ~customer_id, total_revenue: total_revenue)
grain (customer_id) complete where region = 'east'
query '''select 101 as customer_id, 10.0 as total_revenue''';

datasource west_summary (customer_id: ~customer_id, total_revenue: total_revenue)
grain (customer_id) complete where region = 'west'
query '''select 202 as customer_id, 20.0 as total_revenue''';
"""


def probe(label, decl, key):
    x = Dialects.DUCK_DB.default_executor()
    x.parse_text(decl)
    env = x.environment.materialize_for_select()
    unions = get_union_sources(list(env.datasources.values()), [env.concepts[key]])
    print(f"{label}: key={key} unions={[[d.name for d in g] for g in unions]}")
    for ds in env.datasources.values():
        npf = ds.non_partial_for.conditional if ds.non_partial_for else None
        print(f"   {ds.name}: non_partial_for={npf} cols={[(c.concept.address, [m.value for m in c.modifiers]) for c in ds.columns]}")


if __name__ == "__main__":
    probe("WORKING", WORKING, "order_id")
    print()
    probe("MINE", MINE, "customer_id")
