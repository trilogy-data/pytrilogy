"""Sibling of test_membership_having_aggregate_case_groupby (q23). Same family
-- a HAVING that compares a per-key aggregate to a *derived* threshold lowers
into a per-row `_virt_filter` existence column rendered as
`CASE WHEN avg(...) > threshold THEN key ELSE NULL END` inside a grouping CTE --
but the opposite symptom. q23 leaked the aggregate into the GROUP BY; q44 leaked
a *dimension key* (`item_id`) into the CASE THEN while the CTE grouped by the
threshold/store columns instead, so DuckDB raised "column ss_item_id must appear
in the GROUP BY clause". generate_sql succeeded; execution threw.

Regression for TPC-DS q44. The best/worst rank pairing forks the qualifying set
into two consumers, which is what pushes the HAVING into a membership `_virt_filter`
rather than a plain WHERE.
"""

import pytest

from trilogy import Dialects
from trilogy.core.models.environment import Environment
from trilogy.executor import Executor

MODEL = """
key item_id int;
property item_id.product_name string;
key store_id int;
key ss_id int;
property ss_id.net_profit float;
property ss_id.addr int?;

datasource item (id: item_id, pn: product_name) grain (item_id)
query '''
select 10 id, 'itemA' pn union all
select 20 id, 'itemB' pn union all
select 30 id, 'itemC' pn union all
select 40 id, 'itemD' pn''';

datasource store_sales (id: ss_id, i: item_id, s: store_id, np: net_profit, a: addr)
grain (ss_id)
query '''
select 1 id, 10 i, 1 s, 100.0 np, NULL::int a union all
select 2 id, 10 i, 1 s, 120.0 np, 5 a union all
select 3 id, 20 i, 1 s, 5.0 np, NULL::int a union all
select 4 id, 30 i, 1 s, 90.0 np, NULL::int a union all
select 5 id, 40 i, 2 s, 200.0 np, NULL::int a''';

auto avg_net_profit_per_item <- avg(net_profit) by item_id;
auto null_addr_avg_net_profit <- avg(net_profit ? addr is null) by store_id;
"""

QUERY = MODEL + """
with qualifying as
where store_id = 1
select
    item_id,
    product_name,
    avg_net_profit_per_item as avg_net_profit
having
    avg_net_profit_per_item > null_addr_avg_net_profit * 0.9;

with ranked_items as
select
    qualifying.item_id,
    qualifying.product_name,
    qualifying.avg_net_profit,
    rank(qualifying.product_name) over (order by qualifying.avg_net_profit asc) as asc_rank,
    rank(qualifying.product_name) over (order by qualifying.avg_net_profit desc) as desc_rank;

with best as
where ranked_items.asc_rank <= 10
select ranked_items.asc_rank as pair_rank_best, ranked_items.product_name as best_product_name;

with worst as
where ranked_items.desc_rank <= 10
select ranked_items.desc_rank as pair_rank_worst, ranked_items.product_name as worst_product_name;

subset join best.pair_rank_best = worst.pair_rank_worst
select
    best.pair_rank_best as rank,
    best.best_product_name as best_performer_product_name,
    worst.worst_product_name as worst_performer_product_name
order by rank asc;
"""


@pytest.fixture
def executor() -> Executor:
    return Dialects.DUCK_DB.default_executor(environment=Environment())


def test_membership_having_dimension_key_not_in_group_by(executor):
    # generate_sql always succeeded; the bug was invalid SQL DuckDB rejects at
    # execution ("column item_id must appear in the GROUP BY clause").
    # store 1 null-addr avg = (100+5+90)/3 = 65, threshold = 58.5;
    # per-item avg: item10=110 (pass), item20=5 (drop), item30=90 (pass) ->
    # qualifying {itemA, itemC}; best asc {itemC:1, itemA:2}, worst desc
    # {itemA:1, itemC:2}; paired by rank.
    rows = [(int(r[0]), r[1], r[2]) for r in executor.execute_text(QUERY)[0].fetchall()]
    assert rows == [(1, "itemC", "itemA"), (2, "itemA", "itemC")]
