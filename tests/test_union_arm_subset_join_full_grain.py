"""A union(...) arm that subset-joins a second fact on its FULL composite grain
must plan the bridge on every declared key and keep each arm's outputs distinct.

Surfaced on TPC-DS q05 ingest (evals/tpcds_agent
bug_q05_union_arm_full_grain_subset_join_source_map_miss.md): bridging
web_returns -> web_sales on (item, order_number) to recover the site dimension
for the returns arm of a channel union. Three cooperating defects:

1. Unaliased select expressions were not rowset-mangled, so identical
   expressions in different arms (`0.0` in each) shared one address. The
   align items' positional identity collapsed: ``find_source`` resolved the
   wrong arm's column and the real aggregate was pruned as dead — silent
   zeros for the returns measure.
2. Subgraph decomposition kept only the steiner-cheapest path to a merged
   key, so a side whose member is reachable through the pseudonym/scoped-join
   edge never materialized its own column. The equality dropped out of the
   merge join, either stranding the canonical key in the source map (the
   reported ``Missing ws.order_number`` internal error) or fanning out the
   remaining-key join (returns duplicated per site selling the item).
3. The join-key renderer pinned a key to the FROM-base CTE whenever the
   consumer's source_map didn't list the pair's node, even when that node
   renders in the consumer's own join chain — emitting a column the base CTE
   does not have (``Binder Error ... does not have a column named ws_item``).
"""

from pathlib import Path

import pytest

from trilogy import Dialects
from trilogy.core.models.environment import Environment
from trilogy.executor import Executor

SITE = """key site_sk int;
property site_sk.site_id string;

datasource site (
    sk: site_sk,
    id: site_id,
)
grain (site_sk)
query '''select 1 as sk, 'east' as id union all select 2, 'west' ''';
"""

DATES = """key date_sk int;
property date_sk.date date;

datasource dates (
    sk: date_sk,
    d: date,
)
grain (date_sk)
query '''select 1 as sk, '2000-01-01'::date as d union all select 2, '2001-01-01'::date''';
"""

SALES = """import site as web_site;
import dates as sold_date;

key item int;
key order_number int;

properties <item, order_number> (
    sales_amt float?,
);

datasource web_sales (
    i: item,
    o: order_number,
    site_sk: web_site.site_sk,
    date_sk: ~?sold_date.date_sk,
    amt: ?sales_amt,
)
grain (item, order_number)
query '''select 1 as i, 10 as o, 1 as site_sk, 1 as date_sk, 5.0 as amt
union all select 2, 11, 2, 1, 7.0''';
"""

RETURNS = """import dates as date_dim;

key item int;
key order_number int;

properties <item, order_number> (
    return_amt float?,
);

datasource web_returns (
    i: item,
    o: order_number,
    date_sk: ~?date_dim.date_sk,
    amt: ?return_amt,
)
grain (item, order_number)
query '''select 1 as i, 10 as o, 1 as date_sk, 2.0 as amt
union all select 2, 11, 2, 3.5''';
"""


@pytest.fixture
def executor(tmp_path: Path) -> Executor:
    (tmp_path / "site.preql").write_text(SITE)
    (tmp_path / "dates.preql").write_text(DATES)
    (tmp_path / "sales.preql").write_text(SALES)
    (tmp_path / "returns.preql").write_text(RETURNS)
    return Dialects.DUCK_DB.default_executor(
        environment=Environment(working_path=tmp_path)
    )


UNION_ARM_QUERY = """
import sales as ws;
import returns as wr;

with combined as union(
    (where ws.sold_date.date between '2000-01-01'::date and '2000-12-31'::date
     select concat('web_site', ws.web_site.site_id), coalesce(sum(ws.sales_amt),0.0), 0.0),
    (where wr.date_dim.date between '2000-01-01'::date and '2000-12-31'::date
     select concat('web_site', ws.web_site.site_id), 0.0, coalesce(sum(wr.return_amt),0.0)
     subset join wr.item = ws.item
     subset join wr.order_number = ws.order_number)
) -> (eid, sales, ret);
select combined.eid, combined.sales, combined.ret;
"""


def test_union_arm_full_grain_subset_join(executor: Executor) -> None:
    """The reported case: returns arm bridges to sales on the full composite
    grain to recover the site dimension. Raised ``Missing ws.order_number in
    {...}, source map ...`` before the fix; the naming collision alone also
    silently zeroed the returns measure."""
    rows = sorted(tuple(r) for r in executor.execute_query(UNION_ARM_QUERY).fetchall())
    assert rows == [
        ("web_siteeast", 0.0, 2.0),
        ("web_siteeast", 5.0, 0.0),
        ("web_sitewest", 7.0, 0.0),
    ]


def test_union_arm_full_grain_subset_join_reversed(executor: Executor) -> None:
    query = UNION_ARM_QUERY.replace(
        """     subset join wr.item = ws.item
     subset join wr.order_number = ws.order_number)""",
        """     subset join wr.order_number = ws.order_number
     subset join wr.item = ws.item)""",
    )
    rows = sorted(tuple(r) for r in executor.execute_query(query).fetchall())
    assert rows == [
        ("web_siteeast", 0.0, 2.0),
        ("web_siteeast", 5.0, 0.0),
        ("web_sitewest", 7.0, 0.0),
    ]


def test_union_arm_join_partner_only_in_join(executor: Executor) -> None:
    """F5 shape: the arm groups by the returns-side key while the sales side
    appears only in the subset joins. Rendered an
    ``INVALID_REFERENCE_BUG<Missing source reference to ws.order_number>``
    sentinel before the fix; a partial fix left it a silent per-wrong-grain
    broadcast."""
    query = """
import sales as ws;
import returns as wr;

with combined as union(
    (where wr.date_dim.date between '2001-01-01'::date and '2001-12-31'::date
     select wr.order_number, 0.0),
    (where wr.date_dim.date between '2000-01-01'::date and '2000-12-31'::date
     select wr.order_number, coalesce(sum(wr.return_amt),0.0)
     subset join wr.item = ws.item
     subset join wr.order_number = ws.order_number)
) -> (eid, ret);
select combined.eid, sum(combined.ret) as total;
"""
    rows = sorted(tuple(r) for r in executor.execute_query(query).fetchall())
    assert rows == [(10, 2.0), (11, 0.0)]


def test_standalone_arm_control(executor: Executor) -> None:
    query = """
import sales as ws;
import returns as wr;

where wr.date_dim.date between '2000-01-01'::date and '2000-12-31'::date
select
    concat('web_site', ws.web_site.site_id) as eid,
    coalesce(sum(wr.return_amt), 0.0) as ret
subset join wr.item = ws.item
subset join wr.order_number = ws.order_number;
"""
    rows = sorted(tuple(r) for r in executor.execute_query(query).fetchall())
    assert rows == [("web_siteeast", 2.0)]
