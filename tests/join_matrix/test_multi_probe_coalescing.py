"""Multiple side-presence probes over ONE coalescing (`union`) key group in a
single statement: `store AND (web OR catalog)` across three rowsets joined to a
customer axis (the TPC-DS q35 shape,
evals/tpcds_agent/bug_q35_union_join_presence_filters_collapse.md).

Each `member is [not] null` test is a per-side presence probe materialized on
the member's own rowset body pre-merge (test_coalescing_presence_matrix.py).
Pre-fix, only ONE probe survived to the outer WHERE: the rest were re-derived
off the fused coalesced key (never NULL), so extra members were admitted and the
qualifying set silently widened. The break was probe outputs being stripped when
a node re-projected/grouped its parents (`group_if_required_v2` narrowed to the
requested outputs, and completion/enrichment merges dropped them) — the
probe-less node was then cached and reused. `retain_presence_probes` keeps a
parent-exposed probe through every such narrowing.

Data: store={1,2,3}, web={1}, catalog={2}, customer={1,2,3,4};
`store AND (web OR catalog)` = {1, 2}. Membership (`in`) is the reference form.
"""

from pathlib import Path

from tests.join_matrix.harness import sort_rows
from trilogy import Dialects, Executor
from trilogy.core.models.environment import Environment

MODEL = """
key cust_id int;
property cust_id.region string;
datasource customers (i: cust_id, r: region) grain (cust_id)
query '''
select 1 i, 'E' r union all select 2 i, 'E' r
union all select 3 i, 'W' r union all select 4 i, 'W' r
''';

key srid int;
property srid.s_cust int;
datasource store_fact (r: srid, c: s_cust) grain (srid)
query '''
select 1 r, 1 c union all select 2 r, 2 c union all select 3 r, 3 c
''';

key wrid int;
property wrid.w_cust int;
datasource web_fact (r: wrid, c: w_cust) grain (wrid)
query '''
select 1 r, 1 c
''';

key crid int;
property crid.k_cust int;
datasource catalog_fact (r: crid, c: k_cust) grain (crid)
query '''
select 1 r, 2 c
''';

with s as select s_cust as sk;
with w as select w_cust as wk;
with c as select k_cust as ck;
"""


def _run(tmp_path: Path, query: str) -> list[tuple]:
    engine: Executor = Dialects.DUCK_DB.default_executor(
        environment=Environment(working_path=tmp_path)
    )
    statements = engine.parse_text(MODEL + query)
    sql = engine.generate_sql(statements[-1])[-1]
    assert "INVALID_REFERENCE_BUG" not in sql, sql
    return sort_rows([tuple(r) for r in engine.execute_raw_sql(sql).fetchall()])


STORE_AND_WEB_OR_CATALOG = [(1,), (2,)]


def test_store_and_web_or_catalog_key_only(tmp_path: Path):
    rows = _run(
        tmp_path,
        "select cust_id "
        "union join s.sk = cust_id "
        "union join w.wk = cust_id "
        "union join c.ck = cust_id "
        "where s.sk is not null "
        "  and (w.wk is not null or c.ck is not null);",
    )
    assert rows == STORE_AND_WEB_OR_CATALOG


def test_store_and_web_or_catalog_join_order_swapped(tmp_path: Path):
    # Swapping web/catalog join declaration order must not change the set.
    rows = _run(
        tmp_path,
        "select cust_id "
        "union join s.sk = cust_id "
        "union join c.ck = cust_id "
        "union join w.wk = cust_id "
        "where s.sk is not null "
        "  and (w.wk is not null or c.ck is not null);",
    )
    assert rows == STORE_AND_WEB_OR_CATALOG


def test_store_and_web_or_catalog_matches_membership(tmp_path: Path):
    # The canonical q35 spelling (existence membership) is the reference form.
    rows = _run(
        tmp_path,
        "where cust_id in s.sk "
        "  and (cust_id in w.wk or cust_id in c.ck) "
        "select cust_id;",
    )
    assert rows == STORE_AND_WEB_OR_CATALOG


def test_store_and_web_or_catalog_with_property_and_aggregate(tmp_path: Path):
    # Adding an unrelated dimension projection + aggregate must not alter the
    # qualifying key set (the full-q35 shape that took a different bad plan).
    rows = _run(
        tmp_path,
        "select region, count(cust_id) as customers "
        "union join s.sk = cust_id "
        "union join w.wk = cust_id "
        "union join c.ck = cust_id "
        "where s.sk is not null "
        "  and (w.wk is not null or c.ck is not null);",
    )
    # customers 1 and 2 both in region 'E'
    assert rows == [("E", 2)]


def test_two_member_and_intersects(tmp_path: Path):
    # store AND web = {1}: two probes AND-ed, no OR.
    rows = _run(
        tmp_path,
        "select cust_id "
        "union join s.sk = cust_id "
        "union join w.wk = cust_id "
        "where s.sk is not null and w.wk is not null;",
    )
    assert rows == [(1,)]


def test_two_member_or_unions(tmp_path: Path):
    # store OR web = {1,2,3}: presence widened by OR, still per-side.
    rows = _run(
        tmp_path,
        "select cust_id "
        "union join s.sk = cust_id "
        "union join w.wk = cust_id "
        "where s.sk is not null or w.wk is not null;",
    )
    assert rows == [(1,), (2,), (3,)]
