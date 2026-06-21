"""Repros for two bugs hit while converting TPC-DS yoy multiselects (q75, q64)
to the query-scoped `join` form. The self-referential rowset-key grain collapse
(fixed 2026-06-08, see test_join_merge_parity.py) makes the *structural*
shared-parent join work.

Bug A (shared-parent dedup fused into child aggregate) is FIXED 2026-06-08 —
its test is a live assertion now. Bug B (q64 passthrough-dim grain tangle)
remains `xfail(strict=True)` so it flips to a hard failure the moment it starts
passing — that's the signal to delete the marker.

Handoffs:
- evals/tpcds_agent/handoff_q75_join_dedup_fusion.md
- evals/tpcds_agent/handoff_q64_join_grain_resolution.md
"""

from pathlib import Path

import pytest

from trilogy import Dialects, Executor
from trilogy.core.models.environment import Environment

MODELING_DIR = Path(__file__).parent / "modeling" / "tpc_ds_duckdb"

# --- Bug A: shared-parent dedup fused away in the join form -------------------
# `dedup` is a DISTINCT-grain rowset (its grain includes a derived measure `net`
# that spans a sales->returns join). Two child rowsets each `sum(dedup.net)` at a
# coarser grain, then join. The multiselect (merge+align) form materializes the
# dedup as a shared CTE and is correct; the join form FUSES the dedup into the
# child aggregate (sum over raw rows), double-counting rows whose 7-tuple should
# have collapsed.

ITEM_A = """
key item_id int;
property item_id.brand int;
datasource items (iid: item_id, br: brand)
grain (item_id) address items_tbl;
"""

SALES_A = """
import item as item;
key sale_id int;
property sale_id.year int;
property sale_id.qty int;
property sale_id.ret int;
datasource sales (sid: sale_id, iid: item.item_id, yr: year, q: qty)
grain (sale_id) address sales_tbl;
datasource returns (sid: sale_id, r: ret)
grain (sale_id) address returns_tbl;
"""

_HEAD_A = """
import sales as sales;
auto net <- sales.qty - coalesce(sales.ret, 0);
rowset dedup <- where sales.year in (2001, 2002)
select sales.item.brand, sales.year, net;
"""

DEDUP_MULTISELECT = _HEAD_A + """
rowset yp <- where dedup.sales.year = 2002
select dedup.sales.item.brand as brand_c, sum(dedup.net) as c_sum
merge
where dedup.sales.year = 2001
select dedup.sales.item.brand as brand_p, sum(dedup.net) as p_sum
align brand: brand_c, brand_p;
select yp.brand, yp.c_sum, yp.p_sum order by yp.brand asc;
"""

DEDUP_JOIN = _HEAD_A + """
rowset c <- where dedup.sales.year = 2002
select dedup.sales.item.brand as brand, sum(dedup.net) as c_sum;
rowset p <- where dedup.sales.year = 2001
select dedup.sales.item.brand as brand, sum(dedup.net) as p_sum;
inner join c.brand = p.brand
select c.brand, c.c_sum, p.p_sum order by c.brand asc;
"""

# sale1 (qty5,ret0) and sale2 (qty6,ret1) both have net=5 at (brand10, 2001):
# the dedup must collapse them, so p_sum for brand10 is 5, not 10.
DEDUP_EXPECTED = [(10, 7, 5), (20, 9, 3)]


@pytest.fixture
def dedup_engine(tmp_path: Path) -> Executor:
    (tmp_path / "item.preql").write_text(ITEM_A)
    (tmp_path / "sales.preql").write_text(SALES_A)
    env = Environment(working_path=tmp_path)
    eng = Dialects.DUCK_DB.default_executor(environment=env)
    eng.execute_raw_sql("create table items_tbl (iid int, br int)")
    eng.execute_raw_sql("insert into items_tbl values (1,10),(2,20)")
    eng.execute_raw_sql("create table sales_tbl (sid int, iid int, yr int, q int)")
    eng.execute_raw_sql(
        "insert into sales_tbl values "
        "(1,1,2001,5),(2,1,2001,6),(3,1,2002,7),(4,2,2001,3),(5,2,2002,9)"
    )
    eng.execute_raw_sql("create table returns_tbl (sid int, r int)")
    eng.execute_raw_sql("insert into returns_tbl values (1,0),(2,1),(3,0),(4,0),(5,0)")
    return eng


def _rows(engine: Executor, models: Path, text: str) -> list[tuple]:
    engine.environment = Environment(working_path=models)
    return [tuple(r) for r in engine.execute_text(text)[-1].fetchall()]


def test_shared_parent_dedup_fusion(dedup_engine: Executor, tmp_path: Path):
    # reference: the multiselect form materializes the dedup and is correct.
    multi = _rows(dedup_engine, tmp_path, DEDUP_MULTISELECT)
    assert multi == DEDUP_EXPECTED, f"multiselect (reference) wrong: {multi}"
    # bug: the join form fuses the dedup -> brand10 p_sum is 10 (5+5), not 5.
    join = _rows(dedup_engine, tmp_path, DEDUP_JOIN)
    assert join == DEDUP_EXPECTED, f"join double-counts dedup rows: {join}"


# --- Bug B (RESOLVED 2026-06-09): q64 join form passthrough-dim grain tangle ---
# q64 (now query64.preql, the canonical join form) carries many functionally
# dependent passthrough dims (product name, sale & customer address fields,
# first-sales/shipto years) through the per-year aggregates, then inner-joins on
# (item, store name, store zip). These used to pick up grains that mixed the
# join-target keys with each other, dead-ending discovery with "No remaining
# priority concepts". The scoped-INNER-join equivalence fix (build.py mirrors
# merge_concept for derived join keys) resolved it; this is a fast plan-time
# regression guard (test_sixty_four covers full execution). No tpcds data needed.


def test_q64_join_form_plans():
    text = (MODELING_DIR / "query64.preql").read_text()
    env = Environment(working_path=MODELING_DIR)
    eng = Dialects.DUCK_DB.default_executor(environment=env)
    # The two per-year aggregate rowsets are joined on derived keys. A scoped
    # INNER join now mirrors the global-merge equivalence (source<->target
    # pseudonyms + source-identity in alias_origin_lookup), so each side's
    # derivation stays sourceable and discovery resolves the cross-rowset join.
    sql = eng.generate_sql(text)[-1]
    assert sql
    assert "INVALID_REFERENCE_BUG" not in sql
