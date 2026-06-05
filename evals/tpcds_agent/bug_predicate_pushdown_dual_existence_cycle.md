# Bug: PredicatePushdown creates a CTE cycle with two existence filters on the same concept

**Status:** FIXED (found 2026-06-05, fixed 2026-06-05)
**Severity:** medium — blocks a class of legitimate queries; has a known cause + workaround.
**Area:** `trilogy/core/optimization.py` → `PredicatePushdown`

## Fix

`PredicatePushdown._check_parent` now computes the existence sources it would
promote onto a parent CTE *before* mutating anything, and vetoes the whole push
when any of those sources already (transitively) depends on the parent — the
symmetric "A pushed into B while B pushed into A" case. See
`_promotion_would_cycle` / `_transitively_depends_on` in
`trilogy/core/optimizations/predicate_pushdown.py`. Exactly one of the two
mutual pushups is blocked, so the result is still a valid DAG and the surviving
pushup is semantically redundant (every consumer AND-carries both atoms).

Regression test: `tests/optimization/test_pushdown_optimization.py::test_dual_existence_filter_no_cycle`
(self-contained, no TPC-DS dependency).

The q10 rewrite below (Part A + Part B) **landed** with this fix:
`tests/modeling/tpc_ds_duckdb/all_sales.preql` gained a conformed
`purchasing_customer`, and `query10.preql` now uses the two-existence-set form.
`pytest tests/modeling/tpc_ds_duckdb/test_queries.py::test_ten` passes row-by-row
against the DuckDB reference.

## Symptom

A `select` whose `WHERE` contains **two existence (`in`) filters on the same subject
concept** fails at optimization time with:

```
trilogy.core.graph.NetworkXUnfeasible: Graph contains a cycle
```

Stack (frames that matter):

```
trilogy/core/query_processor.py:805   process_query
trilogy/core/optimization.py:730      optimize_ctes
trilogy/core/optimization.py:205      reorder_ctes
trilogy/core/optimization.py:193      reorder_ctes -> nx.topological_sort(G)
trilogy/core/graph.py:519             topological_sort   # ValueError: Graph contains a cycle
```

The CTE dependency graph `G` built in `reorder_ctes` is not a DAG, so the
topological sort raises.

## Minimal reproduction (no TPC-DS dependency)

`dim_cust.preql`:
```trilogy
key cust_id int;
property cust_id.cname string;
datasource dc (cid: cust_id, nm: cname) grain (cust_id) address dc_tbl;
```

`fact.preql`:
```trilogy
import dim_cust as cust;
key txn_id int;
property txn_id.channel string;
property txn_id.amt float;
datasource f (tid: txn_id, cid: cust.cust_id, ch: channel, a: amt)
grain (txn_id) address f_tbl;
```

Driver:
```python
from pathlib import Path
from trilogy.core.query_processor import process_query
from trilogy.dialect.duckdb import DuckDBDialect
from trilogy.parsing.parse_engine_v2 import parse_text

ROOT = Path("...")  # dir holding the two .preql above

def go(label, body):
    env, parsed = parse_text("import fact as sales;\n" + body, root=ROOT)
    try:
        DuckDBDialect().compile_statement(process_query(env, parsed[-1]))
        print(label, "OK")
    except Exception as e:
        print(label, "FAIL:", type(e).__name__, str(e)[:80])

# OK
go("single", """
auto a_buyers <- sales.cust.cust_id ? sales.channel = 'A';
where sales.cust.cust_id in a_buyers
select sales.cust.cname, count(sales.cust.cust_id) -> cnt;""")

# FAIL: NetworkXUnfeasible: Graph contains a cycle
go("double", """
auto a_buyers <- sales.cust.cust_id ? sales.channel = 'A';
auto b_buyers <- sales.cust.cust_id ? sales.channel = 'B';
where sales.cust.cust_id in a_buyers and sales.cust.cust_id in b_buyers
select sales.cust.cname, count(sales.cust.cust_id) -> cnt;""")
```

- **Single** existence filter → OK.
- **Two** existence filters on the same subject (`sales.cust.cust_id`) → cycle.

## Confirmed cause

It is the `PredicatePushdown` optimization pass. Proof:

```python
from trilogy.constants import CONFIG
CONFIG.optimizations.predicate_pushdown = False
# the "double" case above now resolves cleanly (valid DAG)
```

So the *unoptimized* plan is a correct DAG; the pass introduces the cycle.

From the pass's own logs on the failing query, it pushes **each** existence
condition up into the CTE that computes the **other** existence set:

```
[PredicatePushdown] ... pushing up BuildSubselectComparison(left=...cust_id, right=local.a_buyers, IN)   # into the b_buyers node
[PredicatePushdown] ... pushing up BuildSubselectComparison(left=...cust_id, right=local.b_buyers, IN)   # into the a_buyers node
```

Result: the `a_buyers` filter CTE depends on `b_buyers` **and** `b_buyers`
depends on `a_buyers` → mutual dependency → cycle.

The existence pushup logic lives around `trilogy/core/base_optimization.py`
(the `PredicatePushdown` rule emits the "Not pushing up existence ... as it is a
filter node" / "pushing up ..." messages). The bug is that it will push
existence condition X into a node that is itself the source of existence
condition Y which is in turn pushed back into X's node. The two pushups are each
individually "valid" but jointly cyclic.

## Suggested fix direction (for the next agent)

The pass needs a cycle guard for existence pushup. Options, roughly in order of
preference:

1. **Don't push an existence condition into a node that (transitively)
   contributes to that condition's own existence subquery, or whose own
   existence condition is being pushed into the current node.** I.e. detect the
   symmetric "A pushed into B while B pushed into A" case and skip one (or both)
   directions. This is the targeted fix.
2. After pushdown, **detect the introduced cycle and roll back** the offending
   pushup(s) — cheaper to implement, uglier.
3. In `reorder_ctes`, if the graph has a cycle, **fall back** to the
   pre-pushdown ordering for the strongly-connected component instead of
   raising. Treats the symptom, not the cause.

Add the minimal repro above (or the q10 form below) as a regression test. A good
home is a new `tests/` case asserting the "double" query both resolves and
returns correct rows.

## Where this surfaced — TPC-DS q10 rewrite

This was hit while rewriting `tests/modeling/tpc_ds_duckdb/query10.preql` to drop
its three per-channel customer `merge`s in favor of a conformed
`purchasing_customer` on `all_sales`. Because the unified fact has one row per
channel, "bought in STORE and (WEB or CATALOG)" must be expressed as two
existence sets on the same `purchasing_customer.id` — which is exactly the
trigger.

The rewrite is otherwise ready and should land once this bug is fixed. Two parts:

### Part A — `all_sales.preql` (additive; safe to land independently)

Add a conformed customer that maps each channel's q10-appropriate FK
(store→`SS_CUSTOMER_SK`, web→`WS_BILL_CUSTOMER_SK`, catalog→`CS_SHIP_CUSTOMER_SK`):

```trilogy
# after `import customer as ship_customer;`
import customer as purchasing_customer;
# web_sales_unified datasource:   WS_BILL_CUSTOMER_SK: ?purchasing_customer.id,
# catalog_sales_unified datasource: CS_SHIP_CUSTOMER_SK: ?purchasing_customer.id,
# store_sales_unified datasource:  SS_CUSTOMER_SK: ?purchasing_customer.id,
```

Name rationale: it is the customer credited with a purchase per channel (store/
web use the buyer; catalog uses ship-to following the TPC-DS q10 spec quirk).
Not `ship_customer` — that uses `WS_SHIP_CUSTOMER_SK` for web, which is wrong for
q10.

### Part B — `query10.preql` (blocked by this bug)

```trilogy
import all_sales as sales;

auto store_buyers <- sales.purchasing_customer.id ? (
    sales.sales_channel = 'STORE'
    and sales.date.year = 2002
    and sales.date.month_of_year in (1, 2, 3, 4)
);
auto webcat_buyers <- sales.purchasing_customer.id ? (
    sales.sales_channel in ('WEB', 'CATALOG')
    and sales.date.year = 2002
    and sales.date.month_of_year in (1, 2, 3, 4)
);

where
    sales.purchasing_customer.id in store_buyers
    and sales.purchasing_customer.id in webcat_buyers
    and sales.purchasing_customer.address.county in ('Rush County', 'Toole County', 'Jefferson County', 'Dona Ana County', 'La Porte County')
    and sales.purchasing_customer.demographics.gender is not null
select
    sales.purchasing_customer.demographics.gender,
    sales.purchasing_customer.demographics.marital_status,
    sales.purchasing_customer.demographics.education_status,
    count(sales.purchasing_customer.id) as cnt1,
    sales.purchasing_customer.demographics.purchase_estimate,
    count(sales.purchasing_customer.id) as cnt2,
    sales.purchasing_customer.demographics.credit_rating,
    count(sales.purchasing_customer.id) as cnt3,
    sales.purchasing_customer.demographics.dependent_count,
    count(sales.purchasing_customer.id) as cnt4,
    sales.purchasing_customer.demographics.employed_dependent_count,
    count(sales.purchasing_customer.id) as cnt5,
    sales.purchasing_customer.demographics.college_dependent_count,
    count(sales.purchasing_customer.id) as cnt6,
order by
    sales.purchasing_customer.demographics.gender asc,
    sales.purchasing_customer.demographics.marital_status asc,
    sales.purchasing_customer.demographics.education_status asc,
    sales.purchasing_customer.demographics.purchase_estimate asc,
    sales.purchasing_customer.demographics.credit_rating asc,
    sales.purchasing_customer.demographics.dependent_count asc,
    sales.purchasing_customer.demographics.employed_dependent_count asc,
    sales.purchasing_customer.demographics.college_dependent_count asc
;
```

Validate with `pytest tests/modeling/tpc_ds_duckdb/test_queries.py::test_ten`
(compares row-by-row to the DuckDB TPC-DS reference). Baseline (merge version)
currently passes; the rewrite must match it once the optimizer bug is fixed.

The original `query10.preql` (three merges) was restored to keep the suite green.
```
