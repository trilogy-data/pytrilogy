# Bug: unified-model 2-hop `customer.address` group/filter → cross-CTE "table not found"

**Status:** OPEN (found 2026-06-07, enriched eval q6).
**Severity:** medium — grouping/filtering by a customer's address property on the unified
`all_sales` model generates SQL where the address join references the customer-table alias from a
*sibling* CTE, which DuckDB rejects. `generate_sql` succeeds; execution fails. Drove part of q6's
2M-token run (after a bad reviewer kickback pushed it onto the multi-channel path).

## Symptom

```
(_duckdb.BinderException) Binder Error: Referenced table "sales_purchasing_customer_customers" not found!
  Candidate tables: "sales_purchasing_customer_..."
```
The customer table alias is bound inside one CTE, but the `C_CURRENT_ADDR_SK → address` join is
rendered from a different (sibling) CTE that can't see it. Both `purchasing_customer.*` and
`billing_customer.*` address paths fail identically.

## Deterministic reproduction (checked-in enriched model)

Model: `tests/modeling/tpc_ds_duckdb` (`all_sales.preql`). Needs data — fails at execution. Use the
conftest engine (any scale factor):
```python
import sys; sys.path.insert(0, "<repo_root>")
from tests.modeling.tpc_ds_duckdb.conftest import _make_engine
eng = _make_engine(sf=0.01, subdir="memory_sf001")
sql = eng.generate_sql(BODY)[-1]          # succeeds
eng.execute_raw_sql(sql).fetchall()       # BinderException: table not found
```

`BODY` (minimal):
```trilogy
import all_sales as sales;
where sales.date.year = 2001
  and sales.purchasing_customer.address.id is not null
select
    sales.purchasing_customer.address.state as state,
    count(sales.item.id) as n
limit 100;
```

## Minimization

- Trigger = **group by a 2-hop `<role>_customer.address.<prop>` on the unified model + filter on
  that same address path** (`purchasing_customer.address.id is not null`). The avg-by-category
  threshold / `having` from the original q6 are NOT required.
- It is the **unified `all_sales`** multi-arm structure that splits the customer and address joins
  across CTEs. (A single-channel base model with a direct customer→address path does not exhibit
  this — worth confirming the fix covers both.)

## Suggested fix

The address join must render in (or against) the same CTE/scope that binds the customer alias it
depends on. Either keep the customer→address join chain together in one CTE, or qualify the address
join's FROM-source to the CTE that actually exposes the customer key (cf. the analogous cross-CTE
join-key fix in `bug_B2_invalid_reference_codegen.md`, `dialect/common.py _render_left_concept`).

## Provenance

Enriched eval q6 (states with ≥10 customers buying items priced > 1.2× their category average). The
agent navigated `purchasing_customer.address.state` (the customer's current home-address state) on
`all_sales` and hit this on both the purchasing- and billing-customer paths (msgs 47/51). It only
reached the multi-channel `all_sales` path because a reviewer false-kickback wrongly told it the
store-only answer was incomplete (see the reviewer corpus case `agent_log.q06::0`, gold=DONE).
