# Bug: dim-bridge CTE drops binding key referenced by downstream OUTER coalesce → BinderException (q11)

**Status:** FIXED 2026-06-24 — `QueryDatasource.get_alias` now walks the pseudonym
closure and prefers a NON-hidden member, so the merged OUTER key renders from the
dim-bridge CTE's actually-projected join-key alias instead of its hidden binding key.
Regression: `tests/test_scoped_join_dim_bridge_outer_key.py`. (Was: confirmed, 100%
deterministic repro 3/3, plus original q11 query byte-identical.)
**Surfaced by:** TPC-DS q11 enriched eval (run `20260624-133456`), trace msg ~47/50.
**Severity:** Medium. Produces invalid SQL (hard `BinderException`) at execution, not a wrong
answer. In q11 it forced the agent off a correct-looking multi-rowset approach onto an
all-INNER rewrite (the working escape), costing a large reasoning detour.

## Symptom

```
Binder Error: Values list "juicy" does not have a column named "c_id"
```

Generated SQL contains `coalesce("<cte>"."c_id", "<rowset>"."sr0N_cust_id")` where `<cte>` (the
dim-bridge CTE) never projected `c_id`. CTE aliases are randomized per run (`juicy`/`questionable`/
`sweltering` etc.); the structural defect is invariant.

## Minimal repro

Pared down to **store_sales + customer only** (cross-channel/web from the original q11 not required):

```trilogy
import raw.store_sales as ss;
import raw.customer as c;

rowset sr01 <- where ss.date.year = 2001
select ss.customer.id as cust_id, sum(ss.ext_list_price) as s_rev;
rowset sr02 <- where ss.date.year = 2002
select ss.customer.id as cust_id, sum(ss.ext_list_price) as s_rev;
rowset sr03 <- where ss.date.year = 2000
select ss.customer.id as cust_id, sum(ss.ext_list_price) as s_rev;

where sr01.s_rev > 0                     -- WHERE filter on the anchor rowset's aggregate
select c.text_id as code, c.first_name,
    --sr02.s_rev, --sr03.s_rev
inner join sr01.cust_id = c.id           -- INNER to the dim that supplies the binding key
left join sr01.cust_id = sr02.cust_id    -- LEFT #1
left join sr01.cust_id = sr03.cust_id    -- LEFT #2  (two LEFTs required)
limit 100;
```

## Root cause (generated SQL)

The dim-bridge CTE projects only the renamed join-key alias and drops the binding key `c_id`,
even though it joins `customer`:

```sql
questionable as (
SELECT "c_customers"."C_CUSTOMER_ID" as "c_text_id",
       "c_customers"."C_FIRST_NAME"  as "c_first_name",
       "cooperative"."sr01_cust_id"  as "sr01_cust_id"          -- keeps only the join-key alias
FROM "cooperative" INNER JOIN "customer" as "c_customers"
     on "cooperative"."sr01_cust_id" = "c_customers"."C_CUSTOMER_SK"
WHERE "cooperative"."sr01_s_rev" > 0 ),
```

The downstream CTE then references the un-projected column in an OUTER-join coalesce:

```sql
juicy as (
SELECT ...,
   coalesce("questionable"."c_id","yummy"."sr03_cust_id") as "sr03_cust_id"  -- "c_id" not in questionable
FROM "questionable" LEFT OUTER JOIN "yummy" on ...)
```

→ `Binder Error: Values list "questionable" does not have a column named "c_id"`. (In the full
q11 query the failing CTE is one layer deeper — `juicy` — because of the extra web rowset.)

## Trigger condition (all four jointly required)

Each ingredient removed individually makes it pass:

1. An anchor rowset (`sr01`) with a **WHERE filter on its aggregate measure** (`where sr01.s_rev > 0`).
   **Surprising key ingredient** — pure join structure alone is fine; removing the WHERE → passes.
2. An **INNER join to the customer dim** for the binding key (`inner join sr01.cust_id = c.id`).
3. **Two or more OUTER (LEFT/FULL) join rowsets** off the same anchor key — one LEFT passes, two crash.
4. Projecting a **dim property** (`c.text_id`/`c.first_name`) that pulls the customer CTE into the chain.

### Join-mode behaviour

| Variant | Result |
|---|---|
| All-INNER joins | OK — **the agent's escape** |
| Pure all-LEFT joins | OK — pure-LEFT does NOT crash |
| **INNER-to-dim + ≥2 LEFT-rowset mix** | **CRASH** |

Mechanism: the OUTER-join coalesce on the binding key emits `coalesce(<dim>.c_id, <rowset>.key)`,
but the intermediate dim-bridge CTE keeps only the renamed join-key alias (`sr01_cust_id`) and
drops the binding key `c_id` from its `output_columns`. The second LEFT adds the CTE layer where
the missing column is referenced.

## Concepts/addresses involved

- Binding key: `c.id` → `customer.C_CUSTOMER_SK` (the dropped `c_id`).
- Rowset keys: `sr0N.cust_id` ← `ss.customer.id` (`ss_customer_sk`); coalesced against the binding key.
- Dim properties pulled in: `c.text_id` (`C_CUSTOMER_ID`), `c.first_name`.
- Failing CTE pair: dim-bridge (omits `c_id`) → downstream coalesce CTE (references `<cte>.c_id`).

## Likely fix area

Intermediate-CTE column propagation: a dim-bridge CTE must carry the binding key (`c_id`) in its
`output_columns` whenever a downstream OUTER coalesce references it.

## Relation to memory `project_root_outer_source_key_no_coalesce`

That note's render-layer fix (`safe_get_cte_value` falling back to a multi-source pseudonym's
`source_map`) handles the case where the coalesce silently rendered a *raw* column (wrong NULLs).
**This case escapes that fallback**: the planner emits a `coalesce(...)` referencing a column the
upstream CTE never put in `output_columns`, so it is a **hard BinderException before execution**,
not a wrong value. The render-layer guard patches the final-projection path; this bug is in
intermediate-CTE column propagation, so the SQL is structurally invalid before the guard runs.

## Repro harness

Copy the run DB out (avoid lock contention with any in-flight eval), keep workspace as `working_path`
so `raw/` models resolve:

```python
import sys; sys.path.insert(0, 'evals')
from common import scoring
from pathlib import Path
ws = Path('evals/tpcds_agent/results/20260624-133456/workspace')
eng = scoring.make_scoring_engine(ws / 'tpcds.duckdb', ws, 'tpcds')
eng.generate_sql(open('repro.preql').read())   # raises / emits the bad SQL
```
