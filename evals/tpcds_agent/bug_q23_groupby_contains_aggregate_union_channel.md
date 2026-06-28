# Bug: membership against a HAVING-aggregate rowset renders a CASE-over-aggregate that lands in GROUP BY → "GROUP BY clause cannot contain aggregates"

**Status:** OPEN (found enriched eval q23, run `20260628-042638_enriched`). Do NOT fix yet.
**Severity:** high — `generate_sql` succeeds, execution raises a DuckDB BinderException. Trilogy
emits syntactically invalid SQL (violates "never emit invalid SQL"). The agent had no query-side
escape and burned iterations rewriting around it (3× the same error in the run).

## Symptom

```
(_duckdb.BinderException) Binder Error: GROUP BY clause cannot contain aggregates!
LINE 18:     CASE WHEN sum("store_sales_store_sales"."SS_QUANTITY" * "store_sales_...
                       ^
```

The membership filter `x in best_customers.cust_id` (where `best_customers` is a rowset whose HAVING
compares an aggregate to a scalar threshold) lowers the rowset's HAVING into a per-row existence
column `_virt_filter_id_*` rendered as `CASE WHEN <having-cond> THEN <key> ELSE NULL END`. The
`<having-cond>` contains a local `sum(...) > threshold`, so the CASE column **contains an aggregate**.
That column is then wrongly classified as a dimension grouping key and emitted in the CTE's GROUP BY:

```sql
questionable as (
SELECT
    "store_sales_store_sales"."SS_CUSTOMER_SK" as "store_sales_customer_id",
    CASE WHEN sum("…SS_QUANTITY" * "…SS_SALES_PRICE") > "cooperative"."best_customer_threshold"
              and "…SS_CUSTOMER_SK" is not null
         THEN "…SS_CUSTOMER_SK" ELSE NULL END           as "_virt_filter_id_6399582348310959"
FROM "cooperative" LEFT OUTER JOIN "store_sales" … on 1=1
GROUP BY
    1,
    2,                                  -- <<< position 2 = the CASE-over-aggregate column → INVALID
    "cooperative"."best_customer_threshold"
HAVING sum("…SS_QUANTITY" * "…SS_SALES_PRICE") > "cooperative"."best_customer_threshold")
```

## Minimal repro (self-contained over the eval workspace model)

`q23_min.preql` (run against `evals/tpcds_agent/results/20260628-042638_enriched/workspace`):

```trilogy
import raw.store_sales as store_sales;
import raw.catalog_sales as catalog_sales;

auto all_customer_totals <- sum(store_sales.quantity * store_sales.sales_price) by store_sales.customer.id;
auto best_customer_threshold <- (max(all_customer_totals) by *) * 0.5;

rowset best_customers <-
where store_sales.customer.id is not null
select
    store_sales.customer.id as cust_id,
    sum(store_sales.quantity * store_sales.sales_price) as customer_total
having customer_total > best_customer_threshold
;

where catalog_sales.bill_customer.id in best_customers.cust_id
select
    catalog_sales.bill_customer.id as cid,
    sum(catalog_sales.list_price) as t
;
```

Command:

```python
from pathlib import Path
from trilogy import Environment, Dialects
ws = Path("evals/tpcds_agent/results/20260628-042638_enriched/workspace")
env = Environment(working_path=str(ws))
ee = Dialects.DUCK_DB.default_executor(environment=env)
print(ee.generate_sql(Path("q23_min.preql").read_text())[-1])   # succeeds, GROUP BY 1,2,...
# execute against the workspace DB to see the BinderException:
ee.execute_raw_sql(f"ATTACH '{(ws/'tpcds.duckdb').as_posix()}' as t (READ_ONLY); USE t;")
ee.execute_query(Path("q23_min.preql").read_text()).fetchall()  # raises BinderException
```

### Minimal trigger conditions
1. A **rowset** projecting a key plus an aggregate, with a **HAVING that compares an aggregate to a
   scalar** (`customer_total > best_customer_threshold`).
2. The scalar threshold is a **separate derived aggregate** (`max(all_customer_totals) by *`, where
   `all_customer_totals` is its own `sum(...) by customer`). This forces the existence set to be
   built in a *grouping* CTE rather than collapsed into a plain WHERE (the inline form
   `having customer_total > 0.5 * max(customer_total) by *` does NOT trigger — it routes the HAVING
   into a separate WHERE-only CTE `yummy`, see the workspace `query23.preql` final variant).
3. The rowset key is consumed via **membership** (`other_fact.fk in best_customers.cust_id`), so the
   filter is lowered to a `_virt_filter_id` existence column rendered as CASE.

The original q23 (`union(...)` catalog+web channels) just multiplies this: each arm's membership
feeds the same broken `vacuous`/`questionable` CTE.

## Root cause (file:line)

The filter existence column renders as a CASE wrapping the where-condition:
- `trilogy/dialect/base.py:1037-1050` — FILTER lineage → `CASE WHEN <where.conditional> THEN content
  ELSE NULL END` (the condition, which here holds a local `sum(...)`, becomes part of the column
  expression). The "redundant-CASE elision" guard at 1042-1048 does not fire because the CTE's WHERE
  does not imply the aggregate predicate.

The group/aggregate classifier does **not** account for an aggregate inside the filter where-condition:
- `trilogy/core/models/execute.py:450` `QueryDatasource.group_concepts` selects outputs for which
  `check_is_not_in_group(c)` is False.
- `trilogy/core/models/execute.py:469-474` — `has_local_aggregate`'s FILTER branch only inspects
  `c.lineage.content_concept_arguments` (the key → no aggregate). Comment at 467-468 asserts "the where
  condition (which may aggregate) becomes a predicate, not the column expression" — **false when the
  filter is rendered inline as the CASE existence column.** Returns False.
- `trilogy/core/models/execute.py:514-538` — `check_is_not_in_group`'s FILTER branch iterates
  `rendered_concept_arguments` (which DOES include the where args, per
  `trilogy/core/models/build.py:1724-1729`); the dimension args (`customer_id`, the content key) make
  `all(check_is_not_in_group(...))` False, then the fallback `has_local_aggregate(c)` at 536 also
  returns False (per the gap above) → returns False → the CASE-over-aggregate filter concept is added
  to `group_concepts` → GROUP BY.

**Fix sketch (not applied):** `has_local_aggregate`'s FILTER branch should also descend the
`where.conditional` arguments (the rendered condition is part of the CASE expression), so a filter
whose predicate contains a local aggregate is treated as an aggregate output and kept out of GROUP BY.
Confirm it interacts correctly with the elision path at base.py:1042 (when the CASE is elided to bare
content, the where aggregate is NOT in the column and the filter legitimately is a group key).

## Relation to known union/rollup GROUP BY bugs

- **Distinct** from `bug_composite_rollup_agg_union_drops_groupby.md` (FIXED 2026-06-08): that family
  is ROLLUP nullable-propagation + a passthrough rollup key losing its GROUP BY across split CTEs under
  a union model. No rollup/cube here; the offending column is a FILTER-as-CASE existence set, and the
  problem is an aggregate wrongly *included* in GROUP BY (not a key wrongly *dropped*).
- Closer in spirit to the FILTER-CTE family in memory (`project_filter_cte_dropped_metric_binder_bug`,
  `project_q64_nested_membership_two_source_agg_invalid_ref`) — membership/existence lowering of a
  filtered concept — but the specific defect (where-condition aggregate ignored by
  `group_concepts`/`has_local_aggregate`, so a CASE-over-aggregate enters GROUP BY) is new.

## Notes
- Current workspace `query23.preql` (final saved variant) uses the inline
  `having customer_total > 0.5 * max(customer_total) by *` form and generates VALID SQL — the bug only
  surfaces with the derived-threshold variant the agent wrote at log line 45 (run `query23.preql` at
  line 48 → BinderException at result line 49).
- Canonical `tests/modeling/tpc_ds_duckdb/query23.preql` generates valid SQL (no `_virt_filter`): it
  models a single conformed `all_sales` source and expresses membership/threshold within that one
  source, avoiding the cross-source membership-as-CASE existence construction entirely.
