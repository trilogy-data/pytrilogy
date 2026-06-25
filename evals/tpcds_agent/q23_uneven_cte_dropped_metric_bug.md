# Bug: filter-CTE drops grouped metric from projection → BinderException (q23)

**Status:** FIXED 2026-06-24 — `_expose_downstream_referenced_columns`
(query_processor.py, post-`optimize_ctes`) un-hides a producer-CTE column when a
consumer's NON-HIDDEN output actually renders a reference to it and the producer
exposes no visible pseudonym-equivalent (fixpoint; skips inlined DatasourceCTEs).
Restricting to *rendered* references — not all `source_map` entries — is essential:
the broad first cut regressed 9 tests. Regression:
`tests/test_filter_cte_grouped_metric_projection.py`. (Was: confirmed, 100%
deterministic repro 5/5.) Note: on the run DB the repro returns 0 rows — correct,
not a defect: the only group above `0.5*max` is the anonymous `NULL` customer, dropped
by the INNER join to `customer`.
**Surfaced by:** TPC-DS q23 enriched eval (run `20260624-133456`), agent debug probe.
**Severity:** Medium. Produces invalid SQL (hard `BinderException`), not a wrong answer.
The agent's *final* q23 query sidesteps it (uses an id-set concept, never projects the
metric); the bug bit a debug probe and sent the agent on a long false "the planner drops
my column" detour — i.e. it's a token sink and a correctness landmine for a natural query
shape.

## Symptom

```
Binder Error: Values list "uneven" does not have a column named "cust_store_rev"
```

(The filter CTE is given a randomized alias per run — `uneven` in the original trace,
`cooperative`/`thoughtful` in repro runs. Same structural defect regardless of name.)

## Minimal repro

Smallest crashing form (against the run's `workspace/tpcds.duckdb`; any store_sales model works):

```trilogy
import raw.store_sales as store_sales;
auto cust_store_rev <- sum(store_sales.quantity) by store_sales.customer.id;
auto best_cust_rev <- cust_store_rev > 0.5 * max(cust_store_rev) by *;
where best_cust_rev
select store_sales.customer.last_name, cust_store_rev
limit 20;
```

The metric is a plain `sum(quantity)` — the filtered/product metric and the `is not null`
clause from the original trace are **incidental**, not part of the trigger.

## Root cause (generated SQL)

```sql
cooperative as (                                   -- the filter CTE (= "uneven" in trace)
SELECT
    "thoughtful"."store_sales_customer_id" as "store_sales_customer_id"   -- projects ONLY the id
FROM "cheerful" FULL JOIN "thoughtful" on 1=1
WHERE ("thoughtful"."cust_store_rev" > 0.5 * "cheerful"."_virt_agg_max_...") = True
GROUP BY
    1,
    "thoughtful"."cust_store_rev")                 -- cust_store_rev is in GROUP BY but NOT projected
SELECT
    "store_sales_customer_customers"."C_LAST_NAME" as "store_sales_customer_last_name",
    "cooperative"."cust_store_rev" as "cust_store_rev"   -- outer references the un-projected column
FROM "cooperative"
    INNER JOIN "customer" as "store_sales_customer_customers"
        on "cooperative"."store_sales_customer_id" = "store_sales_customer_customers"."C_CUSTOMER_SK"
```

The filter CTE GROUPs BY `cust_store_rev` but omits it from its SELECT list; the outer query
then references `cooperative.cust_store_rev`, which does not exist.

## Trigger condition (all three jointly required)

1. A **boolean filter concept** of the form
   `flag <- metric > 0.5 * (max(metric) by *)` — a grouped metric compared against a
   *global* aggregate — used in **WHERE**; AND
2. the underlying **metric is also projected** in the SELECT; AND
3. an output column is a **property requiring a join off the grouping key**
   (e.g. `customer.last_name`), NOT the grouping key itself.

### Toggle evidence (all run live, deterministic)

| Variation | Result |
|---|---|
| Project grouping **key** + metric (`select customer.id, cust_store_rev`) | OK (no extra join CTE) |
| Project a **property** + metric (`select customer.last_name, cust_store_rev`) | **CRASH** |
| Project the property but **not** the metric (`select customer.last_name`) | OK (metric never demanded) |
| Inline threshold in WHERE instead of a boolean concept (`where cust_store_rev > 0.5*(max(...) by *)`) | OK |
| **Constant** threshold instead of `max(...) by *` | OK |

So it requires the property-join *and* the metric projection *and* the
boolean-concept-over-global-aggregate form, together.

## Concepts/addresses involved

- `store_sales.customer.id` (`SS_CUSTOMER_SK`) — grouping key of the metric.
- `cust_store_rev` = `sum(...) by store_sales.customer.id` — the grouped metric; the column dropped.
- `best_cust_rev` = `cust_store_rev > 0.5 * (max(cust_store_rev) by *)` — boolean filter concept;
  the global max renders as `_virt_agg_max_*`.
- `store_sales.customer.last_name` (`C_LAST_NAME`) — the property whose outer
  `INNER JOIN customer` forces the metric to be carried through the filter CTE that drops it.

## Likely fix area

The filter CTE's projection list must include any column referenced by a downstream consumer.
When a boolean filter concept built on a grouped metric is applied in WHERE and that metric is
also a projected output, the metric must be carried in the filter CTE's SELECT, not only its
GROUP BY. Same family as the "filter concept drops the metric column" notes and
`project_root_outer_source_key_no_coalesce` (a column referenced downstream is absent from an
intermediate CTE's `output_columns`), but here the symptom is a hard `BinderException`.

## Repro harness

```python
import sys; sys.path.insert(0, 'evals')
from common import scoring
from pathlib import Path
ws = Path('evals/tpcds_agent/results/20260624-133456/workspace')
eng = scoring.make_scoring_engine(ws / 'tpcds.duckdb', ws, 'tpcds')
eng.generate_sql(open('repro.preql').read())   # raises / emits the bad SQL
```
