# q23 token sink (687,331 tok, fail) — NO framework bug; bare-max grain-polymorphic tautology re-confirmed on renamed engine

Run: `evals/tpcds_agent/results/20260708-135136_enriched` — q23 status=fail,
ref_rows=4, cand_rows=100, exit_code=0 (ran clean → SILENT wrong result), 20
iterations, 4 tool_errors (all trivial syntax: GROUP BY, missing AS).

## Verdict
**NOT a framework bug.** Re-confirms the prior finding
(`project_q23_bare_max_sibling_rowset_having_tautology`,
`project_q23_degenerate_where_cograin_scalar_max` REVERTED) on the CURRENT
renamed engine (`.id`=business, `.sk`=surrogate). The obstacle is agent
authoring against the documented grain-polymorphic bare-aggregate semantics.
Canonical `tests/modeling/tpc_ds_duckdb/query23.preql` builds and matches
reference (ref=4) on the current engine — `pytest ...::test_twenty_three` PASSES.

## Symptom
Agent's `query23.preql` runs clean but returns 100 rows (limit hit) vs
reference 4. Root: the "best customers" membership set is essentially the
ENTIRE customer population, so `... in best_customers.customer_id` is a no-op
and the final union over-produces.

## Minimal repro (harness on the run's workspace DB)
Agent's best-customer definition:
```
auto cust_store_alltime    <- sum(qty*price ? cust.id is not null) by store_sales.customer.id;
auto cust_store_2000_2003  <- sum(qty*price ? cust.id is not null and year 2000..2003) by store_sales.customer.id;
auto max_cust_store_2000_2003 <- max(cust_store_2000_2003);        -- BARE, no grain
where store_sales.customer.id is not null
select store_sales.customer.id as customer_id
having cust_store_alltime > 0.5 * max_cust_store_2000_2003;
```
| variant                                             | best-customer rows |
|-----------------------------------------------------|--------------------|
| bare `max(cust_store_2000_2003)`      (agent)        | **76412** (all)    |
| `max(cust_store_2000_2003) by *`      (global)       | **4703**           |
| canonical scalar rowset `select max(...) as cmax`    | **4703**           |

Full-query row counts:
| variant                                             | rows |
|-----------------------------------------------------|------|
| agent query as submitted                            | 100  |
| agent query, only `max(...)` → `... by *`           | 60   |
| + frequent items keyed `.sk` (surrogate) not `.id`  | 52   |
| canonical query23.preql                             | 4 (== ref) |

## Root cause (by design, not a bug)
Generated SQL for the bare `max` proves the aggregate is **dissolved to
identity**: the CTE feeding the HAVING is
```
questionable AS (SELECT cheerful.cust_store_2000_2003 AS mx,
                        cheerful.store_sales_customer_id ...)
```
i.e. `mx` = each customer's OWN 2000-2003 total — no global aggregation. A bare
aggregate is grain-polymorphic; consumed in a per-customer select/HAVING it
co-grains to that per-customer grain, so `max(x) by <this customer>` = x. The
HAVING becomes `alltime > 0.5 * own_2000_2003` — a tautology (all-time ⊇
2000-2003 subset ⇒ almost always > 50% of it) ⇒ 76412/76412 pass.

Grain inheritance for the un-pinned aggregate lives in
`trilogy/core/models/author.py:1514 calculate_granularity` /
`build.py` grain layers; the intentional removal of the
`_degenerate_aggregate_cograin` carveout is recorded in the memory topics above.
The engine emits NO warning that `max()` collapsed — that "silence" is the
footgun, but the value is semantically correct per the language design.

## Why the agent churned (secondary, also authoring)
Two more independent authoring errors compound the over-production (residual
52-vs-4), none of them framework:
1. Frequent items grouped on `all_sales.item.id` (business) instead of `.sk`
   (surrogate) — task explicitly wants the surrogate; canonical uses `.sk`.
2. Frequent-items count has NO `channel = 'STORE'` filter (agent counted across
   all channels); canonical/reference count over store_sales only.

## Classification
guidance / agent — grain-polymorphic bare aggregate correctly co-grains; author
must pin the global max with `by *` or a grainless scalar rowset (as canonical
does with `rowset max_total <- select max(...) as cmax`). Framework unchanged.
Possible guidance win: a Syntax/lint hint when a bare `max/min/sum` with no `by`
is consumed inside a HAVING at the same grain as its own inputs (dissolves to
identity).
