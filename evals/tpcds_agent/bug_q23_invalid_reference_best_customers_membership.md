# Bug: q23 — a HIDDEN (`--`) aggregate output of a `with` rowset is dropped from its grouping CTE, then re-derived downstream → `INVALID_REFERENCE_BUG * INVALID_REFERENCE_BUG`

**Status:** OPEN (found TPC-DS agent eval query 23, run `repeat_q23_20260628-155741_enriched`, rep r01;
the same sentinel reproduced 11× across reps). Do NOT fix yet.
**Severity:** high — `trilogy run query23.preql` raises
`Unexpected error in query23.preql: Invalid reference string found in query` (the internal
`INVALID_REFERENCE_BUG` sentinel, "this should never occur"). The agent burned a long iteration loop
on it and never escaped.
**Area:** materialization of a **hidden** (`--`) aggregate output of a `with`/rowset across the
grouping CTE → downstream projection re-derives the aggregate's inner expression from the
already-grouped parent (whose raw operand columns are gone). Sentinel emission at
`trilogy/dialect/base.py:1177-1179` (`INVALID_REFERENCE_STRING`, `BASE_INVALID` at line 253).
Relevant un-hide machinery: `_expose_downstream_referenced_columns`,
`trilogy/core/query_processor.py:893-939`.

## Symptom

The agent modeled q23's "best customers" set as a rowset `customer_totals` projecting `cust_id` plus a
**hidden** lifetime total `--sum(store_sales.quantity * store_sales.sales_price) as lifetime_total`,
then referenced that hidden measure from `auto max_total <- max(customer_totals.lifetime_total)` and a
`best_customers` HAVING (`customer_totals.lifetime_total > threshold`), consumed by membership into a
catalog+web `union`. Running it, the **rowset's own materialization CTE** renders the sentinel
(observed verbatim in `agent_log.q23.r01`, generated SQL):

```sql
sparkling as (SELECT
    "concerned"."_customer_totals_cust_id"      as "customer_totals_cust_id",
    INVALID_REFERENCE_BUG * INVALID_REFERENCE_BUG as "customer_totals_lifetime_total"  -- <<<
 FROM "concerned"),
```

where the parent CTE only grouped the key and **never computed the sum**:

```sql
concerned as (SELECT
    "store_sales_store_sales"."SS_CUSTOMER_SK" as "_customer_totals_cust_id"
 FROM "store_sales" ... WHERE ... GROUP BY 1),   -- note: no sum(...) measure here
```

So the downstream projection re-derives `quantity * sales_price` (the inner expression of `sum`) from
`concerned`, whose raw `SS_QUANTITY` / `SS_SALES_PRICE` columns no longer exist after the GROUP BY →
both operands resolve to the sentinel.

## Minimal repro (smallest snippet still emitting the sentinel)

Model dir = the eval workspace (`raw.store_sales`; no data needed, fails at `generate_sql`). Neither
HAVING, `max(...)`, nor membership is required — the single trigger is a `with` rowset with a **hidden
`--` aggregate output that is referenced downstream**:

```trilogy
import raw.store_sales as store_sales;

with customer_totals as
where store_sales.customer.id is not null
select
    store_sales.customer.id as cust_id,
    --sum(store_sales.quantity * store_sales.sales_price) as lifetime_total
;

select customer_totals.cust_id, customer_totals.lifetime_total limit 5;
-- INVALID_REFERENCE_BUG * INVALID_REFERENCE_BUG as "customer_totals_lifetime_total"
```

Command (`ws` = `.../repeat_q23_20260628-155741_enriched/workspace/_worker_1`):

```python
from pathlib import Path
from trilogy import Environment, Dialects
env = Environment(working_path=ws)
ee  = Dialects.DUCK_DB.default_executor(environment=env)
ee.generate_sql(Path("repro.preql").read_text())   # raises ValueError: Invalid reference string
```

### Trigger matrix (isolates the defect to the `--` marker)

| variant | result |
|---|---|
| hidden `--sum(...)` output, referenced downstream (orig / minimal) | **SENTINEL** |
| same but **visible** `sum(...)` output (drop the `--`) | **CLEAN** |
| hidden `--sum(...)` + `max()`/HAVING `best_customers` + membership consumer (agent's full shape) | **SENTINEL** |

The visible-vs-hidden toggle is the whole story: with the aggregate visible, the grouping CTE
materializes `sum(...)` and the downstream projection passes it through
(`"cheerful"."_customer_totals_lifetime_total" as ...`); marking it `--` drops it from the grouping
CTE's outputs, and the downstream projection falls back to re-deriving the lineage expression.

## Root cause (file:line)

1. The rowset's aggregate output `lifetime_total = sum(quantity * sales_price)` is marked **hidden**
   (`--`). When the rowset is materialized, the **grouping node** that GROUPs BY `cust_id` does **not**
   carry the hidden aggregate as a materialized output column (the grouped CTE `concerned`/`cheerful`
   selects only the key, `GROUP BY 1`).
2. The hidden measure is nonetheless **referenced downstream** (directly selected, or via
   `max(customer_totals.lifetime_total)` / the `best_customers` HAVING). The un-hide pass
   `_expose_downstream_referenced_columns` (`query_processor.py:893-939`) is meant to restore a hidden
   producer column that a consumer actually renders — but it can only un-hide a column that is **present
   in the producer's `source_map`** (lines 918, 935). Here the aggregate was never materialized at the
   grouping node, so there is nothing to un-hide.
3. The downstream projection (`sparkling`/`cooperative`) therefore has no `source_map` entry for
   `customer_totals.lifetime_total` and falls back to rendering it from **lineage** — the inner
   `quantity * sales_price` expression — against the already-grouped parent.
4. `_render_concept_sql` → `safe_get_cte_value(...)` returns `None` for each operand
   (`SS_QUANTITY`, `SS_SALES_PRICE` absent post-GROUP BY) → `INVALID_REFERENCE_STRING(...)`
   (`dialect/base.py:1177-1179`), rendering `INVALID_REFERENCE_BUG * INVALID_REFERENCE_BUG`.

Net defect: a hidden (`--`) aggregate output that is downstream-referenced should be **materialized at
its grouping node** (then optionally hidden from final SELECT), not stripped from the group grain and
re-derived from a parent that no longer holds its raw operands. (Canonical hand-authored
`tests/modeling/tpc_ds_duckdb/query23.preql` generates CLEAN SQL — len 8159, 0 sentinels — because it
models a single conformed `all_sales` source with visible `by`-grain aggregates and never hides a
rowset aggregate output.)

## Verdict — SHARED vs DISTINCT

**DISTINCT from the INVALID_REFERENCE family (q38 / q64 / q75); a different mechanism that the family's
proposed general fix would NOT cover.** The family (per `bug_q64_invalid_reference_eligible_items_membership.md`)
shares one root cause: **output/left-tuple resolution does not follow the inner-join/membership
*equivalence collapse*** — the failing concept exists in some CTE but on the *non-scanned*
join-equivalent side, so the fix is a pseudonym/equivalence-closure redirect onto the twin column that
IS present (q75 left-tuple grain key; q38/q64 the pruned/non-scanned inner-join side).

q23's INVALID_REF arm has **no equivalence collapse and no twin column to redirect to** — the aggregate
measure is simply **not materialized anywhere** (dropped from its own grouping node by the `--` flag)
and is then **re-derived from lineage** over a grouped parent missing the raw operands. A
pseudonym-closure pass would find no source to point at; the fix here is to **force-materialize the
referenced hidden aggregate at the grouping CTE** (an un-hide/promote at the producer, akin to
`_expose_downstream_referenced_columns` but covering aggregates absent from the group grain), which is
orthogonal to the family's output-side redirect. So the general family fix would **not** resolve q23.

**Relationship to the sibling q23 report
(`bug_q23_groupby_contains_aggregate_union_channel.md`): SAME query / agent intent, DIFFERENT defect
and DIFFERENT trigger.** Both arise from the agent's "best customers = HAVING aggregate vs derived
scalar threshold + membership" construction, but:
- The sibling defect: a FILTER-as-CASE existence column containing a local `sum(...)` is wrongly added
  to GROUP BY (`generate_sql` **succeeds**, execution raises DuckDB `GROUP BY clause cannot contain
  aggregates`). Triggered specifically by the **derived-scalar threshold** forcing a grouping existence
  CTE.
- This defect: a **hidden `--` aggregate output** dropped from its grouping CTE and re-derived
  downstream (`generate_sql` itself **fails** with the sentinel). Triggered specifically by the **`--`
  hidden marker** — the minimal repro needs neither HAVING, `max()`, nor membership.

They are independent: the sibling reproduces with a visible aggregate (no `--`); this one reproduces
with no HAVING/threshold/membership at all. Two separate fixes are required.
