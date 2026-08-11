# Handoff: `count(<key>)` returned 274,743 for 160,000 distinct orders (q16)

**Status: unreproduced engine-bug candidate. Isolated from the messy-warehouse
first-20 run; needs a minimal repro before any fix.**

## The claim vs the observation

The agent language reference promises: "`count(<key>)` is already distinct
because keys are unique." In the fresh messy-warehouse run
(`results/20260810-211903_enriched_aggregates/agent_log.q16.jsonl`), a
DeepSeek agent probing `catalog_sales` got `count(cs.order_number) = 274,743`
at single-row grain, while the true distinct order count is **160,000** —
independently confirmed two ways in the same session (94,339 + 65,661 and
93,142 + 66,858 from complementary rowset partitions).

`order_number` is a key, but one key of the fact's composite grain
`<order_number, item.sk>`. 274,743 is neither the distinct order count
(160,000) nor the line count (~1.44M at SF1), which makes it look like a
count over some intermediate node's grain rather than a distinct count of the
key. The agent burned ~10 probe queries on this; the query ultimately FAILED
scoring.

## Evidence (exact queries from the log)

Probe at global grain (debug1, exit 0):

```trilogy
import raw.catalog_sales as cs;

where 1=1
select
    count(cs.order_number) as all_orders,                      # -> 274,743
    count_distinct(cs.warehouse.sk) as all_warehouses,         # -> 5
    count(cs.order_number ? cs.is_returned = true) as returned_orders  # -> 96,277
;
```

Ground truth via rowset partition (debug6, exit 0):

```trilogy
import raw.catalog_sales as cs;

with returned as
where cs.return_quantity is not null
select cs.order_number as order_id;

with no_return as
where cs.order_number not in returned.order_id
select cs.order_number as order_id;

select
    count(returned.order_id) as returned_count,     # -> 93,142
    count(no_return.order_id) as no_return_count    # -> 66,858  (sum = 160,000)
;
```

Note `where 1=1` in debug1 — check whether the tautological filter changes the
sourcing (the second probe went through rowsets, which dedupe to their select
grain by construction).

## Repro guidance

- Model: `tests/modeling/tpc_ds_duckdb/catalog_sales.preql` (the eval seeds its
  workspace from this directory; the eval's physical rename to `fact_*`/`dim_*`
  should be irrelevant). NEVER run two pytest processes concurrently against
  `tests/modeling` — shared memory-duckdb setup produces phantom failures.
- Minimal check: `select count(cs.order_number) as c;` with and without
  `where 1=1`, vs `select count_distinct(cs.order_number) as c;` vs the rowset
  partition sum. Compare against direct SQL
  `SELECT COUNT(DISTINCT cs_order_number) FROM catalog_sales`.
- If it reproduces: inspect the generated SQL — the interesting question is
  which source node hosted the count and at what grain (a pre-aggregation to
  `<order_number, X>` before `count(order_number)` would produce a
  neither-rows-nor-distinct number like this).
- Before chasing, check `index_not_a_bug_verdicts` memory / prior `bug_q*`
  files — count-at-grain semantics have prior verdicts; confirm this case is
  not already adjudicated.

## Outcomes

Either (a) a real distinctness defect in count-over-key planning — fix in the
planner; or (b) the doc promise is over-broad for keys of composite grains —
then the language reference's "`count(<key>)` is already distinct" bullet must
be qualified, because agents build multi-hundred-k-token debugging loops on
exactly this promise.
