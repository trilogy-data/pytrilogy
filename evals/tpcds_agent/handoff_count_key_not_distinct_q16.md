# Handoff: `count(<key>)` returned 274,743 for 160,000 distinct orders (q16)

**Status: REPRODUCED and FIXED.** Real planner defect, not a doc over-promise.
Repro: `evals/tpcds_agent/repro_q16_count_key_not_distinct.py`. Regression test:
`tests/modeling/tpc_ds_duckdb/test_q16_count_key_shared_filter_stream.py`.

## The claim vs the observation

The agent language reference promises: "`count(<key>)` is already distinct
because keys are unique." In the fresh messy-warehouse run
(`results/20260810-211903_enriched_aggregates/agent_log.q16.jsonl`), a
DeepSeek agent probing `catalog_sales` got `count(cs.order_number) = 274,743`
at single-row grain, while the true distinct order count is **160,000** —
independently confirmed two ways in the same session (94,339 + 65,661 and
93,142 + 66,858 from complementary rowset partitions).

Against the un-renamed `tests/modeling/tpc_ds_duckdb` model at sf=1 the same
probe returned **254,337** (the eval's messy schema shifts the number, not the
defect). The doc promise was right; the plan did not honor it.

## Root cause

`count(<key>)` is planned as **dedup-then-COUNT**, not `COUNT(DISTINCT)`: the
aggregate bucket carries `aggregate_input_grain = {cs.order_number}` and a
normalization `GroupNode` collapses the input stream to that grain, after which
a plain `COUNT` of the key is distinct by construction.

That normalization GROUP must also project **every argument the bucket's
aggregates read** (`strategy_builder.build_strategy_node`, the
`normalize_addrs` block). `count(cs.order_number ? cs.is_returned = true)` sits
in the *same* bucket — `_aggregate_input_grain` reduces a FILTER-over-a-key
argument to the key, so both counts key to input grain `{cs.order_number}` —
but its argument is the filter virtual `_virt_filter_order_number_*`, which
`order_number` does **not** determine. It became a second GROUP BY key:

```sql
-- pre-fix
GROUP BY 1, 2   -- (_virt_filter_order_number_*, cs_order_number)
...
SELECT count("wakeful"."cs_order_number") as "all_orders"
```

An order with both returned and unreturned lines survives as two rows, so the
plain COUNT reports neither the distinct order count nor the line count — the
"neither-rows-nor-distinct" number the original report flagged. The *filtered*
count was accidentally immune (its virtual is NULL-or-the-key, so distinct
pairs still map 1:1 to distinct returned orders), which is why only the
unfiltered count was wrong.

The `aggregate_distinct_addrs` machinery for exactly this
(`_fold_distinct_rewritable_buckets`) never fired: it only rewrites a count
folded *across* buckets, and here both counts were already in one bucket.

## Fix

`trilogy/core/processing/v4_helper/group_rules.py` —
`_dedup_widened_distinct_members`: after the fold, flag every
`aggregate_distinct_rewritable` COUNT in a bucket whose members drag a direct
argument outside `input_grain | out_grain`. Those counts lose the dedup
guarantee, so the DISTINCT becomes explicit and they render
`COUNT(DISTINCT ...)`. A lone `count(<key>)` keeps the cheaper
dedup-then-COUNT shape.

Post-fix the probe returns `(160000, 5, 94339)`, byte-identical to the raw-SQL
oracle.

## Footprint

Rendering all 132 `query*.preql` in `tests/modeling/tpc_ds_duckdb` +
`tests/modeling/tpc_h` before and after: **0 changed, 132 byte-identical**. The
benchmark corpus never puts a filtered and an unfiltered count of the same key
in one select; agents do.

## Doc note

No change needed to the "`count(<key>)` is already distinct" bullet — the
promise now holds. Worth keeping the reference's existing steer toward
`count(line_item)` for line-grain questions, which is a different point.
