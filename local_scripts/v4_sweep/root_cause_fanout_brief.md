# V4 bug brief — Fan-out: a grain-determined dimension is split off the aggregate and re-joined ON 1=1

**Planner:** v4 (`CONFIG.use_v4_discovery=True`). Distinct from root cause A
(condition placement) — different code area, safe to fix in parallel.

## One-line statement

When a query selects an aggregate at grain G **plus** a property that is
functionally determined by G (a dimension of the grain), v4 puts that property
in its own dedup group keyed by *itself* (dropping G), then the FINAL merge has
no shared key and falls back to `FULL JOIN … ON 1=1` — a cross product that
fans every aggregate row across every dimension value.

## Smoking gun (TPC-H adhoc07)

`select order.id, order.customer.id, sum(extended_price*(1-discount)) as order_sales`
— `order.customer.id` is functionally determined by `order.id` (one customer
per order).

**v3** (correct): the customer-dim CTE carries both keys and joins on the grain:
```sql
cooperative AS (SELECT order_id, order_customer_id FROM orders JOIN customer …)
SELECT … FROM questionable INNER JOIN cooperative ON questionable.order_id = cooperative.order_id
```

**v4** (wrong): the customer-dim CTE drops `order_id`, and the merge is a cross join:
```sql
questionable AS (SELECT order_id, sum(...) AS order_sales FROM thoughtful GROUP BY 1),
cooperative  AS (SELECT order_customer_id FROM thoughtful GROUP BY 1)   -- order_id dropped!
SELECT order_id, order_customer_id, order_sales
FROM questionable FULL JOIN cooperative ON 1=1                          -- cross product
```
Result: one order's `order_sales` is repeated across many `order_customer_id`
values (e.g. order 255174 emitted with custkeys 2270/6217/4924/7405).

## Affected (this root cause)

- TPC-H **q18** (5 → 100 rows; `1=1` join present), **adhoc07** (custkey
  fan-out), **adhoc01** (first cols come back 100.0 vs 0.19 — same shape, a
  grain-determined dim fanned across the aggregate).

**NOT this bug (separate, triage apart):** q21 / q22 return **0 rows** with **no
`1=1` join** — those are correlated EXISTS/NOT-EXISTS (semijoin) producing
empty, a different root cause.

## Where it lives

Two layers:

1. **Upstream cause — group bucketing.**
   `trilogy/core/processing/v4_helper/group_graph.py` / `group_rules.py`: the
   stage-2 grouping puts `order.customer.id` in a standalone dedup group keyed
   by itself instead of letting it **ride the order-grain aggregate** (it is FD
   by the group key `order.id`). This is the "dims ride the aggregate" theme
   from the q66/q81 work — a dimension that is functionally determined by an
   aggregate's grouping key should be carried by that aggregate group (or at
   minimum keep the grain key so it can join back), not split into its own
   group.

2. **Where the cross-join surfaces.**
   `trilogy/core/processing/v4_helper/strategy_builder.py`, FINAL merge
   (~lines 625-671). `_fold_passthrough_parents(parents, final=True)` (def at
   line 207) is meant to collapse sibling contributors that descend from a
   common parent, but it does **not** fold the customer-dim group here because
   that group emits a column (`order_customer_id`) the aggregate group lacks.
   With no shared output key the `MergeNode` join degrades to `1=1`. `merge_grain`
   is pinned to the grouping grain (order_id), but the dim group already dropped
   order_id so force_group can't re-collapse it.

A correct fix most likely belongs in layer 1 (don't split the FD dimension off
the aggregate / keep the grain key on the dim group so the merge joins on
`order_id`), which also lets `_fold_passthrough_parents` collapse it.

## Reproductions (wired)

```bash
# fast, row-level v3-vs-v4 (today: q18/adhoc07 mismatch, adhoc01 mismatch):
.venv/Scripts/python.exe local_scripts/v4_evals/tpc_h_v4_compare.py 18 adhoc01 adhoc07
#   target after fix: [match] for all three, no 1=1 join in the v4 SQL

# inspect the generated SQL to confirm the 1=1 join is gone:
#   (see the inline generator used in the brief author's session, or
#    add a case to local_scripts/v4_evals/cases/ importing tpc_h lineitem)
```

Targeted pytest (must pass v4-on AND stay passing v4-off):
```bash
TRILOGY_V4_DISCOVERY=1 .venv/Scripts/python.exe -m pytest \
  tests/modeling/tpc_h/test_tpch_queries.py::test_eighteen \
  tests/modeling/tpc_h/instantiated/tpc_h/test_instantiated_tpc_h.py::test_adhoc07 \
  -p no:cacheprovider -q
```

## Guardrails

- v3 is the oracle (`CONFIG.use_v4_discovery=False`); the full suite passes on
  `main`. Compare row multisets, not SQL text.
- This area is delicate — prior fixes (q04 cartesian root-split, q39/q58 FINAL
  merge grain key, q81 dims fan-out) live here. Re-run the TPC-DS eval after any
  change: `python local_scripts/discovery_v4_compare.py --test-set --dataset memory_sf001`
  (baseline 101/102) and the full sweep `python local_scripts/v4_suite_sweep.py`.
- No silent guards: make the dimension joinable, don't paper over the 1=1 by
  deduping the cross product.
