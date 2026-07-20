# q64 — `union join` between two facts silently drops a shared-concept join key (is_returned collapses to ticket-only)

Run: `evals/tpcds_agent/results/20260720-140600` — q64 burned **1,701,124 tokens**
(baseline 910,850, +790k), FAIL: `result set differs from reference` (ref **2 rows**,
candidate **247 rows**). 35 iterations, silent wrong-result (no error signature).

## Symptom
The agent's final `workspace/query64.preql` returns **247 rows**; the reference returns **2**.
The `cnt_1999` / `cnt_2000` columns are `1` on every row — a within-group Cartesian over an
inflated store-sales cohort. The agent noticed the "every 1999 pairs with every 2000" shape
(conversation msg 66) but concluded it was correct and submitted.

## Root cause (SILENT wrong-result, framework)
The agent expressed "store sale that also has a matching store return" (TPC-DS `is_returned`)
as, in `ss_base` (step 4):

```
union join ss.ticket_number = sr.ticket_number and ss.item.item_sk = sr.item.item_sk
... where ... and sr.ticket_number is not null
```

Both `raw/store_sales.preql` and `raw/store_returns.preql` have grain
`(item.item_sk, ticket_number)` and bind `item.item_sk` to the **same shared `item` model**
(one imported concept). So the second equality `ss.item.item_sk = sr.item.item_sk` is
`item.item_sk = item.item_sk` — a self-equality. `domain_graph.join_key_groups()`
(`trilogy/core/domain_graph.py:341`) folds it into a trivial `{item.item_sk}` group, so it is
**never carried as a join constraint**. Only the `ticket_number` group discriminates.

The **union / coalescing** lowering then builds the store_returns presence probe on
`ticket_number` alone. Generated SQL:

```
young as ( SELECT sr_ticket_number, coalesce(sr_ticket_number) as _virt_presence...
           FROM store_returns GROUP BY 1,2 )          -- store_returns reduced to DISTINCT tickets
... INNER JOIN young on premium.sr_ticket_number = young.sr_ticket_number
```

`store_returns` is collapsed to distinct `ticket_number`, dropping the item component of its
`(ticket, item)` grain. `is_returned` therefore degrades from "**this (ticket,item) was
returned**" to "**this ticket had ANY return**", admitting store sales whose ticket had a
return on a *different* item. That inflates the cohort ~7x → the self-pair explodes to 247.

The **subset-join** path builds the correct two-column join; the defect is specific to the
`union join` (coalescing / presence-probe) lowering:
- `trilogy/core/domain_graph.py:341` `join_key_groups()` — same-address equality → trivial group.
- `trilogy/core/processing/node_generators/presence_probe.py` `gen_presence_probe_node`
  (key = single group key, ~L278-281) / `gen_coalescing_axis_node` — probe keyed on the sole
  surviving group (`ticket_number`); the shared-concept co-key is not enforced row-level.

Adjacent column arrangements of the same `union join` instead throw a **loud**
`BinderException: Ambiguous reference to table "sr_store_returns" (duplicate alias)` — the same
path emits a duplicate self-referential `store_returns` join (`on sr.ticket = sr.ticket`). So
the union path is fragile: silent-wrong OR duplicate-alias error depending on selected columns.

## Minimal repro / trigger matrix (against the run's `workspace/`)
`is_returned` filter, holding all other filters constant, counting store-sale lines by year
(reference = 1998:24 1999:37 2000:2 2001:22 2002:35):

| is_returned expression | rows | 1999 | 2000 | verdict |
|---|---|---|---|---|
| `union join ss.ticket=sr.ticket and ss.item=sr.item` + `sr.ticket is not null` | 907 | 216 | 42 | WRONG (ticket-only) / or BinderException |
| `subset join ss.ticket=sr.ticket and ss.item=sr.item` (full ctx) | 130 | 38 | 2 | CORRECT |
| `(ss.ticket_number, ss.item.item_sk) in (select sr.ticket_number, sr.item.item_sk)` | 130 | 38 | 2 | CORRECT |
| `ss.ticket_number in (select sr.ticket_number)` (ticket-only membership) | 907 | 216 | 42 | reproduces the union-join collapse exactly |

End-to-end on the agent's actual file: changing **only** the `ss_base` keyword
`union join` → `subset join` takes the final result from **247 → 2 rows** (exact reference match).
Nothing else in the agent's query is load-bearing to the failure — the independent per-item
catalog subquery (17849 qualifying items vs reference's 17157) is non-discriminating at sf=1
and does not affect the result.

## Classification
Primary: **FRAMEWORK silent wrong-result** — `union join` between two facts on a
`(shared_concept, distinct_key)` pair drops the shared-concept condition and coarsens the
semi-join to the distinct key; same path also has a loud duplicate-alias BinderException in
neighboring column layouts. Same family as the prior q35/q84 coalescing-presence-probe fixes;
the shared/conformed join-key case is an uncovered seam.
Secondary: **guidance gap** — `union join` (full-outer intent) is the wrong tool for
`is_returned`; `subset join` (or tuple membership) is correct. The engine gives no diagnostic
steering the author off `union join` here.

Not fixed (read-only investigation).
