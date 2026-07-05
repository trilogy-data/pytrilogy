# q38 recheck (run 20260705-142435) — documented all_sales inflation PARTIALLY improved; current sink is a DISTINCT cause on the new 3-import model

Re-verification of the q38 token sink documented in `bug_q38_churn_013151.md`. This run:
model `deepseek/deepseek-chat`, q38 **failed** (was pass in a prior run), **1,362,931 tokens**,
final query returned **11438** vs reference **107** ("result set differs", 1 cand row).

## TL;DR verdict

- **Documented all_sales inflation bug: STILL PRESENT but more recoverable.** On the
  `all_sales` model the natural `count_distinct(channel)=3` form still silently returns **155**
  (should be 107) via the *exact* documented mechanism (nullable-FK → LEFT_OUTER preserves
  null-customer fact rows into a phantom (NULL,NULL,date) group). The mechanism line moved but
  is intact: `trilogy/core/processing/join_resolution.py:151-152` (`if left_is_nullable: return
  JoinType.LEFT_OUTER`). **Improvement:** the recovery filter now works at WHERE level —
  `... and s.billing_customer.id is not null` → **107** (the old doc reported this path only
  worked as an `id is not null` guard *inside each CASE*; a plain WHERE `is not null` flipped to
  0). The phase-2 always-preserving join redesign made the null-safe `id`-guard recoverable.

- **The 1.36M-token failure is a DISTINCT cause, not the documented bug.** The task/model in
  this run is the **3-separate-import** shape (`raw.store_sales` / `raw.catalog_sales` /
  `raw.web_sales`), not `all_sales`. On that shape there is **no working idiom** to reproduce
  the reference, and the agent used `union join` (which is *union of domains*, not intersect) →
  **11438**.

## The trap: reference 107 is ENTIRELY a NULL-name artifact

The reference (`query38.sql`, verified = 107) is a 3-way `INTERSECT` on
`(c_last_name, c_first_name, d_date)`. INTERSECT matches NULL=NULL, so **every one of the 107
"matches" is a (last_name NULL and/or first_name NULL, date) phantom group present in all three
channels**. Proof on today's engine:

| query | result |
|---|---|
| ref 3-col INTERSECT | **107** |
| ref 3-col INTERSECT + `c_last_name/c_first_name IS NOT NULL` | **0** |

There are **zero real-named** customers in all three channels on the same date. The "correct"
answer is manufactured by the same NULL-grouping effect the prior doc called a bug. Any idiom
that drops NULL names cannot reach 107 (its ceiling is ~1).

## Fresh trigger matrix (today's engine, run-20260705 workspace db)

Reference / working:

| form | result | note |
|---|---|---|
| `query38.sql` (reference) | **107** | scoring target |
| canonical `query38.preql` (all_sales, `id is not null` inside each CASE) | **107** | works |
| all_sales `count_distinct(channel)=3`, **no guard** | **155** | documented inflation, unchanged |
| all_sales `count_distinct(channel)=3` + `billing_customer.id is not null` (WHERE) | **107** | recovery now works at WHERE |
| all_sales `count_distinct(channel)=3` + `last_name is not null` (WHERE) | **0** | drops the null-name groups |

3-import model (what this run's agent used) — nothing reaches 107:

| form | result | GT | note |
|---|---|---|---|
| **agent final** (`union join`, one-side `is not null`) | **11438** | 107 | union of domains, not intersect |
| 2-way store∩catalog `union join`, one-side not null | 30896 | 318 | ~= catalog domain |
| 2-way `union join`, BOTH sides `is not null` | **89** | 318 (90 w/ concat-null semantics) | correct for non-null keys, drops NULL groups |
| 2-way `subset join`, BOTH sides `is not null` | 30336 | 318 | overcount |
| 2-way membership `store.k in catalog.k` (concat key) | 89 | 90 | correct minus 1 NULL key |
| 3-way `union join`, BOTH sides `is not null` | **0** | 107 | drops null names → 0 real matches |
| 3-way `union join` + non-null presence-flag (keeps nulls) | **0** | 107 | multi-col null-key intersect not reconstructed |
| 3-way membership (concat keys) | **0/[]** | 107 | concat drops NULL names |

Per-channel rowsets are correct (store 47682 / catalog 31723 / web 11788 distinct
`(last,first,date)` tuples). Concat-key ground truths (which match Trilogy's `||` NULL-dropping
semantics): 2-way = 90, **3-way = 1**. So the reference's 107 vs the honest non-null answer of
~1/0 is the entire gap.

## Root cause

Two independent things:

1. **Documented mechanism intact** — `join_resolution.py:151-152`: a nullable-FK fact→dimension
   key still renders `LEFT_OUTER`, preserving null-customer fact rows that collapse into one
   phantom (NULL,NULL,date) group during distinct-tuple aggregation → 155 on the natural
   all_sales form. Silent. Only mitigated (not removed) by the newly-recoverable
   `billing_customer.id is not null` WHERE filter.

2. **Current sink — no expressible NULL-matching cross-rowset INTERSECT** (framework limitation
   + guidance/agent error). By design (`docs/subset_union_join_design.md`) `union join` /
   `subset join` render **row-preserving FULL**; intersection is only an explicit
   `where <side> is not null`. That idiom **structurally cannot reproduce 107**, because the
   reference answer lives entirely in NULL-name groups and `is not null` on the name join-key
   deletes exactly those (→ 0). Even a non-null presence-flag workaround yields 0 — the chained
   multi-column `union join` across two independent rowsets does not reconstruct the
   NULL=NULL INTERSECT. `union join` is also seductively mis-named for "appears in all three"
   (it is union, not intersect) → the agent's 11438. The **only** working path is
   `all_sales` + `count_distinct(channel)=3` + `id is not null`, which the agent explored and
   **wrongly abandoned** (msg 14: concluded all_sales "has purchasing_customer but not separate
   customer access patterns per channel" — false; `all_sales.billing_customer` maps to
   `ss_customer_sk`/`cs_bill_customer_sk`/`ws_bill_customer_sk` and gives 107). Agent guidance
   correctly said "prefer all_sales for cross-channel."

## Classification

- **Documented bug (all_sales 155 inflation): still-present / partially-improved.** Silent
  default unchanged (`join_resolution.py:151-152`); recovery filter is now discoverable at WHERE
  level thanks to the phase-2 join-preservation flip.
- **Current 1.36M sink: NEW/DISTINCT — guidance-model-defect + agent-error, over a real
  framework limitation.** On the 3-import model there is no idiom that reproduces the
  NULL-artifact reference 107; `union join` is union-not-intersect and its `is not null`
  intersect idiom deletes the very NULL groups the answer requires. The pass→fail flip is
  partly weak-model (deepseek) variance, but the >500k tokens is a real obstacle: the agent had
  no reachable-and-obvious correct construct once it left `all_sales`.

## Suggested directions (NOT applied — read-only)

- Teach the agent guidance that cross-channel *set-membership/intersect* questions specifically
  should use `all_sales` + `count_distinct(channel)=N` (the 3-import path has no working
  intersect for NULL-inclusive keys).
- Consider a first-class INTERSECT/`count_distinct(channel)` idiom or a diagnostic when a
  `union join` + one-sided `is not null` is used where an intersect is likely intended.
- The underlying 155 phantom-null-group trap (`join_resolution.py:151-152`) remains the same
  silent hazard flagged in the prior doc.
