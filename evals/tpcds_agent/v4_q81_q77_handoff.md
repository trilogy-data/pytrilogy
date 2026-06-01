# Handoff: last two v4 mismatches — q81 and q77

## Context

The v3 fallback in v4 discovery is fully removed (rowset/multiselect/recursive
native; `_fallback_to_v3` + `factory_dispatch.py` deleted — see
`project_v4_fallback_removed` memory). The curated set is **97/97 match**
(`local_scripts/discovery_v4_compare.py --test-set`). The only two TPC-DS
queries still mismatching are **q81** and **q77** (deliberately left OUT of
`local_scripts/v4_compare/test_set.txt`).

Inspect either: `python local_scripts/discovery_v4_compare.py --query 81`
then read `local_scripts/v4_compare/query81.md` (v4 SQL vs reference SQL + diff).

## q81 — STATUS: join-type half FIXED; grain fan-out remains

**Done (join type):** The FINAL merge now resolves all-INNER (was FULL). Root
cause was NOT `billing_customer.id` — it was `cr.return_address.state` (intrinsic
`?` on `address.state`) being a connecting key marked nullable on BOTH FINAL
contributors, so `get_join_type` → FULL. The real driver is that the query's
`customer_state > scaled_state` HAVING (INNER semantics) was pushed *into* one
branch (`concerned`) instead of staying on the FINAL merge, so the merge's own
`downgrade_join_for_condition` saw no condition. Fix: the merge now also gathers
conditions applied *within* parent branches and demotes FULL joins whose
proven-non-null columns (restricted to the merge's own outputs) sit on one side.
Gated to inferred-join merges (`node_joins is None`) so MULTISELECT aligns —
whose explicit FULL is intentional, e.g. `test_adhoc_two` per-arm
`HAVING web_order_count>0` — are untouched.
  - `trilogy/core/processing/grain_utility.py`: split
    `downgrade_join_for_condition` into a proofs-based `downgrade_join_for_proofs`.
  - `trilogy/core/processing/nodes/merge_node.py`: `_collect_applied_conditions`
    + branch-proof downgrade in `_resolve` (gated on `node_joins is None`).
  - Validated: v4 `--test-set` 97/97; v3 `test_queries.py` 106 passed;
    `tests/nodes/utility` + `test_null_safe_join` pass; ruff/mypy/black clean.

**Remaining (grain fan-out → 97 distinct / 100, want 100/100):** The FINAL
MergeNode adopts `catalog_returns` grain (`item.id/order_number/date.year` leak
in) instead of the output grain `(customer, return_state)`, and its `source_map`
projects the dims + `customer_state` from the *ungrouped* returns-grain
contributor rather than the grouped one (`sparkling` exists at the right grain
but is used only for a join key). A `(customer, return_state)` pair has up to 3
returns rows, so the dims fan out and INNER-join dups through. Fix axis: the
merge should source dims from / collapse to `(customer, return_state)` — i.e. the
`customer_state`-carrying contributor must itself group to that grain rather than
stay at returns grain. (Reference sources dims from the `customer` table at
customer grain and lets `return_state` ride the aggregate — see `wakeful`.)

### Original diagnosis (kept for reference)
## q81 — FULL join should be INNER (shared join-resolution / nullability)

**Symptom:** v4 returns 100 rows / 58 distinct vs reference 100 distinct. The
extra v4 rows have `customer_state = NULL` (last column) and are duplicated.

**Query shape:** `customer_state = sum(return_amt ? year=2000 ...) by
(return_address.state, billing_customer.id)`; `scaled_state = 1.2*avg(
customer_state) by return_address.state`; WHERE `customer_state > scaled_state
and billing_customer.address.state='GA'`; SELECT customer dims (incl
`billing_customer.address.*`) + `customer_state`. A customer can have returns in
multiple states, so the result is at (customer, return_state) grain.

**Reference shape:** INNER JOINs the customer-dims CTE (`wakeful`, customer
grain) with the `customer_state` aggregate (`questionable`, (customer, state)
grain) on `customer.id`, then filters `customer_state > scaled`.

**v4 root cause:** v4 builds the same two FINAL contributors —
`root/root` (GA-filtered customer dims, incl address dims) and
`aggregate/d0` (the filtered `customer_state`) — but the FINAL `MergeNode`
joins them **FULL** instead of INNER, re-introducing non-passing / NULL-customer
rows. The FULL comes from `join_resolution.get_join_type`
(`trilogy/core/processing/join_resolution.py:124`): it returns `FULL` when BOTH
sides are non-"complete" on the connecting key. Here `billing_customer.id`
(`CR_RETURNING_CUSTOMER_SK` FK) is nullable on the dims side AND is a GROUP BY
key on the aggregate side (a NULL group is possible), so both read as
nullable → FULL. v3 gets INNER (the reference confirms).

Note: `aggregate/d0` exposes the DIRECT customer dims (text_id, name — grain
`{customer.id}`) but NOT the address dims (`billing_customer.address.*`, grain
`{address.id}`) — the grouping `can_preserve`/grain check rejects
`{address.id} ⊆ {customer.id}` even though address is FK-determined by the
customer. That's why `root/root` is pulled in as a 2nd contributor at all. Two
fix axes therefore exist:
1. **Join type** (likely the real fix): make the FINAL merge of a filtered
   aggregate + its dims INNER, not FULL — i.e. fix the nullability that makes
   `get_join_type` pick FULL. Reproduce the reference: a GROUP BY key / FK on
   the connecting key shouldn't both read as nullable here.
2. **Dim passthrough** (alternative/secondary): let the filtered aggregate
   carry transitive (FK-determined) dims like `billing_customer.address.*` so it
   is the sole FINAL contributor (no merge). This is the `can_preserve` /
   grain-FD check following the customer→address FK.

**Blast radius:** `get_join_type` is used by EVERY merge — change it carefully
and re-run the full 97-query `--test-set` after each change (watch the v4 vs v3
join types on queries that legitimately need FULL, e.g. multiselect aligns
q05/q46/q64). Useful repro/instrument: dump the FINAL contributors + chosen join
types for q81 (the two contributors are `root/root` with the GA filter and
`aggregate/d0` with `customer_state > scaled_state`).

## q77 — value mismatch (same row count)

Not yet diagnosed. v4 44 rows / ref 44 rows, but 43 differ — same cardinality,
wrong values, so likely a join-key or grain issue rather than fan-out. Start by
diffing the v4 vs reference SQL in `query77.md`.

## Validation
- `python local_scripts/discovery_v4_compare.py --test-set` → expect 97/97 (and
  98/98 once q81 is added back to `test_set.txt`, 99/99 with q77).
- `ruff check trilogy && mypy trilogy && black trilogy tests`.
- `python local_scripts/v4_fallback_audit.py` → `Totals: {}` (fallback stays gone).
