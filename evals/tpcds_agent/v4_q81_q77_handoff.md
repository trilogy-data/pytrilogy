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

## q81 — STATUS: FIXED (100/100, added to test_set.txt)

**Done (grain fan-out):** The FINAL `MergeNode` was built with no `grain`, so it
defaulted to `raw_pregrain` (union of contributor grains) and adopted the dims
contributor's `catalog_returns` row grain — never force-grouping, so the
returns-grain dims fanned the aggregate out (3 dupes). The address-dims
contributor is `filter`-derived at `(item.id, order_number)`; `_wrap_for_grain`
only projects `root` contributors, and even if widened it buckets by each
concept's *declared* grain (`{address.id}`) which can't dedup (address is
FK-determined by customer, not vice-versa). Fix: `_assemble_final_node` now pins
the merge `grain` to the union of its **grouping** (aggregate/window)
contributors' grain (`(customer.id, return_state)` here); the non-grouping dims
contributor no longer widens it, and the merge's existing `force_group` collapses
the fan-out. `grain=None` when there is no grouping contributor, so plain row
merges are unchanged.
  - `trilogy/core/processing/v4_helper/strategy_builder.py::_assemble_final_node`.
  - Validated: v4 `--test-set` 98/98 (q81 added to `test_set.txt`); v3
    `test_queries.py` + `core/processing` 326 passed; ruff/mypy/black clean.

### Earlier this session — join-type half (committed `4a982d45`)

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

## q77 — DIAGNOSED, not fixed (inline-aggregate arm column grain)

**Symptom:** v4 44 rows / ref 44 rows, ~43 differ. The **catalog channel** rows are
wrong: `returns` come out exactly **4×** the correct scalar (`cr_total_returns`),
`sales`/`profit` decoupled from their `id`. 4 = number of `cs.call_center` GROUP BY
groups in the period (3 distinct + 1 NULL group).

**Root cause:** The catalog arm of the `l0_union` multiselect is **inlined**
(`select cs.call_center.id as u_id_c, sum(cs.ext_sales_price)*cr_totals.cr_n_groups
as u_sales_c, ...`), mixing a per-call-center catalog_sales aggregate with a scalar
from a *different* rowset (`cr_totals`). The arm's **grain is correctly
`cs.call_center.id`**, but its output columns (`u_id_c`, `u_sales_c`, `u_profit_c`)
get grain **`(cs.item.id, cs.order_number)`** — the catalog_sales **row grain** —
because grain derivation **descends through**: (a) the bare (no-`by`) aggregate to
its input column `cs.ext_sales_price`, and (b) the FK key `cs.call_center.id` to its
binding `keys = {cs.item.id, cs.order_number}`. With that lying grain the v4 planner
can't see the columns share `cs.call_center.id`, so it builds `u_id_c` (CTE
`abhorrent`) separately from `u_sales_c`/`u_profit_c` (CTE `young`) and recombines
them with `FULL JOIN ... ON 1=1` (cross join) → N² rows → 4× inflation. The store
and web arms are correct because they route through explicit rowset wrappers
(`ss_grouped`, `ws_grouped`) whose rowset items already carry the grouping grain.

**Why it's hard (the grain is re-derived in ≥3 phases, each undoes a single-site fix):**
1. Parse `function_to_concept` (`parsing/common.py:~827`): row-op key traversal
   descends an FK KEY to `x.keys`.
2. Parse `Concept.set_select_grain` → `get_select_grain_and_keys`
   (`models/author.py:~1307`): BASIC else-branch descends `concept_arguments` into
   the aggregate input; no-lineage KEY returns `self.keys`.
3. Build `__build_concept` (`models/build.py:2543`): calls
   `get_select_grain_and_keys(self.grain)` again with the **factory** grain (the
   physical source grain, `(item, order)`), re-deriving the same wrong grain.

**Attempts (all reverted — each fixed catalog but regressed store/web, or got undone
at build):**
- no-lineage KEY → return `{self.address}`: fixed `u_id_c`→`cs.call_center.id` but
  flipped `u_id_s`/`u_id_w` to `Abstract` (multi-pass reconciliation relied on old
  behavior).
- bare-aggregate BASIC → select grain: made `u_sales_c` `Abstract` (a later pass
  passed `Abstract`), didn't co-locate.
- **clamp** (most promising): in the BASIC else-branch, when the descended grain is
  **disjoint** from the passed select grain, clamp to the select grain. This fixed
  the **parse-level** grain (`u_id_c`/`u_sales_c`→`cs.call_center.id`) and left
  store/web untouched — but **build** (#3) re-derives with the factory grain
  `(item, order)` (not disjoint from itself) so the clamp doesn't fire there, and
  the planner had already chosen `(item, order)` for the factory.

**Recommended fix direction:** make the arm's logical grain (`cs.call_center.id`)
authoritative for the inline-aggregate columns *consistently across parse + build*,
so the v4 planner builds the catalog arm at `cs.call_center.id` and co-locates
`u_id_c`/`u_sales_c`/`u_profit_c` (key-join, not `1=1`). Likely needs the clamp at
BOTH `get_select_grain_and_keys` sites AND ensuring the build factory grain for a
multiselect arm is the arm's select grain, not the source row grain. Validate with
v4 `--test-set` + v3 `test_queries.py` (multiselect q05/q14/q33/q49/q64) after each
change — high blast radius (parse + build grain touches every query).

### Standalone-select probe (CONFIRMED it's a select-level bug, found the fix)
Extracting just the catalog arm as a **plain standalone select** (with its
`cr_grouped`/`cr_totals` prereq rowsets) reproduces the bug exactly: build grains
`(item, order)`, four `FULL JOIN ... ON 1=1`, 4× inflation. So it is NOT
multiselect-native. **The fix that makes the standalone arm match the reference
EXACTLY** (4 rows: None/1/2/5, `returns=2008328.89` scalar, `sales`/`profit` per-cc
correct): in `get_select_grain_and_keys` BASIC else-branch, clamp the descended
grain to the select grain when it is **disjoint** from it, **guarded** by
`self.address not in grain.components` (so a grouping key that *is* the select grain,
e.g. `coalesce(fk,-1) as cr_cc_key`, isn't clamped to itself — that de-aggregates
its downstream `count()`).

### CONCLUSIVE: it's a v4-planner bug, NOT a grain bug (author fix abandoned)
Decisive test (local harness `local_scripts/_v3_v4_compare.py` — generates v3 via
`Dialects.DUCK_DB.default_executor().generate_sql()` and v4 via discovery, runs both
on duckdb). On the **catalog arm as a standalone select, with NO author fix**:
- **v3 → 4 rows, CORRECT.**  **v4 → 16 rows, cross-join fan-out, WRONG.**

Same parse/build (same "buggy" `(item, order)` grain) feeds both, yet v3 is right.
So the grain inaccuracy is NOT the bug — **the v4 planner is brittle to it and v3
isn't.** The earlier `_contains_abstract_aggregate` author fix only *masked* it (by
coincidentally re-bucketing) — **reverted**; tree is at baseline.

**Root cause (v4 group graph).** v3's `yummy` CTE derives `u_id_c`, `u_sales_c`,
`u_profit_c` together off `abundant` (the cs aggregate, whose grouping-key column
`cs_call_center_id` IS exposed), cross-joining the `cr_totals` scalar **once** (1
row, no fan-out). v4 sources `u_id_c` from the **raw root** `cs.call_center.id`, so
in `_partition_by_signature_and_grain` (`v4_helper/group_rules.py:336`) the
stop-signatures differ — `sig(u_id_c)={root}` vs `sig(u_sales_c)={aggregate,rowset}`
— and the two co-grain BASICs split into separate buckets, then cross-join (their
shared `(item,order)` grain is a lie for the aggregate, so there's no real join
key). **v4 fails to source an aggregate's grouping key from the aggregate group**,
so column-basics referencing it (`u_id_c`) detach from the aggregate's other derived
columns (`u_sales_c`). Fix direction: make the aggregate's grouping key sourceable
from the aggregate group (assign its `primary_group` to the aggregate, or fold a
key-only basic into the aggregate bucket) so dependent basics co-source — then no
cross-join and no grain patch needed.

**Store arm (separate, also v4-side) — to re-check under this lens:**
- `u_id_s = ss_grouped.ss_store_id` alias → its bucket/grain; and
- `u_returns_s` not exposing `ss_store_id` for the rowset `merge` join.
Both previously chased as grain bugs; given the catalog finding, first confirm with
the v3-vs-v4 harness whether v3 gets the store arm right (likely yes) → fix in v4.

**Multiselect isolation (separate cleanup).** `_resolve_multiselect`
(`concept_strategies_v4.py:152`) reuses the outer env+graph per arm;
`resolve_rowset` (~265) materializes a FRESH per-branch env. Mirroring it is the
right structure but regresses q49 — orthogonal to the above; do after arms are
individually correct.

## Validation
- `python local_scripts/discovery_v4_compare.py --test-set` → expect 97/97 (and
  98/98 once q81 is added back to `test_set.txt`, 99/99 with q77).
- `ruff check trilogy && mypy trilogy && black trilogy tests`.
- `python local_scripts/v4_fallback_audit.py` → `Totals: {}` (fallback stays gone).
