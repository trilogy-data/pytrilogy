# v4 verbosity / plan-size handoff (the migration-gating work)

## 2026-06-28 — q2.2 FIXED + pruned (8856 → 7276, under 7500)

**`_merge_basic_into_window_parent`** (`group_graph.py`). The same-grain `round()`
BASIC group folds INTO its WINDOW producer so the `lead()`s render inline (v3's single
window+round CTE) instead of the window materializing 14 agg + 7 lead passthrough
columns for a separate round node. It's the node-MERGE generalization of
`_regraft_group_sources` (which only routes a source edge), gated to the one case the
optimizer can't express (`CollapseSingleParent` blocks BASIC-into-WINDOW):
full-coverage, same-grain, scalar-BASIC-over-WINDOW. The post-window `is not null`
defers to FINAL for free (`condition_placement._CANNOT_HOST_OWN_OUTPUT`). Runs before
`_inject_conditions`, so the BASIC carries no condition at merge time. Lock:
`tests/core/processing/test_v4_window_basic_merge.py`. TODO: DRY with
`_regraft_group_sources` once validated (currently a separate method by design).

**REVERTED — pseudonym-aware datasource inlining** (`inline_datasource.py`). An earlier
attempt expanded `root_outputs` with pseudonyms so the bare `web_sales`/`catalog_sales`
fact-scan passthroughs fold into the aggregate join (−480 chars each). It REGRESSED
`tests/discovery/test_canonical_collision_merge.py` on BOTH planners: when two columns
of one datasource carry pseudonyms that canonically collide (`d1→s1`, `d2→s2`, s1/s2
same canonical), the blanket expansion lets the inliner over-match and the s1 arm
renders `coalesce("facts"."d1", spine.s1) as "s1"` (a FULL JOIN) instead of the direct
`"facts"."d1" as "s1"`. Reverted; q2.2 passes on the window merge alone (no longer needs
it). Re-land only with a narrowed gate (don't expand a pseudonym whose canonical
collides with another root output's pseudonym). It remains a latent win for q2.1.

**q2.1 still over (8266).** Its round BASIC is at `date.id` grain (the named `*_sales`
intermediate concepts make it finer than the `date.week_seq` window), so the same-grain
merge gate correctly skips it. Needs a separate grain-inference fix (why the named
weekday-sales BASIC lands at `date.id` not `date.week_seq`), then the window merge fires
and q2.1 collapses the same way.

---

Status: OPEN, `_TPCDS_SIZE` (rows correct, SQL longer than v3 → trips
`assert len(query) < ceiling`). **Not correctness** — full v4 sweep is 0 failed. This
is the main engineering left before v4 can be flipped on by default. For the genuine
*crashes* that were mis-bucketed here, see `v4_existence_recursion_handoff.md` (q10,
q2.1 are crashes, NOT size; q02 and q76 are already passing).

## Real-fixture sizes (re-measured 2026-06-26, `local_scripts/v4_real_size.py`)

Run `.venv/Scripts/python.exe -m local_scripts.v4_real_size` for the REAL engine-fixture
lengths (DB imported -> inlining applies). Trust these over `v4_size_compare` (the proxy
has no DB, over-reports, and for q2.1 reports a different blowup path entirely).

| q | test | ceil | v3 | v4 | ratio |
| --- | --- | ---: | ---: | ---: | ---: |
| **2.1** | `test_two_one` | 7500 | 6290 | **60696** | **9.6×** |
| 73 | `test_seventy_three` | 3000 | 2701 | 5223 | 1.9× |
| ~~94~~ | `test_ninety_four` | 5000 | 3549 | **3508** | **FIXED 2026-06-27 (was 5271) — pruned** |
| 2.2 | `test_two_two` | 7500 | 6290 | 10273 | 1.6× |
| 10 | `test_ten` | 7000 | 6420 | **8308** | 1.3× (was 10208 — same shared-ROOT fix) |
| 81 | `test_eighty_one` | 8000 | 7465 | 9096 | 1.2× |
| 23 | `test_twenty_three` | 8500 | 8159 | 8515 | 1.04× |
| 30.alt | `test_thirty_alt` | 12000 | 7152 | 10084 | length PASSES; fails STRUCTURE asserts |

### *** q94: FIXED + pruned 2026-06-27 (5271 → 3508) ***

The conditioned ROOT `web_sales ⋈ ship_address ⋈ ship_date ⋈ web_site` (filtered by
date/state/company + multi-warehouse IN + not-returned NOT IN) was materialized TWICE:
once for the `sum(ext_ship_cost/net_profit)` aggregate and once for
`count(distinct order_number)`. At the group level it is a single shared `grp:root:root:∅`
with two aggregate successors, but Stage 3 `parent_for_consumer` (strategy_builder.py)
SLICES the ROOT per consumer: the count aggregate needs only `order_number` ⊂ the ROOT's
4 outputs, so it called `build_node(... slice ...)` and re-derived the whole join — the
WHERE pins all four dim joins, so the rebuild pruned nothing and just duplicated CTEs
(`cooperative`/`abundant`/`concerned`). Fix: build the slice speculatively, then adopt it
ONLY when `_leaf_datasource_ids(sliced) < _leaf_datasource_ids(node)` (strict subset = it
drops a join the slice no longer spans); otherwise share the already-built ROOT and let
column projection narrow it. Principled — re-source only when it genuinely prunes a join,
never to carry fewer columns. Bonus: q10 10208 → 8308 (identical shape). Full sweep 0
failed; rows byte-identical.

### *** q2.1: catastrophic blowup FIXED 2026-06-26 (60696 → 10231, −83%) ***

**FIXED** by `_CleanFeederCache` in `strategy_builder.py`. The 9.6×/60696-char blowup is
gone — q2.1 is now 10231 (13 CTEs, down from 75; 1 null-safe join, down from 32), ordinary
1.4× verbosity in the same league as q2.2/q10. Rows unchanged (53, byte-identical to ref).
Still over the 7500 ceiling, but the *distinct* self-ref bug class is resolved; the residual
is the shared passthrough-projection bloat (pattern 1 below), now common with q2.2/q73.

**Root cause + fix (landed):** the membership `week_seq in relevent_week_seq` sits on the
shared ROOT, which is a lineage ancestor of the `relevent_week_seq` filter group. The
group-graph drops that existence edge as a back-edge (group_graph.py ~491), so the feeder is
wired post-build by `_attach_existence_sources`. `_existence_parents_for` found only the
(cyclic) filter-group node — whose subtree reads the membership-conditioned ROOT — and
`_deep_copy_node`'d the whole subtree to break the cycle, **15015× recursive copies**,
compounding to 60696 chars. Fix: the set `Y` in `X in Y` is the UNFILTERED set, so when the
only feeder is cyclic, re-source it STANDALONE via `search_concepts(conditions=[])` once and
share the acyclic result (`_CleanFeederCache`), instead of deep-copying the conditioned
subtree. Deep-copy retained as fallback when no standalone feeder builds (preserves the
crash fix). tpc_ds failure set unchanged (12 isolation fails identical with/without — net
zero); membership family (or_membership/existence/recursion) all green; mypy/ruff clean.

#### Historical: the original diagnosis (kept for context)

q2.1 WAS a **9.6× / 60696-char blowup** from a
self-referential membership filter that re-materialized in a ~30-layer fixpoint chain.
Rows are CORRECT (53 rows, byte-identical to `query02.sql` reference — verified
2026-06-26). The query: `relevent_week_seq <- date.week_seq ? date.year in (2001,2002)`
plus `where date.week_seq in relevent_week_seq`, feeding 7 weekday conditional sums
(× web_sales + catalog_sales) and 7 `round_lag` windows.

**Group graph is CLEAN** (~8 nodes: root → {1 filter:date.id producing relevent_week_seq,
2 aggregates at week_seq, 1 window}; aggregates → basic:week_seq → {basic:date.id, window};
all → final). The explosion is **entirely Stage 3 materialization** (75 CTEs from ~8
groups). The first membership application is correct and shared (`questionable` = clean
date_dim scan of relevent_week_seq; `thoughtful`'s WHERE uses `select questionable...`).
But then the planner re-derives relevent_week_seq from the *already-filtered* fact stream
(`thoughtful` = web_sales ⋈ catalog_sales ⋈ date) and re-applies the membership, producing
a linear chain: `thoughtful → project relevent → RIGHT JOIN thoughtful WHERE week_seq in
relevent → project relevent again → re-filter → …` ~30 deep (32 `is not distinct from`
null-safe re-joins to `thoughtful`).

Root cause sits in **`strategy_builder._build_node` condition handling** — `parent_for_consumer`
(strategy_builder.py:510) builds a per-consumer sliced ROOT with `conditions=_wrap_atoms(
attrs[pgid].condition_atoms)`, and the membership condition's RHS concept (relevent_week_seq)
is derived from the SAME root grain being conditioned, so condition injection
(`condition_injection.inject_condition_at_node` + the fixed-point combine) never detects it
has reached steady state and keeps re-sourcing+re-filtering. This is the deferred
"dual-existence / self-ref filter recursion" noted in `project_v4_q02_invalid_alias_union_dim`.

**Fix is high-value but high-risk** (touches the membership-filter family: q02, q08
or_membership, `test_or_membership`). The correct shape: build the relevent_week_seq filter
ONCE (the filter group already exists + is built as `questionable`), reference it as a
single shared IN-subquery feeder, and recognize the membership is idempotent so applying it
once is sufficient — never re-derive relevent_week_seq from the post-filter stream. Needs a
full v4 sweep diffed for membership-family regressions before landing.

#### Confirmed mechanism + a viable fix direction (investigated 2026-06-26)

CONFIRMED by instrumentation (NOT speculation):
- The membership atom `week_seq in relevent_week_seq` is placed (by
  `condition_placement.plan_condition_placements`, reason `UPSTREAM_MOST`) on the shared
  `grp:root:root:∅`. ROOT is a lineage ANCESTOR of the filter group
  `grp:[@condition]filter:d1:date.id` that PRODUCES `relevent_week_seq`. **Filtering an
  ancestor of the set's producer by membership-in-that-set is the self-reference.**
- At materialization, the filter group's built node therefore reads the *conditioned* ROOT,
  so its subtree contains the conditioned MergeNode. When `_attach_existence_sources`
  wires the membership's existence feeder, `_existence_parents_for` finds only that one
  (cyclic) producer node, hits the `_deep_copy_node` "verbose but acyclic" branch
  (strategy_builder.py ~246), and duplicates the whole subtree — **`_deep_copy_node` fired
  15015× for q2.1** (recursive subtree copies), compounding to 60696 chars. The deep-copy
  is LOAD-BEARING: it replaced the earlier `RecursionError` crash (see
  `project_v4_existence_recursion_fixed`), so you cannot just delete it.

VIABLE DIRECTION (partially validated — do NOT ship as-is): push the membership OFF ROOT
onto the row-stream consumers (the aggregates/window) that are NOT ancestors of the
producer, leaving ROOT (and thus the set producer) reading UNFILTERED rows. A prototype in
`plan_condition_placements` — exclude from `restricted` any gid in
`{producer} ∪ nx.ancestors(lineage_ancestors_graph, producer)` when a non-ancestor
placement remains — **improved q10 10208→8706 with no break**, but **raised `IndexError`
on q2.1/q2.2** (`discovery_utility.calculate_effective_parent_grain`: a GroupNode parent
with no datasources). Root of the break: when the membership lands on a consumer group, the
existence feeder is not wired onto that consumer (the current attach only handles
condition-host ROOTs), so the consumer materializes an unresolvable node. **To land this:
finish the consumer-side existence-feeder wiring** (mirror `_attach_existence_sources` /
`_existence_for_group` for a membership injected at a non-ROOT consumer), then the placement
push-down breaks the cycle cleanly for the whole family. Reverted in the tree; reproduce
from this note. Full sweep + membership-family diff required.

## Scope: queries genuinely over ceiling (verbosity only)

Re-measured 2026-06-25 in isolation (`TRILOGY_V4_DISCOVERY=1`, real `engine` fixture):

| q | test | over by | dominant pattern |
| --- | --- | --- | --- |
| 2.1 | `test_two_one` | **9.6×** | **self-ref membership filter re-materialization (see above)** |
| 2.2 | `test_two_two` | size | passthrough bloat + multi-fact many-sibling |
| 30.alt | `test_thirty_alt` | size | dimension re-join (pattern 2) |
| 73 | `test_seventy_three` | size | filter-hoist-to-WHERE + dim re-join (NOT just passthrough — see note) |
| 81 | `test_eighty_one` | size | dimension re-join (pattern 2) |

FIXED 2026-06-25 (`_spine_regraft_parent` in `group_graph.py`): **q47 `test_forty_seven`
and q57 `test_fifty_seven`** — NOT passthrough bloat as previously labeled. The bloat
was a redundant intermediate merge: the scalar BASIC `sum_minus_avg = sum_sales -
avg_monthly_sales` built its own CTE merging the base aggregate with the avg, then FINAL
re-joined the window node (`questionable`) — two joins where v3 needs one. v3 makes the
window the spine (it already exposes `sum_sales`) and joins `avg` straight onto it,
computing the difference inline. The fix adds a group-graph lineage edge from the
same-grain WINDOW sibling to the BASIC so the window's outputs ride through and
`_fold_descendant_contributors` collapses them into one FINAL contributor. Broader v4
modeling sweep failure set unchanged; q72 emits identical SQL with/without the change
(unaffected). Lock: `tests/core/processing/test_v4_window_avg_difference.py`. (q47/q57
still listed in `v4_known_failing.py` — prune after a full sweep; they now xpass.)

(Already cleared by the 2026-06-25 size fixes and now PASSING in isolation — prune from
`v4_known_failing.py` after a full sweep: q02 `test_two`, q76 `test_seventy_six`.)

Measure the v3-vs-v4 gap with `python -m local_scripts.v4_size_compare` (generation
proxy — runs higher than real because it skips datasource inlining; trust the real
test verdict for pass/fail, use the proxy only for relative deltas). Diagnose any one
query with:

```bash
.venv/Scripts/python.exe local_scripts/discovery_v4.py --query 73 \
  --diagnostics --diagnostics-dir local_scripts/v4_diagnostics --no-sql
```

Read `<stem>_strategy.md` (repeated datasources / unexpected fact scans) and
`<stem>_groups.md` (`__final__` merge grain + contributor contracts) before eyeballing
SQL.

## Three patterns, in recommended fix order

### 1. Passthrough-projection bloat — biggest, lowest-risk lever

v4 emits pure single-source projection CTEs (no join/group/agg/window/where) that just
re-project columns and should fold into their consumer. `_fold_passthrough_parents`
(`strategy_builder.py`) is supposed to absorb row-preserving projections but doesn't
fire for these shapes. Find why and widen it (it is pinned as a *physical redundancy*
optimization — it may absorb row-preserving projections but must not dissolve a
row-shape barrier; see `v4_audit.md` "Separation audit notes").

Touches q73, q2.2, and compounds pattern 2 (the `questionable ← cooperative`
passthrough in q81). (q47/q57 were mislabeled here — they were a redundant-merge issue,
now FIXED via `_spine_regraft_parent`; see the table note above.)

Likely investigation: a passthrough CTE survives when its consumer needs a different
*hidden* concept set, or when the parent is referenced by more than one consumer
(fold must be safe for the single-consumer case first). Confirm with the strategy
sidecar which CTE is the bare projection and who reads it.

### 2. Dimension-projection re-join — q81 & q30.alt (identical fingerprint)

The wide customer/address dimension projection gets re-sourced through the fact (4
joins + a dedup GROUP) instead of from `customer ⋈ customer_address` at customer
grain (v3: 1 join). **Full write-up with refined root cause and a regression trap
(q65) already exists: `local_scripts/v4_dimension_projection_rejoin_handoff.md`.**
Read it before touching this — a naive fix got q81 under ceiling but regressed q65;
the robust fix must be per-entity / FD-cluster aware. Higher risk than pattern 1.

### 3. Aggregate over-split — q73 (root cause refined 2026-06-26)

q73 needs MORE than passthrough folding — diffing v3 (2701, **1 CTE**) vs v4 (5223, 4 CTEs)
shows TWO independent gaps:

1. **Filter-hoist-to-WHERE.** v3 hoists the `count(item ? <pred>)` filter to a plain
   `WHERE <pred>` on the join and `count(SS_ITEM_SK)` over survivors. v4 renders it as
   `_virt_filter_id = CASE WHEN <pred> THEN item ELSE NULL END` then `count(_virt_filter_id)`
   (equivalent — count skips nulls), but to do so it carries ALL six filter input columns
   (`date_day_of_month, date_year, buy_potential, dependent_count, vehicle_count,
   store_county`) through the wide `cooperative` CTE and a separate `questionable` projection
   CTE. When a filtered aggregate is the ONLY measure of its group, hoisting the predicate to
   WHERE (instead of CASE-inside-count) drops all those carried columns + the projection CTE.
2. **Dimension re-join (same as pattern 2).** v3 joins the `customer` table at the END keyed
   by the aggregated `customer_id`, so the 4 customer name/flag dims come straight off
   `customer`. v4 carries them through `cooperative` at fact grain and adds `abundant`
   (a GROUP BY customer_id + 4 dims) to dedup — the wide-dim-via-fact-dedup fingerprint.

So q73's `questionable` passthrough fold is real but minor; the wins are the filter-hoist
(gap 1) and the dimension re-join (gap 2, the q81/q30.alt family). Try gap 1 first — it is
local (a filtered-sole-aggregate WHERE-hoist) and lower-risk than the dim re-join.

## Acceptance

- Each query's `test_*` passes under v4 (`len(query) < ceiling`) with rows unchanged.
- No regression in the 2026-06-25 size wins (q12/q20/q50/q62/q23/q94) or the
  join-stream aggregate-reuse locks (`test_v4_virt_filter_extra_cte.py`,
  `test_v4_dimension_projection_group.py`).
- Full v4 sweep stays at 0 failed. Re-run a full sweep before claiming any query
  cleared — the classifier only re-checks already-listed tests.
