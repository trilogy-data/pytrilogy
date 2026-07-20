# v4 compatibility audit (last refreshed 2026-07-20, session 15)

## Current state (after 2026-07-20 session 15)

**Full v4 sweep (session 15): 28 real failed / 5789 passed**
(`local_scripts/v4_sweep_0720_s15b.log`; raw count 29 — minus the
`tests/cli/test_display.py` rich_enabled detached-run console noise; the faa
LLM flake did not fire this run). Honest delta vs s14: **cleared
window_expression_join ×2 + having_bare_max ×1 (−3), ZERO new failures** —
every remaining entry diffs onto the s14 list. TPC-DS battery = the
pre-existing 6 exactly (q72 intact), run standalone AND inside the sweep;
classifier exit 0, no escalations; ruff/mypy(309)/black clean. 82 errors are
clickhouse-server env (not real). Both changed files are v4-only
(`concept_strategies_v4.py`, `v4_helper/source_planning.py`) and the shared
`discovery_utility.py` experiment was reverted to byte-baseline — v3
untouched by construction.

**The 28 remaining (grouped).** TPC-DS ×6 (q70/q29/q81 + q29-feeder +
q64_correlated_filter + or_membership_with_projected_aggregate), fuzzer ×2
(pre-existing corpus stability, env/seed), families (multi_partial_anchor ×2,
duckdb_rowset variance/stddev keys-3 ×2, rollup_scoped_join ×2) and singles
(twin_keeps_scalar_refs, dim_bridge all_subset_unaffected,
cross_rowset_membership expression_key, scoped_derived_rowset exp_rows1,
rowset_generation_matrix islanded, rowset_cross_datasource null_property,
rowset_body_limit, cube_two_windows, membership_having dimension_key,
existence_feeder_pushdown, collapse_basic funnel, union_bare_aggregate
set_semantics, setops except_and_union, constant_in_cross_datasource_merge).

**NEXT options (existence-adjacent singles re-diagnosed END of s15 —
failure modes verified BYTE-IDENTICAL before/after the s15 existence-slice
fix via path-limited `git stash push -- <two files>` A/B, so these notes are
current):**

- `membership_having dimension_key`
  (test_membership_having_aggregate_dimension_key_groupby, q44 family):
  wrong rows — expected the 2 best/worst pairs
  `[(1,itemC,itemA),(2,itemA,itemC)]`, v4 returns 4:
  `[(1,C,A),(1,C,C),(2,A,A),(2,A,C)]`. The worst-side rowset's
  (desc_rank, product_name) correlation is LOST — each rank pairs with BOTH
  products, so the `subset join best.pair_rank_best = worst.pair_rank_worst`
  fans 2×. NOTE the test docstring describes the HISTORICAL bug (dimension
  key missing from GROUP BY, execution error) — v4's current mode is
  decorrelated window-rank pairing, a different defect. Adjacent to the
  s15-fixed window_expression_join (rank used as a join axis), so suspect
  the rank virt's row-identity inside the `worst`/`best` boundaries.
- `cross_rowset_membership expression_key`
  (test_scoped_join_cross_rowset_membership_existence, q2 family): the
  INVALID_REFERENCE assert PASSES (the existence set sources fine now);
  wrong rows — `subset join ftr.ws - 53 = cur.ws` returns the UNION of the
  offset pairing AND the identity pairing (row `(1, 40.0, 40.0)` is exactly
  the plain_key cell's row): expected `[(1,40,50),(2,None,40)]`, got 4 rows.
  The derived-key relation appears to keep BOTH the substituted offset axis
  and a raw `ws = ws` pairing alive. plain_key cell passes.
- `existence_feeder_pushdown` (test_membership_feeders_do_not_chain):
  structural/optimization assert — with 5 membership feeders in one WHERE,
  each feeder CTE references every EARLIER sibling feeder (10 cross-refs =
  O(n²) semijoin chaining). Rows aren't checked; the fix is feeder
  independence (each membership set should plan standalone), likely in how
  feeder N's plan inherits the query WHERE (which still contains memberships
  1..N-1) — compare v3's independent-feeder rendering.
- TPC-DS cluster ×6, or XPASS prune (24 xpassed; classifier exit 0 so no
  label escalations, but a prune pass needs isolation + in-suite green).

## ✅ 2026-07-20 (session 15) — scoped-window connector carries FD-riding bridge concepts (+2) & existence feeders sliced to subselect columns (+1)

Two v4-only fixes; verified by full sweep (31→28 real, zero new), TPC-DS
battery twice (pre-existing 6 exactly), classifier exit 0, ruff/mypy/black
clean. v3 untouched by construction (both files v4-only; a shared-helper
change was tried and REVERTED — see lesson).

1. **`_derived_connector_nodes` (source_planning.py)** — window_expression_join
   ×2. `union join rank orders.oid order by orders.amt desc = customers.rnk`:
   the datasource gap-fill deliberately stands down for non-BASIC merge
   bridges ("the connector supplies that side"), but the connector's nested
   search only carried `[origin] + grain keys` — `orders.amt` (needed by the
   consumer, FD of the connector grain {oid}) had NO provider, and a
   partial-accepting attempt let the bridge through `_bridge_parents_cover`
   unchecked → INVALID_REFERENCE at render. Fix: the connector's mandatory
   also carries uncovered bridge concepts whose grain components are a subset
   of the origin's grain (`carried`). v4 now renders v3's plan one CTE
   tighter (amt rides the window CTE).
2. **`_resolve_condition_sources` (concept_strategies_v4.py)** —
   having_bare_max. An existence feeder's nested plan comes back carrying its
   predicate args as row outputs (`max_total` for a rowset-body HAVING
   membership); any of those shared with the consumer defeats MergeNode's
   `_is_existence_only` and promotes the feeder to a ROW-JOIN candidate — a
   spurious value-join (`ON q.max_total = t.max_total`) whose grain then
   leaks the plan-local `_virt_filter_*` across the rowset boundary, crashing
   `check_if_group_required` in the outer env (UndefinedConceptException).
   Fix: slice the existence parent's outputs to the subselect columns (its
   mandatory contract) at creation. The feeder renders as v3's bare-virt CTE
   + pure `WHERE EXISTS`; the virt never enters any grain. This enforces the
   phase-boundary contract "existence edges are side-channel-only" at the
   feeder source instead of hoping merge resolution demotes it.

LESSONS:
- **The first having_bare_max fix was WRONG and was reverted** (a
  parent-QDS-tree fallback in shared `check_if_group_required` that resolved
  body-scope addresses). Reviewer pushback ("the boundary should only expose
  parent-env-known concepts") forced re-measurement: v3 does NOT co-create
  body virts in the parent build env (instrumented `materialize_for_select`:
  outer env has no virt under either planner) — v3 avoids the crash because
  its plan never puts the virt in a scope-crossing grain (existence sources
  are excluded from `calculate_effective_parent_grain`). When a "fix" makes a
  foreign address resolvable, ask instead why the address crosses the scope
  at all — the leak was a wiring bug, not a lookup gap.
- Rowset-boundary invariant (now enforced where it matters): anything that
  crosses the boundary in outputs OR grain must be outer-env-known; plan
  virtuals stay body-side because membership feeders are existence-only.
- Detached-sweep gotcha #2: `Start-Process powershell -Command "<cmd>"`
  strips the backtick-escaped inner quotes — `-m "not adventureworks..."`
  arrived unquoted again (25-byte "no tests ran" log, ~40 min lost). Write
  the sweep command to a `.ps1` (`local_scripts/run_v4_sweep_s15.ps1`) and
  `Start-Process powershell -File <path>`; health-check the log for
  "no tests ran"/"ERROR: file or directory not found" ~90s after launch.

## ✅ 2026-07-20 (session 14) — window-over-coarser-aggregate merge keys + filter-only peel gate (+2) & ORDER BY alias-source carry (+1)

Three v4-only fixes; verified by TWO full sweeps (34→32→31 real), TPC-DS
battery twice (pre-existing 6 exactly), classifier exit 0, ruff/mypy/black
clean. v3 untouched by construction (group_graph.py is v4-only; the
query_processor change is inside `_get_query_node_v4`).

1. **`_consumer_required_input_grain` (group_graph.py):** a grain component
   COMPUTED by a GROUPING row parent (its primary member) is no longer part of
   the consumer's required input grain / merge join keys. Shape: `rank(entity)
   over (order by part_avg)` where `part_avg <- avg(amount) by part` — the
   WINDOW group's grain is `{entity, part_avg}`, so preserve_keys carried
   `part_avg` and `_widen_merge_join_keys` forced the raw `rows` scan to emit
   the aggregate → "Missing source map entry" crash. The parent's grain
   (`part`, added by the pred loop) IS the join axis. Fixed 4 unreported crash
   variants (any window ordering by a coarser-grain aggregate).
2. **`_split_root_dimension_clusters` (group_graph.py):** the filter-only peel
   (post_aggregate_filter_args) now requires every D0 GROUPING bucket's grain
   to FD-determine the peeled column (group-preserving: the FINAL entity-key
   semijoin then drops WHOLE groups — the q30.alt contract; d1 population
   buckets exempt). Previously `where segment='keep' and part_avg > 60`
   (clause-granular classification: an aggregate ANYWHERE in the clause made
   ALL its row args "post-aggregate") peeled `segment` to a dim bucket that
   fed only the aggregate-recompute branch — the row stream and FINAL never
   saw the filter (silent wrong rows; pre-s14-fix it surfaced as a spurious
   DisconnectedConceptsException instead). `{part} ⊬ segment` blocks the peel;
   `segment` stays on the fact bucket and hosts the WHERE like v3.
3. **ORDER BY alias-source carry (`_get_query_node_v4`, query_processor.py):**
   a plain ORDER BY arg that is only an alias-source of a projected output
   (`order by channel` with `lower(channel) as chan` projected) is now carried
   onto the final node as an INPUT only (never an output), with the parent's
   FINAL-hidden copy un-hidden. Mechanism (matches v3 byte-for-byte): an
   input-only concept gets a final-CTE source_map entry WITHOUT joining
   output_columns → not projected, NOT in GROUP BY, and
   `_order_expr_needs_group_wrap` MIN-wraps it. HARD-WON dead ends: carrying
   it as a (hidden) OUTPUT either flips the dedup GroupNode's grain re-check
   to no-group (dedup silently lost) or, with force_group pinned, lands the
   column IN the rendered GROUP BY (CTE.group_concepts includes hidden) —
   both wrong. `resolve_concept_map` skips parent-hidden outputs, hence the
   parent un-hide. LESSON: when v4 needs a renderer affordance v3 gets "for
   free", diff the v3 final CTE's `output_columns` vs `source_map` keys —
   v3's carry was input-only, and that asymmetry IS the mechanism.

## ✅ 2026-07-19 (session 13) — membership as a bare SELECT output wired via lineage existence args (+4)

Generalized `_filter_existence_only` → `_lineage_existence_only`
(`concept_graph.py`). A concept whose lineage ITSELF exposes
`existence_arguments` — a `BuildSubselectComparison` authored as a SELECT
output (`select (20,1) in (pairs.val, pairs.cat) as present`), or one
propagating through Comparison/Conditional/Parenthetical/Between — now has its
existence-only args (existence minus row args) dropped from row lineage in
`_upstream_default` and wired as side-channel EXISTENCE edges by the existing
filter-nested pass (~line 1155), which now fires for ANY concept with
existence-only lineage args, not just FILTERs. The strategy side needed NO
change: `_group_existence_concepts` already had the BASIC-lineage membership
branch, and once the rowset stopped being a row-lineage parent the probe's
host renders the v3 EXISTS-subselect shape (v4's plan is even one CTE tighter
than v3's). Scalar (`2 in (rs.id)`), explicit-select (`2 in (select rs.id)`),
tuple, derived-flag (`auto flag <- id in (rs.id)` as output AND as WHERE), and
row-LHS (`id in (rs.id)` joined to the row stream) forms all row-match v3
(`local_scripts/repro_tuple_grainless.py`, `repro_scalar_grainless.py`,
scratch flag_where matrix). NOT wired (pre-existing, untouched): membership
nested under a `BuildFunction` wrapper (e.g. inside CASE) — BuildFunction does
not propagate `existence_arguments`, so those args still walk as row lineage.

One-file v4-only change
(`concept_graph.py`; only importers are v4_helper/* + v4_node_generators/* +
concept_strategies_v4 — no v3 sweep needed; unit-test rename in
`tests/core/processing/test_v4_concept_graph.py`). The audit's predicted fix
shape was HALF right: concept_graph did need the row-lineage drop + EXISTENCE
edge, but the predicted strategy-side work ("attach the existence parent on
the probe's host node") was ALREADY in place from an earlier session
(`_group_existence_concepts`'s BuildConceptArgs branch) and became reachable
the moment the graph stopped typing the rowset→probe edge as row lineage.
LESSON: before building the second half of a two-part fix, check whether an
existing partial-wiring branch (added for an adjacent shape) already covers it
— the first half may be the whole fix. Detached-sweep gotcha: PowerShell
`Start-Process -ArgumentList` with comma-separated args SPLITS on spaces
inside elements (`-m "not adventureworks_execution"` became `-m not` +
positional junk → "no tests ran in 0.00s" in 25 bytes); pass ONE single-string
ArgumentList instead, and health-check the log ~60s after launch. Git-bash
`kill -0` cannot see PowerShell-spawned PIDs (false "exited") — use
`tasklist //FI "PID eq N"` in monitors.

<!-- superseded s12 current-state block removed; see the ✅ session-12 entry -->

## ✅ 2026-07-19 (session 12) — grainless rowset handle contributes no FINAL join axis (+3 real)

One guard in `group_graph.py` `_rowset_join_key_addresses` (v4-only file — only
importers are v4_helper/* + concept_strategies_v4, so no v3 sweep needed): when a
mandatory ROWSET concept resolves to NO keys and NO grain components, return the
empty set instead of falling back to `{concept.address}` + lineage-argument
expansion. Cleared duckdb_subquery scalar-in-WHERE ×2 (s11's NEXT) + TPC-DS q14
(stash-verified attributable) and fixed three untested crash variants
(named-rowset WHERE, rowset-output `select id, rs.half`, bare-agg member
`where val = rs.mx` — all now byte-match v3's cross-join plan,
`local_scripts/repro_subq_variants.py`). Sweep 38/5780 (s11: 38/5678 — corpus
+102; real delta −4 known +4 new-corpus pre-existing); zero regressions across
rowset families / join_matrix / TPC-DS battery (q72 intact); classifier clean.

Investigation path worth remembering: the s11 audit's 3-layer diagnosis had the
LOAD-BEARING layer wrong. Layers 1 (build grain `{id}` on the content BASIC) and
2 (`_datasource_renders_derived` descends through AGGREGATE sources) are real
latent defects but the crash was reachable only through the FINAL-contract merge
grain: gen_root for the root group fails on the unsourceable handle, falls back
to `_resolve_root_condition_sources`, and the NESTED search's `_final_merge_grain`
(mandatory now includes the handle) called `_rowset_join_key_addresses`, which
invented a join axis out of the aggregate's value column; the row side then had
to "render" the global aggregate at row grain. Comparing against the WORKING
q22-analog (`auto mx <- max(val) by *; auto half <- mx/2` — identical group
topology, correct plan) isolated the divergence to that one helper. LESSON: when
an audit hands you a multi-layer diagnosis, find a minimal WORKING analog and
diff the two pipelines before touching any layer — two of the three suggested
fixes were unnecessary for the bug (and remain as hardening candidates only).
Also: `plain_auto` (`auto half <- max(val)/2; select id where val > half`)
returning all rows is NOT a bug — v3 does the same; bare (no-`by`) aggregates
co-grain to the consuming select's grain by design. The rowset/subquery boundary
is the documented way to get global scoping.

Session start was a REPO RESCUE: the git index + 215 tracked files (including
this audit) were zero-filled by an NTFS crash. `rm .git/index && git reset
--mixed` rebuilt the index; `git restore .` recovered all zeroed tracked files
(start-of-session status was clean, so nothing real was lost); no untracked
files were zeroed; fsck clean.

## ✅ 2026-07-17 (session 11) — disconnected-error enrichment re-forwarded (+2)

One-line v4-only fix in `discovery_utility.py` `raise_if_disconnected_for`: forward
`environment, g, island_rowsets, line_number` into
`format_disconnected_subgraphs_error` (they were being dropped — the exact
session-4 fix, reverted by the #601 "Scope Feedback" merge). Restores the
"(statement at line N)" locator and the separate-import "did you mean
`all_sales.date.year`" suggestion under v4. Cleared disconnected_e2e ×2
(`test_message_includes_failing_statement_line`,
`test_message_suggests_connected_nested_equivalent`). v4 40→38, ZERO regressions
(v4 core/processing 419/0, v3 disconnected suites 25/0); ruff/mypy/black clean.
The change only alters exception message text — it cannot affect any query plan or
row result. `raise_if_disconnected_for` is v4-only (callers in
concept_strategies_v4 / query_processor); v3 raises via its own post-discovery
site so it was never touched. LESSON: a merge from another branch can silently
revert a prior session's fix — re-check a "known fixed" area's code, not just its
memory entry, when it reappears in a sweep.

## ✅ 2026-07-17 (session 10) — bridge search prefers a non-partial UNION source (+4) & gcat test speedup

Two changes, both regression-checked (full v4 sweep 44→40 −4 zero-new; full v3
sweep clean; ruff/mypy/black clean):

1. **Enum partial_key_union ×4** (`tests/engine/test_enum_unions.py`). Shape:
   `select chan, order_id` where `chan` is an enum discriminator bound via `raw`
   in every arm of two union families (sales — non-partial on order_id; returns —
   `~order_id` partial), and order_id is complete only via sales. The group graph
   is CORRECT (one ROOT group `{chan, order_id}`); the split is pure Stage-3
   source planning. v4 rendered order_id from the sales union but `chan` from the
   RETURNS union (only orders 1,2) FULL-JOINed on order_id → chan NULL for the
   sales-only orders 3,4. Root cause: `determine_induced_minimal_nodes` (the
   Steiner bridge search) picked the returns union via a **partial** order_id
   edge, but the final edge re-add (node_merge_node ~line 231) DROPS partial edges
   when `not accept_partial` — so returns ended up connected only through `chan`,
   and order_id got completed separately by `_complete_partial_requested`
   (re-joining sales), nulling the co-resident `chan`. The search committed to a
   source through an edge the final tree then discards. Fix: penalize (weight 100)
   an edge from a UNION datasource to a concept it only partially covers, gated
   `penalize_partial=True` (v4 only, `not accept_partial` only) so a non-partial
   union covering the same set wins the tie. This also cleared the
   `where return_amount is not null` cells (same mis-sourcing dropped the filter).

   **Hard-won gate — UNION only.** The first cut penalized ALL partial ds edges
   (individual datasources too). That broke gcat `test_join_discovery_two`: there
   `vehicle.name/variant` are `~` bindings on the launch fact, and the FULL join
   to `lv_info` that completes them null-extends orgs-without-launches — a
   load-bearing bridge. Penalizing it re-routed the vehicle keys to the non-partial
   `lv_info` directly and lost the org↔vehicle-through-launches topology. A union
   inherits partiality from its arms' `~`/`?` bindings and completing it re-joins a
   whole sibling family (the enum bug); an individual `~` binding is a normal
   dimension bridge. Restricting to `BuildUnionDatasource` nodes separates them.
   The too-broad cut ALSO stalled the sweep — it was a genuine `|satcat|`-scale
   soft cross join in the mis-planned gcat query, which is what first looked like a
   "runaway."

2. **`test_extra_filter` (gcat) 157s → ~20s.** The test's `date_spine(...,
   -60000 days, ...)` (≈164 yr, back to 1862) is arbitrary — satellite data starts
   ~1957. The decom side unnests that spine PER satcat row (`questionable` CTE =
   `|satcat| × span` soft cross join; the launch side generates it standalone), so
   a wide span is quadratic for zero added coverage. Shrank both spines to
   `-6000` days (16 yr — a conservative margin against ever going empty) and
   updated the SQL-literal assertion. The filter/merge/cumulative/FULL-align
   mechanics — the test's actual value — are untouched; results stay non-empty.
   This is a pre-existing SHARED (v3+v4 byte-identical) planner asymmetry: the
   decom-side spine could be generated standalone like the launch side. Left the
   planner alone (deep, shared, risky); the span shrink is the value-preserving
   speedup.


Current handoff for v4 discovery parity work. Older session logs (pre-rebase,
2026-06-24 → 2026-07-02) were pruned 2026-07-14; they live in git history of
this file and in the project memory. Standing lessons from that era are kept
below.

## ✅ 2026-07-17 (session 9) — filter's bare aggregate co-grains to the content VALUE, not its FD grain (+22)

Single ~4-line change in `models/build.py` (`_build_filter_where`), shared but
verified regression-free on BOTH planners (full v3 + v4 sweeps). Cleared
filter_bare_aggregate_content_grain ×3 AND **`test_grain_function` ×17** (same
root cause) + TPC-DS q72 (+1) + a flaky parse-parity test. v4 66→44 (−22, ZERO
new); v3 ZERO regressions; ruff/mypy(309)/black clean; not in `v4_known_failing.py`.

Shape: `auto sp <- substring(s_descr,1,2); auto fp <- sp ? (count_distinct(...) > 4)`.
The bare aggregate in the filter must group by the FILTERED CONTENT `sp` (the
prefix) — `AA` spans items 1,2 → 5 distinct pairs > 4 ✓. v4 grouped it by
`s_item` (3/2/1, none > 4) → empty. The `grain()` builtin under dimension filters
(`test_grain_function`, #601) is the SAME construct and failed identically.

Root cause: `_build_filter_where` set the filter's `aggregate_grain` to
`content.grain.components`. That is the WRONG grain — `content.grain` is the
concept's **definitional/FD grain**, which for a property or derived scalar
descends to its KEY lineage (`sp`/`name` → `{s_item}`), over-partitioning the
count. The correct grain is the **content's own value grain** = the grain of
`SELECT <content>`, which for any scalar is the content itself (`select sp` /
`select name` GROUP BY the value, deduping to `{sp}` / `{name}`). Fix: use
`Grain({content.address})` for scalar content; keep `content.grain.components`
only for AGGREGATE/WINDOW content (whose declared grain — its `by` keys — already
IS its grouping identity). This is the filter analog of build.py:~4038, where a
SELECT's WHERE co-grains bare aggregates to `base.grain` (the select grain).

Diagnostic that settled it (staged, per the reviewer's plan): the BUILD phase and
the resolved select grain (`{fp}`) are **byte-identical** across v3/v4 — the
built `_virt_agg` count carries grain `{s_item}` under both. So the divergence is
pure discovery: **v3 re-grains** the count up to the select/content grain at the
HAVING fold; **v4 trusts** the built grain. Baking the correct value-grain at
build fixes v4 (which trusts it) and is provably safe for v3 — it makes v3's bare
filter render IDENTICALLY (modulo the virtual concept's hash) to the explicit
`by sp` form, which is exactly what `test_bare_matches_explicit_by_content`
requires. (The pre-fix v3 "clean single-query fold" was v3 rendering the finer,
wrong `{s_item}` grain and rescuing the answer via HAVING — different SQL from the
explicit form; the fix converges them.) A v4-only re-grain port was considered and
rejected as unnecessary once the build value-grain proved v3-safe.

Why the earlier first cut (`derivation == BASIC` only) was wrong: it left the
property-content form (`name ? (count>4)`) still grouping by `{s_item}` → empty;
gating on `content.address` for all non-aggregate/window content is the general
rule and is what unlocked the 17 `grain_function` cells.

## ✅ 2026-07-16 (session 8) — offset/derived-key subset join with a post-merge WHERE (+10)

Two v4-only changes (concept_graph.py + condition_placement.py). Sweep 42→32,
ZERO regressions; offset_join family CLEARED (×4) + 6 collateral (see Current
state); mypy/ruff/black clean; join_matrix 297/0 (+7 xfail); TPC-DS q72 intact
(the placement-change signal); nothing to prune from `v4_known_failing.py` (all
plain failures).

Shape: two rowsets `a`/`b` filter the SAME `orders` table on different statuses,
joined at the outer select on a DERIVED key (`subset join b.oid + 1 = a.oid`),
with a post-merge `where a.amt is not null and b.amt is not null` (the both-sides
null-test idiom that narrows a preserving LEFT to the intersection). v4 aborted
the strategy build with a group-graph constraint CYCLE; after breaking the cycle
it pushed `b.amt is not null` into b's pre-join CTE (filters nothing — b's own
rows are non-null), then the LEFT completion re-admitted the unmatched row
null-extended (`(7,70,None,None)` leaked; v3 renders an INNER join with both
filters at the outer WHERE).

Two root causes:

1. **Cross-rowset constraint edge 2-cycles two rowset groups**
   (`build_concept_graph`, both the main constraint pass and the no-successor
   backfill). The WHERE conjoins outputs of BOTH rowsets, so each condition node
   (`[@condition]a.amt` in group a, `[@condition]b.amt` in group b) got a
   CONSTRAINT edge to the OTHER rowset's d0 outputs (a→b and b→a) → bidirectional
   group-graph constraint edge → `_topological_order` finds a cycle and abandons
   the build. The existing guard skipped this only for presence probes; the
   principle is general — a rowset-scoped condition value lives inside ITS own
   boundary and its test lands at FINAL, never inside a SIBLING rowset's
   independent scan (a different rowset never consumes it as input). Fix: dropped
   the `is_presence_probe(src)` requirement from both guards — skip whenever
   `src.rowset_name != dst.rowset_name and dst.derivation == ROWSET`.

2. **A WHERE over a null-extendable rowset boundary must defer to FINAL**
   (`plan_condition_placements`, new `_rowset_boundary_deferred`). The derived-key
   subset join's endpoint is an EXPRESSION (`b.oid + 1`), so it registers NO
   domain-graph STATEMENT edge and NO `scoped_join_key_groups` axis — the offset
   relation is applied via the rowset-pair materialize path (session-1 derived
   relation members), invisible to `_group_in_active_relation`. So b's boundary
   was never flagged as a preserved side and `b.amt is not null` got hosted
   locally (pre-merge). Fix: when ≥2 ROWSET boundaries flow STRAIGHT to FINAL,
   that merge is a cross-rowset completion join that can null-extend a side; fold
   those boundaries into `active_relation_hosts` so the existing FINAL-routing
   branch fires. FINAL placement is ALWAYS correct — a no-op when the merge is
   INNER, required when preserving. Verified: v4 SQL now matches v3 (INNER join,
   both null-tests at the outer WHERE, one offset-only ON clause).

Bonus collateral all shared ONE of these two causes (cross-rowset WHERE over
independent-rowset boundaries): q83 order_by_measure (nested cross-rowset join),
cross_rowset_join_rowset_as_set ×2, generation_matrix cross_rowset_yoy_join,
membership_existence plain_key.

## ✅ 2026-07-16 (session 7) — bridge scan emits a datasource-materialized aggregate matched by canonical address (+1)

Single v4-only change in `source_planning.py`
(`_local_concept_nodes_for_datasource`). Sweep 43→42, ZERO regressions;
materialized_aggregate_bridge 1→0 (family CLEARED); mypy/ruff/black clean; not in
`v4_known_failing.py` (plain failure). This was session 6's NEXT nut.

Shape: `select customer_id where customer_order_count > 1 and
product_name = 'Mouse'` where `customer_order_count = count(order_id) by
customer_id` is datasource-materialized in `customer_summary`. customer↔product
many-to-many, bridged only through orders. v4 rendered
`HAVING count(INVALID_REFERENCE_BUG<...order_id>)`.

Root cause (SIMPLER than session 6's audit guessed — NOT a dual-scope planning
gap): the bridge-root group's `gen_root`→`plan_source` correctly picked
`customer_summary` for the WHERE arg `customer_order_count` and the merge claimed
it as an output, but the built `customer_summary` SelectNode projected only
`customer_id` — dropping the materialized `customer_order_count` column. The
merge's WHERE then referenced a column the scan never emitted; the renderer fell
back to the aggregate's lineage `count(order_id)` and order_id wasn't in scope.
In `_local_concept_nodes_for_datasource` the aggregate reaches the scan under its
`_virt_agg_*` CANONICAL node address (`canonical_concepts[virt] →
customer_order_count`), but the membership test compared the raw virt address
against `bridge_addresses` (which holds the real `local.customer_order_count`),
so it missed. (`renders_derived_key` handled the same virt-vs-address mismatch,
but only for `Derivation.BASIC` merge keys.)

Fix: added `renders_materialized_canonical` — match when `canonical.address in
bridge_addresses AND _datasource_can_output(datasource, canonical.address)`,
restricted to `Derivation.AGGREGATE/WINDOW`. Two hard-won gates:
- `_datasource_can_output` (column physically in `datasource.output_concepts`):
  without it a fact scan (orders) reaching the aggregate via its reverse-lineage
  edge (order_id→count) would emit+recompute it wrongly. Only the summary table
  owning the column emits it.
- AGGREGATE/WINDOW only: a plain root concept already matches via `address in
  bridge_addresses` (its canonical IS its address); the unrestricted (any
  derivation) first cut re-sourced presence-probe/filter members off the wrong
  scan and broke gcat `decom_spine` (`test_environment_cleanup_multiselect`,
  `test_extra_filter`) — caught by the full sweep, fixed same session.

Result: v4 SQL now matches v3 exactly — `customer_summary WHERE count>1`
(standalone total-count scope) ⋈ (orders GROUP BY customer,product ⋈ products
WHERE Mouse) on customer_id → rows [(101,),(102,)]. Why the PLAIN-select form
(session 6) didn't hit this: there customer_order_count is a mandatory OUTPUT,
sourced through the materialized-agg-as-own-group path, never reaching
`_local_concept_nodes_for_datasource` for customer_summary.

## ✅ 2026-07-15 (session 6) — ROOT-contributor bridge join key kept by membership (+3)

Single v4-only change in `strategy_builder.py` (`_relevant_root_preserve_keys` +
its one caller in `_assemble_final_node`). Sweep 46→43, ZERO regressions;
materialized_aggregate_bridge 3→1 (only the WHERE-form left); mypy/ruff/black
clean; not in `v4_known_failing.py` (plain failures, nothing to prune).

Shape: `select customer_id, product_name, customer_order_count` where
`customer_order_count = count(order_id) by customer_id` and the customer_summary
metric is datasource-materialized. customer_id and product_name are many-to-many,
connected ONLY through the orders fact. v4's concept_graph correctly built one
ROOT group with members `{customer_id, order_id, product_name}` (the bridge —
order_id links customer_id↔product_id). But FINAL assembly rendered
`quizzical (count by customer) FULL JOIN wakeful (products) ON 1=1` — a cartesian
customer×product (extra (102,Laptop) etc.).

Root cause: `_cover_groups_for_mandatory` assigns customer_id to the aggregate
contributor (more downstream), leaving the ROOT contributor with only
product_name. The bridge join key customer_id is supposed to ride back onto the
ROOT via `preserve_keys` (= merge_grain), but `_relevant_root_preserve_keys`
dropped it: it keeps a preserve key only if the key is a projected output OR
FD-determines one. customer_id does NOT functionally determine product_name (it's
a many-to-many bridge, not an FD), so it was discarded → no shared join key →
`ON 1=1`.

Fix: keep a preserve key that the ROOT group carries as its OWN member
(`attrs[gid]` primary/secondary members, threaded in as `member_addresses`).
Membership IS the bridge signal — concept_graph only placed customer_id beside
product_name in one ROOT bucket because a shared finer member (order_id)
connects them (`_split_root_dimension_clusters` would have split them into
separate buckets otherwise). With customer_id kept, `_projection_root_concepts`
pulls product_id (product_name's key), plan_source bridges orders⋈products →
(customer_id, order_id, product_id, product_name), `_wrap_for_grain` keeps it
WHOLE (product_name is not FD by the {customer_id} merge grain → the "keep whole"
guard fires), the merge joins the aggregate on customer_id, and the FINAL dedup
collapses order rows to the (customer_id, product_id) output grain. No merge-grain
widening needed — required_grain already carried product_id and the final dedup
did the rest.

Also fixed the FILTER form of the same bridge
(`test_mixed_filter_matches_where_form`, filter file) as collateral — its filter
concept forces the same ROOT bridge, which now keeps its key. The WHERE form
(`test_materialized_where_form_matches_filter_form`) is a DIFFERENT, harder shape
(dual-scope, see Current state) and remains open.

## ✅ 2026-07-15 (session 5) — filter WHERE-push governed by predicate grain vs sibling grain (+2)

Single v4-only change in `v4_node_generators/filter.py` (Path-2 aggregate
pushdown gate). Sweep 48→46, ZERO regressions; discovery filter family 3→1;
mypy/ruff/black clean; not in `v4_known_failing.py` (plain failures, nothing to
prune).

A filter concept `X ? COND` renders either as a pushed WHERE (drops
non-matching rows) or a preserving `CASE WHEN COND THEN X ELSE NULL` (keeps
rows, NULLs the value). The old Path-2 gate for aggregate predicates decided
this with two wrong proxies: (a) it required every aggregate arg to be at the
content's OWN grain (`agg.grain == content_grain`) — too strict, so
`product_name ? count(order) by customer > 1` (agg at customer, content at
product, bridged through the orders fact) fell through to a CASE and leaked a
NULL group (`sole_output_filter_has_no_null_group`); (b) it allowed a non-filter
sibling that IS the content (`... <= (content_grain | content_args)`) — too
loose, so `customer ? count>1 and product_name='Mouse', customer` pushed to
WHERE and dropped the non-qualifying customer rows the `, customer` output must
preserve (`filter_with_optional_preserves_non_qualifying_rows`).

Replaced both with one principle: **WHERE and the preserving CASE agree exactly
when the predicate is constant across each non-filter sibling's rows.** Compute
`pred_grain` = union of the grains of the predicate's row arguments; push iff for
every non-filter sibling S, `pred_grain ⊆ S.grain` (plus the existing gates:
single distinct predicate, no existence arg, all args already parent outputs).
A sole filter output (no siblings) always pushes. Verified against all six shapes
in the test file:
- `customer ? count>1` (pred {customer}, no explicit sibling) → WHERE ✓
- `customer ? count>1 and Mouse` sole → WHERE ✓
- `customer ? count>1 and Mouse, customer` → pred {customer,product} ⊄ {customer}
  → CASE preserve ✓
- `product_name ? count>1` sole → WHERE (bridged agg, grain differs) ✓
- `customer ? count>1 and customer!=102` → WHERE ✓
- `customer ? count>1 and sum>50` → WHERE ✓

LIMITATION (untested, accepted): `customer ? count>1 as filtered, customer`
(explicit content sibling, pred at content grain) is indistinguishable from the
sole form to gen_filter and pushes to WHERE rather than preserving. The scalar
Path-1 gate keeps its stricter content-exclusion (`pushable_siblings =
content_grain - content_args`) because a scalar predicate is always at content
grain, so pred_grain ⊆ content always and the pred_grain rule would wrongly push
every content-sibling scalar filter.

## ✅ 2026-07-15 (session 4) — mixed filter grain leak + disconnected-message enrichment (+4)

Two independent v4-only fixes, both regression-checked (discovery −2 clean,
non-modeling message suites clean, TPC-DS = pre-existing 6, mypy/ruff/black
clean, v3 disconnected suites green).

1. **Filter group's own output leaks into its input-grain contract**
   (`_consumer_required_input_grain`, group_graph): a filter concept's grain IS
   itself (`grp:filter:...:local.filtered` has grain_components `{filtered}`).
   The seed grain therefore demanded `filtered` from parents; a parent merge
   that joins the row dims but LACKS the aggregate arg then re-derived it as a
   per-row CASE (`CASE WHEN order_id IS NOT NULL THEN 1 ELSE 0 END > 1` — always
   false → all-NULL). Fix: subtract `attrs[gid].primary_members` from the seed
   grain (a group can never source its own derived output from a parent). Aggregate
   groups are unaffected (their grain is the grouping key, primary is the agg —
   disjoint). Fixed `test_mixed_aggregate_and_row_predicate_filter` (semijoin:
   bare content + WHERE over the joined `_virt_agg_count`) and
   `test_mixed_filter_over_materialized_aggregate`.
2. **v4 disconnected-error message dropped its enrichment args**
   (`raise_if_disconnected_for`, discovery_utility): the function accepts
   `environment`, `g`, `island_rowsets`, `line_number` but called
   `format_disconnected_subgraphs_error(subgraphs)` with none of them — so v4's
   pre-discovery gate always emitted the bare "missing a join or merge" form,
   never the "(statement at line N)" locator or the "did you mean
   `all_sales.date.year`" separate-import suggestion. v3 produces these via a
   different post-discovery raise site (that's why the tests pass under v3).
   Fix: forward all four. `raise_if_disconnected_for` is v4-only (both callers in
   concept_strategies_v4 / query_processor), so v3 is untouched. Fixed the two
   disconnected_e2e message tests; full disconnected_e2e 18/18.

## How to verify (the rules)

- The authoritative skip list is `tests/v4_known_failing.py`; reclassify with
  `python local_scripts/v4_classify.py` when changing planner behavior. The
  classifier runs listed tests in ISOLATION (pessimistic — some pass in-suite)
  and exits non-zero on a label ESCALATION.
- **A full v4 sweep is the parity gate.** A safe prune needs the entry passing
  in isolation AND in-suite:

  ```bash
  TRILOGY_V4_DISCOVERY=1 pytest -m "not adventureworks_execution" -q
  ```

- The harness's 10-min background cap kills a full sweep — run detached
  (`Start-Process` → log + monitor). Never run v3+v4 sweeps concurrently
  (duckdb hard-crash).
- TPC-DS runs dirty checked-in zquery*.log files; `git stash pop` can conflict
  SILENTLY and leave "post-change" checks running the PRE tree. Verify pops;
  `git checkout -- tests/modeling` before stashing.
- Placement changes near preserving relations MUST run the TPC-DS battery —
  q72 is the sole regression signal for pre-aggregation atom hosting.
- Full repo checks after repo-wide changes: `ruff check . --fix`,
  `mypy trilogy`, `black .`.

## Standing lessons (earned 2026-06 → 2026-07)

- **Green shape-only sweep ≠ row parity.** Several tracked tests assert SQL
  shape and never execute; a green sweep once masked a cartesian-product
  wrong-rows bug (`rowset_alias`). Never condition a shape-only v4 test green
  without executing rows.
- **Isolation failures can be untracked.** Some tests crash alone but pass via
  suite ordering and aren't in `v4_known_failing.py`. Run suspect families in
  isolation before believing a family is closed.
- **CONFIG leaks poison sweeps.** `CONFIG.use_v4_discovery` is a process-global
  singleton; a test restoring it to a hardcoded value once silently ran most of
  every "v4" sweep under v3 (~77 masked failures). The conftest autouse
  snapshot/restore of `use_v4_discovery` + `optimizations` guards this now.
- **Timeouts are runaways, not size issues.** Investigate the SQL for cross
  joins and soft cross joins (join on non-unique key before aggregating).
- All TPC-DS `_TPCDS_SIZE` ceiling work is CLOSED (q2.1/q2.2, q10, q23, q30.alt,
  q47/q57, q73, q81, q94 all fixed + pruned by 2026-06-30).

## Diagnostics: `local_scripts/discovery_v4.py`

Primary graph/strategy harness — use before eyeballing SQL:

```bash
.venv/Scripts/python.exe local_scripts/discovery_v4.py --query 81 --diagnostics --diagnostics-dir local_scripts/v4_diagnostics --no-sql
.venv/Scripts/python.exe local_scripts/discovery_v4.py --query query30-alt.preql --diagnostics --diagnostics-dir local_scripts/v4_diagnostics --no-sql
```

Outputs: `<stem>.png` (concept graph), `<stem>_groups[_reordered].png` (group
graphs), `v4_diagnostics/<stem>_diagnostics.json` (machine-readable),
`<stem>_groups[_merged].md` (group attrs/IO/contracts incl. `__final__`),
`<stem>_strategy.md` (materialized StrategyNode tree). Fast loop: check
`__final__` merge grain/contracts → search strategy for repeated datasources →
use the JSON for scripted comparisons. Real-fixture size numbers come from
`local_scripts/v4_real_size.py` (the `v4_size_compare` proxy skips inlining and
reads high).

## ✅ 2026-07-14 (session 2) — union_reproject family 10/10 (q14 composite subset join onto union-reprojected rowset)

test_duckdb_union_reproject_subset_join.py 6 → 0 (all 10 pass). Bonus:
rollup_scoped `two_key_subset_join_no_rollup_builds` now passes (3 → 2 in
that family). Collateral verified at baseline: join_matrix 297/0 (+7 xfail),
TPC-DS battery = the pre-existing 6 only, tests/core/processing +
where-scoping = pre-existing 3, rowset/scoped collateral = pre-existing set,
duckdb_rowset = the 3 residuals, mypy/ruff/black clean. Six root causes
(v4-only except the inert `statement_output_addresses` plumbing):

1. **Aggregate demand drags in an undemanded subset anchor** (concept_graph):
   `subset join nov.k = qualifying.k` canonicalizes nov handles to
   Grain<qualifying.k>; `_walk_aggregate_grain_inputs`' ROWSET branch then
   demands qualifying.* as the aggregate's row identity and the anchor union
   rowset becomes a real FINAL contributor (RHS-anchored LEFT + FULL rejoin =
   arm fanout + NULL-extended anchor-only rows). Fix:
   `_walk_scoped_aggregate_grain_inputs` + `_aggregate_authored_grain`
   redirect canonicalized grain keys back to the walked handle's OWN rowset
   members / the authored `by` keys. THREE hard-won gates on
   `_collapsible_anchor`: (a) plain-SelectLineage anchors only — a
   union/multiselect anchor participates for real at its multi-arm grain
   (direct-RHS cell: fanout + NULL-extension pinned); (b) identity-path mates
   only (address preserved; a substituted member is owned by the substitution
   plan); (c) OUTPUT-authored anchors never redirect — new
   `statement_output_addresses` (outputs-only closure, WHERE excluded): a
   WHERE-only reference is population-scope d1 demand, but an output-side
   reference makes the anchor a first-class contributor whose canonical
   co-grain siblings must keep sharing (redirecting broke the twin-rollup
   zip's same-key narrowing — `composite_both_plain_left_join_stays_left`
   rendered FULL).
2. **Undemanded-anchor subset partial never clears**
   (`_clear_groupmate_completed_partials`): with the anchor absent from the
   plan the subset-side stamps survived to the final no-complete-source guard
   (UnresolvableQueryException). Clear when a relation's mates are absent
   from ALL parents' outputs (pure domain metadata — v3 collapses to the
   subset side alone); the mate-completed clearing is unchanged.
3. **Foreign condition-arg handle hijacks a boundary group**
   (strategy_builder): a ROWSET group's outputs can carry ANOTHER rowset's
   handles (deferred WHERE args exposed through the relation);
   `resolve_rowset` plans the rowset of the FIRST BuildRowsetItem in the
   outputs list, so the nov_data group planned the cross_channel body and the
   measure silently vanished (`_cover_groups_for_mandatory` skips uncovered
   mandatory concepts QUIETLY — watch this seam). Fix: order the group's own
   PRIMARY members first.
4. **FINAL feeder re-join destroys ROLLUP subtotals** (sole-contributor
   path): a FINAL-deferred row condition on a ROLLUP contributor re-joined
   the feeder on grouping keys the ROLLUP NULLs at subtotal rows. Fix: push
   the condition onto the aggregate's INPUT stream (the axis merge already
   carries the args) via a pre-filter SelectNode wrapper — non-standard
   grouping detected by the `:grp:` bucket discriminator in the gid
   (`rollup_concepts` is EMPTY on the built GroupNode; don't trust it).
   Plus `_subtree_applies_conditions` guard to skip re-application when a
   lower host already implies the WHERE.
5. **Union boundary grain overclaims uniqueness** (`resolve_rowset`): a
   multiselect/union boundary projecting a SUBSET of its align outputs (`ch`
   never demanded) stamped the demanded subset as its grain; the downstream
   aggregate then elided its GROUP BY over the per-arm fan (sum fanned to two
   rows of 10 instead of re-aggregating to 20). Stamp the grain over EVERY
   align output.
6. `statement_output_addresses` (build_environment + query_processor):
   outputs-only authored closure alongside `statement_authored_addresses`,
   `include_where=False` param on `_authored_reference_addresses`. Shared
   file but v3 behavior unchanged (new field only read by v4).

## ✅ 2026-07-14 — duckdb_rowset 7 → 2 + union_join_rowset_grain 3 → 0 (scoped-relation axis at FINAL)

test_duckdb_rowset.py 7 → 2 (top_n_rank, composite stddev/variance keys≤2,
count), test_duckdb_union_join_rowset_grain.py 3 → 0. Collateral: join_matrix
297/0 (+7 xfail), TPC-DS v4 = the 6 pre-existing only (q72 intact),
tests/core/processing + where-scoping = pre-existing 3, full rowset/scoped
collateral = pre-existing 16 only, v3 green on the touched files, mypy/ruff/
black clean. Five root causes fixed (all v4-only):

1. **Condition-arg feeder hides its grain key** (`_resolve_condition_sources`,
   concept_strategies_v4): the standalone feeder plan for a HAVING/WHERE row
   arg (rank window) hides its own grain keys at its FINAL layer (non-mandatory
   there), and hidden outputs are invisible to downstream join inference — the
   merge back onto the consumer cross-joined (rank<=5 over 6 states = 30
   rows). Un-hide feeder outputs the consumer also carries; keyless (`by *`)
   feeders still cross-join.
2. **Mixed root↔rowset relation axis never in FINAL merge grain**
   (`_final_merge_grain`, group_graph): `union join return_demos.demo_id =
   c_demo` with neither member an output → no join key declared → FULL JOIN
   1=1 cartesian. The authored members ARE the axis: added whenever a member
   lives on a FINAL rowset boundary and another contributor exists.
3. **Boundary WHERE not deferred for mixed relations**
   (`_group_in_active_relation`, condition_placement): a rowset boundary
   participates through its member HANDLE even when undemanded (namespace
   match), and a ROOT mate (`c_demo`, not any group's member) is located
   through the mate's keys (env fallback for undemanded mates —
   `plan_condition_placements` now takes `environment`). Without this the
   boundary pre-filtered one side of the preserving relation and the
   completion merge re-admitted the rows NULL-extended (carol leak).
4. **FINAL feeder join keys physically materialized** (strategy_builder +
   `_compute_concept_sets`): a ROWSET group feeding a FINAL-deferred filter
   exposes its scoped member handles (stage 2, not cap-gated — the boundary
   owns its handles); `_apply_final_conditions` widens contributor+feeders
   with the relation axis; the sole-contributor path takes the probe-style
   raw-first branch for relation-paired feeders; `_widen_merge_join_keys`
   learned to widen a pure-passthrough dedup GroupNode (widen the inner scan,
   then the group — v3's `quizzical` groups by c_demo, c_name; force_group
   irrelevant since the group being widened does the dedup).
5. **Aggregate grain under statement-scoped joins** (`concept_graph`,
   stage 1): a no-`by` aggregate whose inputs ride a STATEMENT-scoped
   preserving relation computes per coalesced-axis row, not per its authored
   dimension grain (v3 renders it at the joined relation grain via the
   grain-match formulas; outer select dedups). `grain_components` widened by
   `aggregate_input_grain ∩ statement-scoped relation members`
   (`_statement_scoped_relation_members`; global `merge` identities excluded).
   Fixed composite stddev/variance keys≤2 + count-still-groups. HARD-WON
   GATE: only DIMENSION-grained aggregates widen — an empty/all_rows grain
   (q97 presence counts `sum(case when probe...)`) stays ONE total row over
   the joined relation; the ungated version regressed TPC-DS q97.1/q97.2 +
   coalescing_presence_matrix ×4 (caught by the battery, fixed same
   session).

Still open in-family (3): composite keys-3 ×2 (`union join quantity =
return_quantity` — the substitution collapse lets the SALES scan answer for
`r_filtered.return_quantity`, so the count reads its own quantity instead of
the joined return-side value: needs coalescing-axis side-ownership when the
aggregate arg IS a relation member) + `order_by_measure_through_nested_
rowset_join_groups` (nested cross-rowset boundary render: Missing source
reference to inner body columns — different family, q83 shape).

Also pruned from `v4_known_failing.py` (xpassed in-suite + verified in
isolation): duckdb_rowset `tvf_union_arm_local_join` +
`scoped_left_join_coalesce_keeps_unmatched`. Classifier run clean (exit 0,
no escalations).

## ✅ 2026-07-13 (session 3) — rowset_cross_datasource family cleared (9 → 0); v4 sweep 87 → 74 (0 new)

All 9 open failures in `test_rowset_cross_datasource_outer_read.py` fixed
(subordinate coalesced-key readback ×7 + intersection-key readback ×2), plus
one latent wrong-rows bug found and fixed beyond the tests (outer WHERE atom
silently dropped/pre-applied around a preserving read-back relation — new lock
`test_rowset_key_read_back_aligns_with_source_null_property`, runs both
planners). Collateral verified: join_matrix 297/0 (+7 xfail), TPC-DS v4 back
to exactly the 6 pre-existing fails (a q72 wrong-rows regression from the
first placement generalization was caught by the TPC-DS battery and fixed
with the successor gate below), tests/core/processing 415/2 (both
pre-existing disconnected_e2e), where-scoping files 121/1 (pre-existing twin
residual), full rowset/scoped collateral = pre-existing set only, mypy/ruff/
black clean. v3 spot-set green (449 passed on the sensitive files).

Five root causes fixed (all v4-only):

1. **Shared build caches leak across scoped-join scopes**
   (`_build_nested_select`): BuildCaches are keyed on address identity, which
   is wrong when a rowset BODY carries its own scoped joins — the outer
   resolution's cached build of the join key (no pseudonym link to its body
   mate) was reused inside the body, detaching the inner aggregate from its
   grouping key (global count + FULL JOIN 1=1, count=3 vs 1). Fresh caches
   when the body adds joins the outer never saw. The CONVERSE case (outer
   joins excluded via `exclude_derived`) must KEEP the shared caches —
   boundary pairing reads the outer join's pseudonym stamps off them
   (subset_presence_probe regressed under a `!=` comparison; `any extra`
   is the correct trigger).
2. **Coalesced handle content not produced** (`resolve_rowset`): a
   `subset/union/full` body collapses the authored key onto the canonical, so
   a demanded handle's content (`a.aid`) had no produced entry → key dropped,
   render sentinel. Re-expose the content on the inner producer via the
   produced canonical's pseudonyms (port of v3 `_expose_coalesced_key_sources`).
3. **ROOT split can't see scoped-collapsed keys** (`group_rules` +
   `ConceptAttrs.pseudonyms` new field): the property→key co-source edge
   matched `data.keys` by address only; with `left join a.aid = b.bid` the
   b-side property's key (b.bid) exists only as a pseudonym of a.aid → the two
   sides split into disconnected ROOT buckets and the body's WHERE vanished
   (intersection_k returned 3 rows). Pseudonym-aware key_node fallback.
4. **Preserving-relation WHERE placement generalized beyond rowset
   boundaries** (`condition_placement`): `_boundary_in_active_relation` →
   `_group_in_active_relation`. An atom whose host is one SIDE of a
   statement-scoped relation whose mate lives in a different group must not
   pre-filter (the completion merge re-admits filtered rows NULL-extended).
   THREE hard-won gates: (a) statement-scoped relations only for non-rowset
   groups — a global `merge` is an INNER identity, and gating on it floated a
   rowset body's WHERE above its aggregate (subset_presence_probe 450 = both
   years summed); (b) own-side identity via member KEYS only, never
   pseudonyms — a boundary key's pseudonym IS the other side, and counting it
   empties `mates` (the gate silently self-disables); (c) non-rowset flagged
   hosts leave the candidate pool QUIETLY (survivors still win; empty pool
   falls through to the FINAL_RECONVERGENCE tail) — routing straight to FINAL
   sent q72's pre-aggregation atoms (`inv.date.year = 1999`) above the
   aggregation = wrong counts. Also requires the flagged group's ONLY
   group-graph successor to be FINAL (its rows must BE final-merge rows).
   Mate detection sees through enrichment properties (a group hosting only
   `a.aw` relates via a.aw's key a.aid — `group_relatable` = members ∪
   member-keys ∪ member-pseudonyms; own-side = members ∪ member-keys).
5. **plan_source's carried-args contract** (`_fresh_final_root_projection` +
   `_assemble_final_node`): `_conditions_met` counts a conditioned request
   COMPLETE when the plan merely CARRIES the condition args (v3's discovery
   loop applies the WHERE after; v4's FINAL re-slice has no such step) — wrap
   the fresh projection in a conditioned SelectNode unless the plan provably
   implies it (`condition_implies`). And a filter-only FINAL condition arg a
   contributor already supplies now rides the merge as a HIDDEN input —
   otherwise the merge WHERE references a column it never carried and join
   resolution re-joins the producer as a second, PRE-filtered sibling.

Pruned from `v4_known_failing.py` (isolation + in-suite verified):
join_propagation, outer_read_key, left_k_aw, the stale `readback_inner_k`
(test renamed intersection_k), scoped_join shared_base_no_fanout,
outer_blend ×2, three_source star + two_source. Still open in-family:
FULL-body readback cells ×5 + `resolves_correctly` (a-side property
key-carry — `_V4_ROWSET_XDS_RESIDUAL`).

## ✅ 2026-07-13 (session 2) — where-scoping #599 ported; v4 sweep 140 → 87 (0 new)

The #599 dual-scope contract (WHERE cross-row references gate at POPULATION
scope; select outputs recompute over admitted rows) is now honored by v4.
`test_window_where_pushdown_matrix` 28 → 0, `test_where_select_dual_scope`
24 → 1. Full v3 sweep 5685/0 (shared author.py grain change clean); TPC-DS v4
back to the 6 pre-existing fails. Six root causes (condensed; all v4-only
except #5):

1. **d1-scan filter propagation gated on GROUP-ATOMICITY**
   (`_propagate_raw_filters_to_d1_roots`): propagate main-root row atoms to
   the pristine d1 scan only when every row arg is FD-determined by EVERY
   downstream d1 aggregate's grouping grain (q74 NEEDS the atomic case;
   non-atomic propagation violates population scope). WINDOW/GROUP_TO
   downstream blocks propagation entirely.
2. **WINDOW groups host pre-window filters**: placement lets a WINDOW group
   host atoms not referencing its own outputs; strategy builder peels injected
   conditions into a pre-window SelectNode wrapper (gen_window was silently
   DROPPING them into `preexisting_conditions`).
3. **Derived filter-arg lineage closure** (`_pre_aggregate_filter_args`):
   expand WHERE row args through lineage so `_split_root_dimension_clusters`
   can't strand one ROOT input of a derived arg; synthetic regraft skips D1
   groups.
4. **FINAL coverage completion** (`PlacementReason.FINAL_UNCOVERED_CONTRIBUTOR`):
   re-place the atom at FINAL when a mandatory-output group is not downstream
   of any host and can expose the atom's inputs. SKIPPED under
   ROLLUP/CUBE/GROUPING SETS (q05 totals).
5. **Shared BASIC grain fix** (author.py `get_select_grain_and_keys`): a BASIC
   mixing by-aggregates with row scalars keeps the row scalars' key identity
   in its grain (`_non_aggregate_row_refs` walk).
6. **Grouping-parent bridge keys** (`_refresh_input_contracts`): declare the
   GROUPING parent's grain G in `preserve_keys` so row-grain siblings keep the
   join bridge instead of degrading to 1=1.

Still open in-family: `test_twin_keeps_scalar_refs_environment_resolved` —
`_satisfy_parent_projection_contract` conflates parent-of-parent availability
with sibling-provided when excluding projection-grain keys from `fd_needed`;
fixing needs actual sibling-node outputs (broader reshape-heuristic change,
q81/q30/q10/q76 tuned) — its own session.

## ✅ 2026-07-13 (session 1) — rowset base-WHERE contract + derived relation-member obligation; sweep → 140 (0 new)

rowset_join_base_where_matrix 15 → 0, composite_matrix derived cells 4 → 0,
coalescing_presence cast ×2 → 0, rowset_offset narrows_to_anchor ×2 → 0.
Three root causes (condensed):

1. **Base-model WHERE silently dropped over rowset outputs**
   (`plan_condition_placements`): a row atom whose EVERY candidate host is
   outside `main_lineage_groups` now raises DisconnectedConcepts (was: gate
   group pruned at FINAL assembly → filter vanished). EXEMPTION: an atom whose
   row inputs are all condition row-args of the output rowsets' bodies (q44
   outer WHERE restating both bodies' filters) — historic harmless drop.
2. **Subset-side partial cleared on the sole-contributor FINAL path**
   (`_assemble_final_node`): `_clear_groupmate_completed_partials` also runs
   on a single covering contributor.
3. **Derived relation members materialize for rowset pairs**
   (`resolve_rowset`): a derived member (`cur.wk + 53`) whose every lineage
   arg is a handle of this boundary materializes NON-HIDDEN at its own side.
   Gates: outer relations / subset-source member only (anchor-side derived
   keys resolve through the scoped-merge collapse — materializing displaces
   it, widening LEFT→FULL); skip when the collapse substituted the member
   (`environment.concepts[member].address != member`); skip BuildRowsetItem
   handles.

## ✅ 2026-07-12 (sessions 1–3, condensed) — rebase onto new-join-engine main + rowset-pair port; 201 → 145

Branch rebased onto main (new join engine, join_matrix + rowset matrices +
presence probes + #596 cograin rules — all validated only on v3). Honest
post-rebase v4 sweep 201 → 177 (presence-probe side identity via
`_datasource_renders_probe` + probe `canonical_name`; rowset body
LIMIT interposed; post-aggregation condition placement w/ cograin fixes)
→ 145 (coalescing-axis port: probes→ROOT ride the bridge pin, rowset-member
probes as boundary obligations, probe/member atoms → FINAL keyed feeder,
rowset↔rowset subset partials → anchor-LEFT+coalesce, full-cover bridge
dead-last, partition_roots PROPERTY→KEY FD-connect)
→ 117 (session 3, rowset-pair key-carry: boundary nullability restricted to
key-like handles (q29 guard), derived-nullable stamp counts unprojected arg
columns (shared — v3 green), post-join WHERE→FINAL when the relation mate is
in-graph, axis-only projection keeps whole_grain, authored keys pin the
rowset-pair merge grain).

## Phase boundary contract

The intended v4 path is:

1. Source concept demand: `concept_graph.py` walks mandatory output concepts and
   condition inputs back to root concepts, preserving row-vs-existence dataflow
   with typed edges. This phase should not pick concrete datasources or build
   `StrategyNode`s.
2. Group concepts: `group_graph.py` applies per-derivation grouping rules,
   injects conditions at groups, computes group IO, and appends the FINAL sink.
   This phase should decide which concepts can be sourced together, but should
   not render or materialize query nodes.
3. Materialize groups: `strategy_builder.py` walks the group DAG and dispatches
   each group to `v4_node_generators`. ROOT groups call `source_planning.py` for
   datasource selection, bridge planning, partial completion, and pinned rollup
   sourcing. This phase satisfies declared contracts; it should not infer
   projection grain, join keys, or final contributor contracts from physical
   sibling shape.
4. Zip final query: `_assemble_final_node` merges the minimum built contributors
   that cover the mandatory outputs, applies final-only filters, hides join keys
   that were carried only for assembly, and dedups to the requested output grain.
   Optimization happens after this planner returns a normal strategy node.

## Separation audit notes

The boundary is now enforced in code rather than only described here:

- `FinalAssemblyContract` and `FinalContributorContract` are Stage 2 outputs.
  Stage 3 requires them and no longer refreshes or synthesizes missing final
  contracts.
- `GroupInputContract` is the only source for parent projection grain and
  per-group bridge join keys. `_satisfy_parent_projection_contract` physically
  satisfies that grain, but no longer falls back to group grain or shaped sibling
  outputs.
- `_widen_merge_join_keys` only widens for declared join key addresses. It no
  longer scans sibling outputs for "key-ish" concepts.
- FINAL rowset joins declare the row-stream lineage key (for example
  `local.order_id`) in Stage 2, rather than relying on Stage 3 to infer it from
  nullable rowset aliases.
- `_fold_passthrough_parents` is pinned as a physical redundancy optimization:
  it can absorb row-preserving projections, but cannot dissolve row-shape
  barriers.
- Sole-contributor FINAL hiding is pinned as output hygiene: it hides
  non-mandatory carried grain keys only at the FINAL layer.
- `_group_to_grain_if_required` is pinned to `FinalAssemblyContract`; it may
  skip or perform physical deduping, but does not choose the logical output
  grain.

Still-watch areas:

- `source_planning.py` is the right home for concrete datasource selection. Its
  bridge and partial-completion helpers are sourcing concerns, not grouping
  concerns.
- `_regraft_group_sources` in `group_graph.py` is topology-only: it may add a
  better existing parent edge or a synthetic dimension ROOT bucket, but it must
  not call `plan_source`, inspect datasources, or build `StrategyNode`s.
- Existence edges must stay side-channel-only. They order subselect sources but
  should not be treated as row-stream JOIN parents.
- Rowset, recursive, aggregate, window, and group-to concepts remain row-shape
  barriers. Fixes should preserve them as materialized boundaries rather than
  decomposing their roots into another phase.
