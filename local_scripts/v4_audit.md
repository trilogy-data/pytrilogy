# v4 compatibility audit (last refreshed 2026-07-16, session 8)

Current handoff for v4 discovery parity work. Older session logs (pre-rebase,
2026-06-24 → 2026-07-02) were pruned 2026-07-14; they live in git history of
this file and in the project memory. Standing lessons from that era are kept
below.

## Current state (after 2026-07-16 session 8)

**Full v4 sweep (session 8): 32 failed / 5594 passed**, 16 xpassed, 20 skipped,
51 xfailed. Log: `local_scripts/v4_sweep_0716_s8.log`. Diffed against the
session-7 42-set: **−10 net, ZERO new/regressions.** Session-8 fixed the
**offset_join family (×4, CLEARED)** plus 6 collateral: q83 `order_by_measure`
(duckdb_rowset residual 3→2), **cross_rowset_join_rowset_as_set ×2 CLEARED**,
generation_matrix `cross_rowset_yoy_join` (2→1), membership_existence `plain_key`
(2→1), and q64-transitive (TPC-DS 6→5). Session-7 baseline was 42/5584
(`v4_sweep_0716_s7b.log`).

Session-8 fix (two v4-only changes, offset/derived-key subset join with a
post-merge WHERE — see session-8 entry):
1. **Cross-rowset constraint-edge guard generalized** (concept_graph.py) — breaks
   a bidirectional group-graph constraint cycle.
2. **Rowset boundary deferral for cross-rowset FINAL merges**
   (condition_placement.py) — routes a WHERE over a null-extendable rowset
   boundary to FINAL.

**Open families by sweep count (32):** TPC-DS ×5 (q14, q81, q29-feeder,
or_membership, q64-correlated), filter_bare_aggregate_content_grain ×3,
duckdb_rowset residual ×2 (composite stddev/variance keys-3), rollup_scoped ×2
(three_key_executes, two_key_partial_builds), multi_partial_anchor ×2,
expression_keys ×2, pushdown_partitioned ×2, subquery ×2, + singles
(rowset_body_limit, scoped_derived exp_rows1, collapse_basic_into_group,
union_bare_aggregate, setops, orderby_derived_expr, constant_def_macro,
membership_having, generation_matrix single_rowset_islanded, membership_existence
expression_key, where_select_dual_scope twin_keeps_scalar_refs, cograin
having_bare_max_matches_where).

**NEXT candidate:** filter_bare_aggregate_content_grain ×3
(`test_filter_bare_aggregate_content_grain.py`) is the largest single-file
cluster. Alternatively duckdb_rowset residual ×2 or one of the ×2 families
(pushdown_partitioned, subquery, multi_partial_anchor, expression_keys).

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
