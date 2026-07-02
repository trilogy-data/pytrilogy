---
name: project_scoped_inner_join_collapse_semijoin
description: scoped INNER join = set intersection; rebuilt off collapse via de-collapse + WHERE-injection + pseudonym-candidate + property-key anchoring
metadata:
  type: project
---

IN PROGRESS (2026-07-01, large uncommitted WIP). A query-scoped `inner join a = b`
must mean SET INTERSECTION (both sides materialized, output = intersection), not the
old union-find COLLAPSE (fold both keys to one canonical, source one side, filter
lost). Driver: TPC-DS q38/q14/q72 + the `_validate_cross_rowset_inner_joins` guard.
Acceptance matrix: `tests/test_scoped_inner_join_intersection.py` (value-set: project
left/right/both/count × null-both/neither/one-side; property single-key & two-key;
rowset↔rowset). Human-readable SQL dump: `evals/scoped_inner_join_intersection_matrix.md`.

THE FIX, in layers (all landed & matrix-green except where noted):
1. **De-collapse** (build.py `scoped_inner_identity_sources`, gated ROOT=ROOT): scoped
   INNER endpoints keep own identity + mutual pseudonym (like LEFT/FULL), exempt from
   `_build_concept` subst + `_normalize_grain_components` + `scoped_key_merge_map`.
2. **WHERE-injection** (query_processor.py ~789): inject `a.k IS NOT DISTINCT FROM b.k`
   as a WHERE term (new `FunctionType.IS_NOT_DISTINCT` — enums/functions.py/dialect
   base.py, renders `is not distinct from`). Adds to SOURCING (row-args) WITHOUT the
   output grain. Null-safe = the intended NULL==NULL "NULL is a valid member". NOTE:
   this WHERE is now largely redundant (the emitted join filters) — future cleanup, do
   NOT rely on it to filter (it folds to `X is not distinct from X` tautology when one
   side isn't sourced — that was the whole bug).
3. **Removed the pseudonym exclusion** in `generate_candidates_restrictive`
   (discovery_utility.py ~408-409, the two `x.address not in priority.pseudonyms` lines).
   THAT is what let the de-collapsed partner (`rval`) be co-sourced so the real INNER
   JOIN forms and filters. WARNING: this is GLOBAL and breaks rowset connectivity (q64
   DisconnectedConcepts) — MUST be re-scoped to `environment.scoped_inner_join_keys`.
4. **Datasource nullability** (`?lval`): NULL==NULL only matches when the join renders
   `is not distinct from`, which needs the column marked nullable (get_modifiers).
5. **Property-key ANCHORING** (select_merge_node.py `gen_select_merge_node`): a property
   join key (fact attribute via an FK, e.g. `inv_week` keyed by a date FK) floats to its
   DIMENSION standalone, decorrelating from the fact's OTHER join key (item vs week leak,
   q72/commutativity). Fix: when the merge sources a scoped-inner-join key that is
   `purpose==PROPERTY and derivation==ROOT`, inject its `.keys` into the PARENT sourcing
   (`all_concepts`) so the fact co-sources both keys from one scan; then the MergeNode
   outputs the PRE-anchor concepts with `force_group=True` (group away the anchor → dedup,
   no fan-out). Gate to PROPERTY/ROOT — else it breaks rowset↔dim joins (KEY / ROWSET
   keys have own machinery). Discovery insight: the aggregated fact gets a free anchor
   (its `cs_id` is the count input); the restriction fact has none, so its property key
   floats — the anchor injection gives it one. The decisive drop was
   `source_scoring.py:405` "Dropping subgraph ... subset"; a guard there is INSUFFICIENT
   (fact re-pruned downstream since its only contribution is the FK) — anchoring is the fix.

Layer 2 refinements (landed): the WHERE-injection is now gated on
`build_environment.scoped_inner_identity_sources` (new field, set from the Factory's
ROOT=ROOT de-collapsed endpoints) — a derivation check MISCLASSIFIES rowset keys that
alias onto a ROOT (q64). Layer 3 pseudonym-exclusion is re-scoped: restored the skip in
`generate_candidates_restrictive` EXCEPT for ROOT scoped-inner partners (rowset partners
still skipped = the bridge).

q64 FINAL DIAGNOSIS (2026-07-01, STILL OPEN — both gating levers FAIL): the disconnect
is NOT the injection and NOT fixable by gating de-collapse. Sequence proven this session:
(a) `has_existence` gate on the WHERE-injection => injection SKIPS for q64 (confirmed: no
firing), q64 STILL DisconnectedConcepts. (b) Author-level `_disable_inner_decollapse`
(gate de-collapse OFF for existence-membership queries, via `authored_join_endpoints=set()`
to BOTH base_factory + materialize) => `_disable=True` fires at the top build, q64 STILL
disconnects. So neither lever is the cause. ROOT CAUSE: de-collapse SURFACES a REAL join
that main HID. On main `ss=pr` COLLAPSES (`pr.item.id -> ss.item.id`,
`pr.ticket_number -> ss.ticket_number`) so store_returns contributes nothing distinct and is
silently DROPPED (join is a no-op; the return-filter is lost -- the test only checks it
BUILDS). With de-collapse `store_returns` is a distinct fact that MUST join; its 4 keys
`{ss.item.id, pr.item.id, ss.ticket_number, pr.ticket_number}` form a discovery ISLAND that
does not bridge to the existence-only rowset `catalog_item_agg` (reachable only via
`ss.item.text_id in ...` membership + a row-arg `cat_ext_list_price>2*cat_refund` at the
rowset's own grain). `ss` provides ALL THREE roles (a join key, the output product_name/
line_item, AND the membership key text_id) so it SHOULD be the hub bridging the island to the
output -- but the de-collapsed join-key island is not being merged onto that fact hub. FIX
(NOT DONE, deep): bridge the de-collapsed scoped-inner join-key island to its shared fact hub
in discovery graph construction (analogous to the island-rowset Steiner weak-merge,
[[project_weak_merge_island_rowsets_steiner]] / [[project_q02_island_rowsets_severs_downstream_consumer]]).
Current committed state: injection carries a `has_existence` gate + `_side_contributes`
guard; de-collapse authored-gating attempt REVERTED (didn't help). Matrix 10 passed/1 xfail;
`test_q64_rowset_join_with_second_fact_join_hoist` is the 1 known regression (32 passed in the
non_benchmark suite). Older superseded note below.

q64 CORRECTED (2026-07-01): the TEST `test_q64_rowset_join_with_second_fact_join_hoist`
uses an INLINE query (NOT query64.preql!) that `import store_returns as pr` and AUTHORS
`inner join ss.ticket_number=pr.ticket_number and ss.item.id=pr.item.id` — so `ss=pr` IS a
genuine authored ROOT=ROOT intersection (store_sales lines that were returned). The
injection correctly fires. The DISCONNECT: the query also filters by a rowset
`catalog_item_agg` (a cs/cr aggregation) via a membership (`ss.item.text_id in
catalog_item_agg.item_id`, an EXISTENCE arg = not a graph edge) + a row-arg comparison
(`catalog_item_agg.cat_ext_list_price > 2*cat_refund`) at the rowset's own grain. On main
`ss=pr` COLLAPSES so `pr` (store_returns) is never a distinct mandatory concept -> graph is
`{ss, catalog_item_agg}`, connects (but silently drops the return-filter; the test only
checks it BUILDS/executes, not correctness). The injected `is_not_distinct(ss.item.id,
pr.item.id)` WHERE forces `pr` (a new datasource) into the mandatory sourcing at the level
where the rowset lives -> graph = `{ss,pr}` joined + `{catalog_item_agg}` reachable only via
existence -> DISCONNECT. FIX (option 1, principled, not done): route the injected
intersection through the EXISTENCE/semijoin path (make `pr` an existence set on `ss`, like
the rowset membership) so it never co-mingles with the rowset's connectivity scope. Could
NOT isolate a clean toy: minimal repros of "rowset-aggregate + membership in WHERE"
disconnect even WITHOUT the join (a more fundamental shape), unlike q64 which builds on main.
(Superseded finding, kept for context — this line's premise was from the WRONG query file:)
q64 has LEGITIMATE ROOT=ROOT scoped inner joins
(`ss.item.id=pr.item.id`, `ss.ticket_number=pr.ticket_number`, store_sales↔returns) that
DO get the WHERE term. Both facts are used elsewhere so the join forms on its own; the
redundant `is not distinct from` term then DISCONNECTS the co-existing rowset
(catalog_item_agg) in discovery. So the WHERE-injection MUST be CONDITIONAL — only when a
join side is JOIN-ONLY (contributes no other output). Tried a "does the endpoint's
datasource provide any output/output-input" check → it BREAKS the two-key property case,
because that count is grouped BY the canonical (de-collapsed) join keys, so the
restriction fact's key IS in the output grain → looks contributing. The side-absent
detector is confounded by de-collapse mapping the grain to the canonical side. NEXT: a
correct side-absent signal (maybe: an endpoint is join-only if its datasource provides no
NON-JOIN-KEY output — exclude the scoped-inner keys themselves from `contributing`).

STILL OPEN: (a) q64 conditional-injection (above); (b) full commutativity
(`test_scoped_join_bridge_commutativity`) still leaks — its keys are DIM keys via FKs on
BOTH sides (shared item/date imports) + a cross-fact `inv.qoh<cs.qty` WHERE; the minimal
2-key repro (fact props + dim week) PASSES, so this is extra structure to isolate;
(c) rowset↔rowset intersection (retire the guard, route through de-collapse); (d) 3
`test_buildenv_*` contracts assert old substitution → rewrite to identity+pseudonym;
`test_rowset_key_read_back_aligns_with_source` asserts the OLD unfiltered bug → update.
Family: [[project_q38_cross_rowset_inner_join_intersect_sentinel]],
[[project_crossrowset_inner_join_grainless_scalar]].
