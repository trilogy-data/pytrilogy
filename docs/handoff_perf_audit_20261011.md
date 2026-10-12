# Handoff: planning perf + simplification audit (2026-10-11)

Open items from the audit that landed the GC pause (PR #720). Each item is
independent; take one per PR. Percentages are of corpus planning CPU.

## Gate recipe (every item)

- SQL identity: `local_scripts/sql_ab/corpus_sql.py` in a detached base
  worktree and in the branch, then `sqldiff.py` must report `0 moved`
  (`local_scripts/sql_ab/README.md`). Items marked *suite-oracle* change
  paths the corpus does not reach: also run the `sqlcap` suite A/B.
- Every memo/cache item: first land it with a temporary check that recomputes
  on each hit and asserts equality, run the corpus and the suite, then remove
  the check.
- Timing on this machine swings 2x with load. Compare call counts, or
  interleave base/branch runs with `time.process_time()`; never trust one run.
  Profile with sampling where possible: cProfile overstates small
  Python-heavy functions (a `_carried` rewrite looked like 2s under cProfile
  and was 0.2s real).

## 1. One scope factory per rowset (small)

`_build_rowset_item` (`build.py`) makes one scope factory per column (4,028
of 4,336 calls are for a rowset already seen). Sharing one per rowset object
also shares its `local_concepts`, so gate it like a cache.

## 2. Share the heal's concept graph / keyspace with the plan (medium risk)

`decide_heal` (`partial_bridging.py`), the rowset witness
(`rowset_witness._witness`) and the plan (`_build_from_graph_traced`) build
the same concept graph and keyspace for the same request: 148 of 433
`build_concept_graph` calls repeat an earlier one in the statement (heal ->
plan 98, witness -> plan 24, heal -> witness 26). 3-4%.

Change: dedupe at the caller (record the heal's request + answer on the
`ScopeDatasources` it ran over; reuse only when the later request compares
equal). Invariants: reuse only when heal/exclusion left the scope object
unchanged (healed bindings keep identifiers but are new objects); compare
requests ORDERED (node insertion order reaches SQL); the key must cover
conditions incl. `statement_filter_population`, staged conditions,
materialized roots, `span_scope`/`owned_spans`; never hand out a shared
mutable nx graph (`build_group_graph`, plan_trace read it).
`statement_keyspace` adds rowset witnesses on top: only the base keyspace
is shareable.

## 3. Scope facts recomputed for identical inputs (low risk)

`scope_facts` caches per `ScopeDatasources` object (`keyspace.py` ~305), but
every plan seam builds a fresh holder (`statement_scope.py` ~102), so 48 of
312 `_compute_facts` calls repeat identical datasources + canonical map
(38% of facts time). `_stamped` (`keyspace.py` ~441) is a pure function of
its sources: 221 of 361 inputs are duplicates across the corpus (82 within a
file, still 46% of stamping time, the big TPC-DS scopes); all duplicates
verified identical.

Change: memo `_stamped` on `(identifier, grain, grain_is_partial, sorted
bound)` per source, or reuse the parent's holder when bindings are the same
objects and heal/exclusion changed nothing. Facts also read `env.concepts`
(`_generated_domains`) and FD minimization: an overlay adding a generated
KEY or an FD must miss. 1.5-3%.

## 5. Delta environments inherit `value_classes` (medium risk)

243 of 263 `build_environment_delta` results have `value_classes` identical
to the baseline's (`spelling_classes` 104/263). `_ranked_classes` is ~2.4s
profiled per corpus; the unscoped `address_roots()` dict (141k entries) is
rebuilt on every `_compute_facts`. Inherit when the replaced/added concepts'
`(key, address, pseudonyms)` and the `alias_origin_lookup` entries match the
baseline; cache the unscoped roots dict. A stable canonical map also gives
item 3 a cheap identity key. Ranking reads `alias_origin_lookup` and
`concepts` membership: verify both.

## 6. Optimizer: used-map renders

`render_cte_used_map` (`core/optimizations/utils.py` ~261) fully renders a
CTE; 771 calls per corpus after the single-parent skip landed. Measured with
`process_time` around the call it is ~0.4s of ~15s corpus CPU (the earlier
"38% of optimizer time" was a cProfile reading), so this is worth <3%.

A cross-phase used-map cache keyed by a per-CTE mutation counter
(`HideUnusedConcepts` rebuilds its cache every run). Missing a writer
silently mis-prunes: land with a debug recompute-and-assert mode.

## 7. Optimizer: duplicate re-fire phases (suite-oracle)

Seven phases repeat an earlier phase's rule and arguments, and the fixpoint
already sweeps their leftovers: `merge_irrelevant_group_by.after_predicate_remove`,
`collapse_single_parent.after_pushdown`, `predicate_pushdown.after_final_upgrade`,
`predicate_pushdown.after_union_dim`, `inline_datasource.after_existence_fold`,
`union_dim_pushdown.after_existence_fold`, `predicate_pushdown.after_existence_fold`.
Dropping all seven: phase runs -15%, corpus SQL identical; fails the two
plan-name assertions in `test_optimization_pipeline.py` (expected) and
`test_optimizer_fixpoint.py::test_every_where_atom_moves_into_having_in_one_pushdown`
(q04 needs >3 passes; keep `predicate_pushdown.after_final_upgrade` for it).
Real time saved is ~0.3s/corpus since work moves into later passes; worth it
mostly for the shorter plan. Count "still changing" warnings in the suite.

## 8. Small, safe, measured (~0.5-1.5% each)

- `BuildConcept._with_default_grain` (`build.py` ~1430) is an uncached
  property: 117k calls, 97% return `self` after building a `BuildGrain`.
  NOT a plain cache: `lineage` is assigned after construction
  (`_build_multi_select_lineage` sets `local_build_cache[k].lineage`) and the
  PROPERTY branch reads it. Short-circuit the cheap cases instead.
- `_bound_addresses` (`discovery_utility.py` ~597) walks every node through
  the Rust boundary per gate call; it equalled the set derivable from the
  graph's datasource edges 311/311. Record it in `generate_graph`.
- `_raise_if_disconnected` and `raise_if_filter_disconnected` compute the
  same `_component_map` twice on 52 of 315 calls: compute once, pass down
  (`excluded_addresses` stays in the key).
- `_env_finish` -> `gen_concept_list_caches` reruns on a delta that replayed
  nothing: reuse the baseline's.
- `query_processor.py` ~949: `rebuild_cache()` runs twice on ordered
  statements; one top-node `_resolve` saved.

## 9. Simplification (lines, behaviour-neutral)

Verified reachable-only-as-described; corpus + suite gate each.

- `parsing/v2/concept_factory.py`: five `*_v2` wrappers that only swap
  `context` for `context.environment`; call `parsing/common.py` directly,
  delete the file (~110 lines, 28 call sites).
- `optimization.py` ~277-700: 31 `if opts.X: plan.append(...)` blocks to a
  table (~130 lines; drops `_enabled_dependencies`). Preserve the multi-flag
  guards and the order `test_optimization_pipeline.py` asserts.
- `build.py`: ~30 `@_build_dispatch.register def _` stubs whose
  `_build_x` has no other caller: fold the body into the stub (~120 lines);
  eight identity handlers (`build.py` ~4968-5020) collapse into one union
  registration. Keep `_build_concept`, `_build_grain`, `_build_function`.
- `LooseBuildConceptList` / `CanonicalBuildConceptList` differ by one
  attribute; `concept_arguments`/`row_arguments`/`existence_arguments` are
  triplicated across `BuildConditional`, `BuildComparison`, `BuildBetween`
  (~70 lines).
- Union-find: `model_ambiguity.py` ~38 and `dialect/mock.py` ~600 copy
  `network_model.py` ~595; six nested `def find` closures elsewhere. One
  `trilogy/core/union_find.py` for `find`; keep each caller's `union`
  (which root wins differs and matters, e.g. `domain_graph.py` roots on the
  anchor).
- `predicate_pushdown._transitively_depends_on` and
  `union_dim_pushdown._derives_from` reduce to `semi_join_pushdown.reaches`
  (+ an `internal_ctes` flag); hand-rolled `unique(..., "address")` in
  `v4_node_generators/constant.py` and `common.py`.
- `History` exists only to be rewrapped as `V4History`
  (`query_processor.py` ~1056/849): one class.
- `g=None` fallbacks that rebuild the graph in `discovery_utility.py`
  (~677, 740, 1118) never fire in production; only
  `tests/core/processing/test_disconnected_subgraphs.py` uses them.
- *suite-oracle*, high risk, ~200-350 lines: `gen_select_merge_node`'s
  multi-parent MergeNode branch, `_merge_condition_routing` and its retries
  (`select_merge_node.py` ~327-381, 583-631, 677-724, 749-845) never run on
  the corpus (0 MergeNodes from 118 direct sources). `tests/generators/`
  calls the path directly; prove 0 hits across the sqlcap suite before
  deleting.
- Decision for the owner, not a task: the Lark backend (~1,150 lines + the
  `lark` dependency) survives as a test parity oracle and a no-wheel
  fallback.

## Hazard found (correctness, not perf)

`Factory.__init__` does `self.build_cache = build_cache or {}`
(`build.py` ~3023), so an EMPTY shared cache is silently replaced by a
private one (2,102 factories). Forcing sharing moves `tpc_ds_duckdb/query23#0`
and `query64#0`: cache hits return concepts built in another context, so the
key (namespace.address.canonical_name.grain + grouping token, ~3644) is
missing context (pin anchors, scope salt, join scope candidates). The same
hazard is live wherever sharing does happen (nested selects). Decide whether
the cache is shared; if so complete the key, using q23/q64 as the oracle.
This also blocks checking the cache before building lineage (21k calls build
a lineage, then hit).

## Hazard: where-less grain cache ignores scoped merges (correctness)

`Factory._build_grain` caches a where-less grain whole under
`(frozenset(components), None)`, but `_normalize_grain_components` reads the
calling factory's `scoped_merge_map` (and `environment.concepts`). A grain
first built by a factory without a scoped merge comes back unnormalized in
one that has it: on `tpc_ds_duckdb/query84` the hit returns
`ss.return_customer_demographic.sk` where a fresh build gives
`customer.current_demographics.sk`; a recompute-on-hit assert fires on 14
TPC-DS files. Where grains already re-normalize per call. Fixing this
(normalize per call, or key on the merge map) moves plans: check rows.

## Closed: do not re-chase

- Lazy / demand-driven environment build: 89% of each environment's concepts
  are read during planning (`network_build`, `group_graph`).
- Pruning the reference graph to the statement's closure: removes 7% of
  seeds and breaks adhoc06, adhoc07, tpc_ds query3.
- Dirty-CTE optimizer worklist: skips 4k of 41k `optimize` calls;
  `hide_unused_concepts` touches nearly every statement in pass 1.
- `_carried` worklist/index rewrite: identical results, ~1%.
- Rowset witness planning the body: the body's own plan reuses it
  (`history.nested_builds`, 62/65 hits); the witness moves the cost earlier,
  it does not add it.
- Undirected copy in the connectivity gate: `to_undirected` is ~0.5%;
  `_detach_bridging_derivations` needs whole-graph components.
- GC: paused during `process_query` (PR #720). Parse gains nothing from it.
