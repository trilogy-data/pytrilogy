# Handoff: planning perf + simplification audit (2026-10-11)

Open items from the audit that landed the GC pause (PR #720). The
2026-10-12 follow-up landed items 5, 7, 8 (three of four) and most of 9;
item 1 landed once multiselect/union outputs got stable canonical names
(`_gen_msl_name` had hashed a growing repr); item 8c is closed below. Percentages are of corpus planning CPU.

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

## 2. Share the heal's concept graph / keyspace with the plan (medium risk)

`decide_heal` (`partial_bridging.py`), the rowset witness
(`rowset_witness._witness`) and the plan (`_build_from_graph_traced`) build
the same concept graph and keyspace for the same request: 148 of 433
`build_concept_graph` calls repeat an earlier one in the statement (heal ->
plan 98, witness -> plan 24, heal -> witness 26). Re-measured 2026-10-12: concept graphs
~1.1s and keyspaces ~1.2s of ~21s corpus CPU; requests repeating an
earlier one exactly (ordered outputs, conditions, roots, staged, span
scope, datasource objects) cost ~0.16s of the graphs, so ~2% in all.

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

`_stamped` is memoized on content (`keyspace.py`; `_carried` calls
17,486 -> 7,197). Still open: `scope_facts` caches per `ScopeDatasources`
object (`keyspace.py` ~305), but every plan seam builds a fresh holder
(`statement_scope.py` ~102), so 48 of 312 `_compute_facts` calls repeat
identical datasources + canonical map. Reuse the parent's holder when
bindings are the same objects and heal/exclusion changed nothing. Facts also
read `env.concepts` (`_generated_domains`) and FD minimization: an overlay
adding a generated KEY or an FD must miss.

Re-measured 2026-10-12: `_compute_facts` is ~0.9s over 312 calls; every
repeat comes with a different (delta) environment object, so the key needs
environment content. Item 5 landed: an inheriting delta now returns the
baseline's `address_roots()` dict object, a cheap identity for the
`canonical` input. ~0.14s to win.

## 6. Optimizer: used-map renders

`render_cte_used_map` (`core/optimizations/utils.py` ~261) fully renders a
CTE; 771 calls per corpus after the single-parent skip landed. Measured with
`process_time` around the call it is ~0.4s of ~15s corpus CPU (the earlier
"38% of optimizer time" was a cProfile reading), so this is worth <3%.

A cross-phase used-map cache keyed by a per-CTE mutation counter
(`HideUnusedConcepts` rebuilds its cache every run). Missing a writer
silently mis-prunes: land with a debug recompute-and-assert mode.

## 8. `rebuild_cache()` twice on ordered statements

`query_processor.py` (~949): one top-node `_resolve` saved per ordered
statement. Not a plain skip: the second resolve bakes `ordering` into the
QDS, whose cached identifier hashes it under a limit, and MergeNode hands
parent QDS objects through.

## 9. Simplification (lines, behaviour-neutral)

Open:

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

## Hazard: where-less grain cache masks a subset-join wrong-rows bug

`Factory._build_grain` caches a where-less grain whole under
`(frozenset(components), id(None))`, but `_normalize_grain_components` reads
the calling factory's `scoped_merge_map` (and `environment.concepts`). A grain
first built without a scoped merge comes back unnormalized under one
(`tpc_ds_duckdb/query84`: `ss.return_customer_demographic.sk` where a fresh
build gives `customer.current_demographics.sk`).

Normalizing per call (cache only the built where) was tried and reverted:
- `tests/core/processing/test_v4_fd_dimension_merge_grain.py` goes WRONG
  ROWS. `subset join cust_id = bought.sale_cust` maps `local.cust_id ->
  bought.sale_cust`; customer properties then grain on the rowset handle and
  the final join becomes `RIGHT OUTER` from customers, keeping `bob` (no
  sales) with a NULL total. The stale cache hid this.
- `tpc_ds_duckdb/query29` grows 6 -> 9 CTEs (rows still match): `per_pair`
  grains on `store_sales.customer/item.sk` instead of the `subset join`'s
  `catalog_customer_item.cat_*`, and `item.desc`/`item.id` come through an
  extra `store_sales` distinct scan.

Open question first: which side should a subset join's scoped merge make
canonical, and does the planner honour subset containment when a base
concept's grain is spelled with a rowset handle. Fix that, then stop caching
components.

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
- Reusing the baseline's concept-list caches in `_env_finish` (item 8):
  `gen_concept_list_caches` is ~0.9% of corpus CPU and only 13 of 456
  materializations qualify (no replay, no changed address).
- Dropping `collapse_single_parent.after_pushdown` (item 7): the corpus does
  not see it, but ten suite membership plans gain a CTE; the passthrough has
  to collapse before later phases build around it. The other five re-fires
  went.
