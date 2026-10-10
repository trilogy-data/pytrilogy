# v4-native network discovery — single-pass source search (design)

Status: **LANDED — THE ONLY PLANNER**. The legacy recursive planner, its
`CONFIG.use_v4_discovery` switch and the pre-v4 cover search are gone; there is
nothing to compare against or fall back to. What remains is SHAPE and COST, not
correctness. The migration history (s32–s38: the ladder, shadow mode, the
burndown) lives in git history; this document keeps the current state, the
module layout, the rules the search enforces and why, and the carry-over list
every change must preserve.

Comparison figures against the legacy planner are dated measurements from the
migration, not reproducible checks.

## 0.10 Current state (2026-07-30, s50) — read this first

**Correctness.** The known-failing registry burned 30 → 0 over s39–s49 and was
deleted along with the legacy planner it tracked gaps against.

**An empty registry is not the same as no gaps.** s50's size audit found TPC-H
q02 silently DROPPING a WHERE atom: `supplier.nation.region.name = 'EUROPE'`
was hosted on the internal `[@condition]filter` group of
`min(supply_cost ? region = 'EUROPE')`, so it restricted the aggregate's INPUT
and never the output population. The battery passed only because no
non-European supplier ties the European minimum at sf=0.1 or sf=1. Two fixes:

- **`_nested_scope_swallows_atom` (`condition_placement.py`).** A d1 scope sits
  lineage-UPSTREAM of the statement's rows, so `_upstream_most` elects its
  groups over a ROOT that could host the atom just as well. That is harmless
  exactly when the scope's value re-enters the outer plan keyed BY the atom's
  concept. The atom is forced onto a non-nested host when (1) a candidate
  FILTER scope's own condition already implies it, AND (2) no nested candidate
  exposes the atom's row inputs in its GRAIN. q02 fails (2) (grouped by part,
  joined back on part); q30/q30-alt/q81 satisfy it (grouped BY state, joined on
  state), and forcing them out costs a second `web_returns` scan. Pinned by
  `tests/core/processing/test_v4_condition_placement.py`.
- **`_resolve_root_condition_sources` seeds the node's own row identity.** The
  guard query (`tests/modeling/tpc_h/query02-region.preql`) still returned 100
  rows against 47: ANY aggregate in the WHERE reproduced it. An aggregate
  WHERE-arg is a terminal no datasource binds, so `_network_source`
  intentionally declines and the fallback sources the WHERE args in a separate
  search — which was seeded only with the args' grain keys, materializing
  `region.name` at (region, part) and rejoining on part alone. The search is now
  also seeded with the node's grain (or its KEY outputs when a fresh ROOT scan
  has none), falling back to the unseeded search when that identity is
  unbindable.

**Two lessons worth keeping.** The second defect first read as a merge-join-key
bug; isolating it against a query with NO filter scope and a plain `min(...)`
moved the cause to the condition-source fallback. Reproduce the ingredient,
don't read the SQL and guess. And a regression test written to guard one bug
found a second, unrelated one in the same shape.

**Predicate audit (s50).** Deleting each AND-atom from a statement's WHERE and
diffing the regenerated SQL (byte-identical = the atom did nothing) swept both
corpora: q02 no longer appears; tpch q18 `order.id is not null` and tpcds q11's
`channel`/`year` atoms are implied by construction (q18's only by an INNER join
on that key — latent fragility if it ever goes LEFT); tpcds q11
`billing_customer.sk is not null` was confirmed wrong rows (an aggregate grouped
BY a nullable key reached through a LEFT join kept its NULL group), since fixed
and pinned by `test_not_null_on_aggregate_grain_key_is_enforced`.

**Flags.** None. The network search IS sourcing.

**Size** (2026-07-30, both benchmark corpora, v3 vs v4 rendered chars):

| suite | v3 chars | v4 chars | ratio | smaller | identical | larger |
|---|---:|---:|---:|---:|---:|---:|
| TPC-DS (109) | 524,631 | 450,223 | **0.858** | 43 | 36 | **30** |
| TPC-H (22) | 30,789 | 31,974 | **1.038** | 8 | 8 | **7** |

Against hand-written reference SQL both planners sit at ~2.3×, which is the real
headroom. On TPC-DS, repeated table scans went 113 → 66 and dedup GROUP BYs
83 → 59, against unfolded passthrough CTEs 24 → 36 and split aggregates 33 → 39.
The two debts are one coupling: a same-parent dedup bucket beside an aggregate
gives the scan two consumers, which blocks `collapse_single_parent` from folding
either back in.

**Generation cost (s51).** Measure call counts, not wall-clock (dev-machine runs
vary 45–75 s). q05, the widest request, went 370 M → 85 M function calls with
the number of enumeration states and `_label_chain_state` walks unchanged — the
search explored the same space and stopped recomputing inside it:

- **Truncation is not silent.** `SearchResult.truncated` derives from a typed
  `SearchLimit`, `_network_source` logs it, and `SearchResult.exhausted`
  separates a truncated search that still has a plan from one that emitted no
  cover at all (not a decline — it makes no claim that no solution exists).
- **Identical searches are memoized** on `SourceNetwork.signature()` (terminals,
  candidate bindings, grains, join requirements, axis families — no build
  information) in `V4History.search_cache`, scoped to one build request.
- **Cover-independent quantities are memoized on the network:**
  `functional_into`, `row_complete`, `full_binders`, `chain_completers`.
- **Branch-and-bound has no cost spread to prune.** q05's 4,096 covers
  (`COVER_LIMIT`) reduce to exactly ONE source set: a 7-source base cover times
  the powerset of 12 partition ARMS, each a legal partial `cover` satisfier
  later discarded by `_reduce`. The waste is arm-vs-union branching, not a
  missing bound.

**Bridging is an obligation.** There is no greedy bridge fabricator: `connected`
is an `ObligationKind` discharged by the same machinery as every other invariant
(`network_obligations.compute_pending_obligations`), so every alternative bridge
enters dominance.

**Open, in priority order.** (1) The passthrough/split-aggregate coupling above —
highest measured size value. (2) Search cost: arm-vs-union branching. (3)
Predicate pushdown back onto scans (TPC-H q20's `CANADA` is deferred past a
join). (4) The recorded S4/S5 and provider-choice shape families. (5) Only the
`SENSITIVE`/`IMPLIED_EXACT` condition labels exist; richer labels as dominance
inputs were never read and were deleted. (6) Emission is still an adapter: the
search prices `completions` and `partial_terminals`, but `_network_source`
passes only `sources` and `connectors` to the bridge emitter, which re-derives
the rest — so the search and the emitter can disagree.

To check that a planner change moved no SQL, use `local_scripts/sql_ab/`.

### Module layout (s57)

The search outgrew a single `network_search.py` (1,964
lines). It is now a layered stack under `v4_helper/`, ordered so nothing above
the bottom layer can reach a build model:

| module | holds |
| --- | --- |
| `network_model` | the vocabulary — `SourceCandidate`, `SourceNetwork`, `Obligation`, `SolutionCost`, and the answers derivable from the labels alone |
| `network_build` | stage A: labeling the network. **The only module that reads build models** |
| `network_coalescing` | stage A: presence-probe pinning and union-join axis families |
| `network_topology` | what a chosen set of sources looks like — components, blends, declared-relation pairing |
| `network_obligations` | what a partial cover still owes |
| `network_search` | stages B/C: enumerate, reduce, cost, choose |

That layering is the point, not the line counts: "this module is pure — it
selects sources and reports why, but builds no StrategyNodes" used to be a
claim in a docstring and is now an import boundary. `network_topology` is
shared by obligations and cost deliberately — the search DEMANDS a structure
that the cost CHARGES for its absence, and a predicate stated twice lets the
search discharge an obligation the cost still charges for (the merged-key
predicate had five spellings before s55).

### The second cover search is gone (s58)

Until s58, `plan_source` ended in a pre-v4 cover search: after `_network_source`
declined, `_direct_source` -> `gen_select_node` -> `gen_select_merge_node` ->
`_source_concepts_via_graph` ran `create_pruned_concept_graph` +
`resolve_subgraphs` over the reference graph and merged whatever came back. That
is an independent search with none of the search's connectivity rules, and it
could accept a cover the network had just judged disconnected (the `ON 1=1`
plan behind `test_duckdb_derived_key_union_lookup`: a union scan merged with a
lookup regrouped to a single non-key property). It is deleted. `_direct_source`
keeps ONE role: rendering a solution the network already found (a one-scan
answer, or one the bridge emitter cannot carry), and it is called only behind a
`NetworkDecision`. The search's verdict on a cover is final.

An instrumented pass over the corpora and the test chunks recorded every
request the fallback used to answer. They were four shapes, each of which now
has a home:

- **Additive rollup at a coarser grain** (a summary at (origin, destination,
  date) serving (origin, date)). The graph's rollup edges are drawn at each
  metric's declared grain, so the network never saw the binding; only the
  fallback recomputed `get_additive_rollup_concepts` per request.
  `network_build.rollup_concepts_by_node` now labels the candidate with the
  request-grain binding (FULL, not stored), `_network_source` draws the same
  edge on the bridge's private graph so the emitter's neighbor walk attaches
  it, and `_merge_component_sources` SUM-rolls at the merge through the same
  `aggregate_rollup.merge_rollup_concepts` the legacy merge used. A one-scan
  rollup still renders through `_direct_source`, whose graph carries the
  rollup edges; that block in `create_pruned_concept_graph` stays for it and
  for the grand-total shape below.

  Extracting that predicate exposed a gap in it. It asked only whether some
  target component was unreached by the merge's grain, which is also true of
  a merge that reaches the target grain exactly and carries dimension
  ATTRIBUTES beside it: thelook q17 reads a (user, product) pair rollup
  alongside both keys' attributes, and grouping there re-sums one row per
  group for an identical answer and a spurious GROUP BY. It now also requires
  that a grain component actually be summed AWAY (`merge_components -
  target_components` non-empty), which is what distinguishes rolling a
  per-customer count up to region from reading a pair rollup at its own
  grain. The guard corrected the legacy merge too: thelook q18's outer
  `sum(...) GROUP BY` over a CTE already at the requested grain is gone. That
  is the ONE plan change in the corpora (below).

  A second, worse defect in the same binding came out of adversarial review
  rather than any suite. A summary is only a legal source for a rolled
  aggregate if every filter the statement applies is applied BEFORE the roll,
  and a binding cannot carry that requirement: it says "this source can
  produce that address" and the emitter routes predicates on its own. Given
  `select origin_region, flight_count where flight_date = ...` against a
  summary keyed (origin, destination, date), the roll summed every row and the
  plan then INNER-joined it to a filtered, non-distinct fact scan and re-summed
  -- dropping the filter and fanning the aggregate out in one step (`west 6,
  east 2` over a five-row table). `rollup_concepts_by_node` now withholds the
  binding whenever `filter_finer_row_args` sees a filter below the target
  grain, leaving the shape to `_plan_finer_filter_rollup`, which serves it
  safely by PINNING one datasource that carries the aggregate and the finer
  column together. The filter is usually not on the request that asks for the
  aggregate -- `gen_root` re-plans the row scan unconditioned and applies the
  WHERE above -- so `SourceRequest.deferred_conditions` carries the dropped
  clause for LABELING only, and joins the network verdict cache key so two
  requests differing only in what was deferred cannot share a verdict. The
  information has to travel: the unconditioned sub-request is otherwise the
  same request as an unfiltered query, whose correct answer IS the rollup.
  Inside the planner, `_deferred_conditions(request)` is the one seam a
  sub-request that drops the WHERE reads (this request's clause AND whatever
  it was already deferring); `gen_root` sets it at the top.
- **A connector alone as the cover.** A `connector~` candidate binds the merged
  key's class, reads zero scans and so out-prices the one scan holding the
  column (`select l_key subset join web_cust.cust_sk = l_key`); the emitter,
  with nothing to scan, declined. `network_search._reads_a_scan` refuses a
  cover with no scan in it. Pinned at the search level in
  `test_v4_network_search.py` and end-to-end by
  `test_subset_join_rowset_onto_root.py`.
- **A request with no join axis** (grand-total aggregates, a `<*>` watermark).
  Single-row concepts are dropped from the terminals, so the search had nothing
  to connect and declined without judging anything. `plan_source._no_join_axis`
  routes such a request straight to the render role: a cross product of scalar
  scans is its meaning. It is defined as "`terminal_addresses` is empty", so it
  cannot drift from what the search actually drops.
- **The error surface.** `validate_query_is_resolvable` lived only on the
  fallback path. `plan_source` now calls the same helper on a decline, so a
  requested ROOT concept bound nowhere under any spelling raises
  `NoDatasourceException` instead of returning `None` into a render.

Also gone: the `union_derived_concepts` injection in
`create_pruned_concept_graph`, added by the derived-key union fix purely to keep
the fallback consistent with the network's union candidates. The `[v4]` decline
log no longer speaks of "falling through to the single-scan planners". The two
typed tails after a decline (`_cross_component_source`, the unconditioned
retry) remain: an instrumented pass showed the retry answering conditioned
rollup requests in the discovery suite and the cross-component assembly firing
in the engine suite; neither fires on TPC-DS generation.

## Connectivity is a graph-correctness requirement (s34)

Connectivity was first modelled as "the chosen sources form one component",
which is far too weak: any shared key satisfies it, so a dimension can attach to
the fact side through a 3-valued discriminator while the key that actually
identifies it sits unclaimed on a candidate nobody selected (q05). In authored-join
suites, a merged key sourced ONCE satisfies coverage while one side of the
declared equality has no way to produce it. The fix builds the spanning
structure out of the keys that IDENTIFY rows and requires every declared
relation to be materialized on both sides.

### Rule 1 — the minimum-blend spanning tree (`network_topology.blend_joins`)

`joins_functionally(a, b)` asks whether the keys two sources share cover ONE
side's grain. If they do the join is a lookup: it can restrict, never multiply.
If they cover neither it is a **blend** — legitimate when two facts are related
only through conformed dimensions and nothing finer exists (`BRIDGE_MODEL`), a
wrong-rows defect when something finer does (q05). Which one it is, is a
property of the whole cover, so the cost is the number of blend edges in a
**minimum-blend spanning tree**: functional edges are free, and blends are only
paid where no functional path exists.

Minimising over spanning trees rather than summing over pairs makes this
**un-launderable**: an extra source adds a node the tree must span, so it can
only lower the count by supplying a functional PATH — co-locating the key, which
is the actual fix. Bolting on an unrelated source to manufacture keys leaves the
fact↔dimension edge as it was and costs a source, so it is dominated. It is a
COST, not a connectivity predicate, so it forbids nothing: the `BRIDGE_MODEL`
fact↔fact blend still plans, it just prices at 1 where nothing can do better.

### Rule 2 — declared relations are paired on both sides (`unpaired_join_keys`)

`JoinRequirement` carries a declared relation's canonical build address plus
EACH side's own keys. A side the solution does not touch imposes nothing; a side
it reads through a carrier that cannot produce the merged key has dropped the
authored equality, and the sides then pair on whatever they happen to share
(`sku` in the q17/q25 shape). It is the leading cost axis. The declared
relations come from `relevant_authored_join_pairs`, the same source of truth as
`inject_authored_join_key_terminals`. The gap was never the key algebra
(build-time canonicalization already unifies ⊑/≡ endpoints); it was that nothing
required the key to appear twice.

### The `paired` and `colocated` obligations

Neither rule can be satisfied by dominance alone, because coverage-driven
enumeration stops branching on an address the cover already binds: a merged key
is bound the moment one side's dimension is in, and a dimension's grain key is
bound by the dimension itself, so the better cover is never generated. Both are
asked for explicitly as obligations (`ObligationKind.PAIRED` /
`ObligationKind.COLOCATED` in `compute_pending_obligations`):

- `paired` adds the far-side hop of a declared relation.
- `colocated` adds, for a source none of whose joins covers its grain, a
  candidate that binds that grain AND joins functionally to a source OTHER than
  the blended one — otherwise the blend has moved rather than closed.

This makes the rules *reachable* rather than order-dependent: gcat's
`test_aggregate_optimization` plan `fuel_aggregates + launch_info` is exactly a
blend co-location (`launch_info` supplies the keys the aggregate table lacks).

### `_reduce` is minimality over VALUES *and* STRUCTURE

Both rules would be undone by a profile-only minimality test: q05's hybrid
provides no value the dimension does not, and the far-side dimension scan
provides no value the near side does not. `_reduce` refuses any drop that
leaves an obligation pending — redundant means redundant for both.

### A one-scan solution belongs to `_direct_source`

Routing a single-source solution through the bridge emitter drops the `GROUP BY`
that collapses a scan read at finer grain than the request (`subset join`
rowset-onto-ROOT, duplicate rows). A union candidate stays on the bridge path.
`_network_source` returns a typed `NetworkDecision`: `None` is a DECLINE, while
`bridge=None` is a success whose renderer is `_direct_source`, which
`plan_source` calls directly so a multi-source join cannot beat a cover the
search already judged sufficient (q23).

## What MUST carry over

These are accreted bug fixes. Every one of them is load-bearing; losing any is a
silent wrong-rows regression, not a build error.

- **Single-row / abstract-grain concepts never drive connectivity** (they join
  by cross product; driving the search with them invents a spurious join key
  and raises false ambiguity). The terminals drop
  `Granularity.SINGLE_ROW` concepts.
- **`__preql_internal` concepts are declared `SINGLE_ROW`**, so the same
  filter keeps them out; there is no separate name test.
- **Derivation purge**: CONSTANT / AGGREGATE / FILTER nodes are not path
  material — EXCEPT a mandatory concept whose canonical is
  datasource-materialized (a summary table binding `count(x) by k` makes that
  aggregate directly selectable; without the exemption the dijkstra seed
  references a deleted node → `NodeNotFound`).
- **`filter_downstream`**: a derived concept whose parents are all being
  searched for is decomposable — but BASIC concepts directly bound to a
  datasource column, and ROWSET outputs, are exempt (a rowset is one opaque
  unit and anchors a join like ROOT).
- **BASIC non-`ATTR_ACCESS` edges are expensive** (weight 50 today): routing a
  join through a computed expression is worse than through a stored column.
- **Non-BASIC merge origins are supplied by `_derived_connector_nodes`**, never
  by a raw scan, and the datasource gap-fill must stand down for them (a
  re-pointed datasource lets the bridge scan the merged key directly and
  strands the connector). A BASIC merge origin computes inline and is fine.
- **The connector's own mandatory set carries uncovered bridge concepts whose
  grain components are a subset of the origin's grain** (s15: `orders.amt`
  riding the window CTE, otherwise INVALID_REFERENCE at render).
- **Union sources** (graph nodes since `union_sources` at generation) and union
  exact-match semantics: a child whose partition the conditions fully satisfy beats its
  union.
- **Partial markings survive onto built nodes** — they drive the partial→FULL
  join contract downstream. The solution must carry them explicitly.
- **`complete where` partials are conditionally full**: under a query implying
  `non_partial_for`, the partial is the PREFERRED (pre-filtered, smaller)
  source and its WHERE is pushed onto its scan so `partial_is_full` clears the
  flag. Its partiality is never dominance evidence.
- **A Steiner solution can traverse a node minted in another build scope** (a
  rowset body's key under different scoped joins); it proves connectivity but
  cannot be planned here; resolve what this scope knows and drop the rest.
- **Synonym handling**: pseudonym mates must be re-added or a side never
  materializes its own member and the equality drops out of the merge join
  (q05 fan-out). The network path needs no re-injection;
  `reinject_common_join_keys_v2` covers the `select_merge_node` render path.
- **Determinism**: no `hash()`-derived ordering anywhere; sort by address /
  datasource name; stable tie-breaks (the s29 discriminator lesson).
