# Concept domain graph — design sketch (phase 3)

Status: **step 1 landed** (2026-07-03; `trilogy/core/domain_graph.py`,
`tests/core/test_domain_graph.py`). The graph exists and today's registries
are derived from it (see "Landed" below); steps 2–5 of the migration plan
remain. Successor to the registry/stamp workarounds phase 2 landed
(docs/subset_union_join_design.md, "Deferred / residuals") and to the bespoke
grain/FD reasoning scattered through join and discovery machinery.

## Landed (step 1, 2026-07-03)

- `trilogy/core/domain_graph.py`: `DomainEdge` (SUBSET/EQUAL/INCOMPARABLE ×
  declared/structural/binding provenance × global/statement/rowset scope ×
  optional condition), `BindingEdge` population facts, `FDEdge` with
  population scope, and `DomainGraph` with `relation()`, `determines()`
  (transitive FD closure over ≡-classes, complete-binding globalization),
  `with_overlay()`, and `contradicts()`.
- Author/build split (owner constraint): the author `Environment` stores NO
  graph and gains no serialized state — declared edges derive on demand from
  `merges` / statement join clauses. The full graph (declared + structural +
  binding + FD) is assembled per build and lives on
  `BuildEnvironment.domain_graph`.
- Registry compat shims: `Factory` (`scoped_merge_map`,
  `scoped_partial_sources`, `scoped_full_join_keys`,
  `scoped_left_anchor_keys`, `scoped_join_key_groups`) and
  `process_query` (`subset_join_map`, `subset_binding_sources`,
  `full_join_keys`, `equal_join_keys`) are now graph queries with unchanged
  outputs; `_build_scoped_merge_index` is dissolved into
  `DomainGraph.canonical_map()`. The EQUAL-vs-∦ distinction previously
  recovered via the `statement_full_keys` provenance hack is now a
  first-class relation label + scope tag.
- Author-time contradiction lint (`Environment._lint_merge_declaration`):
  a global merge declaring the reverse of a conditioned structural subset
  (the "reversed operands?" case) or conflicting with ∦ fails at parse.
- Structural minting covers FILTER derivations and rowset outputs (filtered
  body → conditioned ⊑, unfiltered → ≡); BASIC image edges deferred (open
  question 2's conservative default), union arms not yet minted.

Open questions settled: (1) no interning — conditions live on edges,
populations are (node, condition) pairs at query time; (3) FD scopes
reference binding populations by datasource identifier; (4) the overlay
rides the existing `scoped_joins` parameter path; (2) and (5) deferred as
written.

## Landed (step 2, partial, 2026-07-03)

- The narrowing pass is GRAPH-FED: `process_query` → `optimize_ctes` →
  `build_optimization_rule_plan` pass the statement `DomainGraph` (declared
  edges + author binding facts); the five-registry plumbing
  (`full_join_keys`, `equal_join_keys`, `subset_join_map`,
  `scoped_canonical`, `subset_binding_sources` parameters) is deleted.
  `UpgradeOuterFromKeySetEquivalence(domain_graph, narrow_equal_domain_joins)`
  derives what it still needs internally.
- Directional narrowing's declared-counterpart check is a `relation()`
  traversal (`_declared_subset_of`): chained relations and EQUAL hops
  resolve by ⊑-reachability over ≡-classes, subsuming the old
  canonical-group-membership approximation (and legitimately strengthening
  it: a concept EQUAL-merged onto a declared subset side now counts).
- Battery-verified identical: TPC-DS zquery SQL logs byte-stable; join
  matrix + scoped-join/rowset suites green.
- **2b.3 (origin threading, first tranche — landed)**: the build now threads
  origin domain nodes forward at the BINDING level:
  `BuildColumnAssignment.origin_address` records the authored address a
  scoped join/merge substituted onto the relation's canonical
  (`Factory._build_column_assignment`). The narrowing pass's same-address
  arbitration reads these stamps (`_side_origins`) instead of inferring the
  subset side from physical scan identifiers × datasource binding scans —
  `subset_binding_sources` and `_scan_identifiers` are DELETED, and the
  optimizer's coupling to physical layout with them. Per-column stamps also
  discriminate where identifiers cannot (one table binding several relation
  endpoints). 19 dependent tests (root-key scoped LEFT rendering, the
  readback family, permutation matrix) arbitrate; all green + battery
  byte-stable.
- **Finding**: the remaining conservative one-datasource shape (e.g.
  `left join emp.eid = emp.mid`, both endpoints in one table) degrades
  UPSTREAM of narrowing — the join planner renders the folded endpoint with
  the anchor's own column (`e = e`; row-correct only via grain uniqueness).
  Endpoint identity must survive source-binding selection in
  `get_node_joins` before any narrowing evidence can exist. Same seam as
  the pinned q59 shared-canonical bug. The binding-level origin stamps are
  the substrate for that fix.
- **Residual**: `partial_closure` / `ignore_partial_addrs` stamp carve-outs
  in `_complete_distinct`/`_complete_values` remain (they speak to the
  RELATION vs own-coverage split; deletable once completeness consults
  origin nodes + the graph directly). Rowset-boundary origin stamps (for
  shared-base self-join rowset keys) not yet minted — those pairs currently
  render distinct addresses via the identity path, so nothing consumes them
  yet.

## Landed (step 4, first consumer, 2026-07-03)

- `reduce_concept_pairs` consumes FD closure: a greedy pre-pass over the
  surviving determinant set prunes any join pair both of whose sides are
  functionally determined by the remaining joined keys
  (`domain_graph.determines`, threaded from
  `BuildEnvironment.domain_graph` at the `get_node_joins` call site). This
  closes the doc's "cannot see through bindings" gap — a dimension binding
  A and B completely at grain (A) now proves A → B globally and the
  redundant B pair drops from scan-level joins — plus transitive chains
  (A → B → C). Grain pairs and mutually-dependent determinants are
  protected (exactly one of a mutual pair survives). The existing property
  and grain-subset heuristics are retained unchanged alongside.
- New FD/cardinality matrix tier: `tests/join_matrix/test_fd_matrix.py`
  (junk-dimension enrichment: join must ride the determining key alone, no
  fan-out, oracle rows) + unit coverage in `tests/nodes/utility/test_joins.py`
  (through-binding, transitive, mutual, grain-protected).
- TPC-DS battery byte-stable (its multi-key joins are grain/property pairs
  the old heuristics already reduce) — the prune fires on shapes the old
  code could not see, never differently on ones it could.
- SCOPE NOTE (owner, 2026-07-03): v4 discovery FD paths are EXCLUDED from
  step 4 — v4 is a parallel migration target; drive v3 to the ideal state
  first.

## Landed (step 4 second consumer + step 3, 2026-07-03)

- Grain satisfaction consumes the FD closure:
  `_join_right_preserves_cardinality` (now taking the environment) accepts a
  right grain whose uncovered components the join keys functionally
  determine — the "join on A can never fan out B" proof — and
  `grain_satisfied_by_pregrain` accepts pregrain components the target grain
  determines (group-by on {A, B} reduces to {A}). Both use global-scope
  closure only (declared property FDs + complete-binding-globalized grain
  FDs); population-scoped trust is deliberately not extended here.
- Step 3: `declared_domain_relations` regenerates from the graph's declared
  edges — and this FIXED A REAL BUG: the merge-tuple reading checked the
  REVERSED containment for subsets (merge tuples store the anchor first, so
  `merge a into ~b` was validated as b ⊆ a). The adversarial proof data
  carries one exclusive value per side, so the old cells were
  direction-blind; they now assert the checked source is the declared subset
  side. The author-time contradiction lint (the other half of step 3) landed
  with step 1.
- TPC-DS battery byte-stable across all of the above.
- Remaining after this: the q59 endpoint-identity seam (upstream of
  narrowing; origin stamps are the substrate), the
  `partial_closure`/`ignore_partial_addrs` carve-outs, step 5
  (plan-time `get_join_type` on the graph), and — deferred with v4 — the
  discovery FD paths and step-5 unification.

## Landed (step 5 mechanical + endpoint-identity first tranche, 2026-07-03)

- **Step 5, mechanical half**: `get_node_joins` and the rowset enrichment
  paths consult `BuildEnvironment.domain_graph` directly
  (`outer_relation_keys()` for the FULL veto, `left_anchor_keys()` for
  anchors, `subset_sources()` for the rowset advertised-key partial); the
  `scoped_full_join_keys` / `scoped_left_anchor_keys` / BuildEnvironment
  `scoped_partial_sources` shim fields are DELETED. Battery byte-stable.
- **Step 5, ruling on the semantic half**: under the phase-2
  always-preserving flip, plan-time typing is deliberately conservative
  (any subset evidence → preserving render; narrowing restores direction
  with origin arbitration downstream). Per-side origin discrimination at
  plan time therefore cannot change the *type* decision — the remaining
  behavioral value of "join-pair rulings as graph queries" lives entirely
  in pair emission and column selection, i.e. the endpoint-identity seam.
  Step 5 and that seam are ONE project from here. `scoped_partial_derived`
  (graph facts × author derivations) stays until per-side origin nodes
  subsume it.
- **Endpoint-identity, first tranche (the one-table shapes)**:
  - Partiality de-smeared at BOTH address-level summaries: a datasource
    address bound complete AND partial (a relation folded a second endpoint
    onto it) is no longer partial for the source
    (`BuildDatasource.partial_concepts`, scan-node partials in
    `datasource_nodes.py`). Partiality is a binding-level fact.
  - `get_alias` picks the NATIVE (unsubstituted, non-partial) binding when
    several columns bind one address — the concept as itself never renders
    a folded endpoint's column.
  - Result: `left join eid = mid` / `subset join` with both keys in ONE
    table now resolves with exactly the two-table semantics (the anchor
    column is the unified axis). FULL/UNION one-table is REJECTED clean at
    build (the unified key must coalesce across two reads of the table —
    needs a two-instance plan discovery cannot yet produce); the error
    points at the double-import idiom, which works and is pinned.
  - Guards: `tests/test_scoped_join.py` — union-rejected, subset unified
    axis, outside-binding, double-import self-relation.
- **Recalibrated goal (owner, 2026-07-03)**: byte-stability of the TPC-DS
  SQL logs is NOT a requirement — correctness plus performance is. The
  graph should be used to safely unblock MORE optimized joins (INNER over
  outer, plain `=` over `is not distinct from`) wherever provable; log
  diffs that narrow joins are wins. First named target: q64 (33s→53s after
  the always-preserving flip; the readback/shared-base shapes that still
  render FULL).

## Landed (cross-CTE null-rejection propagation, 2026-07-03)

- `UpgradeJoinOnGuards` now consumes proofs from DOWNSTREAM CTEs
  (`_external_forced_map`, join_upgrade.py): per producer, the output
  addresses EVERY consumer forces non-null — via its WHERE proofs (plain
  single-source projections only; COALESCE masks a one-sided NULL), its
  rendered INNER equalities on the producer's columns, and transitively its
  own forced set through plain pass-throughs (gated to group keys across an
  aggregation). Opaque consumers (EXISTS reads, UnionCTEs, window-computing
  CTEs) kill the set — every consumer must reject, or none may. Application
  at the producer is gated the same way: group keys only when grouped, and
  single-source projections so an output-level rejection maps onto raw join
  columns.
- q64: the readback `LEFT OUTER JOIN busy` (four-key agg join) → INNER; the
  final `WHERE cnt_00 <= cnt_99` lives two CTEs downstream. Whole battery
  correct (106/106), exactly this one join changed — the rule is surgical.
- **Remaining q64 FULLs (rule B, designed not built)**: the four authored-∦
  `full join` FULLs in the enrichment CTE (`sedate`) carry the narrowing
  veto, but their subset direction is PROVABLE from the graph: the agg-side
  keys derive from `ss.store.id`/`ss.item.id`/address ids whose dimension
  bindings are complete, so agg ⊑ dim by construction → FULL→LEFT
  (preserving the dim side), whose dim-only rows the downstream now-INNER
  join then rejects → INNER end-to-end. Needs: value-set evidence
  (structural ⊑ through rowset/aggregate lineage + complete binding) applied
  where the ∦ veto currently blanket-blocks. The one-per-battery census
  (only q64's join changed) says same-CTE + cross-CTE null-rejection is
  exhausted; rule B is where the remaining FULLs are.
- Also mapped: q04 carries six `is not distinct from` equalities
  (null-safe reduction target), q09/q97/q77/q05 FULLs are partly legitimate
  channel unions.

## Landed (rule B + carve-out deletion, 2026-07-03)

- **Rule B — graph-proven subset narrowing through the ∦ veto**
  (value_set_join_upgrade.py / domain_graph.py / query_processor.py):
  - `DomainGraph.proven_subset(sub, sup)` — directed ⊑ reachability between
    ≡-classes that IGNORES ∦ declarations: the containment evidence alone.
    Same-class is deliberately unproven (EQUAL narrowing stays config-gated).
  - `structural_domain_edge` now also mints a structural EQUAL edge for a
    pure `alias()` BASIC (the identity image, trivially injective) — the hop
    that connects a rowset's aliased output (`select x as y`) to its source
    concept. Without it the q64 chains break at `local._agg_99_*`.
  - `process_query` assembles the FULL graph (`assemble_full_graph` over the
    declared overlay) for the optimizer — structural/binding edges were
    previously only on `BuildEnvironment.domain_graph`. Registry shims all
    filter DECLARED provenance, so canonical maps and veto sets are
    unchanged.
  - `optimize()` no longer blanket-skips ∦-touching joins: they fall through
    to `_narrow_directionally(graph_proof_only=True)` — only a proven ⊑ path
    (`_proven_subset_of`, né `_declared_subset_of`) against a complete,
    filter-free superset side narrows; the equivalence upgrade and stamp
    heuristics stay vetoed. The ∦ stays declared.
  - Same-address directional narrowing accepts a GENUINE coverage stamp
    (`_genuine_partial_stamp`): a `~`-binding partial on the sub side, at an
    address outside the graph's declared subset endpoints, absent on the sup
    side — the vehicle→launch enrichment shape. This is what flipped the two
    pre-existing gcat failures (`test_join_inclusion`,
    `test_joint_join_concept_injection`) to passing, plus q23 (fact→item
    LEFT→INNER), q77/q93 (store_sales↔store_returns FULL→LEFT), and
    gcat `test_join_discovery_two` (FULL→RIGHT preserving the dim —
    expectation updated).
  - q64: the 3 readback FULLs in `sedate` → LEFT OUTER (structural ⊑ chains
    `agg_99.* ⊑ alias ⊑ ss_rows_99.* ⊑ base` against complete dim sides).
    Battery 106/106; q64 single-run exec 37.2s vs 39.0s at clean HEAD.
  - NOT narrowed: the enrichment `customer FULL JOIN customer_address`
    (C_CURRENT_ADDR_SK = CA_ADDRESS_SK). Both bindings are complete-in-schema
    with no `~` and no declared/structural ⊑ — narrowing it would mean
    trusting undeclared scan completeness for a fact-FK, which
    `_authoritative_scan` deliberately rejects. A veto-refinement via origin
    stamps (unveto same-address pairs with no ∦-member origins) was built and
    REVERTED: it broke the veto's contract
    (`test_full_join_key_veto_blocks_upgrade`) and bought nothing — the pair
    fails the standard machinery anyway.
- **Chain null-extension guard** (`_null_extended_before`): a sub side's
  full-match claim is about ITS OWN rows; when the sub side is a chain
  member that an EARLIER outer join in the same CTE's FROM chain
  null-extended, the chain carries rows where the sub side's key is NULL —
  plain equality never matches them, so the target join's preservation is
  load-bearing and must stay. Caught by `test_join_grain`
  (stores→orders→products: store3's row has NULL product_id and only
  survives via the FULL); guards both `_narrow_directionally`'s
  left-matches-right direction and `_upgrade_to_inner`. Residual mapped but
  NOT built: the symmetric PRE-DROP hole (an earlier INNER dropping the sup
  side's rows before the target join reads its values) predates this work.
- **Scoped-join-body mint gate**: a rowset whose BODY carries scoped joins
  mints NO structural domain edges for its outputs — the collapse makes an
  output's domain the join GROUP's (a FULL body key is the union of both
  members, a proper superset of either), so neither ≡ nor ⊑ against the
  single content concept holds. A lying ≡ let rule B narrow the outer
  readback FULL of `with rs as full join a.aid = b.bid select a.aid as k`
  and drop union-only rows (`test_rowset_key_readback_full_k_aw`).
- **Same-class ⊑ proofs**: `proven_subset` accepts a same-≡-class pair only
  through STRUCTURAL equality (`_structurally_equal` — alias/unfiltered-
  rowset identity images, containment by construction); a class merged by a
  DECLARED ≡ is not a proof (EQUAL narrowing stays config-gated). Needed
  because structural ≡ edges now reach the optimizer and would otherwise
  erase declared ⊑ direction between rowset keys (the scoped-rowset matrix
  regression). Consequence: two identical unfiltered rollups zipped on a
  scoped LEFT now upgrade to INNER (twin-rollup, row-identical) — the
  matrix control cell's contract was updated to row-based.
- **Carve-out deletion**: `partial_closure` / `ignore_partial_addrs`
  parameters and the `subset_join_map`-derived ignore sets are gone.
  `_complete_distinct` keeps only the closure semantics (the equivalence
  claim — relation stamps anywhere in the group disqualify, which is
  load-bearing: ignoring them there would let a scoped-LEFT subset side claim
  equivalence and upgrade to INNER). The directional claim has its own
  predicate `_own_coverage_partial(concept, cte, graph)`: exact-address
  stamps outside `graph.subset_sources()` (now cached) block; relation
  stamps speak to the relation. `_complete_values` /
  `_equal_intersection_complete` / `_complete_via_preserved_base` take the
  graph instead of ignore sets. `subset_join_map` survives only for the
  same-address ORIGIN arbitration (its legitimate use).

One environment-level directed graph over concepts, carrying TWO edge
families:

1. **Domain relations** — how concepts' VALUE SETS relate (subset / equal /
   declared-incomparable), with conditions on edges.
2. **Functional dependencies** — which concepts UNIQUELY DETERMINE which
   others, scoped to the population where the dependency holds.

Both families answer planner questions that today are answered by proxies
reconstructed after the build has erased the underlying facts.

## Motivation

### Seam 1: partiality provenance (from phase 2)

Partiality is stamped by ADDRESS (`Modifier.PARTIAL` on a binding,
`partial_concepts` up the CTE chain), but partiality is a fact about a
RELATION. When several declared relations collapse onto one canonical key
group, the stamps land symmetrically and lose direction — the narrowing pass
cannot tell which side of a same-address join pair is the subset of *this*
relation. Phase 2 worked around it with a stack of registries
(`subset_join_map`, `scoped_canonical`, `subset_binding_sources`) and stamp
carve-outs (`partial_closure`, `ignore_partial_addrs`) in
`value_set_join_upgrade.py`. The datasource-identity workaround
(`subset_binding_sources`) reads *physical* lineage to guess *logical*
direction — it fails exactly where physical identity stops discriminating
(shared-base self-joins, two subset sides bound in one table) and couples the
optimizer to physical layout. All of these are shadows of one missing datum.

The flat pseudonym/canonical collapse is the root cause: it is an UNDIRECTED
equivalence class, and it destroys exactly the direction the planner needs.
The domain graph is the directed replacement — pseudonym pairs become labeled
edges, and any pseudonym pair resolves by traversal.

A sharpening observation (owner): a rowset output like `rs.k <- select a.aid
... where cond` is not "the same key as a.aid with baggage" — it is a NEW
concept whose relation to `a.aid` is *derivable*: filtered body → proper
subset carrying `cond`; unfiltered body → value-equal (grouping preserves the
value set). Declarations against it are brand-new associations between
distinct nodes. Treating it that way makes the same-address ambiguity vanish
by construction instead of needing arbitration.

### Seam 2: keys / cardinality (the FD family)

Today `property <k>.x` declares the concept-level FD k → x, and a datasource
`grain (k)` declares per-source row uniqueness — but the two never compose
into queryable knowledge. Binding key B as a column on a source with grain
(A) does NOT mint "A uniquely determines B" anywhere: the planner cannot see
that joining that source on A can never fan out B, that a group-by on {A, B}
reduces to {A}, or that the dependency composes transitively (A → B, B → C ⇒
A → C — *recursive cardinality reduction*). The knowledge exists at parse
time and is discarded.

Current hacks this feeds (inventory, to be replaced by graph queries):

- `reduce_concept_pairs` (join_resolution.py) — drops property join pairs
  whose keys are already joined, and grain-subset pairs: hand-rolled FD.
- `calculate_joined_pregrain` / `grain_satisfied_by_pregrain` — bespoke grain
  arithmetic that cannot see through bindings (the "<grain key> uniquely
  defines <other key>" gap).
- v4 discovery FD grain preservation
  (tests/core/processing/test_v4_fd_grain_preservation.py,
  test_v4_fd_dimension_merge_grain.py) — measure `by` keys vs functional
  inputs (discovery_utility.py:568) reasoned locally per call site.
- Fan-out guards in the join matrix rely on data-shape adversarial rows
  because the planner cannot *prove* no-fan-out statically.

## The structure

A DAG of ≡-classes over concept addresses. Union-find collapses declared/
derived equalities into classes; the partial order lives over classes.
Nodes are ORIGIN addresses (never the post-merge canonical — rendering
addresses are a display concern).

### Domain edges

`a ⊑ b` (subset), `a ≡ b` (equal), `a ∦ b` (declared incomparable), each with
a provenance tag and an optional condition:

| provenance | minted from | example | condition |
|---|---|---|---|
| declared | `merge a into ~b`, `subset join a = b` | a ⊑ b | — |
| declared | non-partial `merge a into b` | a ≡ b | — |
| declared | `union join a = b` / `full join` | a ∦ b (never narrow) | — |
| structural | filter derivation `x' <- x ? cond` | x' ⊑ x | cond |
| structural | rowset output, filtered body | rs.k ⊑ a.aid | body WHERE |
| structural | rowset output, unfiltered body | rs.k ≡ a.aid | — |
| structural | union()/UnionDatasource arm | arm ⊑ combined | arm filter |
| structural | BASIC derivation `f(x)` | image edge: dom(f(x)) = f(dom(x)) | — |
| binding | `~` column binding | source projection ⊑ concept | non_partial_for |
| binding | complete column binding | source carries the node's full domain | — |

Conditions on edges are how partiality-from-FILTERING stops being a special
case: a filtered population is just a subset edge carrying a `BoolExpr`, and
two filtered populations of one concept compare by `condition_implies` over
their accumulated edge conditions. This subsumes the narrowing pass's
`_accumulate_filter` / `_filters_equivalent` walk.

### FD edges (keys hang off the same nodes)

`{A, ...} → B` ("A-tuple uniquely determines B"), with provenance and — the
key subtlety — a POPULATION SCOPE:

| provenance | minted from | scope |
|---|---|---|
| declared | `property <k1,k2>.x` | global (the concept's full domain) |
| binding | datasource `grain (A)` binding column B | that source's row population — i.e. the domain node the binding edge grounds |
| structural | rowset/group derivation `select k, f(...) as m` | the rowset's population: k → m |
| structural | 1:1 derivations (alias, injective BASIC where provable) | inherited from input |

An FD that holds on a *population* is attached to the domain node for that
population, not to the bare concept — a dimension table's `grain (date_id)`
FD `date_id → week_seq` is global only because the binding is complete; a
filtered or partial source's FD holds on its subset node. Domain edges and FD
edges therefore compose: to use an FD during planning you traverse domain
edges to check the rows you're reasoning about fall inside the FD's scope.

Closure is computed lazily and transitively (Armstrong-style, but only the
fragment needed: transitivity + augmentation over ≡-classes). "Recursive
cardinality reduction" = closure queries: `grain {A} ∪ FD-closure ⊇ {B}` ⇒
group-by reduction, no-fan-out join proofs, redundant-pair pruning.

## Resolution semantics

For any pair of nodes (or CTE-carried populations) L, R:

- `relation(L, R)` → one of `⊑`, `⊒`, `≡`, `∦`, `unknown`, computed by
  traversal over ≡-classes with condition implication upgrading/downgrading
  along the way (L = x|c1, R = x|c2, c1 ⇒ c2 gives L ⊑ R).
- `determines(K, x, population)` → FD closure membership within scope.

Each CTE output tracks its **domain node**: origin address + accumulated
condition. This is provenance the CTE chain almost carries already — the
change is threading it FORWARD from the build instead of reconstructing it
backward from stamps. Join-pair rulings (plan-time typing AND CTE-level
narrowing) become graph queries against the two sides' nodes; the
same-address case is no longer special because the two sides map to different
origin nodes regardless of rendered address.

### Contradiction lint (free win)

Declared edges can contradict structural ones at build time: declaring
`a.aid ⊑ rs.k` while the structural edge says `rs.k ⊑ a.aid` (proper, body
filtered) is inconsistent unless equal — detectable the moment the
declaration lands, with a precise "reversed operands?" error. Similarly a `~`
binding on a concept whose only source is that binding, an `∦` between nodes
already connected by ⊑, etc. This catches lying-declaration bugs before any
data runs; `validate_domains` (trilogy/core/domain_validation.py) remains the
opt-in DATA check and gets regenerated per declared edge against origin nodes
(fixing its current clean-environment awkwardness).

## Consumers (what this deletes or simplifies)

| today | becomes |
|---|---|
| `subset_join_map` / `scoped_canonical` / `subset_binding_sources` registries (query_processor.py → value_set_join_upgrade.py) | graph queries |
| `partial_closure` / `ignore_partial_addrs` stamp carve-outs | gone — stamps replaced by domain nodes |
| `_accumulate_filter` / `_filters_equivalent` | edge conditions + `condition_implies` |
| `_complete_values` evidence stack (scan trust, passthrough, preserved-base, lineage transfer) | `relation(side_node, concept_node) ⊒` |
| `_equal_intersection_complete` EQUAL-trust recursion | ≡-class membership |
| `full_join_keys` veto registry | `∦` edges |
| `get_join_type` partials/nullables dicts (plan time) | same graph, one source of truth with CTE narrowing |
| `reduce_concept_pairs` property/grain heuristics | FD closure |
| pregrain/grain-satisfaction special cases that can't see through bindings | FD closure ("A → B" from source grains) |
| `scoped_partial_sources` / `scoped_partial_derived` build plumbing | derived FROM the graph (compat shims during migration) |

Nullability is NOT in scope: domains exclude NULL by definition (NULL is not
a value — phase 2 invariant), and the nullability axis stays exactly where it
is.

## Storage

- Environment-level (`environment.domain_graph`), sibling to `merges` /
  `alias_origin_lookup`; long-term the flat `pseudonym_map` is DERIVED from
  it for rendering rather than being the source of truth.
- Declared edges minted at parse; structural edges minted lazily from lineage
  at build (linear in model size: one per derived concept, one per binding);
  FD edges from property declarations + datasource grains.
- Query-scoped joins apply as a statement-level OVERLAY (copy-on-write view),
  never a mutation — same discipline as `scoped_joins` today. Rowset-body
  joins land in the overlay via the same collection path phase 2 added
  (`_collect_rowset_scoped_joins`).
- Must serialize with the environment (build caches, `datasource_build_cache`)
  and survive `with_namespace` / import re-namespacing — edge endpoints are
  addresses, so namespacing maps over them like everything else.

## Migration plan (strangler)

1. **Build the graph + derive today's registries from it** as compat shims
   (`subset_join_map`, `full_join_keys`, `scoped_partial_sources`, …). Pure
   refactor; `tests/join_matrix/` arbitrates. No consumer changes.
2. **Narrowing pass** consumes the graph directly (it is the consumer in the
   most pain); delete the value_set_join_upgrade registries and stamp
   carve-outs. Re-run the matrix + TPC-DS battery; expect the double-relation
   readback family and shared-base shapes to narrow where they conservatively
   stay FULL today (perf win — q64 is the timing target, 33s→53s post-flip).
3. **Contradiction lint + validate_domains** regeneration against origin
   nodes.
4. **FD edges + closure**, consumed first by `reduce_concept_pairs`, then by
   grain satisfaction (the "<grain key> uniquely defines <other key>" gap),
   then v4 discovery FD paths. New join-matrix tier: FD/cardinality cells
   (join on A, project B where A → B: assert no fan-out and no spurious
   group-by; the oracle already computes from row data).
5. **Plan-time `get_join_type`** consults the graph (last — widest blast
   radius), unifying plan-time typing and CTE narrowing on one source of
   truth.

Each step lands green independently; the matrix + battery are the harness.

## Invariants

- Domains are VALUE sets; row multiplicity is the FD family's job and the
  two never mix in one edge.
- NULL is not a value: no domain contains NULL; nullability is a separate
  axis (unchanged from phase 2).
- Declared edges are TRUSTED (lying declaration = author error; the narrowed
  plan drops violating rows — phase 2 ruling); structural and binding edges
  are true by construction; only declared edges need data validation.
- Graph queries must be deterministic and order-independent (sorted
  traversals), like the rest of join planning.

## Open questions (settle at session start)

1. Node identity for anonymous populations: intern condition-bearing nodes by
   (origin, canonical condition signature) or keep conditions purely on edges
   and represent populations as (node, accumulated-condition) pairs at query
   time? (Leaning: the latter — no interning, no cache-invalidation problem.)
2. How far to trust injectivity for BASIC image edges (x+1 injective, abs(x)
   not) — needs an operator property table; conservative default: image
   edges transfer COMPLETENESS (value coverage) but not DISTINCTNESS unless
   the operator is on the injective allowlist.
3. FD scope representation: attach to domain nodes (proposed) vs separate
   population objects; interaction with `accept_partial` resolution.
4. Overlay lifetime plumbing: today scoped registries thread through
   `Factory(scoped_joins=...)` — does the overlay ride the same parameter or
   a history/context object?
5. Whether `pseudonym_map` derivation can happen in step 1 or must wait for
   step 5 (it feeds address canonicalization everywhere).
