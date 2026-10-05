# Concept domain graph

`trilogy/core/domain_graph.py`, `tests/core/test_domain_graph.py`.

One environment-level directed graph over concepts, carrying TWO edge
families:

1. **Domain relations** — how concepts' VALUE SETS relate (subset / equal /
   declared-incomparable), with conditions on edges.
2. **Functional dependencies** — which concepts UNIQUELY DETERMINE which
   others, scoped to the population where the dependency holds.

Both families answer planner questions that were previously answered by
proxies reconstructed after the build had erased the underlying facts.

Domains are VALUE sets (NULL is never a member; nullability is a separate axis)
and row multiplicity is the FD family's job — the two never mix in one edge.
Declared edges are TRUSTED; structural and binding edges are true by
construction. Graph queries are deterministic (sorted traversals).

The sections below record what each migration step landed; code cites them.

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
  `process_query` (`subset_join_map`, `full_join_keys`, `equal_join_keys`)
  are now graph queries with unchanged outputs; canonical grouping is
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
  `scoped_canonical` parameters) is deleted.
  `UpgradeOuterFromKeySetEquivalence(domain_graph, narrow_equal_domain_joins)`
  derives what it still needs internally.
- Directional narrowing's declared-counterpart check is a `relation()`
  traversal (`_proven_subset_of`): chained relations and EQUAL hops
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
  subset side from physical scan identifiers × datasource binding scans,
  removing the optimizer's coupling to physical layout. Per-column stamps also
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
- **Residual**: rowset-boundary origin stamps (for shared-base self-join
  rowset keys) are not minted — those pairs render distinct addresses via the
  identity path, so nothing consumes them yet.

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
- SCOPE NOTE (owner, 2026-07-03): discovery FD paths were EXCLUDED from step 4
  while the planner migration was in flight. The legacy planner is gone; the
  exclusion is now just a note on what step 4 covered.

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
  narrowing; origin stamps are the substrate) and step 5 (plan-time
  `get_join_type` on the graph).

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
    (`_proven_subset_of`) against a complete,
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
  side's rows before the target join reads its values) predates this work;
  closed 2026-07-03 as unreachable (`test_predrop_chain_narrowing.py`).
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
- **Carve-out deletion**: the `subset_join_map`-derived stamp ignore sets
  are gone. `_complete_distinct` keeps only the closure semantics (the equivalence
  claim — relation stamps anywhere in the group disqualify, which is
  load-bearing: ignoring them there would let a scoped-LEFT subset side claim
  equivalence and upgrade to INNER). The directional claim has its own
  predicate `_own_coverage_partial(concept, cte, graph)`: exact-address
  stamps outside `graph.subset_sources()` (now cached) block; relation
  stamps speak to the relation. `_complete_values` takes the graph instead
  of ignore sets (the EQUAL-trust and preserved-base recursions are gone;
  `_pair_equal_declared` is the only EQUAL exemption). `subset_join_map` survives only for the
  same-address ORIGIN arbitration (its legitimate use).
