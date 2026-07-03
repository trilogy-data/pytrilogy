# Concept domain graph — design sketch (phase 3)

Status: **design, not started** (2026-07-03, owner-approved direction; start in
a clean session). Successor to the registry/stamp workarounds phase 2 landed
(docs/subset_union_join_design.md, "Deferred / residuals") and to the bespoke
grain/FD reasoning scattered through join and discovery machinery.

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
