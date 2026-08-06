# Handoff: generation cost, s73 (continues s72)

Picks up `docs/handoff_generation_audit_s72.md`. Four wins landed; the
handoff's headline open item (A — scope materialization to the query's
closure) is **measured and closed as not-viable on this corpus**.

## Scoreboard (same box, this session, min-of-3 per query)

| metric | s73 start | s73 end |
|---|---:|---:|
| corpus total (132 queries) | 10.785s | **9.147s** (1.18x) |
| parse | 0.703 | 0.594 |
| processing | 9.575 | **8.120** |
| render | 0.498 | 0.426 |
| median | 56ms | **48ms** |
| p90 | 145ms | **112ms** |
| max (q77) | 1061ms | 872ms |
| **under 50ms** | 53/132 | **71/132** |

The s72 numbers (9.349s / 50ms median) were taken on a differently-loaded
box; only the two columns above are comparable to each other. Every leg was
run in one process against the current tree, per
`feedback_gate_against_tree_not_goldens`.

**Gate for all four changes together:** 132/132 SQL byte-identical against
the pre-session baseline (`hashes.py`, same tree, before/after); full suite
`-m "not adventureworks_execution"` = **7133 passed / 110 skipped / 1
xpassed / 0 failed**; ruff `--select E,F,I` clean, black clean, mypy clean
(348 files).

The suite exceeds the 10-minute per-command ceiling (1585s in one process),
so the final leg was run as six sequential chunks partitioned over
`tests/*`. The chunk totals sum to exactly the single-process totals, which
is the check that the partition lost nothing. Run them **sequentially** —
concurrent pytest processes share the `tests/modeling/*/memory` duckdb files
and produce phantom failures.

## Landed

### 1. `assemble_full_graph` was rebuilding an identical graph 275x

s66 cached the *minted* edges per author environment, but the assembly on
top — re-inserting ~2,500 edges into a fresh `DomainGraph`, each through
`add_edge`'s identity/dedup path — ran every call.

The assembled graph is a pure function of `(declared edges, minted edges)`
and no consumer mutates a built graph (`with_overlay` copies), so
`_ASSEMBLED_CACHE` keys `id(environment)` -> `(minted tuple, {declared edge
identities: graph})`. Freshness rides on the minted tuple's **object
identity**: `_minted_edges` returns a new tuple whenever the environment's
mutation stamp moves, so `entry[0] is minted` proves the whole entry without
a second stamp comparison. Eviction is the existing `_MINTED_CACHE` weakref
callback, extended to drop both maps.

- `assemble_full_graph` cum **1.22s → 0.78s**

### 2. The concept-graph recursion inserted nodes one at a time

s72 identified this and left it: `generate_adhoc_graph` already batched its
per-datasource edge inserts, but `add_concept`'s recursion called
`g.add_node` / `g.add_edge` per node, each a Python→Rust crossing plus a
structure-cache invalidation.

`_GraphSink` accumulates the recursion's nodes and edges and flushes through
`add_nodes_from` / `add_edges_from`. The subtlety is **ordering**: node
order is observable downstream, and the core creates edge endpoints on
demand, so the sink records a node on *first touch* — an edge endpoint
counts — which reproduces the per-call insertion order exactly. The
pseudonym-dedup check that read `in g.edges` now reads the sink's edge set,
since the edges are no longer in the graph when it runs.

- `add_node` crossings **140,988 → 33,683**
- `add_edge` crossings **35,623 → 17,841**

### 3+4. `HideUnusedConcepts` re-rendered the same CTEs every fixpoint loop

s72 measured 1,057 `render_cte_used_map` calls over 340 distinct CTEs and
concluded a naive `id(cte)` cache is wrong 4% of the time, needing "a
mutation stamp on `CTE`, which is the real work item."

**The stamp is unnecessary at this call site.** Optimizer rule instances are
phase-local — `optimize_ctes` calls `phase.make_rule()` once per phase — so
a cache on the *instance* lives exactly one phase. Within a phase the only
mutations that can occur are this rule's own `hidden_concepts` writes; the
4% divergence s72 measured is across phases, which a per-instance cache
never spans. Each write evicts the mutated object, plus any cached
`UnionCTE` whose render included it (`_unions_of`, registered transitively
for nested unions — a union's used map is a function of its branches'
renders).

- `render_cte_used_map` calls **1,057 → 585**, cum **2.01s → 0.81s**
- `render_cte` (all callers) **1,717 → 1,186** calls, cum **2.98s → 2.04s**

The same trick does *not* transfer to the other four `render_cte_used_map`
callers unexamined: `inline_datasource` and `join_hoist` mutate CTE
internals their own rules don't own. Those still want s72's mutation stamp.

## A is closed: the closure of a fact-table query IS most of the model

s72's open question was whether materialization and graph construction can
be BFS-scoped to a statement's reachable set, the way s66 scoped the network
build with `_relevant_nodes`.

`closure_census.py` (scratchpad) measures it directly: BFS from each
statement's referenced addresses over lineage arguments, keys and pseudonyms,
with **whole-datasource pull-in** (a datasource is only usable whole, so any
datasource containing a closure concept contributes all its columns).

```
n=131   ratio min=0.09  p25=0.93  median=0.94  p75=0.94  max=0.96
        106 queries at ~0.9, 6 at ~1.0, exactly one below 0.7
```

**The median query needs 94% of its environment.** The environment is
already import-scoped, and a star schema is densely connected: pull in
`store_sales` and its dimension tables arrive with it. This deliberately
*overapproximates* — a real scoped materialization would need at least this
much — so a 0.94 median is decisive, not suggestive. Scoping the pass would
add bookkeeping to save 6%.

**Do not re-chase this.** The remaining floor levers are:

- **B (handoff s72)** — baseline+delta across *join sets*, not just nested
  arms under one set. Still the largest single item: ~250ms on q77 and
  ~280ms on q64, ~3.6% corpus-wide.
- **C (handoff s72)** — content-signature bundle cache surviving a fresh
  `Environment`. Worth nothing on this corpus by construction; it is the
  serve/LSP/CLI-repeat case.
- Making per-unit build cheaper, rather than running fewer units.

## Where the cost sits now (cProfile, 26.2s profiled)

| | calls | cum |
|---|---:|---:|
| `process_query` | 132 | 23.04 |
| ├ `get_query_datasources` | 132 | 19.97 |
| │ ├ `_get_query_node_v4` (search + planning) | 132 | 14.38 |
| │ │ └ `_network_source` | 397 | 5.30 |
| │ │   └ `build_source_network` | 376 | 2.49 |
| │ └ `materialize_baseline` | 143 | 4.82 |
| └ `optimize_ctes` | 132 | 2.57 |
| `parse_text` | 132 | 2.39 |
| `compile_statement` | 133 | 1.25 |
| `generate_adhoc_graph` | 202 | 1.97 |
| `build_fd_closure` | 5,626 | 0.92 |

`optimize_ctes` dropped from 14% to ~10% of processing on the back of the
render cache. The search and planning layer (`_get_query_node_v4`, 58% of
`process_query`) is where any further large win has to come from — and s72's
Rust analysis still holds for it: the boundary costs more than the compute,
so the port to target is the **topology layer as a resident handle**
(`_partners` / `components` / `join_keys`, 0.77s of Python self-time,
`join_keys` called 285,209x), not another labels-in/labels-out call.

## Harnesses

The s72 scratchpad was gone (session-scoped temp). Rebuilt from that
handoff's Method section, ~30 minutes: `sweep.py` (layer split, min-of-3),
`hashes.py` (byte-identity gate; takes a repo root so a worktree can produce
the base leg), `corpus_prof.py` (aggregate cProfile), `floor.py` (trivial-
query floor + its profile), `closure_census.py` (the A verdict above).

**Trap:** corpus `.preql` files contain `Comment` statements that
`generate_queries` raises `NotImplementedError` on. Filter them before
generating, or every harness dies on the first file.
