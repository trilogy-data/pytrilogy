# Handoff: fresh generation-cost audit (s72)

A from-scratch re-profile of query GENERATION after the s66–s71 push (nested-env
delta, domain-graph memo, rust walk, parse-layer caches). Three wins landed; the
rest is a measured, prioritized map of what is left.

## Scoreboard (same harness, same box, this session)

| metric | before | after |
|---|---:|---:|
| corpus total (132 queries, min-of-3) | 12.655s | **9.349s** (1.35x) |
| median | 64ms | **50ms** |
| p90 | 168ms | 133ms |
| max (q77 / q64) | 1154ms | 855ms |
| **under 50ms** | 48/132 | **68/132** |
| corpus profiled | 35.26s | **25.80s** |
| total function calls | 60.9M | 57.7M |

## Method

`scratchpad/sweep.py` — every `query*.preql` under `tests/modeling/tpc_ds_duckdb`
(109) and `tests/modeling/tpc_h` (23), one process, **fresh `Environment` per
query**, timing split four ways: env construction / `parse_text` / `_generate`
(processing) / `compile_statement` (render). `corpus_prof.py` cProfiles the same
sweep for attribution; `hashes.py` dumps per-query SQL sha for the byte-identity
gate.

## The shape of a corpus render (pre-session, min-of-3 per query)

| layer | seconds | share |
|---|---:|---:|
| env construction | 0.008 | 0.1% |
| parse | 0.785 | 6.2% |
| **processing** | **11.349** | **89.7%** |
| render | 0.512 | 4.0% |
| total | 12.655 | |

median 64ms, p90 168ms, max 1154ms (q77), **48/132 under 50ms**.

Parse is no longer in the frame — s68–s71 took it to 6%. Everything below is
processing.

Attribution (cProfile over the same sweep, seconds cum, before → after this
session's three landings):

| | before | after | share (after) |
|---|---:|---:|---:|
| `process_query` | 33.9 | 24.8 | 100% |
| ├ `get_query_datasources` | 28.8 | 20.7 | 83% |
| │ ├ `_get_query_node_v4` (search + planning) | 21.3 | 14.5 | 58% |
| │ └ `materialize_baseline` | 6.0 | 5.3 | 21% |
| └ `optimize_ctes` | 4.25 | 3.47 | 14% |

After the three landings, **`materialize_baseline` and `optimize_ctes` are a
larger share than they were** — the search shrank around them. Findings A–D
below are ordered against the post-change profile.

## Landed

### 1. `build_fd_closure` was recomputing 4.6x

`_fd_facts` is memoized per `BuildEnvironment` (s53) but **the closure itself
was not** — and it is a fixpoint loop over every entry in the environment, the
single largest `tottime` item in the corpus (2.43s of 35.3s, 9.2%).

Census: **5,640 calls, 1,216 distinct `(environment, determinants,
include_empty_grain)` keys.** The memo lives on `_FDFacts` itself, so it needs
no new soundness argument (the closure is a pure function of the facts) and no
new eviction story (it dies with the facts entry).

- computations **5,640 → 1,216**
- `build_fd_closure` cum **3.26s → 0.99s**

### 2. `_emitted_addresses` walked each node's neighbors three times

`build_source_network` already builds `emitted_by_node` for every datasource
node; `_probe_offers` and `_candidate_for` then re-derived the same sets from
the graph. The graph does not change between them, so both now read the map.

- calls **47,942 → 17,172**
- `build_source_network` cum **3.95s → 3.25s**

### 3. `_network_source` dropped ~457 graph nodes one call at a time

Reducing the copied reference graph to the chosen cover ran `remove_node` per
node — **180,623 calls corpus-wide**, each a separate core crossing plus a
structure-cache invalidation. `ReferenceGraph.remove_reference_nodes` does the
batch in one crossing.

- `remove_node` **181,804 → 1,181** calls
- `_network_source` cum **7.88s → 5.60s**

**Do not** spell this as an override of `remove_nodes_from`. The first attempt
did, and the corpus gate failed three queries with `KeyError` on a concept
node: the inherited `remove_nodes_from` deliberately leaves `concepts` /
`datasources` populated, and `select_merge_node` / `node_merge_node` read those
maps for nodes they have already removed. That asymmetry with `remove_node`
(which does prune them) is a latent trap worth resolving separately — under a
name of its own it is at least explicit.

**Gate (all three, together):** 132/132 SQL byte-identical against a
`git worktree` at HEAD (`hashes.py` run in both trees), full suite
`-m "not adventureworks_execution"` = 7137 passed / 110 skipped / 1 xpassed / 0
failed, ruff `--select E,F,I` / mypy (348 files) / black clean.

## Open, in priority order

### A. The per-statement floor is O(environment), not O(query) — the real 50ms blocker

Measured directly (`scratchpad/floor.py`): `select store_sales.item.brand_name
limit 10` against a **337-concept** env costs **15.5ms** to generate. Profiled
split of that floor:

| | share of a trivial generation |
|---|---:|
| `materialize_baseline` (394 units) | 35% |
| `generate_graph` / `generate_adhoc_graph` | 15% |
| `assemble_full_graph` (domain graph) | 13% |
| everything else incl. the actual search | ~37% |

Every one of those is a full pass over the environment. On the full 1,537-concept
tpc_ds env the same fixed cost is ~40ms — which is why the median sits AT 50ms
after this session's wins and 64/132 queries still miss it. **No amount of search
tuning gets under this floor.** s66 already fixed the equivalent problem inside
the network build with
`_relevant_nodes` (BFS from the requested addresses); the open question is
whether materialization + graph construction can be scoped the same way, or
whether the closure of a fact-table query is simply most of the model.

### B. A baseline is rebuilt per scoped-join set, and ~99% of it is identical

s67's baseline+delta shares one materialization across nested arms **under one
join set**. A different join set forces a full rebuild (measured pre-session, so
the totals are the pre-change ones):

| query | baselines | cost | divergence between join sets |
|---|---:|---:|---|
| q77 (1154ms total) | 4 | 315ms | **14 of 1,537** entries |
| q64 (1124ms total) | 3 | 443ms | **19–24 of 1,020** entries |

The same delta machinery applies, but the change-detection footprint is harder:
across join sets a unit's result can differ through `scoped_merge_map`,
canonical collapse and pseudonym stamping, not only through `local_concepts`.
`_env_shell` (domain graph, `scoped_join_key_groups`, `scoped_partial_derived`)
must be recomputed regardless. Worth ~250ms on q77 and ~280ms on q64 — the two
worst queries — and ~3.6% corpus-wide.

### C. The cross-statement bundle cache cannot survive a fresh `Environment`

`_SESSION_CACHE_STORE` is keyed by `id(environment)` with a `content_version`
stamp, so two `Environment` objects holding *the same objects* (the s68 import
store shares them) share nothing. Every statement re-materializes.

Corpus-wide this is worth nothing (each query imports a different model subset),
but it is exactly the serve/LSP/CLI-repeat case the 50ms goal is about. A
content signature — `(namespace, tuple of (address, id(concept)), tuple of
(name, id(datasource), status), alias map, join key)` — is ~0.1ms to compute
against 16–80ms of materialization. Note `content_version` is a per-instance
counter, **not** a content hash: equal counters across two instances prove
nothing, so it cannot be the key by itself. Needs a bound on the store (the key
is a value, so there is nothing to weakref).

### D. `optimize_ctes` is 14% of processing, and `render_cte_used_map` is half of it

Counts below are from the pre-session profile; the optimizer is untouched, so
they hold — only its share grew (12.5% → 14%) as the search shrank.

| rule / step | calls | cum |
|---|---:|---:|
| `HideUnusedConcepts` | 850 | 1.56s |
| └ `render_cte_used_map` (all callers) | 1,057 | 2.01s |
| `reorder_ctes` | 2,906 | 0.56s |
| `inline_datasource` | 6,292 | 0.43s |
| `filter_irrelevant_ctes` | 2,906 | 0.33s |
| `collapse_single_parent` | 3,096 | 0.25s |

`render_cte_used_map` renders a CTE to throwaway SQL to learn which parent
columns it consumes. Census: 1,057 calls over **340 distinct CTEs**; of the 717
repeats, **671 returned an identical map and 46 did not** — so a naive
`id(cte)` cache is wrong 4% of the time. It needs a mutation stamp on `CTE`,
which is the real work item.

`reorder_ctes` + `filter_irrelevant_ctes` run once per phase (22 phases per
statement, 0.89s combined) whether or not the phase changed anything. Skipping
them on `phase_changed == False` is a no-op **only if no rule mutates while
reporting `False`** — and `reorder_ctes` also calls `canonicalize_graph`, which
mutates. That precondition is an 18-rule audit, so it was left alone here
rather than bought with a corpus gate that would only probably catch a
violation. If taken up: hoist one cleanup pass before the phase loop first, or
the ordering invariant breaks for a plan where no phase ever fires.

### E. Small, mechanical

- `assemble_full_graph` 273 calls / 1.22s and `mint_fd_edges` 131 / 0.40s —
  minted edges are already cached per author env (s66); this is the ~2,500-edge
  re-insertion on top, and the assembled graph is a pure function of
  `(declared, minted)`. Worth ~0.2s.

## Harnesses (session scratchpad)

`sweep.py` (layer split), `corpus_prof.py` (aggregate cProfile), `prof.py`
(one query's process phase), `floor.py` / `floor_prof.py` (the trivial-query
floor), `hashes.py` (byte-identity gate, run in a worktree for the base leg),
`probe_fd.py`, `probe_baseline.py`, `probe_joinset.py`, `probe_render_map.py`
(the censuses quoted above).
