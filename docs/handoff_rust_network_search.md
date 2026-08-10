# Handoff: Rust port of the v4 network-search walk

## STATUS: increment 1 LANDED (2026-08-03, s67)

The recommended first increment shipped: the walk + obligations + topology run
as one Rust unit (`trilogy/scripts/dependency/src/network_search.rs`, exposed
as `enumerate_network_covers` on `_preql_import_resolver`).
`_enumerate_covers` now calls Rust, passing the network's labels as plain
strings/bools once per search and reading the budget globals at call time so
test monkeypatching keeps working. The Python walk survives as
`_enumerate_covers_py` — the executable spec — and
`TestRustWalkParity` in `test_v4_network_search.py` pins the two to exact
equality (covers, order, reported limit) including under truncation, where
push order decides which covers survive. Determinism carries through interning:
node and address ids are assigned in sorted-name order, so integer comparison
reproduces Python's string ordering in every satisfier sort and the
`(len, identity)` tiebreak.

Gates run 2026-08-03: unit 54/54 (incl. 3 new parity tests), corpus
byte-identity **132/132** vs a baseline captured from the unported tree,
modeling suites 484/484, ruff/mypy/black/cargo test clean.

Measured (release build, same process A/B vs `_enumerate_covers_py`,
best-of-3): q23 walk slice 0.061s -> 0.005s (12x; gen wall 0.267 -> 0.210s),
and a 96-candidate soft-branch lattice that truncates at COVER_LIMIT (4096
covers) 0.79s -> 0.033s (24x). Corpus-wide walk slice 0.481s -> 0.105s;
median query unmoved (~76ms), as predicted. Not yet ported (still Python, per
the plan below): `_reduce`, cost, `_solution_for`, `_split_terminals`,
`_seed_cover`. The Rust walk does not populate `network._obligation_cache`,
so `_reduce` runs against a cold memo — MEASURED harmless (2026-08-03):
corpus-wide `_reduce` wall is 16ms cold vs 15ms walk-warmed over 616 calls,
with exactly 1 obligation recompute inside `_reduce` on the whole cold-cache
corpus, because `_reduce` short-circuits on the profile and connectivity
checks before ever asking `pending_obligations`. Not a lever for increment 2.

---

Written 2026-08-03 (s66), immediately after the search-perf hardening passes.
Audience: an agent starting fresh on porting the v4 source-search hot kernels
into the existing Rust extension. Treat the CURRENT Python implementation as
the spec — the walk's semantics changed in s66 (level-order + dominance +
seed fallback) and any design notes older than that are stale.

## Goal and how success is judged

Move the enumeration walk and its per-state machinery to Rust. Success is
judged **within the search slice, not against total generation** (explicit
maintainer directive): search-heavy queries (tpc_ds q23 spends ~0.27s in
generation, of which the walk is ~0.1s) and, more importantly, the worst
case — a pathological wide model
burns the 10k-state budget in ~4.5s of Python today; in Rust with bitset
states that budget should cost milliseconds, which changes what the budget
*means* (brute force to conviction instead of capped guessing).

Do NOT judge by corpus medians: the median query's search is ~6 states and
will not move.

## What makes this portable

The search layer is deliberately **pure over strings, sets and ints** — no
build objects cross into it. This is a structural property the module
docstrings assert and `SourceNetwork.signature()` enforces (nothing
build-scoped may be smuggled through). The layering:

- `network_build.py` — the ONLY module that reads build models. Builds the
  `SourceNetwork` (candidates with bindings/grains/conditions as labeled
  data). **Stays in Python. Do not port.**
- `network_model.py` — the vocabulary: `SourceCandidate`, `Obligation`,
  `SolutionCost`, `SearchResult`, plus lazy memo tables (`_partners()`,
  `binder_set`, `bound_terminals`, `_adjacency`, `chain_completers`). Pure.
- `network_topology.py` — `components` / `joined_pairs` / `blend_joins` /
  `unpaired_join_keys`. Pure set/union-find machinery.
- `network_obligations.py` — `compute_pending_obligations`: the per-state
  cost center (COVER / AXIS / PAIRED / LABELABLE / COLOCATED / CONNECTED).
- `network_search.py` — `_enumerate_covers` (the walk), `_reduce`,
  `_binding_profile`, `_solution_for`, cost ordering, `_split_terminals`
  (certificate), `_seed_cover` (fallback), `search_sources` (orchestration).

Recommended port boundary, first increment: **the walk + obligations +
topology as one Rust unit** — intern candidate node names and addresses to
u16 indices at entry, represent states as bitsets (candidate pools are
~20–170 nodes after reachability pruning), return covers as index lists.
Keep `_reduce`, cost, `_solution_for`, `_split_terminals`, `_seed_cover` in
Python initially: they run per-cover (dozens), not per-state (thousands),
and `_seed_cover` calls `_reduce`. Port them later only if profiles say so.

## Semantics that MUST survive the port exactly

These are recent, deliberate, and gate-verified. Divergence = wrong plans.

1. **Level-order walk**: states processed in ascending set-size levels;
   within a level, first-pushed pops first. Push order is load-bearing — it
   fixes which covers survive `COVER_LIMIT` truncation.
2. **Dominance prune, PROPER-superset only**: a popped state is skipped
   (visited, not expanded) when some emitted cover is a *proper* subset with
   an identical binding profile. Profile = per-terminal bound level
   (2 full / 1 partial / 0), axis-aware (`_bound_level`).
3. **Branch choice**: the pending obligation with the fewest satisfiers,
   ties broken by `identity` — deterministic. Satisfier lists are sorted at
   mint time; preserve their order in pushes.
4. **Visited dedup by state SET** — same set reached along different
   discharge orders is one state.
5. **Budgets**: `STATE_LIMIT = 10_000` visited states (sized empirically:
   max successful corpus query uses 826, median 6), `COVER_LIMIT = 4096`
   emitted covers. Truncation must report WHICH limit (`SearchLimit`).
   Tests monkeypatch `network_search.STATE_LIMIT` — whatever the Rust entry
   looks like, the Python-side override must keep working.
6. **Soft branches**: after emitting a cover, for each terminal not fully
   bound, push cover ∪ {full binder} for each full binder, in `binders()`
   (sorted) order.
7. **Winner selection**: `min(solutions, key=(cost.axes(), sources))` —
   fully order-independent; keep it that way.
8. **The rungs before the walk** (Python, keep): `unreachable` (terminal
   with no binder), `split` certificate (no single pool component holds a
   binder for every terminal → proven decline, `SearchResult.split`), and
   AFTER the walk the lazy `_seed_cover` fallback (only when the walk
   produced no solution; never competes with walk solutions — letting it
   compete was tried and changed 4 corpus plans, all larger).

## Where the time actually goes (measured, s66)

Per-state cost after the Python optimization passes: `pending_obligations`
dominated by the COLOCATED scan (set intersections against
`functional_partners`), the LABELABLE chain walk (`_label_chain_state`), and
`components` (union-find over `join_partners`). All pairwise predicates are
precomputed once per network into adjacency sets (`SourceNetwork._partners`)
— in Rust these become bitset rows and the per-state loops become AND/OR
words. q23's big search: 772 states visited, 260 obligation computations
(the rest dominance-pruned), 534→~50 covers. The lattice has fan-in ≈
fan-out: pop-time pruning cannot shrink `visited`, only per-state work —
in Rust, consider push-time dominance (profile check per push), which was
measured break-even in Python but is nearly free with bitsets.

## Existing Rust infrastructure

- Crate: `trilogy/scripts/dependency/` (`Cargo.toml` there;
  `pyproject.toml [tool.maturin] manifest-path` points at it).
- `src/graph.rs` → `GraphCore`, exposed as `PyGraphCore` in
  `src/python_bindings.rs`; consumed by `trilogy/core/graph.py`, which is a
  NetworkX-compatible `DiGraph` shim (NetworkX-style exceptions included).
  This is the "rust networkx backend" — precedent for the binding style.
- Python extension module name: `_preql_import_resolver`; lib name
  `trilogy_parser`; crate `trilogy-parser`. Three names, on purpose — do
  not "fix" this.
- Build: `maturin develop` in the project venv (`.venv/Scripts/python.exe`
  on Windows). Wheels go through `.scripts/build_backend.py`. pyo3 is
  pinned at 0.26 for the wasm/pyodide wheel builds — do not bump casually.
- Prior art verdict (s55): a port was once rejected when the search was
  9.9% of generation. The judgment basis has changed (see Goal), but reread
  that analysis before repeating its measurements.

## Non-negotiable gates (run ALL, in this order, per increment)

1. Unit: `tests/core/processing/test_v4_network_search.py` (51 tests; the
   budget tests monkeypatch `STATE_LIMIT` and `_seed_cover`).
2. **Corpus byte-identity** — the primary gate. Render every
   `query*.preql` under `tests/modeling/tpc_ds_duckdb` (109) and
   `tests/modeling/tpc_h` (23); record sha256 of generated SQL per query;
   diff against a baseline captured from the UNPORTED current tree (capture
   it yourself first — gate against the tree you branched from, not any
   stored golden). 132/132 identical or the increment does not land.
3. Modeling suites:
   `pytest tests/modeling/tpc_ds_duckdb/test_non_benchmark_queries.py
   tests/modeling/tpc_ds_duckdb/test_queries.py
   tests/modeling/tpc_h/test_tpch_queries.py tests/join_matrix`
   (483 tests, result-validating, ~2.5 min).
4. `ruff check --select E,F,I <changed>`, `mypy trilogy`, `black .`,
   `cargo test` in the crate.
5. NEVER run two pytest processes concurrently (shared on-disk fixtures
   produce phantom failures). A/B against a `git worktree add --detach`
   copy — never `git stash`/`checkout` in the shared tree.

## Perf measurement protocol

The dev box is spiky: judge by **call counts and state counts first**, walls
second (best-of-N, quiet box). A corpus wall sweep and per-query state
instrumentation existed as session scratch (probe records
`len(network._obligation_cache)` per query as the states proxy; wall sweep
renders each query twice, keeps the best, splits env-parse from generation)
— they are ~60 lines each to recreate from this description. Current
reference numbers (2026-08-03, quiet box): gen median 76ms / p90 166ms;
q23 ~0.27–0.31s (search ~0.1s); unsourceable q64-nested repro 0.25–0.30s
(certificate path, must not regress); 10k-state budget walk ~4.5s.
