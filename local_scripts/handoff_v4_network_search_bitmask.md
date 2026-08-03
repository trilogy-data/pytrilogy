# Handoff — intern-to-index + bitmask representation in `network_search.py`

Owner: unassigned. Prereqs: none (the arm-vs-union prune it depends on landed in
s53, commit `743c6c6a2`). Expected size: one full session — this touches nearly
every function in an 1,800-line file, and the full suite is ~35 min per run.
Written s53 (2026-07-30). Parent: `handoff_rust_candidates.md` step 3.

## The one-paragraph version

`network_search.py` is **45% of TPC-DS generation tottime** (62.2 s, 121.8 M
calls) and drives most of the 30% that shows up as `<builtin>` dict/set/`any`
calls (173 M). It is not a slow algorithm — it is a fast algorithm over
string-keyed Python sets. Intern every address and node name to a dense `int`
once per network, represent bindings / grains / cover source-sets as **bitmasks
in plain Python `int`s**, and the hot predicates become single machine
operations that CPython already does in C. This is worth doing on its own
merits, AND it is the prerequisite for any future Rust port, because the
index/bitset form is exactly what would cross the FFI. Do not port to Rust
first: most of the available win is representational, and locking an FFI
boundary around the current string contract would be locking in the wrong one.

## Why this file and not another

From `local_scripts/v4_rust_candidates.py 109` (s51 profile, post-memo):

| module | tottime | share | calls |
|---|---:|---:|---:|
| `network_search.py` | 62.21 s | **45.2%** | 121,762,413 |
| `<builtin>` (mostly called FROM network_search) | 42.26 s | 30.7% | 173,169,466 |
| `build.py` | 6.94 s | 5.0% | 15,018,426 |

Hot functions inside it: `_label_chain_state` (2.29 M calls, 30.3 s cum),
`_compute_pending_obligations` (102 k / 49.7 s cum), `_find`+`_union` union-find
(16.3 M), `functional_into` (22.9 M), `join_keys` (13.5 M), `_blend_joins`
(271 k / 19.0 s cum), `_reduce` (42.7 k / 28.2 s cum).

Those counts predate the s53 arm-prune, which cut enumerated covers corpus-wide
from 41,303 to 3,450 (max in any one search 4,096 → 534). **Re-profile before
you start** — the ranking should hold but the absolute numbers will not, and you
want your own baseline anyway.

## What to build

Intern once per network, in `build_source_network`, after the candidate dict is
final:

- every equivalence-class address -> dense `int`
- every candidate node name -> dense `int`

Then:

- `SourceCandidate.bindings` keys, `.grain`, `network.address_grain` values,
  `binding_keys`, `full_binders`, `binders` -> `int` bitmasks
- cover source-sets (`chosen`) -> `int` bitmask; `chosen | {node}` becomes
  `chosen | bit`
- `join_keys(a, b)` becomes `masks[a] & masks[b]`
- `binds` / `binds_fully` become one bit test
- `_components` / `_blend_joins` / `_find` / `_union` become union-find over
  small ints
- the memo caches (`_join_key_cache`, `_binding_key_cache`, `_binder_cache`,
  `_reach_cache`, `_obligation_cache`, `_full_binder_cache`, `_completer_cache`,
  `_functional_cache`, `_row_complete_cache`) become int- or
  int-tuple-keyed. This is a large part of the win: `_obligation_cache` is
  currently keyed by `frozenset[str]`, so every lookup hashes a set of strings.

Keep the PUBLIC contract in strings. The module's consumers —
`source_planning.py` (`build_source_network`, `search_sources`, `SourceNetwork`,
`SearchResult`, `CONNECTOR_NODE_PREFIX`) and `concept_strategies_v4.py`
(`SearchResult`) — must keep seeing addresses and node names. Convert at the
boundary in `SourceSolution` construction. The interning table is an
implementation detail of one search.

## Traps — read these before writing any code

These are the ways this refactor silently changes plans. Each is real and
already documented in the file; a bitmask rewrite steps on all of them by
construction.

1. **Iteration order is load-bearing in `_enumerate_covers`.** There is an
   explicit comment there: "`binders` is sorted; the push order fixes which
   covers survive truncation, so it must not become set-iteration order."
   Bitmask iteration naturally yields low-bit-first, which is NOT the current
   sorted-by-name order unless you intern in sorted order. **Intern in sorted
   order and the two coincide** — do that deliberately and say so in a comment,
   or every truncated search changes its answer. Same applies to
   `min(pending, key=lambda o: (len(o.satisfiers), o.identity))`: the tiebreak
   is on the obligation identity STRING, so either keep identities as strings
   or make the index order agree with the name order.

2. **`SourceNetwork.signature()` must stay structural.** Its docstring is
   explicit: it holds only addresses and node names, never a `BuildConcept`, so
   a stale environment cannot be smuggled through the `V4History.search_cache`
   memo (`concept_strategies_v4.py:116`, used at `source_planning.py:319`).
   Indices are per-network and meaningless across networks — if the signature
   becomes index-based, two DIFFERENT requests can collide on it and one will
   silently get the other's solution. Keep `signature()` spelling out strings.

3. **`subsumed_arms` (added s53) must be interned alongside `axis_families`.**
   Both are input, not memo cells. `_prune_subsumed_arms` runs on every
   `_compute_pending_obligations` call and does `node in arms` /
   `arms[node] in present` — that becomes a bitmask test, and it is on the
   hottest path in the file.

4. **`ConditionFit.IMPLIED_EXACT` participates in `binds_fully`** (via
   `condition.partial_is_full`), so a binding's "full" bit is NOT a static
   property of the binding — it depends on the candidate's condition. Either
   precompute the full-binding mask per candidate at intern time (correct, and
   cheaper) or you will reintroduce the per-call branch you are trying to
   remove.

5. **Presence probes and connector nodes are not datasources.**
   `SourceCandidate.datasource` is `None` for `connector~*` nodes, and
   `_pin_unoffered_probes` MUTATES candidate bindings after they are built
   (adding `injected=True` bindings). Intern AFTER that mutation, or the masks
   will be missing the injected bits.

## Gate — the same shape as the s53 prune

`local_scripts/v4_arm_prune_ab.py` is the model: render the whole corpus twice
in ONE process, once with the change off and once on, and diff the two outputs
against each other. Make the change toggleable by one function you can
monkeypatch (there, `ns._subsumed_arms`; here, the interning entry point).

- `v4_sql_golden/` was current as of s53 (`v4_sql_snapshot.py check` = 109
  identical, 0 changed), so it is usable as a second gate — but confirm that
  again before leaning on it, and keep the tree A/B as the primary. See
  `feedback_gate_against_tree_not_goldens`.
- Expect **109/109 byte-identical on TPC-DS and 23/23 on TPC-H**. This refactor
  is a pure representation change; ANY plan drift is a bug in the refactor, not
  an improvement. That makes it an unusually clean thing to gate.
- Full suite: `pytest tests -m "not adventureworks_execution"` — 6243 passed /
  102 skipped / 6 xfailed / 11 xpassed as of s53. The xfail/xpass COUNTS are
  part of the signal.
- `mypy trilogy`, `ruff check --select E,F,I <changed>`, `black .`

## Measurement

Report **call counts, not seconds** — this box varies 45–75 s on identical runs
(`feedback_measure_call_counts_not_seconds`). Include an unchanged control row
(e.g. total states visited, or covers enumerated) to prove the search did the
same work and only the per-operation cost moved.
`local_scripts/v4_q05_profile.py <queryNN>` gives a cProfile with a state-count
control row; `local_scripts/v4_rust_candidates.py 109` gives the module table.

## After this

Re-profile and only THEN decide whether `search_sources` crosses into Rust —
with data on what is actually left. The FFI boundary is attractive
(`search_sources(network) -> SearchResult` is called just 274 times for both
corpora, so one crossing per call is free) but the case for it has to be made
against the post-bitmask numbers, not the current ones.

Sibling item from `handoff_rust_candidates.md` that is independent of this one:
`functional_dependency.build_fd_closure` (~4%) — being done in s53.
