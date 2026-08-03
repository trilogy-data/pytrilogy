# Audit — what should move to Rust (s51, 2026-07-30)

Basis: `local_scripts/v4_rust_candidates.py 109` — a cProfile of TPC-DS
generation for all 109 queries, taken AFTER the s51 search-cost memos. Percentages
are `tottime` share, which is machine-independent enough to rank by; the call
counts are exact. Read `feedback_measure_call_counts_not_seconds` first: seconds
on this box are not quotable, call counts are.

## Where generation time is now

| module | tottime | share | calls |
|---|---:|---:|---:|
| `network_search.py` | 62.21 s | **45.2%** | 121,762,413 |
| `<builtin>` (dict/set/any, mostly called FROM network_search) | 42.26 s | 30.7% | 173,169,466 |
| `build.py` | 6.94 s | 5.0% | 15,018,426 |
| `functional_dependency.py` | 6.68 s | 4.9% | 11,088,650 |
| `author.py` | 2.53 s | 1.8% | 2,859,412 |
| `domain_graph.py` | 1.78 s | 1.3% | 2,064,289 |

The existing Rust — `_preql_import_resolver.parse_trilogy_syntax_tuple` — is
11.86 s in **135 calls**. Parsing is already off the critical path per call; it
shows up only because it is a big fixed cost paid once per file.

## Ranking

### 1. `network_search.py` — the only large, clean candidate

Everything about it fits:

- **Pure by contract.** Its module docstring already states it: "This module is
  pure: it selects sources and reports why, but builds no StrategyNodes." It
  never touches a `BuildConcept` or a `StrategyNode` after
  `build_source_network` has labeled the network.
- **Plain data in and out.** Inputs are address strings, node-name strings,
  frozensets, three small enums and ints. The output `SourceSolution` is node
  names, frozensets of addresses and eight integers.
- **Coarse FFI boundary.** `search_sources(network) -> SearchResult` is called
  **274 times for both corpora**. One crossing per call is free. Contrast the
  inner loops it would absorb: `functional_into` 22.9 M calls, `join_keys`
  13.5 M, `_find` 13.4 M, `binds` 14.9 M, `_label_chain_state` 2.3 M.
- **The work is exactly Rust's strength**: set intersection, union-find,
  minimum-spanning-tree over a ≤46-node graph, and a LIFO fixpoint search.

Hot functions inside it, in order: `_label_chain_state` (2,291,822 calls,
30.3 s cumulative), `_compute_pending_obligations` (102,111 / 49.7 s cum),
`_find`+`_union` union-find (16.3 M), `functional_into` (22.9 M), `join_keys`
(13.5 M), `_blend_joins` (271,286 / 19.0 s cum), `_components`, `_reduce`
(42,720 / 28.2 s cum).

**But do the Python representation change FIRST.** Most of what Rust would buy
here comes from representation, not from the language:

- intern every address and node name to a dense `u32`/index once per network;
- represent each candidate's bindings, grain, and each cover's source set as a
  **bitmask** (a plain Python `int` is already a C-level bitset).

Then `join_keys(a, b)` is `masks[a] & masks[b]`, `binds_fully` is one bit test,
`_components`/`_blend_joins` are union-find over small ints, and `chosen |
{node}` is `chosen | bit`. Python does all of those in C. That is a contained,
testable refactor behind the existing 109/109 snapshot gate, it removes most of
the 173 M builtin calls, and — critically — **it is also the prerequisite for a
Rust port**, because the index/bitset form is exactly what you would hand across
the FFI. Measure after it; the remaining gap is the honest case for Rust.

Sequencing note: the arm-vs-union branching fix in
`handoff_v4_search_cost.md` reduces the *number of states* explored. Do that
first if both are on the table — it is a smaller change and it shrinks the input
to whatever optimization follows.

### 2. `functional_dependency.build_fd_closure` — fix in Python, do not port

5,208 calls, 5.71 s tottime, 7.92 s cumulative (~4% of generation). It looks
like a Rust candidate — a string-set fixpoint — but it is not, yet: it re-walks
`environment.concepts` and every datasource's `output_concepts` **inside the
`while changed` loop**, so it is paying for BuildConcept attribute access on
every iteration.

The plain-data path already exists next to it: `concept_attr_fd_closure` takes a
`dict[str, ConceptAttrs]`. The fix is to derive that table once per
`BuildEnvironment` (memoized on the environment or on `BuildCaches`) and route
`build_fd_closure` through it. That is a Python change worth doing before anyone
considers FFI, and it may remove the candidacy entirely.

### 3. Not candidates

`build.py` (5.0%), `author.py` (1.8%), `environment.py`, `build_environment.py`
— this is the model layer itself: constructing and traversing `BuildConcept`
graphs. Moving it to Rust means moving the object model, which is a rewrite with
a very chatty FFI boundary (15 M calls), not an optimization. `domain_graph.py`
(1.3%) and `graph.py` are already backed by `PyGraphCore`.

## Recommended order

1. `build_fd_closure` → the existing `ConceptAttrs` plain-data path, memoized
   per environment. Small, self-contained, ~4%.
2. Arm-vs-union branching (see `handoff_v4_search_cost.md`) — fewer states.
3. `network_search.py` intern-to-index + bitmask representation, in Python,
   behind the 109/109 gate.
4. Re-profile. Only then decide whether `search_sources` crosses into Rust —
   with the data to say what it would actually buy.

Gate for every step: `local_scripts/v4_sql_snapshot.py check` at 109/109 and the
full suite (`pytest tests -m "not adventureworks_execution"`, 6221 passing).
