# Handoff: generation cost — open backlog (s72/s73)

Scoping materialization to a statement's closure is **closed as not-viable**:
BFS from each statement's referenced addresses (with whole-datasource
pull-in) needs a median **94%** of its environment, so scoping would add
bookkeeping to save 6%. Do not re-chase it.

## Open

- **B — baseline + delta across join sets.** `Environment.materialize_baseline`
  is reused across nested-select arms under one scoped-join set, but each
  distinct join set rebuilds its baseline from scratch. Largest single item
  when measured: ~250ms on q77, ~280ms on q64, ~3.6% corpus-wide.
- **C — a build-cache bundle that survives a fresh `Environment`.**
  `_SESSION_CACHE_STORE` (`query_processor.py`) keys bundles by
  `id(environment)` plus a mutation stamp, so a re-parsed but identical model
  starts cold. A content-signature key would fix that. Worth nothing on the
  benchmark corpus by construction; it is the serve/LSP/CLI-repeat case.
- **Rust port of the topology layer as a resident handle.** Search and
  planning (`_get_query_node_v4`) was ~58% of `process_query`. The boundary
  costs more than the compute, so the target is `SourceNetwork._partners` /
  `network_topology.components` / `SourceNetwork.join_keys` (0.77s of Python
  self-time, `join_keys` called ~285k times over the corpus) held on the Rust
  side, not another labels-in/labels-out call. The existing bridge is
  `trilogy/scripts/dependency/src/network_search.rs`.

## Method

Measure call counts and min-of-3 timings in one process against the current
tree; gate on SQL identity with `local_scripts/sql_ab/`. Corpus `.preql`
files contain `Comment` statements that `generate_queries` raises
`NotImplementedError` on — filter them before generating.
