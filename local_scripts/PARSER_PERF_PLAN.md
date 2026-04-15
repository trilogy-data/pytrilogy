# Parser & Compile Performance Plan

Living doc tracking the v2 parser perf roadmap. Baseline captured on
`parser-perf` branch, 2026-04-15.

## Baselines

Harnesses:
- `local_scripts/profile_test_queries_parse.py` — parse-only (10 iter) + full (5 iter) on `tests/modeling/tpc_ds/query3.preql`
- `tests/modeling/tpc_ds_duckdb/adhoc_perf.py` — parse+compile (25 iter) on a ~500-line TPC-DS script

Baseline numbers (pre-P0, pest backend):

| Run | Metric | Value |
|---|---|---|
| parse_only x10 | wall | 0.235 s |
| parse_only x10 | parse_syntax cum | 0.078 s |
| parse_only x10 | rust parse | 0.013 s |
| parse_only x10 | `_walk_inject` + `_scan_comments` | 0.028 s |
| parse_only x10 | `syntax_from_parser` | 0.033 s |
| parse_only x10 | hydration + imports | 0.178 s |
| adhoc_perf x25 | wall | ~30.2 s |
| adhoc_perf x25 | parse path cum | ~6.94 s |
| adhoc_perf x25 | `syntax_from_parser` tottime | 1.386 s |
| adhoc_perf x25 | `_walk_inject` tottime | 0.565 s |
| adhoc_perf x25 | `determine_induced_minimal_nodes` tottime | 0.709 s |
| adhoc_perf x25 | `create_pruned_concept_graph` tottime | 0.780 s |
| adhoc_perf x25 | `__build_concept` tottime | 0.628 s |

## Priorities

| P | Title | File(s) | Status |
|---|---|---|---|
| P0 | Fuse pest walk + comment injection | `trilogy/parsing/v2/pest_backend.py`, `trilogy/parsing/v2/syntax.py` | done |
| P1 | Emit `SyntaxNode`/`SyntaxToken` directly from rust | `trilogy/scripts/dependency/src/trilogy_parser.rs`, `trilogy/parsing/v2/pest_backend.py` | done |
| P2 | Process-level parse-tree cache | `trilogy/parsing/parse_engine_v2.py` | pending |
| P3 | Tighten `hydrate_rule` dispatch | `trilogy/parsing/v2/hydration.py` | pending |
| P4 | Flatten `SyntaxMeta` | `trilogy/parsing/v2/syntax.py` | pending |
| P6 | Memoize `__build_concept(address, grain_key)` | `trilogy/core/models/build.py` | pending |
| P7 | Cache undirected graph in Steiner path | `trilogy/core/processing/node_generators/node_merge_node.py` | pending |
| P8 | Reduce `ReferenceGraph.copy` in pruned graph | `trilogy/core/processing/node_generators/select_merge_node.py` | pending |

## Constraints

- **Do not cache `Environment` instances** — they are mutable-ish; parse-tree caching is fine.
- Comment retention can be more permissive than v1's strict "gobbler-only" rule, as long as export-time rendering still drops the non-gobbler ones. Permissive retention is acceptable if it unlocks a single-pass walk.
- After each P passes the full test set, commit and update this doc.

## Progress log

### P0 — 2026-04-15

Fused `_walk_inject` + `syntax_from_parser` into a single pass over the pest
tree (`_pest_to_syntax` in `pest_backend.py`). Comment reinjection happens
inline at gobbler boundaries; `SyntaxNode.children` switched from tuple to
list so trailing comments can still attach after the recursive build returns.

Result on `profile_test_queries_parse.py` (parse_only x10):
- `parse_syntax` cum: 0.078 s → 0.068 s (~13% faster)
- `getattr` calls: 104,620 → 81,340 (~22% fewer)
- walk tottime: `_walk_inject` 0.028 s + `syntax_from_parser` 0.033 s →
  `_pest_to_syntax` 0.050 s (~18% faster than combined baseline)

Full test suite: `pytest -m "not adventureworks_execution"` — 2144 passed,
17 skipped.

### P1 — 2026-04-15

Switched the pest backend from the `PestNode`/`PestToken` pyclass path to
the rust tuple emitter (`parse_trilogy_syntax_tuple`) and rewrote the
walker to consume tuples by subscript rather than duck-typed `getattr`.
Tuple shape: `(name, children_tuple|value_str, line, col, end_line,
end_col, start_pos, end_pos)` — node vs token distinguished by whether
`element[1]` is a `str`. Default-arg bindings hoist `SyntaxNode`,
`SyntaxToken`, `SyntaxMeta`, and the kind maps to fast locals, and
`SyntaxMeta` is constructed inline instead of via `from_parser_meta`.

Result on `profile_test_queries_parse.py` (parse_only x10):
- `parse_syntax` cum: 0.068 s → 0.038 s (~44% faster vs P0, 51% vs baseline)
- `_tuple_to_syntax` cum: 0.050 s (P0 walker) → 0.023 s (~54% faster)
- `getattr` calls in walker: 81,340 → 0 (dropped out of top 30)
- `parse_only` wall: 0.221 s → 0.190 s (~14% faster)

`parse_trilogy_syntax` and the `PestNode`/`PestToken` pyclasses are now
dead rust code (Python calls only `parse_trilogy_syntax_tuple`). Leaving
them in place for now — removal requires a wheel rebuild and is decoupled
from this P.

Full test suite: `pytest -m "not adventureworks_execution"` — 2144 passed,
17 skipped.
