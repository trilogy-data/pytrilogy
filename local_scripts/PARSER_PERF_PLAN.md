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
| P2 | Process-level parse-tree cache | `trilogy/parsing/parse_engine_v2.py` | done |
| P3 | Tighten `hydrate_rule` dispatch | `trilogy/parsing/v2/hydration.py` | done |
| P4 | Flatten `SyntaxMeta` | `trilogy/parsing/v2/syntax.py` | done |
| P6 | Cross-factory `_build_grain` cache | `trilogy/core/models/build.py` | done |
| P7 | Skip default weight writes + drop redundant `.copy()` in Steiner path | `trilogy/core/processing/node_generators/node_merge_node.py` | done |
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

### P2 — 2026-04-15

Added an `lru_cache(maxsize=256)` keyed on `(text, backend)` in
`parse_engine_v2._parse_syntax_cached`. Parse trees are effectively
immutable after construction (verified: no `.children.append/insert/[]=`
or attribute writes anywhere outside the walker), so sharing cached
`SyntaxDocument` instances across `parse_text` calls is safe.
`Environment` is **not** cached — it is mutated during hydration.

Result on `profile_test_queries_parse.py` (parse_only x10):
- `parse_syntax` cum: 0.038 s → 0.0003 s (~99% reduction, cache hits 11/12
  unique inputs on iter 2+)
- `parse_only` wall: 0.190 s → 0.160 s (~16% faster vs P1, ~32% vs baseline)

Full test suite: `pytest -m "not adventureworks_execution"` — 2144 passed,
17 skipped.

### P3 — 2026-04-15

Tightened `hydrate_rule` / `hydrate_token` dispatch: replaced
`isinstance(element, SyntaxToken)` with `type(element) is SyntaxToken`
(concrete dataclasses, no subclassing), inlined `self.rule_context()` →
`self._cached_rule_context` to drop a method call per rule, hoisted
`element.kind` to a local, and switched `if handler:` to
`if handler is not None:`. Nothing in the dispatch path allocates now
beyond the handler call itself.

Result on `profile_test_queries_parse.py` (parse_only x10):
- `hydrate_rule` cum: 0.061 s → 0.055 s (~10% faster)
- `parse_only` wall: 0.156 s → 0.148 s (~5% faster)
- `isinstance` calls: 54,830 → 47,300 (~14% fewer, matches the 6330
  hydrate_rule isinstances eliminated)

Full test suite: `pytest -m "not adventureworks_execution"` — 2144 passed,
17 skipped.

### P4 — 2026-04-15

Deleted the `SyntaxMeta` dataclass and inlined its six position fields
(`line`, `column`, `end_line`, `end_column`, `start_pos`, `end_pos`)
directly onto `SyntaxNode` and `SyntaxToken`. `SyntaxMeta` survives as a
type alias to the element type for backwards compat, and each class
exposes a `meta` property returning `self` so existing consumers that
read `element.meta.line` keep working without edits. The walker and
`syntax_from_parser` now construct nodes/tokens with positional args —
kwargs dispatch on a 9-field slots dataclass is ~2× slower than the
positional form.

One `meta is None` guard in `hydrate_concept_block` was tightened to a
direct `end_line is None` check since the property path is never None.

Result (micro-benchmark on TPC-DS query3, walker-only, 2000 iter):
- `_tuple_to_syntax`: 117 µs → 84 µs (~28% faster)
- `parse_pest` end-to-end: 635 µs → 575 µs (~9% faster)

`profile_test_queries_parse.py` wall (parse_only x10, cache-dominated so
the walker only runs once): 0.148 s → 0.140 s (~5% faster).

Full test suite: `pytest -m "not adventureworks_execution"` — 2144 passed,
17 skipped.

### P6 — 2026-04-15

Originally scoped as `__build_concept(address, grain_key)` memoization.
Profiling that path showed the existing `local_concepts` check already
catches ~1% of calls and — more importantly — the shared `build_cache`
catches ~50% but only after the expensive work runs. The real opportunity
turned up in `_build_grain`: 114k calls, only 214 unique by
`(frozenset(components), where_clause_id)`, 99.9% without a where clause.

Added a `grain_build_cache: dict[tuple, BuildGrain]` sibling to
`build_cache` in `Factory.__init__`, propagated it to every sub-factory
that already inherits `build_cache` (`_build_select_lineage` main +
where, `_build_datasource` under `datasource_build_cache`, the internal
where-factory inside `_build_grain`). Cache key is
`(frozenset(base.components), None)` — the with-where path is 0.1% of
calls and stays uncached to keep the key simple and correctness trivial.

Result on `tests/modeling/tpc_ds_duckdb/adhoc_perf.py` (25 iter):
- wall: 24.50 s → 22.66 s (~7.5% faster)
- `_build_grain` cum: 1.251 s → 0.717 s (~43% faster)
- `_build_grain` tottime: 0.198 s → 0.073 s (~63% faster)
- `__build_concept` cum: 2.937 s → 2.141 s (~27% faster; children
  dominated by `_build_grain`)

Full test suite: `pytest -m "not adventureworks_execution"` — 2144 passed,
17 skipped.

### P7 — 2026-04-15

Pivoted from full undirected-graph caching (not straightforward because
`search_graph` mutates across ambiguity-check iterations) to two
targeted fixes in `determine_induced_minimal_nodes`:

1. Dropped three redundant `.copy()` calls on results of `to_undirected`,
   `steiner_tree`, and `subgraph` — each of those already returns a
   fresh graph with an independent rust core and copied attrs, so the
   extra `.copy()` was a full second clone.
2. Skipped the per-edge `H.edges[edge]["weight"] = 1` default write.
   The original loop wrote weight 1 to every edge through the layered
   edge-view API (~874k `__setitem__` calls per 25-iter adhoc_perf run),
   even though `_weight_triples` already treats a missing `weight` key
   as 1.0. The replacement writes directly to `H._edge_attrs` only for
   the rare BASIC non-ATTR_ACCESS case (weight 50).

Result on `tests/modeling/tpc_ds_duckdb/adhoc_perf.py` (25 iter):
- wall: 22.66 s → 20.79 s (~8% faster)
- `determine_induced_minimal_nodes` cum: 6.189 s → 4.507 s (~27% faster)
- `determine_induced_minimal_nodes` tottime: 0.647 s → 0.339 s (~48% faster)
- `clone_graph` calls: 3000 → 2250 (the three dropped `.copy()`s)

Full test suite: `pytest -m "not adventureworks_execution"` — 2144 passed,
17 skipped.
