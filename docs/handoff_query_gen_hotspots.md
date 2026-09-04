# Handoff: query-generation hotspots after the import store (2026-09-04)

## STATUS: not started (evidence from the dict_resolver_perf pass, PR #667)

Audience: an agent starting fresh on the next query-generation perf increment.
`docs/handoff_dict_resolver_import_store.md` removed import hydration from the
studio request path; what is left is planner- and optimizer-bound, and flat.
Nothing below is a single dominant cost, so pick by risk, not by size.

## How to measure

- Studio benchmark: `trilogy-studio-core/pyserver/scripts/benchmark_endpoints.py`
  run with `PYTHONPATH=<this checkout>` (the studio venv has its own
  site-packages pytrilogy; without the override it measures the released
  wheel). Payloads in `pyserver/tests/payloads/`. Judge by call counts under
  cProfile first, walls second; the box is spiky.
- Corpus gate: store off / cold / warm SQL sha256 over all 132 `query*.preql`
  in one process must stay identical, and the same 132 must render
  identically across `PYTHONHASHSEED` 0..3 (one subprocess per seed).

## Warm `generate_query` profile (tpch payload, 20 iterations under cProfile)

Total 1.07 s / 20 = 53 ms profiled (18.7 ms real). Shares of the request:

| region | share | calls / request | note |
|---|---:|---:|---|
| `process_query` | 86% | 1 | everything below |
| `search_concepts` -> `build_strategy_node` | 45% | 1 | planning |
| `merge_node._resolve` | 28% | 3 (6 nested) | `generate_joins` 10%, `calculate_joined_pregrain` 2% |
| `get_node_joins` | 10% | 6 | `resolve_join_order_v2` 3% |
| `optimize_ctes` | 14% | 1 | 18 rule-loop iterations |
| `reorder_ctes` | 4.5% | 18 | rebuilt per loop iteration |
| `gen_inverse_map` | 1.1% | 21 | rebuilt per loop iteration |
| `hide_unused_concept.optimize` | 2% | 6 | |
| `inline_datasource.optimize` | 1.2% | 24 | |
| `render_cte` | 8% | 6 | |
| `parse_text` (the query itself, 2 parses) | 7% | 2 | not import hydration |
| `extent_null_addresses` | 3.5% | 74 (recursive) | `join_resolution.py:151` |
| `utility.unique` | 3.5% | 432 | see below |
| `concepts_to_build_grain_concepts` | 5% | 87 | `build.py:489`, sorts each call |
| `source_bindings` | 4.5% | 450 | now bypassed by `dependency_nodes` (PR #667) |
| `inlined_alias_map` | 2% | 213 | property, sorts each call |
| `_io.open` | 0.6% | 6 | stdlib closure validation reads |

## Candidates, cheapest first

1. **`utility.unique` callers.** 432 calls per request: `CTE.__post_init__`
   (execute.py:157, 1120 calls/20), `CTE.__add__` (execute.py:296, 1800/20),
   `base_node.__init__` (920/20), `optimize_ctes` look_at (420/20). `unique`
   builds a dict per call. Most callers pass lists that are already unique;
   a cheap check (`len(set(...)) == len(...)` is still O(n)) will not beat
   it, so look at whether `__post_init__` needs to dedup at all when the
   constructor sites already do.
2. **`inlined_alias_map` / `concepts_to_build_grain_concepts`.** Recomputed
   per access; both sort. `inlined_alias_map` depends on `parent_ctes` and
   `inlined_parents`, which optimization rules mutate in place, so a cached
   value needs invalidation on those writes (there is no setter today;
   `add_dependency` / `replace_dependency` / `add_inlined_datasource` are the
   known writers, but rules also assign `cte.parent_ctes = ...` directly:
   `merge_irrelevant_group_by.py` does). Grep for `parent_ctes =` before
   caching. `concepts_to_build_grain_concepts` is pure over its inputs and
   could memoize on a tuple of addresses.
3. **Stdlib text cache.** `ImportHydrationService._read_closure_text` reads
   the six stdlib files from disk once per parse (text_lookup is per parse).
   A process-wide `{path: (st_mtime_ns, st_size, text)}` cache keyed off
   `os.stat` removes it. ~1% here; more on container disks. Left out of
   PR #667 because it adds a staleness surface for a small win.
4. **`optimize_ctes` incremental graph.** `reorder_ctes` +
   `gen_inverse_map` rebuild the whole CTE dependency graph on every rule
   loop iteration (18-21 per request). Rules return whether they acted; an
   incremental update (or rebuilding only after an action, which is what the
   `while not complete` loop already tracks via `actions_taken`) would cut
   most rebuilds. Check `_optimization_visit_order` too. This is the largest
   self-contained win but touches the driver every rule depends on: run the
   corpus gate and the full suite.
5. **Planner.** `merge_node._resolve` and `get_node_joins` are the biggest
   pieces but they are the algorithm; `extent_null_addresses` recursing 74
   times per request (`join_resolution.py:151`) is the one spot that looks
   like repeated work over the same inputs. Profile before touching; the
   v4 planner memories in MEMORY.md list the shapes that broke last time.

## Gates

`tests/parsing/test_import_env_store.py`, the corpus store gate, the four-seed
determinism gate, `tests/modeling/tpc_ds_duckdb/test_queries.py` +
`tests/modeling/tpc_h`, then the full suite
`-m "not adventureworks_execution and not clickhouse_server"` (35 min here).
