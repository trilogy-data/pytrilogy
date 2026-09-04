# Handoff: enable the cross-parse import env store for `DictImportResolver`

## STATUS: LANDED 2026-09-04 (branch dict_resolver_perf)

All seven items below are in. What changed, in the terms of the list:

1. **Gate.** `use_store` is true for `DictImportResolver`; the
   `IMPORT_ENV_STORE_ENABLED` flag still opts out.
2. **Key.** Dict-resolver store keys carry `hash(text)` of the target
   (`None` for filesystem keys, which are unchanged), so models sharing an
   address coexist.
3. **Closure validation reads through the resolver.** Closure deps are
   `ClosureKey(on_disk, target)`: an absolute path read off disk, or a dict
   resolver's canonical `content` key. `_store_lookup` takes a reader
   callable (`ImportHydrationService._read_closure_text`); a key absent from
   the dict invalidates the entry and never falls through to the filesystem.
4. **`copy_for_root`.** `DictImportResolver` no longer has its `content`
   narrowed per child. It carries a `prefix` (the importing file's dotted
   directory, set by `copy_for_root`) and `resolve(address)` returns the
   canonical key: `prefix + address` if present, else `address`. So a nested
   file's `import x` is `nest.x` when that exists and the top-level `x`
   otherwise, mirroring a filesystem import trying the file's directory and
   then `import_paths`. Every parse-local and store key is the canonical key,
   so a file reached relatively from one importer and absolutely from another
   parses once. The studio's plain-deepcopy override keeps working unchanged
   (its keys are already absolute, and its `prefix` stays empty); both shapes
   are pinned (`test_narrowed_resolver_closure_validates_through_copy_for_root`,
   `test_flat_config_closure_validates_with_absolute_keys`,
   `test_nested_dict_import_falls_back_to_the_absolute_address`).
5. **Parameters.** Child envs are built with `parameters=dict(...)`; a later
   `set_parameters` on the importer no longer reaches a stored child, and a
   different value is a different key
   (`test_stored_child_env_does_not_observe_a_later_set_parameters`).
6. **Concurrency.** `test_same_dict_model_parsed_concurrently_is_identical`
   (4 threads x 16 parses, one store).
7. **Memory.** `set_import_env_store_max(n)`.

**Pre-existing bug fixed on the way.** With a dict resolver, `x` at the top
level and `x` under `nest` (reached as `nest.child`'s relative `import x`)
collided in `parsed_environments`, `text_lookup` and `in_flight_imports`,
which were keyed by the resolver-relative target string; both import orders
failed with an undefined concept. The same collision was silently *relied on*
by `tests/modeling/geography/test_proper_resolution.py`, whose nested files
import top-level modules by absolute name: the old narrowing config could not
resolve those, but the leaked top-level text made it work when the import
order happened to parse the top-level file first. Canonical keys fix both:
relative shadows absolute, order-independent. Pinned by
`test_nested_dict_import_does_not_collide_with_a_top_level_name` and
`test_fallback_import_resolves_its_own_imports_from_its_own_directory`.

**Also in this change.** `CTE.dependency_nodes` / `UnionCTE.dependency_nodes`
return the parent list directly instead of building `SourceBinding`s and
discarding them (the optimizer called it ~430 times per studio request).

**Measured.** The studio venv imports its own site-packages pytrilogy, so
`benchmark_endpoints.py` run as documented measures the released wheel, not
the checkout; the numbers below are the same checkout with the store flag
off vs on (`PYTHONPATH=<checkout>`, 20 iterations, medians):

| payload / case | store off | store on |
|---|---:|---:|
| tpch generate_query | 28.5 ms | 18.7 ms |
| tpch validate_query (no filters) | 5.9 ms | 1.5 ms |
| tpch validate_query (4 filters) | 9.2 ms | 3.9 ms |
| tpch parse_model | 39.5 ms | 21.0 ms |
| tpch format_query | 5.9 ms | 1.5 ms |
| tpch generate_queries x8 (batch + per-query) | 203 ms | 166 ms |
| small_names validate_query (no filters) | 2.5 ms | 0.6 ms |

Warm `generate_query` hydrates zero import files (instrumented: every store
lookup hits); the residual is the query's own parse (~7%) and the planner.
The store fills to 7 entries for the tpch model and 11 after `parse_model`.

**Gates run.** `tests/parsing/test_import_env_store.py` (31 with
`test_reparse_content_version.py`); store off / cold / warm SQL sha256 over all
132 `query*.preql` (tpc_ds_duckdb + tpc_h) plus the tpch dict model under
`StudioEnvironmentConfig`, one process, all identical; ruff / mypy / black.

**Determinism fix found on the way.** `tests/modeling/tpc_ds_duckdb/zquery29.log`
changed a CTE name between runs. Not the store: on origin/main the same query
rendered `sparkling` under `PYTHONHASHSEED=0/2` and `sweltering` under seed 1.
Planning and the pre-optimization CTE graph were identical across seeds; the
divergence was `query_processor.generate_source_map`, which dedup'd each
provider list with `list(set(v))`. With two equivalent parent CTEs (`macho`
had `abhorrent` and `late`, one the `_extent_free_` variant of the other),
the list order set the CTE's base alias and the renderer's `used_map`, so
`MergeIrrelevantGroupBy` kept whichever came first in hash order. Now
`list(dict.fromkeys(v))`, and the `BuildDatasource` pass is sorted by
`safe_identifier`. All 132 corpus queries render identically under four hash
seeds (`scratchpad corpus_seeds.py` pattern: one subprocess per seed);
`test_q04_generation_determinism.py` is parametrized to cover query29.

**Not done, by choice.** Closure validation still re-opens stdlib files from
disk once per parse (6 `open`s per studio request, ~0.3 ms under cProfile);
a stat-validated process-wide text cache would remove it but is ~1% here and
adds a staleness surface, so it is left as a note.

---

## Original ask (written 2026-09-04 from a trilogy-studio-core perf audit)

Audience: an agent starting fresh on extending the import environment store
(`trilogy/parsing/v2/import_service.py::_IMPORT_ENV_STORE`) so that parses
driven by a `DictImportResolver` benefit from it. Read
`docs/handoff_immutable_import_envs.md` first for the store's design, its
integrity stamp, and the namespaced-projection cache that hangs off each
entry; this handoff is about the one resolver type the store currently
refuses to serve.

## Why

The trilogy-studio-core resolver service (`pyserver/`) compiles every request
against a model the client ships in the request body. It builds a fresh
`Environment` per request with

```python
Environment(config=StudioEnvironmentConfig(import_resolver=DictImportResolver(content=..., data_files=...)))
```

where `content` maps dotted addresses (`part`, `nest.child`) to source text
(`pyserver/env_helpers.py::parse_env_from_full_model`). Because the store gate
in `ImportHydrationService.execute` is

```python
use_store = IMPORT_ENV_STORE_ENABLED and (
    request.is_stdlib
    or self.in_stdlib
    or isinstance(environment.config.import_resolver, FileSystemImportResolver)
)
```

every request re-hydrates every imported file from scratch. Measured on
pytrilogy 0.3.343, Python 3.13, TPC-H benchmark payload (11 sources, 6.5 KB,
`pyserver/scripts/payloads/tpch_large_duckdb.json`):

| path | cost per request | share |
|---|---:|---:|
| `generate_query`: env build + import hydration | 3.4 ms | ~13% of 25 ms |
| `parse_model` (one parse per source, 11 sources) | 58 ms before sharing hydration within the call, 36 ms after | ~5 ms per hydrated file |
| `build_namespace_projection` | 8 calls per `generate_query` | cached only via the store |
| Lark/pest `parse_syntax` | ~0 (its `lru_cache` hits: 7,928 hits / 17 misses in the run) | not the problem |

So the cost is hydration (`hydrate_rule`, `_sort_and_create_concepts`,
`add_import`, `build_namespace_projection`), not parsing, and it scales with
model size: a 50-file model pays roughly 250 ms of hydration per request
before any planning happens. Filesystem and stdlib parses already avoid all
of this through the store; the studio service, which is the highest-volume
caller of the parser, cannot.

The upstream comment gives the reason for the exclusion:

> Filesystem/stdlib texts come off disk and are process-stable, so the parsed
> env can be shared across parses; dict-resolver texts are scoped to one
> environment's resolver.

That is a statement about the *key*, not about the mechanism. The store
already validates an entry on reuse by re-hashing the transitive text closure
(`_store_lookup`), so a dict-resolver entry keyed on content rather than on
path is exactly as safe as a filesystem entry.

## What to change

1. **Gate.** Let `use_store` be true for `DictImportResolver` too. Keep the
   flag so a caller can opt out.

2. **Key.** For dict resolvers the address is not unique across callers:
   two studio users can both have a file called `customer` with different
   contents, and the service hosts many models in one process. Add
   `hash(text)` of the target to `store_key` when the resolver is a dict (the
   closure hash of the target is already checked in `_store_lookup`, but with
   it in the key, alternating models no longer evict each other from the
   LRU; they coexist). Filesystem keys can stay as they are.

3. **Closure validation must read through the resolver.** `_store_lookup`
   validates dependencies with `text_lookup.get(Path(path))` and, on a miss,
   `safe_open(path)`. For a dict resolver a miss must read from
   `environment.config.import_resolver.content` (see `_read_import_text`,
   which already does this) and a key absent from the dict must invalidate
   the entry rather than hit the filesystem. `_store_lookup` does not
   currently receive the environment; thread it through (or pass a reader
   callable).

4. **`copy_for_root`.** The base `EnvironmentConfig.copy_for_root` narrows
   the content dict to the keys under `root` so nested relative imports
   resolve. The store's `root` key component already accounts for this on
   the filesystem side. Check that the dict-narrowed resolver produces the
   same `request.target` strings the closure was recorded under, otherwise
   validation will look up the wrong keys. Note that the studio service
   overrides `copy_for_root` to a plain `deepcopy` (no narrowing,
   `pyserver/env_helpers.py::StudioEnvironmentConfig`), so both shapes need
   a test.

5. **Parameters are shared by reference.** A child env is built with
   `parameters=environment.parameters`, the same dict object as the parent,
   and `_params_fingerprint(environment.parameters)` is part of the key.
   The studio calls `env.set_parameters(...)` on the *parent* after imports
   have been parsed (`pyserver/query_helpers.py::filters_to_conditional`),
   which mutates the dict the cached child holds. This is pre-existing for
   filesystem parses, but the studio path exercises it on every filtered
   dashboard query, so pin it: a stored child env must not observe a later
   `set_parameters` on the importer, and a lookup under different parameters
   must miss.

6. **Concurrency.** The studio service parses on a thread pool (two threads
   per process by default). The store's lock only guards the `OrderedDict`;
   the shared envs rely on the read-only contract plus the integrity stamp,
   the same as a directory run. Add a test that parses the same dict model
   from several threads at once and gets identical output.

7. **Memory.** `_IMPORT_ENV_STORE_MAX = 128` entries, each a whole
   `Environment` plus its projections. A multi-tenant service will fill that
   with many small models; that is fine, but make the cap settable (config or
   `set_import_env_store_max`) so a deployment can size it.

## How to verify

- Unit: parse the same dict model twice with fresh `Environment`s and assert
  the second parse performs no hydration of the imported files (count
  `NativeHydrator.parse` calls for child contexts, or assert
  `_IMPORT_ENV_STORE` hits). Then change one imported file's text and assert
  a re-hydration of that file and of everything that transitively imports
  it, but nothing else.
- Cross-talk: two resolvers with the same address and different contents,
  parsed alternately, must each get their own concepts; and after the change
  they must both stay resident (key includes the content hash).
- Existing suite: `tests/` must stay byte-identical on SQL; the store-off /
  cold / warm three-leg comparison from `handoff_immutable_import_envs.md`
  is the template.
- End to end: in trilogy-studio-core, `pyserver/scripts/benchmark_endpoints.py`
  prints per-endpoint medians for the two benchmark payloads. With the store
  serving dict resolvers, the `generate_query` env+import share should drop
  to near zero on a warm model and `parse_model` should approach the cost of
  hydrating each source's own statements only. Compare against
  `pyserver/benchmark_baseline.md` and the numbers above.

## Pointers

- Gate and store: `trilogy/parsing/v2/import_service.py` (`execute`,
  `_store_lookup`, `_store_fill`, `_env_integrity`, `_ImportEnvEntry`).
- Dict resolver and `copy_for_root`: `trilogy/core/models/environment.py`.
- Studio caller: `trilogy-studio-core/pyserver/env_helpers.py`
  (`parse_env_from_full_model`, `StudioEnvironmentConfig`,
  `parse_source_into_env` which already shares `parsed_environments` and
  `text_lookup` across one `parse_model` call as a stopgap).
- Studio benchmark: `trilogy-studio-core/pyserver/scripts/benchmark_endpoints.py`.
