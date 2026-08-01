## Refresh Pipeline Architecture

The refresh system has two distinct paths:

**Single file**: `execute_script_for_refresh` (in `single_execution.py`) → `create_refresh_plan` → `execute_refresh_plan`

**Directory**: `_preview_directory_refresh` (in `refresh.py`) → preview phases → `run_parallel_execution` with a `ManagedRefreshNode` graph

### Phase structure in directory refresh

1. **Phase 1 — parse only, no DB**: all scripts parsed to collect `address_map`, `ds_to_scripts`, `ds_is_root`, `ds_is_refreshable_root`, `all_needed_concepts`, `root_addr_to_concepts`
2. **Phase 2a — root watermark collection**: root datasources (excluding refreshable roots) probed *once per physical address*; results merged into `all_root_watermarks`
3. **Phase 2b — managed asset probing**: each owner script probed with pre-injected root watermarks so roots are never queried twice. Refreshable-root probes (subprocess) run inside `get_stale_assets` here too.
4. **`phys_graph` build**: every probe address gets a `ManagedRefreshNode`, **not just stale-at-preview ones**. Nodes downstream of a script-stale node are tagged "unknown" — their staleness will be re-evaluated at execute time. Nodes that are neither stale nor unknown are pruned from the graph (no chance of needing refresh).

### Refreshable roots in the directory pipeline

A datasource that is `is_root=True` AND has both `freshness_probe` and `refresh_script` is a **refreshable root** — managed by trilogy via subprocess, not SQL persist. Specific behaviors:

- `probe_addrs` includes addresses where any datasource is a refreshable root (extending the original "non-root only" filter).
- `_validate_probe_coverage` excludes refreshable-root addresses from the watermark-coverage requirement — they're managed via probe + script, not watermark.
- Display surfaces "(x) stale, (y) unknown" when a refreshable root probed stale at preview, with the unknown set being `nx.descendants(phys_graph, script_stale_nodes)` filtered to nodes without pre-classified assets.

### Deferred staleness at execute time

`execute_managed_node_for_refresh` re-evaluates staleness against the live DB at execute time, not from the preview snapshot:

- Each node builds its own `BaseStateStore`, calls `is_stale` per `ds_id` in `node.datasource_ids`, refreshes if stale (dispatching on `kind`), and skips otherwise.
- Pre-classified `node.assets` are only used to honor what the caller asked for by name (`StaleAsset.explicit`); everything else goes through the live `is_stale` check.
- **Anything from `RefreshParams` that narrows a plan must reach this function**, not just the preview probe. Re-deciding staleness here is the point of the phase, so a `--partition` slice chosen at preview would be widened back to whatever is stale — `execute_managed_node_for_refresh` takes the `RefreshPolicy` and re-applies the selector against its own datasource ids.
- `update_count == 0` signals "skipped" to `run_parallel_execution`, which reports it in the final summary.

This is what closes the cross-script cascade gap: by the time a downstream node executes, upstream script-kind nodes have already mutated the live DB through `phys_graph` topo order, so the deferred check sees the post-refresh state.

`cascade=False` is passed to `_run_refresh_plan` in directory mode so `execute_refresh_plan`'s own cascade pass doesn't double-refresh dependents that are already separate managed nodes.

### Key deduplication rules

- Deduplication is always by **physical address** (`ds.safe_address`), never by `ds_id` or script.
- **Snapshot asset keys are stable, not checkout-absolute** (`snapshot.py::stable_asset_key`). They dispatch on `AddressType` — never on the shape of the address string — and contain **nothing logical**: no script, no datasource name. Two scripts writing the same file key to the same asset, because it is the same asset.
  - `TABLE` and remote URLs (`gs://`, `s3://`, `http://`) pass through verbatim — already stable, and shared by every model pointing at the same object.
  - Local files (`CSV`/`TSV`/`PARQUET`/`SQL`) are keyed by their **project-relative path**. Note `AddressType.SQL` is a `.sql` *file*; only inline `query '''...'''` is `AddressType.QUERY`.
  - Two types aren't plain data artifacts and carry a type label: `PYTHON_SCRIPT` → `script::<project-relative path>` (a procedure that emits rows), and `QUERY` → `query::<16-hex digest of whitespace-collapsed SQL>` (no artifact at all; raw inline SQL is multi-line and churns on reformatting).
- **The project root is `trilogy.toml`'s directory** (`state.py::project_root_for`), falling back to the input directory when there is no config. This is the only anchor every invocation agrees on — a subdirectory script, a single script run directly, and a whole-directory run all key identically. Anchoring on the script's own directory instead cannot express `../data/...` and silently reverts to absolute, unportable paths.
- **The owning script is attribute data** (`PhysicalAssetState.owner_script`, project-relative), set only where trilogy manages the address — `addr_to_owner` also names the script that merely *declares* an unmanaged root.
- **Every datasource is an asset, roots included.** Unmanaged is `PhysicalAssetState.managed = False`, never an omission from `assets[]`. Roots are still never *seeded* from a snapshot (`managed_states_by_address` excludes them) — they are the expected side of every staleness comparison and must be re-probed live.
- `--state-input` seeding recomputes the key from the physical address against the reader's own project root and looks it up directly (`persistence.py::_recorded_state`); the raw address is tried first for snapshots written before stable keys.
- **Partitioned datasources record per-slice state** (`DatasourceState.partitions`), keyed by a hive-style `partition_id` on the physical column. `--state-partition <id>` scopes a written snapshot to the slices one worker owns (`partitions_complete: false`) and `trilogy state-merge` folds those deltas back — order-independently, because each worker only speaks for its own slices. See `trilogy/execution/state/AGENTS.md` for the rules and `local_scripts/partitioned_state_demo/` for the end-to-end loop.
- `addr_to_owner`: maps each physical address to its single most-upstream owner script (lowest topological index).
- `skip_ids` in `_probe_owner_node`: `addr_to_owner.get(addr) != owner_node` — uniform for both root and non-root.
- Root watermarks pre-injected via `initial_watermarks` into `create_refresh_plan`; `watermark_all_assets` skips already-populated entries.
- Refreshable-root scripts run exactly once per address because each `ManagedRefreshNode` has one owner_script.

### Concept namespacing — never reconcile across scripts

The same concept appears under different namespaced addresses in different scripts (`data_updated_through` in `etl.preql`, `engine.data_updated_through` in `engine.preql`, etc.). **Never try to match or deduplicate concept addresses across scripts.**

The correct approach: within each script's executor (while it's still live in Phase 1), directly match non-root `freshness_by`/`incremental_by` refs against root output concepts in the same environment. Collect the physical address of any matching root. This produces `root_addr_to_needed_concepts: dict[str, set[str]]` — physical address → concepts — with no cross-script namespace issues. Deduplication is then purely at the physical address layer.

### Before assuming anything about root counts or probe counts — read the files

Always check the actual `.preql` files before concluding how many physical roots exist. In a typical project:
- A single `root datasource` with an update-time concept appears under many namespace aliases
- Many other root datasources (raw ingest files, remote parquets) exist but have no freshness concepts
- Only roots that were directly matched to a needed concept **within the same executor context** during Phase 1 are probed — see `root_addr_to_needed_concepts`

### Display helpers (display.py)

- `show_managed_asset_list`: prints physical probe addresses before probing
- `show_root_concepts`: table of root address → matched concepts (root on left, deduplicated)
- `show_root_probe_breakdown`: post-probe table showing per-root values and derived max
- `show_asset_status_summary`: per-asset staleness status
- `probe_progress` / `root_probe_progress`: Rich progress bars for the two probe phases


### Shared execution helper

`_plan_and_execute_refresh` in `single_execution.py` is the single path for display + interactive confirm + execution + result reporting. Both single-file and directory refresh flow through it. Do not duplicate this logic.


## Serve: the on-disk state cache

`serve_helpers/state_cache.py` caches `/state` under `<served dir>/.trilogy/state`.
Computing a snapshot re-parses the target, builds an executor (running `[setup]`
scripts) and re-probes the warehouse — seconds per request, and money per request
on a billed warehouse. Without a cache no consumer can show state passively, which
is why the studio gated it behind a button.

- **The cached value is a `StateSnapshot`**, byte-for-byte what the endpoint returns
  and what `trilogy state -o` writes. Serve still has no state shape of its own (see
  `execution/state/AGENTS.md`); cache bookkeeping lives in a sidecar `.meta.json`, and
  cache status rides in `X-Trilogy-Cached` / `X-Trilogy-Computed-At` **headers** rather
  than in the body. Those headers must stay in the CORS `expose_headers` list or a
  browser cannot read them.
- **Validity is a fingerprint, not a TTL** — size+mtime of every model file in the
  served directory. It is deliberately directory-wide: a target's state depends on
  what it imports, and resolving imports would mean parsing, which is the cost being
  avoided.
- **The cache is also the jobs' state store.** `/run` and `/refresh` subprocesses get
  `--state-input` (the cached snapshot, when live) and `--state-file`; on completion
  the server adopts the written snapshot as the entry for that target. A refresh
  therefore leaves `/state` correct with no re-probe. `--no-state-cache` disables both
  halves together — they are one trust decision.
- **Job completion clears every other entry.** Not narrowed to the job's target: jobs
  rewrite assets, targets overlap (a directory contains its files), and state flows
  downstream. Anything narrower would have to model the dependency graph, and being
  wrong shows a stale "fresh".
- What no server-side cache can see is a table loaded **outside** trilogy. That is why
  `snapshot_ts` is load-bearing rather than informational and `?refresh=true` exists.

### On-disk DuckDB is the case that finds concurrency bugs

Directory probes build executors on a thread pool. Against the default in-memory
DuckDB each owns a private catalog, so they never interact; against one on-disk
warehouse they share it. Two consequences, both fixed in `executor.py`:

- Connect-time setup DDL races. `_execute_setup_ddl` retries the catalog write-write
  conflict, and `_duckdb_macro_exists` skips the write entirely when the guard macro
  is already defined.
- **A read that leaves a transaction open is not harmless.** `_execute_with_retry`
  claims a transaction only when the statement itself began one, so a stray open
  transaction makes every later write look caller-managed; `_flush_transaction` then
  declines to commit and `close()` discards it. The failure is silent — a refresh
  reports success having written nothing. Any connect-time read must restore the
  connection's transactional state (see `test_duckdb_persistence.py`).
