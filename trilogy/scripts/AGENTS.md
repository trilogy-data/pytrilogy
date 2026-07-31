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
- Pre-classified `node.assets` are only used to honor forced rebuilds (their `reason == "forced rebuild"`); everything else goes through the live `is_stale` check.
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
