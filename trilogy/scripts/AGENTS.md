## Refresh Pipeline Architecture

The refresh system has two distinct paths:

**Single file**: `execute_script_for_refresh` (in `single_execution.py`) → `create_refresh_plan` → `execute_refresh_plan`

**Directory**: `_preview_directory_refresh` (in `refresh.py`) → three phases → `run_parallel_execution` with a `ManagedRefreshNode` graph

### Phase structure in directory refresh

1. **Phase 1 — parse only, no DB**: all scripts parsed to collect `address_map`, `ds_to_scripts`, `ds_is_root`, `all_needed_concepts`, `root_addr_to_concepts`
2. **Phase 2a — root watermark collection**: root datasources probed *once per physical address*; results merged into `all_root_watermarks`
3. **Phase 2b — managed asset probing**: each owner script probed with pre-injected root watermarks so roots are never queried twice

### Key deduplication rules

- Deduplication is always by **physical address** (`ds.safe_address`), never by `ds_id` or script.
- `addr_to_owner`: maps each physical address to its single most-upstream owner script (lowest topological index).
- `skip_ids` in `_probe_owner_node`: `addr_to_owner.get(addr) != owner_node` — uniform for both root and non-root.
- Root watermarks pre-injected via `initial_watermarks` into `create_refresh_plan`; `watermark_all_assets` skips already-populated entries.

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
