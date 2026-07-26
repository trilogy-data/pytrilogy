## State Store & Watermarks

### BaseStateStore

Central class for watermark collection and staleness detection.

- `watermark_all_assets`: queries DB for watermarks. **Skips any `ds_id` already in `self.watermarks`** — callers can pre-seed with `initial_watermarks` to avoid redundant queries.
- `is_stale(env, executor, ds_id, root_assets=None, force=False)`: single-asset staleness check. Returns a `StaleAsset` (with `kind`) if stale, else `None`. Honors `force=True` to short-circuit and emit a forced asset with the right kind. Lazily populates watermarks/concept_max as needed.
- `get_stale_assets`: thin wrapper that calls `is_stale` for every datasource in `env`. Use this for whole-environment staleness scans.
- `invalidate(ds_id)` / `invalidate_address(env, address)`: drop cached watermarks + probe memo entries after a refresh so subsequent `is_stale` calls re-query the post-refresh state. `invalidate_address` walks the env to drop every ds_id pointing at that physical address (root + non-root may share data).
- `run_freshness_probe_cached(path)`: memoized wrapper around `run_freshness_probe`. Same probe path used by N datasources in one refresh invocation = one subprocess call. Memo keyed by path; cleared by `invalidate_address` for any ds whose `freshness_probe` matches.
- A `threading.Lock` guards cache mutations — managed nodes evaluate in parallel, so reads/writes must serialize.

### Refreshable roots

A root datasource (`is_root=True`) carrying both `freshness_probe` and `refresh_script` is a **refreshable root**: trilogy doesn't refresh it via SQL persist, but it does drive an opaque subprocess. `is_stale` emits these with `kind=RefreshKind.SCRIPT`; non-root SQL staleness uses `RefreshKind.SQL`. Plain roots without `refresh_script` remain untouchable — `is_stale` returns `None` for them regardless of probe.

### create_refresh_plan

Accepts `initial_watermarks: dict[str, DatasourceWatermark] | None` — pre-inject root watermarks collected externally (e.g. from a deduplication phase) so the state store skips re-querying them.

`skip_datasources`: ds_ids to ignore entirely (owned by another script in a multi-script run).

Forced rebuilds (`force_sources`) are tagged with the right `kind` at construction time: a refreshable root in the force set gets `RefreshKind.SCRIPT`, everything else gets `RefreshKind.SQL`.

### Concept max watermarks and derived concepts

`concept_max_watermarks` is built from root datasource watermarks and represents the "expected" value each non-root should be at. Derived concepts (those with `lineage`) that don't appear directly on any root are resolved via `get_concept_max_watermark_abstract`.

`_ensure_concept_max_watermarks` lazily rebuilds this dict on first read after `invalidate*` clears it. It calls `watermark_all_assets` first (idempotent — only re-queries missing entries), so a root whose watermark was just dropped by an invalidate gets re-queried against the post-refresh DB before the max is recomputed. This is what closes the cross-script cascade for downstream non-root staleness.

### execute_refresh_plan

Deferred-eval execution pipeline:

1. Process script-kind assets first; each refresh invalidates its address before the post-refresh re-probe.
2. Re-evaluate non-script assets in `plan.refresh_assets` against the live store before running them — a SQL refresh may no longer be needed if its upstream root was invalidated and the post-refresh watermark caught up.
3. If `cascade=True` (default for single-file mode) and any script ran, do a final pass over every datasource in `env` calling `is_stale`. This catches dependents that probed fresh against the pre-refresh root watermark.
4. Hides other not-yet-refreshed SQL assets from the query planner during each step (temporarily pops them from `executor.environment.datasources`) so generated SQL doesn't read through stale upstream tables.
5. Defensive guard: a SQL-kind asset that points at a root datasource raises `RefreshAssetError` rather than emitting confusing SQL — only refreshable roots (with `refresh_script`) are managed.

`cascade=False` is set by directory mode where the orchestrator handles cross-managed-node cascade through `phys_graph`; per-node cascade would double-refresh dependents that are already separate managed nodes.

### Post-refresh probe contract

After a script-kind refresh, the freshness_probe is re-run once. If it still returns false, the refresh raises `RefreshAssetError` — the script claimed success but the probe disagrees, which is either a buggy script or a buggy probe; surface it loudly rather than letting downstream eval read inconsistent state.

### Persisted state (state files)

`snapshot.py` defines the serializable `StateSnapshot`; `persistence.py` is the read half.

- The snapshot's unit of identity is the **physical address**. Never key on `ds_id` or script — concept addresses are namespaced per script and are deliberately never reconciled across scripts.
- `managed_states_by_address` excludes roots on purpose: a root watermark is the *expected* side of every staleness comparison, so reusing a recorded one would hide an upstream that has since moved. Roots are always re-probed live.
- `watermarks_for_datasource` re-keys a recorded watermark onto the reading model's concept names via the shared **physical column** (`WatermarkValue.column`). Watermark keys are concept names, so without this a renamed model's seeded values are silently never compared. Key conventions mirror `watermarks.py`: full address for `KEY_HASH`, bare name otherwise, literal `update_time` passes through.
- `SnapshotStateStore` seeds **once**, on the first env-aware call. It must NOT re-seed afterwards — `invalidate_address` drops entries so post-refresh evaluation re-reads the warehouse, and re-seeding there would resurrect the pre-refresh value.

### Injecting a store

Two seams, in precedence order:

1. An explicit `state_store=` argument on `create_refresh_plan` / `execute_refresh_plan` / `refresh_stale_assets` / `execute_managed_node_for_refresh`.
2. The ambient factory (`state_store_factory(...)` context manager). `new_state_store()` — which every implicit construction goes through — consults it. A *factory*, not an instance: refresh evaluates managed nodes on parallel threads and each needs its own mutable store. A plain module global, not a ContextVar, for the same reason as `report.py`'s sink (worker threads).

The CLI installs the factory in `scripts/state.py::state_input_scope` from `--state-input` / `TRILOGY_STATE_INPUT`.
