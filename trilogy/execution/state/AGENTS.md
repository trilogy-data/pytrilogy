## State Store & Watermarks

### Watermark key convention (load-bearing)

A watermark key is the concept's **full address** as known to the emitting
environment (`order_item.created_at.date`), or the literal `update_time` for a
table-mtime watermark. Never a bare concept name: names collide across
namespaces (`events.created_at` and `orders.created_at` are both `created_at`,
so a name-keyed `concept_max_watermarks` merges unrelated roots' maxima), and a
derived property's dotted name (`created_at.date`) is ambiguous against both
addresses and bare names. Addresses make every pairing — observed vs
`concept_max`, `missing_derived` resolution, `UpdateKey.to_comparison`'s
concept lookup, snapshot column-binding — an exact, deterministic lookup.

Keys still never cross environments as text. Within one environment addresses
are canonical; across environments the bridge is **physical**:

- A snapshot read by a different model re-keys through `WatermarkValue.column`
  (`snapshot._rekey_for`) onto the reader's own addresses.
- Cross-script root injection (`initial_watermarks`) is keyed by datasource
  identifier, which only matches when the namespaces — and therefore the inner
  addresses — match too; a non-matching identifier just re-probes.

### BaseStateStore

Central class for watermark collection and staleness detection.

- `watermark_all_assets`: queries DB for watermarks. **Skips any `ds_id` already in `self.watermarks`** — callers can pre-seed with `initial_watermarks` to avoid redundant queries.
- `is_stale(env, executor, ds_id, root_assets=None, force=False)`: single-asset staleness check. Returns a `StaleAsset` (with `kind`) if stale, else `None`. Honors `force=True` to short-circuit and emit a forced asset with the right kind. Lazily populates watermarks/concept_max as needed.
- `get_stale_assets`: thin wrapper that calls `is_stale` for every datasource in `env`. Use this for whole-environment staleness scans.
- `invalidate(ds_id)` / `invalidate_address(env, address)`: drop cached watermarks + probe memo entries after a refresh so subsequent `is_stale` calls re-query the post-refresh state. `invalidate_address` walks the env to drop every ds_id pointing at that physical address (root + non-root may share data).
- `run_freshness_probe_cached(path)`: memoized wrapper around `run_freshness_probe`. Same probe path used by N datasources in one refresh invocation = one subprocess call. Memo keyed by path; cleared by `invalidate_address` for any ds whose `freshness_probe` matches.
- A `threading.Lock` guards cache mutations — managed nodes evaluate in parallel, so reads/writes must serialize.

### Model-fingerprint staleness

`is_stale` checks the asset's model fingerprint before the schema probe (pure
CPU, no warehouse query): when a recorded effective hash (see
`trilogy/core/fingerprint.py`) differs from the current parse's, the asset is
stale with reason "model changed since last build" and empty filters — a full
rebuild, because an incremental filter would keep rows computed under the old
definition.

Recorded hashes come from, in precedence order: the seeded snapshot's
per-asset `DatasourceState.model_fingerprint` (stamped by both snapshot
producers), then the ambient `model_fingerprint_baseline` (installed by
`refresh` from a deployment env's recorded fingerprint; keyed by LOGICAL
location — env prefixes stripped, since fingerprints are env-invariant).

Rules that are load-bearing:

- `invalidate`/`invalidate_address` mark the asset in `_model_refreshed`, and
  the check never fires for marked assets. The recorded hash describes the
  PRE-refresh build and lives in an immutable file, so without the mark the
  post-refresh re-evaluations (`execute_refresh_plan`'s cascade, directory
  execute-time re-decides) would see the same mismatch and refresh twice —
  the same rule as never re-seeding watermarks after invalidation.
- Fingerprinting must never fail staleness or snapshot production; a compute
  error skips the check / leaves the field None. Old snapshots read the same
  way, so the feature is opt-in by data presence.
- `refresh` maintains but never ESTABLISHES an env fingerprint record
  (auto-record only when a baseline existed, the run was unscoped and not
  dry, and something rebuilt). First records come from `run` or `trilogy env
  fingerprint` — recording after a watermark-only refresh would claim tables
  were built with code that never ran.

### Allowable lag (`within`)

A datasource may declare `within <n> [unit]` — how far behind its upstream it
can run and still count as fresh. Ordering (`_compare_watermark_values`) says
*whether* an asset is behind; a tolerance needs distance, which is what
`_watermark_distance` / `within_allowed_lag` add. Only temporal and numeric
watermarks have a distance: a unit-bearing lag (`within 5 minutes`) requires a
temporal watermark, a bare number (`within 500`) requires a numeric one, and a
mismatch raises rather than silently deciding. The parser rejects both
mismatches up front when the concept's datatype is known.

**Non-roots only.** The tolerance states what the asset being judged may
tolerate, so it lives on that asset — two consumers of the same realtime feed
legitimately differ. A root never has its own freshness judged, so `within`
there is a parse error rather than a silent no-op. Inheriting a tolerance from
the upstream root was considered and dropped: it makes the verdict depend on a
value declared elsewhere. Adding it later is additive and non-breaking; a
model-level default would be the better answer to fan-out repetition.

`within` is its own datasource clause rather than part of the update-trigger
clause because that keeps it independent of a trigger form it doesn't belong to
(it still *requires* one — see below).

A missing watermark value is never lag — an asset with no rows is empty, not
behind, so it stays stale regardless of tolerance. Probe-based freshness can't
take a lag: a probe returns a bool, so there's nothing to measure.

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

**`StateSnapshot` is the universal interchange format** — CLI state files, `trilogy serve`'s `/state`, the studio UI, and the cloud service all speak it verbatim. There is exactly ONE producer per input shape: `scripts/state.py::snapshot_for_parsed_script` for a single parsed script (used by both `trilogy state` and `serve`), and `_snapshot_from_directory` over the refresh probe for a directory. Serve deliberately has **no state model of its own** — the older per-datasource `StateResponse` was removed rather than kept as a second rendering. Do not reintroduce a per-surface shape: a format only stays an interchange while one implementation defines it, and every divergence in this subsystem has started as "just one more view of the same data".

- The snapshot's unit of identity is the **physical address**. Never key on `ds_id` or script — concept addresses are namespaced per script and are deliberately never reconciled across scripts.
- `managed_states_by_address` excludes roots on purpose: a root watermark is the *expected* side of every staleness comparison, so reusing a recorded one would hide an upstream that has since moved. Roots are always re-probed live.
- `watermarks_for_datasource` re-keys a recorded watermark onto the reading model's own concept addresses via the shared **physical column** (`WatermarkValue.column`). Watermark keys are concept addresses, which are namespaced per script, so without this a different model's seeded values are silently never compared. `update_time` (no concept) passes through, as does a legacy name-keyed entry with no recorded column — it falls out of comparisons like any unknown key.
- `SnapshotStateStore` seeds **once**, on the first env-aware call. It must NOT re-seed afterwards — `invalidate_address` drops entries so post-refresh evaluation re-reads the warehouse, and re-seeding there would resurrect the pre-refresh value.

### Partition state (`partitions.py`)

A datasource with `partition by` is N independently refreshable slices behind one address, so `DatasourceState.partitions` records each one. Two probes, one query each regardless of slice count:

- `probe_observed_partitions` — `GROUP BY` the partition columns on the physical table (missing/reshaped target reads as "no slices", not an error).
- `probe_expected_partitions` — a trilogy query with every non-root hidden, so only authoritative sources can answer. Failure is a real answer (the key may not be derivable from roots) and returns `[]` rather than failing the snapshot.

Rules that are load-bearing:

- **`partition_id` is keyed on the PHYSICAL column**, not the concept address — same reason asset keys are physical. Values render canonically (ISO temporals, `__NULL__`), because the id is compared across processes and a driver-dependent `str()` would split one slice in two.
- **Partition columns are excluded from a slice's watermark keys.** `MAX(order_date)` inside `order_date=2024-01-03` is the slice's own name, never a signal. What remains is `freshness_by`/`incremental_by` minus the partition keys.
- `expected and not observed` -> **stale, "partition missing"** — the case a table-level MAX structurally cannot see, and the reason this exists. `observed and not expected` is NOT stale: nothing is asking for that slice (`expected=False` says what it is).
- A stale slice makes the whole datasource stale, even when its table-level watermark looks caught up.
- Roots are never partitioned assets (`is_partitioned` excludes them) — a root is the expected side, never judged.
- Partition state is **seeded from `--state-input` on exactly the same terms as watermarks** (`partitions_for_datasource`, the twin of `watermarks_for_datasource`). Supplying a snapshot means "trust these observations instead of re-reading the warehouse", and that must hold for every observation in the record — an out-of-band change invalidates a watermark as easily as a slice, so probing one and not the other buys no safety, only an inconsistent meaning for the same flag. Both sides round trip: each slice carries `observed`/`expected` flags and its own two watermark lists.
- Seeded partition values are **restored typed, using the READER's declared datatype** (`_restore_partition_value`) — the refresh filter needs real values, not the rendered strings the format stores. Same principle as `_rekey_for` bridging a watermark through the physical column.
- `partitions_for_datasource` returns None (meaning *probe normally*) when there is nothing trustworthy to seed: the reader declares no partitioning, the writer recorded none (older snapshot), or the record is a **partition-scoped delta** (`partitions_complete=False`), which speaks for only some slices and would understate the rest as absent.
- `managed_states_by_address` admits an entry with **watermarks OR partitions** — a partitioned datasource may carry its whole state per slice with no table-level watermark. `seeded_watermarks` then skips entries with no watermarks, so a slice-only record does not suppress a watermark probe that should run.
- `BaseStateStore.partitions` caches per invocation and is dropped by `invalidate`/`invalidate_address` alongside watermarks, so a post-refresh re-probe sees what the refresh wrote.

### Slice-aware refresh

`is_stale` checks slices **before** the table-level watermark comparison, and `StaleAsset.partitions` carries the stale ones. This is not an optimization — it is required for correctness: a missing slice's rows can be OLDER than the table's MAX, so the coarse check reports fresh while a hole sits in the middle of the range. Without it `trilogy state` and `trilogy refresh` disagree about the same asset. When no slice is stale it falls through to the coarse check, so an unprobeable expectation never reads as "fresh".

`StaleAsset.partitions` and `.filters` are **mutually exclusive**. A missing slice may hold rows older than the incremental watermark, so ANDing the two would filter out exactly the rows the refresh exists to write — `update_datasource` lets slices replace the incremental filter rather than narrow it.

`partition_filter` builds an `IN` list for a single key and an OR-of-ANDs for several (row-value `IN` is not portable). A NULL slice is selected with `IS NULL`, never `= NULL` — the read-side twin of the null-safe partition delete; with `=` the slice would be reported stale forever and never written.

The DELETE reads the STAGED keys, so **one statement replaces exactly the N slices its select produced** — writing a set of partitions is the same operation as writing one. `MAX_PARTITION_FILTER_VALUES` chunks only to stay inside statement-size limits; how wide to fan out is the orchestrator's decision, not trilogy's.

**Iteration hazard**: the expected-partition probe hides non-root datasources for the duration of its query, mutating `env.datasources`. Any loop calling `is_stale` over that dict must iterate a materialized copy (`get_stale_assets`, `execute_refresh_plan`'s cascade).

### Hiding datasources and probe statements (planning-cache neutrality)

Never pop/restore `env.datasources` by hand — use `isolation.py::hidden_datasources`. The dict's `pop`/`update` bump `content_version` unconditionally, and that counter stamps the cross-statement planning caches in `query_processor`; a bare pop/restore evicted every cached build baseline once per probe (O(N²) refresh planning). The context manager restores the counters on exit exactly when the restore is object-identical, so the full environment's caches survive the window while the hidden window keeps its own honest stamp (datasource membership is part of the stamp).

Probe statements (`get_concept_max_watermarks_abstract`, `probe_expected_partitions`) run through `Executor.execute_ephemeral`, not `execute_query`: an ephemeral parse rolls back instead of committing, so probe aliases never land in the durable concept dict (each landing was one more eviction — and worse, made same-membership hidden windows with *different* alias lineages stamp-collide). Derived-concept MAX probes batch into one statement (`_ensure_concept_max_watermarks`); a batch that fails planning falls back to per-concept probes to preserve the "unanswerable → null, not exception" contract. `tests/execution/state/test_planning_cache_stability.py` pins the whole mechanism: ≤2 `materialize_baseline` calls per refresh plan.

### Targeted refresh (`RefreshPolicy` / `--partition`)

What the caller asked for travels as one `RefreshPolicy` (force set + partition selector), not as loose keyword arguments — a new kind of intent then reaches every planning call site by construction, and `RefreshParams.policy()` is the single CLI→plan mapping.

The selector is addressed **by concept, not by physical column**: a caller naming a slice works from the model, and the column is whichever datasource binds it. `selected_slice` bridges the two per-datasource; naming only part of a multi-column key raises, because that identifies a range and silently widening a targeted refresh is the failure the flag exists to prevent.

A selector matching **no** datasource is the same failure arriving by typo, and both of its halves are silent: the plan keeps whatever staleness decided *and* `selector_partition_ids` returns empty, so the written snapshot claims the whole table. `validate_partition_selector` (the `--partition` twin of `validate_force_sources`) rejects it up front, against the union of every declared partition key — the union, because a selector legitimately names only some scripts' assets, so per-datasource "does not apply" must stay legal. Do not move this check into `selected_slice` or `target_partition_selector`: both run per-datasource and per-node, where matching nothing is the normal case.

`target_partition_selector` refreshes a named slice **whether or not it looks stale** (a backfill of a day the watermark is already past looks fresh) and *replaces* any whole-table entry for that datasource — planning it twice would rebuild everything alongside the one slice.

**`StaleAsset.explicit` is what makes that survive execution.** Staleness is deliberately re-decided after planning — `execute_refresh_plan` re-evaluates SQL assets once a script-kind refresh has run, and directory mode re-probes at execute time to close the cross-script cascade. Both would discard a targeted slice (or drop the asset entirely, since it may look fresh). Anything the caller named — `--force`, `--partition` — carries `explicit=True` and is not re-decided. Do not reintroduce the `reason == "forced rebuild"` string check: it is the sentinel that silently excluded the second kind of intent.

Because directory mode builds its own plan per node, the policy has to reach `execute_managed_node_for_refresh`, which re-applies the selector against that node's datasources. A selector applied only at preview time is silently widened back.

`refresh --partition` also **implies `--state-partition` for the same slices** (`selector_partition_ids` + `scope_to_partitions` in `maybe_write_state_snapshot`), so a fan-out cannot write one slice and then claim the whole table. That function matches **recorded slices first**: only they know how the writer's datatype rendered the value (`2024-01-03` against a `datetime` column is recorded `2024-01-03T00:00:00`), and it has no datatype to consult. It falls back to a rendered id rather than returning empty, because empty means "do not scope" — an unscoped snapshot is exactly the whole-table claim being prevented.

### Merging deltas (`scope_to_partitions` / `merge_snapshots`)

`partitions_complete` is the whole concurrency story. A whole-asset probe sets it True; a run scoped to the slices it owns (`--state-partition`) sets it False, meaning "these slices, and nothing about the others" — necessary because a worker's post-run probe sees the *whole* table, including slices peers are mid-write on.

`merge_snapshots` overlays a scoped delta by `partition_id` and lets a complete one replace the list, then re-derives the datasource status from the merged slices. Because each worker speaks only for slices it owns, **the result is independent of merge order** and replaying a delta is idempotent. That is what lets a file-backed store support parallelism: N workers write N distinct files with no coordination, one coordinator merges.

Snapshots are **built and written complete**. A slice budget belongs to the consumer reading the file, not to the format, so it is opt-in: `--state-max-partitions` / `TRILOGY_STATE_MAX_PARTITIONS` (unset = every slice, `0` = summaries only, `MAX_REPORTED_PARTITIONS` = 200 is a sane number for a client that has a budget but no particular figure in mind). `cap_snapshot` applies it on the way out (`maybe_write_state_snapshot`, `trilogy state`), and `DatasourceState.partition_summary` counts the whole probed set either way — trimming changes what a reader can enumerate, never what it can conclude.

Do not push the budget back into `build_partition_states`, and do not give it a non-null default. Keeping it opt-in at the boundary is what lets a client that moves to a transport without the size limit simply stop passing it, with nothing upstream to change and no information destroyed earlier where it could not be recovered. Because a trim is possible at all, `partitions_complete=True` promises the *probe* was whole, not that the list is exhaustive — check `partition_summary.truncated`.

That distinction decides how a complete delta merges. **Untruncated it replaces the list** (an absent id is the only signal that a partition was dropped from the table, and that has to keep propagating); **truncated it overlays**, because there an absent id means "did not fit", and replacing would shrink the accumulated work list to one payload's worth. So `stale_partitions` accumulates across probes and a table further behind than the cap drains over successive rounds instead of being pinned at 200.

Overlaying can retain a slice the newer probe would have called fresh. Where it can be ruled out it is: the cap spends its budget on stale slices first, so when a delta carried every stale slice its summary counts (`_carries_every_stale`), any base entry it does not mention is provably no longer stale and is dropped. Where the stale set itself overflowed, a phantom costs one idempotent dispatched run that corrects the record — cheap, and self-healing, unlike a silently truncated queue.

A scoped delta keeps its summary (a fan-out where every run is targeted would otherwise never report totals), but on merge that aggregate is **not** preferred over the base's adjusted counts, tempting as its recency is: it was probed at an arbitrary point in the fan-out, so letting it win makes the merged counts — and the status derived from them — depend on which file was folded last. It bootstraps a base with no counts and nothing else.

Datasources are merged **by `datasource_id` alone, never by `(script, datasource_id)`** — a delta legitimately comes from the per-partition build script while the base came from the model, and keying on the pair files the same asset twice. (Note `merge_into_snapshot`, which dedups *within* one probe, still uses the pair: there two scripts really are two views.)

### Injecting a store

Two seams, in precedence order:

1. An explicit `state_store=` argument on `create_refresh_plan` / `execute_refresh_plan` / `refresh_stale_assets` / `execute_managed_node_for_refresh`.
2. The ambient factory (`state_store_factory(...)` context manager). `new_state_store()` — which every implicit construction goes through — consults it. A *factory*, not an instance: refresh evaluates managed nodes on parallel threads and each needs its own mutable store. A plain module global, not a ContextVar, for the same reason as `report.py`'s sink (worker threads).

The CLI installs the factory in `scripts/state.py::state_input_scope` from `--state-input` / `TRILOGY_STATE_INPUT`.
