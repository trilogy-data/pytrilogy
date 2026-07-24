import threading
from collections.abc import Callable
from dataclasses import dataclass, field

from trilogy import Executor
from trilogy.core.enums import Purpose
from trilogy.core.models.datasource import UpdateKey, UpdateKeys, UpdateKeyType
from trilogy.core.models.environment import Environment
from trilogy.execution.state.cache import ColumnStatsCache
from trilogy.execution.state.watermarks import (
    DatasourceWatermark,
    RefreshKind,
    StaleAsset,
    _compare_watermark_values,
    get_concept_max_watermark_abstract,
    get_concept_max_watermarks,
    get_freshness_watermarks,
    get_incremental_key_watermarks,
    get_last_update_time_watermarks,
    get_unique_key_hash_watermarks,
    has_schema_mismatch,
    is_missing_local_file,
    run_freshness_probe,
    run_refresh_script,
)


class BaseStateStore:

    def __init__(self, cache: ColumnStatsCache | None = None) -> None:
        self.watermarks: dict[str, DatasourceWatermark] = {}
        self.concept_max_watermarks: dict[str, UpdateKey] = {}
        self._cache = cache
        # Probe path -> result; deduplicates subprocess calls for the same probe
        # script during one refresh invocation.
        self._probe_results: dict[str, bool] = {}
        # Mutations to the caches happen from parallel managed-node executions;
        # serialize them to keep the dicts consistent.
        self._lock = threading.Lock()

    def _run_freshness_probe_cached(self, probe_path: str) -> bool:
        """Memoized wrapper around run_freshness_probe.

        Probes are deterministic for the duration of one refresh invocation;
        running the same script repeatedly across N is_stale evaluations is
        wasteful. Cache is invalidated by invalidate_probe_path().
        """
        with self._lock:
            cached = self._probe_results.get(probe_path)
        if cached is not None:
            return cached
        result = run_freshness_probe(probe_path)
        with self._lock:
            self._probe_results[probe_path] = result
        return result

    def invalidate(self, ds_id: str) -> None:
        """Drop cached watermark for a single datasource.

        Also clears concept_max_watermarks (cheap to recompute and depends on
        the full root set).
        """
        with self._lock:
            self.watermarks.pop(ds_id, None)
            self.concept_max_watermarks.clear()

    def invalidate_address(self, env: Environment, address: str) -> None:
        """Drop cached watermarks and probe memo for every datasource at a
        physical address.

        Called after a refresh completes so downstream evaluations re-query
        against the post-refresh state. Multiple datasources may share an
        address (root + non-root pointing at the same table); they all share
        the underlying data so all entries must be evicted.
        """
        affected_ids: list[str] = []
        affected_probes: set[str] = set()
        for ds in env.datasources.values():
            if ds.safe_address == address:
                affected_ids.append(ds.identifier)
                if ds.freshness_probe:
                    affected_probes.add(ds.freshness_probe)
        with self._lock:
            for ds_id in affected_ids:
                self.watermarks.pop(ds_id, None)
            for probe in affected_probes:
                self._probe_results.pop(probe, None)
            self.concept_max_watermarks.clear()

    def watermark_asset(self, datasource, executor: Executor) -> DatasourceWatermark:
        if is_missing_local_file(datasource):
            watermarks = DatasourceWatermark(keys={})
            self.watermarks[datasource.identifier] = watermarks
            return watermarks
        if datasource.freshness_by:
            watermarks = get_freshness_watermarks(datasource, executor)
        elif datasource.incremental_by:
            watermarks = get_incremental_key_watermarks(datasource, executor)
        else:
            key_columns = [
                col
                for col in datasource.columns
                if executor.environment.concepts[col.concept.address].purpose
                == Purpose.KEY
            ]
            if key_columns:
                watermarks = get_unique_key_hash_watermarks(datasource, executor)
            else:
                watermarks = get_last_update_time_watermarks(datasource, executor)

        self.watermarks[datasource.identifier] = watermarks
        return watermarks

    def get_datasource_watermarks(self, datasource) -> DatasourceWatermark | None:
        return self.watermarks.get(datasource.identifier)

    def check_datasource_state(self, datasource) -> bool:
        return datasource.identifier in self.watermarks

    def watermark_all_assets(
        self,
        env: Environment,
        executor: Executor,
        skip_datasources: set[str] | None = None,
    ) -> dict[str, DatasourceWatermark]:
        """Watermark all datasources in the environment."""
        skip_datasources = skip_datasources or set()

        needed_concepts: set[str] = set()
        for ds in env.datasources.values():
            if not ds.is_root and ds.identifier not in skip_datasources:
                for ref in ds.freshness_by:
                    needed_concepts.add(env.concepts[ref.address].address)
                for ref in ds.incremental_by:
                    needed_concepts.add(env.concepts[ref.address].address)

        for ds in env.datasources.values():
            if ds.identifier in skip_datasources:
                continue
            if ds.identifier in self.watermarks:
                continue
            if ds.is_root:
                if needed_concepts:
                    target_refs = [
                        ref
                        for ref in ds.output_concepts
                        if ref.address in needed_concepts
                    ]
                    if target_refs:
                        watermark = get_concept_max_watermarks(
                            ds, target_refs, executor
                        )
                        if watermark.keys:
                            self.watermarks[ds.identifier] = watermark
            else:
                self.watermark_asset(ds, executor)
        return self.watermarks

    def _ensure_concept_max_watermarks(
        self,
        env: Environment,
        executor: Executor,
        root_assets: set[str],
    ) -> None:
        """Lazily populate self.concept_max_watermarks from root watermarks.

        Cleared by invalidate(); recomputed on first read after invalidation so
        cascade decisions see post-refresh root values. Calls
        watermark_all_assets first (idempotent — only re-queries missing
        entries) so any root watermark dropped by invalidate_address is
        re-fetched against the post-refresh DB state before we recompute the
        max.
        """
        if self.concept_max_watermarks:
            return
        self.watermark_all_assets(env, executor)

        concept_max_watermarks: dict[str, UpdateKey] = {}
        for ds_id, watermark in self.watermarks.items():
            if ds_id in root_assets:
                for key, val in watermark.keys.items():
                    if (
                        val.type
                        in (UpdateKeyType.INCREMENTAL_KEY, UpdateKeyType.UPDATE_TIME)
                        and val.value is not None
                    ):
                        existing = concept_max_watermarks.get(key)
                        if existing is None or existing.value is None:
                            concept_max_watermarks[key] = val
                        else:
                            try:
                                is_newer = (
                                    _compare_watermark_values(val.value, existing.value)
                                    > 0
                                )
                            except TypeError as e:
                                raise TypeError(
                                    f"Cannot compare watermarks for field '{key}' across root datasources"
                                    f" (datasource '{ds_id}'): {e}"
                                ) from e
                            if is_newer:
                                concept_max_watermarks[key] = val

        # Derived concepts (e.g. `auto x <- greatest(a, b)`) that don't appear
        # directly on any root — query their expected value using only roots.
        missing_derived: dict[str, str] = {}
        for ds_id, watermark in self.watermarks.items():
            if ds_id in root_assets:
                continue
            for key, val in watermark.keys.items():
                if (
                    val.type
                    in (UpdateKeyType.INCREMENTAL_KEY, UpdateKeyType.UPDATE_TIME)
                    and key not in concept_max_watermarks
                    and key not in missing_derived
                ):
                    concept = next(
                        (c for c in env.concepts.values() if c.name == key), None
                    )
                    if concept is not None and concept.lineage is not None:
                        missing_derived[key] = concept.address

        for key, concept_address in missing_derived.items():
            wm = get_concept_max_watermark_abstract(
                concept_address, executor, root_assets
            )
            if wm.value is not None:
                concept_max_watermarks[key] = wm

        self.concept_max_watermarks = concept_max_watermarks

    def is_stale(
        self,
        env: Environment,
        executor: Executor,
        ds_id: str,
        root_assets: set[str] | None = None,
        force: bool = False,
    ) -> StaleAsset | None:
        """Single-asset staleness check.

        Returns a StaleAsset (with the right `kind`) if the datasource needs
        refresh, else None. Uses cached watermarks/probes from self; populates
        them lazily as needed. After invalidate(), the next call re-queries.
        """
        ds = env.datasources[ds_id]
        if root_assets is None:
            root_assets = {d.identifier for d in env.datasources.values() if d.is_root}

        is_managed_root = bool(ds.is_root and ds.refresh_script and ds.freshness_probe)

        if force:
            kind = (
                RefreshKind.SCRIPT
                if (ds.is_root and ds.refresh_script)
                else RefreshKind.SQL
            )
            return StaleAsset(datasource_id=ds_id, reason="forced rebuild", kind=kind)

        if is_managed_root:
            if not self._run_freshness_probe_cached(ds.freshness_probe):  # type: ignore[arg-type]
                return StaleAsset(
                    datasource_id=ds_id,
                    reason=f"refreshable root probe '{ds.freshness_probe}' returned false",
                    filters=UpdateKeys(),
                    kind=RefreshKind.SCRIPT,
                )
            return None

        # Non-managed roots remain untouchable.
        if ds.is_root:
            return None

        # Non-root: ensure we have its watermark.
        if ds_id not in self.watermarks:
            self.watermark_asset(ds, executor)

        if ds.freshness_probe:
            if not self._run_freshness_probe_cached(ds.freshness_probe):
                return StaleAsset(
                    datasource_id=ds_id,
                    reason=f"freshness probe '{ds.freshness_probe}' returned false",
                    filters=UpdateKeys(),
                )

        if has_schema_mismatch(ds, executor, cache=self._cache):
            return StaleAsset(
                datasource_id=ds_id,
                reason="schema changed: column mismatch",
                filters=UpdateKeys(),
            )

        if is_missing_local_file(ds):
            return StaleAsset(
                datasource_id=ds_id,
                reason="file not found",
                filters=UpdateKeys(),
            )

        # Watermark lag — needs concept_max_watermarks for the comparison.
        self._ensure_concept_max_watermarks(env, executor, root_assets)
        watermark = self.watermarks.get(ds_id)
        if watermark:
            for key, val in watermark.keys.items():
                if val.type == UpdateKeyType.INCREMENTAL_KEY:
                    max_val = self.concept_max_watermarks.get(key)
                    if max_val and max_val.value is not None:
                        try:
                            is_behind = val.value is None or (
                                _compare_watermark_values(val.value, max_val.value) < 0
                            )
                        except TypeError as e:
                            raise TypeError(
                                f"Cannot compare watermarks for field '{key}'"
                                f" in datasource '{ds_id}': {e}"
                            ) from e
                        if is_behind:
                            filters = (
                                UpdateKeys(keys={key: val})
                                if val.value
                                else UpdateKeys()
                            )
                            return StaleAsset(
                                datasource_id=ds_id,
                                reason=f"incremental key '{key}' behind: {val.value} < {max_val.value}",
                                filters=filters,
                            )
                elif val.type == UpdateKeyType.UPDATE_TIME:
                    max_val = self.concept_max_watermarks.get(key)
                    if max_val and max_val.value is not None:
                        try:
                            is_behind = val.value is None or (
                                _compare_watermark_values(val.value, max_val.value) < 0
                            )
                        except TypeError as e:
                            raise TypeError(
                                f"Cannot compare watermarks for field '{key}'"
                                f" in datasource '{ds_id}': {e}"
                            ) from e
                        if is_behind:
                            return StaleAsset(
                                datasource_id=ds_id,
                                reason=f"freshness '{key}' behind: {val.value} < {max_val.value}",
                                filters=UpdateKeys(),
                            )

        return None

    def get_stale_assets(
        self,
        env: Environment,
        executor: Executor,
        root_assets: set[str] | None = None,
        skip_datasources: set[str] | None = None,
    ) -> list[StaleAsset]:
        """Find all assets that are stale and need refresh.

        Args:
            env: The environment containing datasources
            executor: Executor for querying current state
            root_assets: Optional set of datasource identifiers that are "source of truth"
                         and should not be marked stale. If None, uses datasources marked
                         with is_root=True in the model.
            skip_datasources: Optional set of datasource identifiers to skip entirely
                              (won't be watermarked or checked for staleness)

        Returns:
            List of StaleAsset objects describing what needs refresh and why.
        """
        if root_assets is None:
            root_assets = {
                ds.identifier for ds in env.datasources.values() if ds.is_root
            }
        skip_datasources = skip_datasources or set()

        self.watermark_all_assets(env, executor, skip_datasources=skip_datasources)
        self._ensure_concept_max_watermarks(env, executor, root_assets)

        stale: list[StaleAsset] = []
        for ds_id in env.datasources:
            if ds_id in skip_datasources:
                continue
            asset = self.is_stale(env, executor, ds_id, root_assets=root_assets)
            if asset is not None:
                stale.append(asset)
        return stale


@dataclass
class RefreshResult:
    """Result of refreshing stale assets."""

    stale_count: int
    refreshed_count: int
    root_assets: int
    all_assets: int

    @property
    def had_stale(self) -> bool:
        return self.stale_count > 0


@dataclass
class RefreshPlan:
    """Computed refresh plan before any assets are updated."""

    stale_assets: list[StaleAsset]
    forced_assets: list[StaleAsset]
    watermarks: dict[str, DatasourceWatermark]
    concept_max_watermarks: dict[str, UpdateKey]
    root_assets: int
    all_assets: int
    root_watermarks: dict[str, DatasourceWatermark] = field(default_factory=dict)

    @property
    def refresh_assets(self) -> list[StaleAsset]:
        return self.stale_assets + self.forced_assets

    @property
    def stale_count(self) -> int:
        return len(self.refresh_assets)

    @property
    def had_stale(self) -> bool:
        return self.stale_count > 0


def create_refresh_plan(
    executor: "Executor",
    force_sources: set[str] | None = None,
    cache: ColumnStatsCache | None = None,
    skip_datasources: set[str] | None = None,
    initial_watermarks: dict[str, DatasourceWatermark] | None = None,
) -> RefreshPlan:
    """Compute which assets would be refreshed without executing updates.

    skip_datasources: ds_ids to completely ignore (already covered by another owner script).
    initial_watermarks: pre-collected watermarks (e.g. root watermarks from a prior phase).
    """
    state_store = BaseStateStore(cache=cache)
    if initial_watermarks:
        state_store.watermarks.update(initial_watermarks)
    force_sources = force_sources or set()
    extra_skip = skip_datasources or set()
    all_skip = force_sources | extra_skip

    stale_assets = state_store.get_stale_assets(
        executor.environment, executor, skip_datasources=all_skip
    )

    stale_ids = {a.datasource_id for a in stale_assets}
    forced_assets: list[StaleAsset] = []
    for ds in executor.environment.datasources.values():
        if (
            ds.identifier in force_sources
            and ds.identifier not in stale_ids
            and ds.identifier not in extra_skip
        ):
            kind = (
                RefreshKind.SCRIPT
                if ds.is_root and ds.refresh_script
                else RefreshKind.SQL
            )
            forced_assets.append(
                StaleAsset(
                    datasource_id=ds.identifier,
                    reason="forced rebuild",
                    kind=kind,
                )
            )

    root_ds_ids = {
        ds.identifier for ds in executor.environment.datasources.values() if ds.is_root
    }
    root_assets = len(root_ds_ids)
    all_assets = len(executor.environment.datasources)
    root_watermarks = {
        ds_id: wm
        for ds_id, wm in state_store.watermarks.items()
        if ds_id in root_ds_ids
    }

    return RefreshPlan(
        stale_assets=stale_assets,
        forced_assets=forced_assets,
        watermarks=state_store.watermarks,
        concept_max_watermarks=state_store.concept_max_watermarks,
        root_assets=root_assets,
        all_assets=all_assets,
        root_watermarks=root_watermarks,
    )


class RefreshAssetError(RuntimeError):
    """Raised when refreshing a specific asset fails. Wraps the underlying error
    with the datasource id and refresh reason for clearer diagnostics."""

    def __init__(self, datasource_id: str, reason: str, original: BaseException):
        self.datasource_id = datasource_id
        self.reason = reason
        self.original = original
        super().__init__(
            f"Failed to refresh datasource '{datasource_id}' "
            f"(stale because: {reason}): {type(original).__name__}: {original}"
        )


def _execute_one_asset(
    executor: "Executor",
    store: BaseStateStore,
    asset: StaleAsset,
    pending_sql_ds_ids: set[str],
    on_refresh: Callable[[str, str], None] | None,
    on_refresh_query: Callable[[str, str], None] | None,
    dry_run: bool,
) -> None:
    """Run a single refresh asset, dispatching on kind.

    For SQL-kind, hides other not-yet-refreshed SQL datasources from the planner
    so generated SQL doesn't read through stale upstream tables. After a
    successful refresh (real or dry-run), invalidates the store's cache for this
    physical address so downstream evaluations re-query.
    """
    if on_refresh:
        on_refresh(asset.datasource_id, asset.reason)
    datasource = executor.environment.datasources[asset.datasource_id]

    if asset.kind == RefreshKind.SCRIPT:
        if not datasource.is_root or not datasource.refresh_script:
            raise RefreshAssetError(
                asset.datasource_id,
                asset.reason,
                RuntimeError(
                    "script-kind refresh on a datasource without is_root + refresh_script"
                ),
            )
        if dry_run:
            if on_refresh_query:
                on_refresh_query(
                    asset.datasource_id,
                    f"# refresh script (dry-run): {datasource.refresh_script}",
                )
            return
        try:
            run_refresh_script(
                datasource.refresh_script,
                cwd=str(executor.environment.working_path),
            )
        except Exception as e:
            raise RefreshAssetError(asset.datasource_id, asset.reason, e) from e
        # Invalidate before the post-refresh re-probe so the probe reads fresh.
        store.invalidate_address(executor.environment, datasource.safe_address)
        if datasource.freshness_probe and not store._run_freshness_probe_cached(
            datasource.freshness_probe
        ):
            raise RefreshAssetError(
                asset.datasource_id,
                asset.reason,
                RuntimeError(
                    f"refresh script '{datasource.refresh_script}' exited 0 "
                    f"but probe '{datasource.freshness_probe}' still returned false"
                ),
            )
        return

    # SQL kind
    if datasource.is_root:
        raise RefreshAssetError(
            asset.datasource_id,
            asset.reason,
            RuntimeError(
                "SQL refresh attempted on a root datasource — only "
                "refreshable roots (with refresh_script) are managed"
            ),
        )
    hidden = {
        ds_id: executor.environment.datasources.pop(ds_id)
        for ds_id in pending_sql_ds_ids
        if ds_id in executor.environment.datasources
    }
    try:
        try:
            sql = executor.update_datasource(
                datasource, keys=asset.filters, dry_run=dry_run
            )
        except Exception as e:
            raise RefreshAssetError(asset.datasource_id, asset.reason, e) from e
        if on_refresh_query and sql is not None:
            on_refresh_query(asset.datasource_id, sql)
    finally:
        executor.environment.datasources.update(hidden)
    # Invalidate so any downstream re-eval queries the post-refresh state.
    store.invalidate_address(executor.environment, datasource.safe_address)


def execute_refresh_plan(
    executor: "Executor",
    plan: RefreshPlan,
    on_refresh: Callable[[str, str], None] | None = None,
    on_refresh_query: Callable[[str, str], None] | None = None,
    dry_run: bool = False,
    state_store: BaseStateStore | None = None,
    cascade: bool = True,
) -> RefreshResult:
    """Execute a refresh plan with deferred staleness for cross-script cascade.

    Order of operations:
      1. Process script-kind assets first; each refresh invalidates its address
         so subsequent SQL evaluations read the post-refresh state.
      2. Re-evaluate each SQL-kind asset against the now-current cache —
         a refresh may not be needed any more, or its filters may have shifted.
      3. After the original plan's assets are processed, if any script-kind
         assets ran, walk every other managed datasource through `is_stale` to
         catch cascade dependents that probed fresh against the pre-refresh
         watermark.

    `state_store=None` builds one seeded from the plan's watermarks.
    `cascade=False` skips step 3 — used by directory-mode managed nodes where
    cross-managed-node cascade is the orchestrator's responsibility.
    """

    store = state_store
    if store is None:
        store = BaseStateStore()
        store.watermarks.update(plan.watermarks)
        if plan.concept_max_watermarks:
            store.concept_max_watermarks = dict(plan.concept_max_watermarks)

    refreshed = 0
    total_stale = plan.stale_count
    handled: set[str] = set()
    has_scripts = any(a.kind == RefreshKind.SCRIPT for a in plan.refresh_assets)

    # Process scripts first so their invalidations precede SQL eval.
    initial = sorted(
        plan.refresh_assets,
        key=lambda a: 0 if a.kind == RefreshKind.SCRIPT else 1,
    )
    pending_sql = {a.datasource_id for a in initial if a.kind != RefreshKind.SCRIPT}

    for asset in initial:
        if asset.datasource_id in handled:
            continue

        # SQL-kind assets may have been invalidated by a script-kind refresh
        # earlier in this same loop. Re-evaluate against the live store, except
        # for explicit `forced rebuild` which bypasses staleness checks.
        if (
            asset.kind != RefreshKind.SCRIPT
            and asset.reason != "forced rebuild"
            and has_scripts
            and not dry_run
        ):
            current = store.is_stale(
                executor.environment, executor, asset.datasource_id
            )
            if current is None:
                handled.add(asset.datasource_id)
                pending_sql.discard(asset.datasource_id)
                continue
            asset = current

        if asset.kind != RefreshKind.SCRIPT:
            pending_sql.discard(asset.datasource_id)

        _execute_one_asset(
            executor,
            store,
            asset,
            pending_sql,
            on_refresh,
            on_refresh_query,
            dry_run,
        )
        refreshed += 1
        handled.add(asset.datasource_id)

    # Cascade: any non-root, non-handled datasource that became stale because a
    # script-kind refresh moved its upstream root.
    if cascade and has_scripts and not dry_run:
        cascade_assets: list[StaleAsset] = []
        for ds_id in executor.environment.datasources:
            if ds_id in handled:
                continue
            candidate = store.is_stale(executor.environment, executor, ds_id)
            if candidate is not None and candidate.kind != RefreshKind.SCRIPT:
                cascade_assets.append(candidate)

        cascade_pending = {a.datasource_id for a in cascade_assets}
        for asset in cascade_assets:
            cascade_pending.discard(asset.datasource_id)
            _execute_one_asset(
                executor,
                store,
                asset,
                cascade_pending,
                on_refresh,
                on_refresh_query,
                dry_run,
            )
            refreshed += 1
            total_stale += 1
            handled.add(asset.datasource_id)

    return RefreshResult(
        stale_count=total_stale,
        refreshed_count=refreshed,
        root_assets=plan.root_assets,
        all_assets=plan.all_assets,
    )


def refresh_stale_assets(
    executor: "Executor",
    on_stale_found: Callable[[int, int, int], None] | None = None,
    on_refresh: Callable[[str, str], None] | None = None,
    on_watermarks: (
        Callable[[dict[str, DatasourceWatermark], dict[str, UpdateKey]], None] | None
    ) = None,
    on_approval: (
        Callable[[list[StaleAsset], dict[str, DatasourceWatermark]], bool] | None
    ) = None,
    force_sources: set[str] | None = None,
    on_refresh_query: Callable[[str, str], None] | None = None,
    dry_run: bool = False,
    cache: ColumnStatsCache | None = None,
) -> RefreshResult:
    """Find and refresh stale assets.

    Args:
        executor: The executor with parsed environment
        on_stale_found: Optional callback(stale_count, root_assets, all_assets)
        on_refresh: Optional callback(asset_id, reason) called before each refresh
        on_watermarks: Optional callback(watermarks_dict) called after collecting watermarks
        on_approval: Optional callback(stale_assets, watermarks) called before refresh.
                     Return True to proceed, False to skip.
        force_sources: Optional set of datasource names to force rebuild (skip detection)
        cache: Optional column stats cache to avoid redundant metadata DB queries
    """
    plan = create_refresh_plan(
        executor,
        force_sources=force_sources,
        cache=cache,
    )

    if on_watermarks:
        on_watermarks(plan.watermarks, plan.concept_max_watermarks)

    if on_stale_found:
        on_stale_found(plan.stale_count, plan.root_assets, plan.all_assets)

    if on_approval and plan.refresh_assets:
        if not on_approval(plan.refresh_assets, plan.watermarks):
            return RefreshResult(
                stale_count=plan.stale_count,
                refreshed_count=0,
                root_assets=plan.root_assets,
                all_assets=plan.all_assets,
            )

    return execute_refresh_plan(
        executor,
        plan,
        on_refresh=on_refresh,
        on_refresh_query=on_refresh_query,
        dry_run=dry_run,
    )
