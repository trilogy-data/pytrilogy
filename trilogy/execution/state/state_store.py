import threading
from collections.abc import Callable, Iterator, Mapping
from contextlib import contextmanager
from dataclasses import dataclass, field
from types import MappingProxyType
from typing import Protocol, runtime_checkable

from trilogy import Executor
from trilogy.constants import logger
from trilogy.core.enums import Purpose
from trilogy.core.models.datasource import (
    Datasource,
    UpdateKey,
    UpdateKeys,
    UpdateKeyType,
)
from trilogy.core.models.environment import Environment
from trilogy.execution.state.cache import ColumnStatsCache
from trilogy.execution.state.isolation import hidden_datasources
from trilogy.execution.state.partitions import (
    PartitionObservation,
    is_partitioned,
    probe_expected_partitions,
    probe_observed_partitions,
    selected_slice,
    stale_slices,
)
from trilogy.execution.state.phases import get_phase_recorder
from trilogy.execution.state.watermarks import (
    DatasourceWatermark,
    RefreshKind,
    StaleAsset,
    _compare_watermark_values,
    get_concept_max_watermarks,
    get_concept_max_watermarks_abstract,
    get_freshness_watermarks,
    get_incremental_key_watermarks,
    get_last_update_time_watermarks,
    get_unique_key_hash_watermarks,
    has_schema_mismatch,
    is_missing_local_file,
    run_freshness_probe,
    run_refresh_script,
    within_allowed_lag,
)

LOGGER_PREFIX = "[STATE_STORE]"


@runtime_checkable
class StateStore(Protocol):
    """The contract the refresh planner/executor requires of a state store.

    ``BaseStateStore`` (in-memory, re-derives from the warehouse) is the
    default implementation; alternate backends (file, sqlite, remote/orchestrator
    -managed) implement this Protocol and are injected via the ``state_store``
    parameters on :func:`create_refresh_plan` / :func:`execute_refresh_plan` /
    :func:`refresh_stale_assets`. Mirrors the ``ColumnStatsCache`` pattern in
    ``cache.py``.
    """

    watermarks: dict[str, DatasourceWatermark]
    concept_max_watermarks: dict[str, UpdateKey]

    def invalidate(self, ds_id: str) -> None: ...

    def invalidate_address(self, env: Environment, address: str) -> None: ...

    def watermark_asset(
        self, datasource: Datasource, executor: Executor
    ) -> DatasourceWatermark: ...

    def get_datasource_watermarks(
        self, datasource: Datasource
    ) -> DatasourceWatermark | None: ...

    def check_datasource_state(self, datasource: Datasource) -> bool: ...

    def watermark_all_assets(
        self,
        env: Environment,
        executor: Executor,
        skip_datasources: set[str] | None = None,
    ) -> dict[str, DatasourceWatermark]: ...

    def is_stale(
        self,
        env: Environment,
        executor: Executor,
        ds_id: str,
        root_assets: set[str] | None = None,
        force: bool = False,
    ) -> StaleAsset | None: ...

    def get_stale_assets(
        self,
        env: Environment,
        executor: Executor,
        root_assets: set[str] | None = None,
        skip_datasources: set[str] | None = None,
    ) -> list[StaleAsset]: ...

    def partition_asset(
        self,
        env: Environment,
        executor: Executor,
        ds_id: str,
        root_assets: set[str] | None = None,
    ) -> tuple[list[PartitionObservation], list[PartitionObservation]] | None: ...

    def run_freshness_probe_cached(self, probe_path: str) -> bool: ...


class BaseStateStore:

    def __init__(self, cache: ColumnStatsCache | None = None) -> None:
        self.watermarks: dict[str, DatasourceWatermark] = {}
        self.concept_max_watermarks: dict[str, UpdateKey] = {}
        # ds_id -> (observed slices, expected slices) for partitioned assets.
        self.partitions: dict[
            str, tuple[list[PartitionObservation], list[PartitionObservation]]
        ] = {}
        self._cache = cache
        # Probe path -> result; deduplicates subprocess calls for the same probe
        # script during one refresh invocation.
        self._probe_results: dict[str, bool] = {}
        # Model-fingerprint baseline: logical location -> effective hash the
        # asset was last built with (installed from an env's recorded
        # fingerprint; SnapshotStateStore also reads its snapshot's per-asset
        # record). Empty means the check never fires.
        self._model_baseline: dict[str, str] = get_model_fingerprint_baseline() or {}
        # ds_id -> current effective hash; pure function of the parsed model,
        # so cacheable for the store's lifetime (stores are per-script).
        self._model_fp_cache: dict[str, str] = {}
        # Assets refreshed during this invocation: the recorded fingerprint
        # describes the PRE-refresh build, so once invalidate*() runs the
        # claim must not resurrect — same rule as re-seeding watermarks.
        self._model_refreshed: set[str] = set()
        # Mutations to the caches happen from parallel managed-node executions;
        # serialize them to keep the dicts consistent.
        self._lock = threading.Lock()

    def run_freshness_probe_cached(self, probe_path: str) -> bool:
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
            self.partitions.pop(ds_id, None)
            self._model_refreshed.add(ds_id)
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
                self.partitions.pop(ds_id, None)
                self._model_refreshed.add(ds_id)
            for probe in affected_probes:
                self._probe_results.pop(probe, None)
            self.concept_max_watermarks.clear()

    def recorded_model_fingerprint(
        self, env: Environment, ds: Datasource
    ) -> str | None:
        """The effective model hash the asset was last built with, if known."""
        if not self._model_baseline:
            return None
        from trilogy.core.fingerprint import datasource_logical_location

        location = datasource_logical_location(ds)
        if location is None:
            return None
        return self._model_baseline.get(location)

    def _current_model_fingerprint(
        self, env: Environment, ds: Datasource
    ) -> str | None:
        with self._lock:
            cached = self._model_fp_cache.get(ds.identifier)
        if cached is not None:
            return cached
        # Staleness must never fail on fingerprinting; None skips the check.
        try:
            from trilogy.core.fingerprint import datasource_effective_hash

            current = datasource_effective_hash(env, ds)
        except Exception as e:
            logger.debug(f"Model fingerprint of {ds.identifier} skipped: {e}")
            return None
        with self._lock:
            self._model_fp_cache[ds.identifier] = current
        return current

    def _model_changed(self, env: Environment, ds: Datasource) -> bool:
        if ds.identifier in self._model_refreshed:
            return False
        recorded = self.recorded_model_fingerprint(env, ds)
        if recorded is None:
            return False
        current = self._current_model_fingerprint(env, ds)
        return current is not None and current != recorded

    def watermark_asset(
        self, datasource: Datasource, executor: Executor
    ) -> DatasourceWatermark:
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

    def partition_asset(
        self,
        env: Environment,
        executor: Executor,
        ds_id: str,
        root_assets: set[str] | None = None,
    ) -> tuple[list[PartitionObservation], list[PartitionObservation]] | None:
        """Observed and expected slices for a partitioned datasource.

        None for anything unpartitioned (and for roots, which are the expected
        side of every comparison and never judged themselves). Cached per
        invocation and dropped by ``invalidate*`` alongside watermarks, so a
        post-refresh re-probe sees the slices the refresh just wrote.
        """
        ds = env.datasources[ds_id]
        if not is_partitioned(ds):
            return None
        with self._lock:
            cached = self.partitions.get(ds_id)
        if cached is not None:
            return cached
        if root_assets is None:
            root_assets = {d.identifier for d in env.datasources.values() if d.is_root}
        probed = (
            probe_observed_partitions(ds, executor),
            probe_expected_partitions(ds, executor, root_assets),
        )
        with self._lock:
            self.partitions[ds_id] = probed
        return probed

    def get_datasource_watermarks(
        self, datasource: Datasource
    ) -> DatasourceWatermark | None:
        return self.watermarks.get(datasource.identifier)

    def check_datasource_state(self, datasource: Datasource) -> bool:
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
                    # Keys are concept addresses, so this is an exact lookup —
                    # which may lazily materialize an auto-derived property
                    # (``created_at.date``) rather than scanning for a name.
                    concept = env.concepts.get(key)
                    if concept is not None and concept.lineage is not None:
                        missing_derived[key] = concept.address

        if missing_derived:
            derived_maxes = get_concept_max_watermarks_abstract(
                list(missing_derived.values()), executor, root_assets
            )
            for key, concept_address in missing_derived.items():
                wm = derived_maxes[concept_address]
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

        is_managed_root = ds.is_refreshable_root

        if force:
            kind = (
                RefreshKind.SCRIPT
                if (ds.is_root and ds.refresh_script)
                else RefreshKind.SQL
            )
            return StaleAsset(
                datasource_id=ds_id,
                reason="forced rebuild",
                kind=kind,
                explicit=True,
            )

        if is_managed_root:
            if not self.run_freshness_probe_cached(ds.freshness_probe):  # type: ignore[arg-type]
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

        if ds.freshness_probe and not self.run_freshness_probe_cached(
            ds.freshness_probe
        ):
            return StaleAsset(
                datasource_id=ds_id,
                reason=f"freshness probe '{ds.freshness_probe}' returned false",
                filters=UpdateKeys(),
            )

        # Model change: the definition the asset was last built with no longer
        # matches the current model. Checked ahead of the schema probe (pure
        # CPU, no warehouse query); like schema change, empty filters force a
        # full rebuild — an incremental filter would keep old-definition rows.
        if self._model_changed(env, ds):
            return StaleAsset(
                datasource_id=ds_id,
                reason="model changed since last build",
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

        # Per-slice verdict first for a partitioned asset: a missing slice can
        # hold rows OLDER than the table's MAX, so the table-level comparison
        # below reports fresh while a hole sits in the middle of the range.
        # Falling through when nothing is stale keeps the coarse check as the
        # backstop (an unprobeable expectation must not read as "fresh").
        probed = self.partition_asset(env, executor, ds_id, root_assets=root_assets)
        if probed is not None:
            slices = stale_slices(*probed)
            if slices:
                shown = ", ".join(obs.id for obs in slices[:3])
                suffix = "" if len(slices) <= 3 else f", +{len(slices) - 3} more"
                return StaleAsset(
                    datasource_id=ds_id,
                    reason=f"{len(slices)} stale partition(s): {shown}{suffix}",
                    partitions=slices,
                )

        # Watermark lag — needs concept_max_watermarks for the comparison.
        self._ensure_concept_max_watermarks(env, executor, root_assets)
        watermark = self.watermarks.get(ds_id)
        if watermark:
            for key, val in watermark.keys.items():
                if val.type not in (
                    UpdateKeyType.INCREMENTAL_KEY,
                    UpdateKeyType.UPDATE_TIME,
                ):
                    continue
                max_val = self.concept_max_watermarks.get(key)
                if not max_val or max_val.value is None:
                    continue
                try:
                    is_behind = val.value is None or (
                        _compare_watermark_values(val.value, max_val.value) < 0
                    )
                except TypeError as e:
                    raise TypeError(
                        f"Cannot compare watermarks for field '{key}'"
                        f" in datasource '{ds_id}': {e}"
                    ) from e
                if not is_behind:
                    continue
                lag = ds.allowed_lag
                if lag is not None:
                    if within_allowed_lag(val.value, max_val.value, lag, key):
                        logger.debug(
                            "%s '%s' behind on '%s' (%s < %s) but within allowed lag %s",
                            LOGGER_PREFIX,
                            ds_id,
                            key,
                            val.value,
                            max_val.value,
                            lag.render(),
                        )
                        continue
                    suffix = f" (exceeds allowed lag {lag.render()})"
                else:
                    suffix = ""
                if val.type == UpdateKeyType.INCREMENTAL_KEY:
                    return StaleAsset(
                        datasource_id=ds_id,
                        reason=f"incremental key '{key}' behind: {val.value} < {max_val.value}{suffix}",
                        filters=(
                            UpdateKeys(keys={key: val}) if val.value else UpdateKeys()
                        ),
                    )
                return StaleAsset(
                    datasource_id=ds_id,
                    reason=f"freshness '{key}' behind: {val.value} < {max_val.value}{suffix}",
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
        # Materialized: is_stale's partition probe hides non-root datasources for
        # the duration of its query, mutating this dict mid-iteration.
        for ds_id in list(env.datasources):
            if ds_id in skip_datasources:
                continue
            asset = self.is_stale(env, executor, ds_id, root_assets=root_assets)
            if asset is not None:
                stale.append(asset)
        return stale


StateStoreFactory = Callable[[ColumnStatsCache | None], StateStore]

# Ambient factory for the current invocation, letting a caller redirect every
# implicit store construction at once (e.g. seeding from a persisted snapshot —
# see ``persistence.py``). A plain global, not a ContextVar, for the same reason
# as report.py's sink: refresh evaluates managed nodes on worker threads a
# ContextVar set in the entrypoint would not reach.
_ACTIVE_FACTORY: StateStoreFactory | None = None


def set_state_store_factory(factory: StateStoreFactory | None) -> None:
    global _ACTIVE_FACTORY
    _ACTIVE_FACTORY = factory


def get_state_store_factory() -> StateStoreFactory | None:
    return _ACTIVE_FACTORY


def new_state_store(cache: ColumnStatsCache | None = None) -> StateStore:
    """Construct a state store: the installed factory's, else in-memory.

    Every implicit ``BaseStateStore()`` in the refresh pipeline goes through
    here, so installing a factory redirects the whole pipeline. An explicit
    ``state_store=`` argument always wins over the ambient factory.
    """
    if _ACTIVE_FACTORY is not None:
        return _ACTIVE_FACTORY(cache)
    return BaseStateStore(cache=cache)


@contextmanager
def state_store_factory(factory: StateStoreFactory | None) -> Iterator[None]:
    """Scope an ambient factory to a block, restoring the previous one."""
    previous = _ACTIVE_FACTORY
    set_state_store_factory(factory)
    try:
        yield
    finally:
        set_state_store_factory(previous)


# Ambient model-fingerprint baseline: logical physical location -> effective
# hash the asset was last built with. Same plain-module-global pattern (and
# reason) as _ACTIVE_FACTORY: stores are constructed on refresh worker
# threads a ContextVar set in the entrypoint would not reach. Installed by
# refresh from a deployment env's recorded fingerprint; snapshot-seeded
# stores prefer their snapshot's own per-asset record.
_MODEL_FINGERPRINT_BASELINE: dict[str, str] | None = None


def get_model_fingerprint_baseline() -> dict[str, str] | None:
    return _MODEL_FINGERPRINT_BASELINE


@contextmanager
def model_fingerprint_baseline(baseline: dict[str, str] | None) -> Iterator[None]:
    """Scope an ambient fingerprint baseline to a block."""
    global _MODEL_FINGERPRINT_BASELINE
    previous = _MODEL_FINGERPRINT_BASELINE
    _MODEL_FINGERPRINT_BASELINE = baseline
    try:
        yield
    finally:
        _MODEL_FINGERPRINT_BASELINE = previous


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


@dataclass(frozen=True)
class RefreshPolicy:
    """What the caller asked a refresh to do, as opposed to how a particular
    call site is wired up. ``skip_datasources``/``initial_watermarks``/``cache``/
    ``state_store`` genuinely differ per site and stay separate arguments.

    Grouping intent is not tidiness. When the four planning call sites took it
    as loose keyword arguments, ``partition_selector`` reached one of them and
    was silently ignored on the others: the flag parsed, the run succeeded, and
    it rebuilt slices the caller had not asked for. A new field here reaches
    every site by construction, and only
    :meth:`~trilogy.scripts.common.RefreshParams.policy` has to learn about it.

    One policy is shared by every managed node in a directory refresh, which
    evaluates them on a thread pool — hence frozen, and hence the selector is
    copied behind a read-only view rather than aliasing the caller's dict.
    """

    #: Datasource names to rebuild regardless of staleness (``--force``).
    force_sources: frozenset[str] = frozenset()
    #: Concept address -> value naming the slice this run owns
    #: (``--partition``). Empty means "let staleness decide".
    partition_selector: Mapping[str, str] = field(default_factory=dict)

    def __post_init__(self) -> None:
        object.__setattr__(
            self, "partition_selector", MappingProxyType(dict(self.partition_selector))
        )

    def __hash__(self) -> int:
        # The generated one would hash the (unhashable) mapping, leaving a frozen
        # dataclass that raises wherever anything treats it as a value.
        return hash(
            (self.force_sources, tuple(sorted(self.partition_selector.items())))
        )


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
    # ds_id -> (observed slices, expected slices); partitioned datasources only.
    partitions: dict[
        str, tuple[list[PartitionObservation], list[PartitionObservation]]
    ] = field(default_factory=dict)

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
    policy: RefreshPolicy | None = None,
    cache: ColumnStatsCache | None = None,
    skip_datasources: set[str] | None = None,
    initial_watermarks: dict[str, DatasourceWatermark] | None = None,
    state_store: StateStore | None = None,
) -> RefreshPlan:
    """Compute which assets would be refreshed without executing updates.

    policy: what the caller asked for — forced sources, a partition selector.
        See :class:`RefreshPolicy`; it is one object precisely so a new kind of
        intent cannot reach some planning call sites and not others.
    skip_datasources: ds_ids to completely ignore (already covered by another owner script).
    initial_watermarks: pre-collected watermarks (e.g. root watermarks from a prior phase).
    state_store: alternate StateStore backend; defaults to a fresh in-memory
        BaseStateStore. Pre-seeded watermarks on the store are respected
        (watermark_all_assets skips already-present ds_ids).
    """
    policy = policy if policy is not None else RefreshPolicy()
    if state_store is None:
        state_store = new_state_store(cache=cache)
    if initial_watermarks:
        state_store.watermarks.update(initial_watermarks)
    force_sources = set(policy.force_sources)
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
                    explicit=True,
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

    # Per-slice state for partitioned targets. Two extra queries per partitioned
    # datasource; nothing at all for the unpartitioned majority.
    partitions = {}
    # Materialized: partition_asset's expected probe hides non-root datasources
    # for the duration of its query, mutating this dict mid-iteration.
    for ds_id, ds in list(executor.environment.datasources.items()):
        if ds_id in all_skip or not is_partitioned(ds):
            continue
        probed = state_store.partition_asset(
            executor.environment, executor, ds_id, root_assets=root_ds_ids
        )
        if probed is not None:
            partitions[ds_id] = probed

    plan = RefreshPlan(
        stale_assets=stale_assets,
        forced_assets=forced_assets,
        watermarks=state_store.watermarks,
        concept_max_watermarks=state_store.concept_max_watermarks,
        root_assets=root_assets,
        all_assets=all_assets,
        root_watermarks=root_watermarks,
        partitions=partitions,
    )
    if policy.partition_selector:
        # ``extra_skip``, not ``all_skip``: a forced source is skipped by
        # detection because it is rebuilt regardless, which is not a reason to
        # withhold the narrowing. ``--force ds --partition day=X`` means "rebuild
        # ds's X, stale or not" — dropping the selector there would rebuild every
        # slice while the state delta still claimed only X.
        target_partition_selector(
            executor, plan, dict(policy.partition_selector), extra_skip
        )

    # Begin-phase capture: the planning probe is the last look at state
    # before anything executes. First-wins inside the recorder, so re-plans
    # and the post-run snapshot's own probe never overwrite the true begin.
    recorder = get_phase_recorder()
    if recorder is not None:
        recorder.record_plan(executor.environment, plan, skipped=extra_skip)

    return plan


def target_partition_selector(
    executor: "Executor",
    plan: RefreshPlan,
    selector: dict[str, str],
    skip: set[str],
) -> None:
    """Point the plan at exactly the slice ``selector`` names. Mutates ``plan``.

    A named datasource is refreshed **whether or not it looks stale** — a tick
    that owns 2026-07-30 must load it, and the slice may be absent from state
    entirely (never loaded, or a backfill of a day the watermark is past) — and
    narrowed to that slice, so healthy neighbours are untouched. Datasources the
    selector does not name plan normally.
    """
    targeted: dict[str, StaleAsset] = {}
    for ds_id, ds in executor.environment.datasources.items():
        if ds_id in skip or ds.is_root:
            continue
        slice_ = selected_slice(ds, executor.environment, selector)
        if slice_ is None:
            continue
        targeted[ds_id] = StaleAsset(
            datasource_id=ds_id,
            reason=f"partition {slice_.id} requested",
            kind=RefreshKind.SQL,
            partitions=[slice_],
            explicit=True,
        )
    if not targeted:
        return

    # Replace rather than append: an asset already judged stale would otherwise
    # be refreshed twice, once whole-table and once per slice, and the
    # whole-table pass is exactly what the selector is asking us not to do.
    plan.stale_assets = [
        targeted.get(asset.datasource_id, asset) for asset in plan.stale_assets
    ]
    placed = {a.datasource_id for a in plan.stale_assets}
    plan.forced_assets = [
        asset for asset in plan.forced_assets if asset.datasource_id not in targeted
    ] + [asset for ds_id, asset in targeted.items() if ds_id not in placed]


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
    store: StateStore,
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
        if datasource.freshness_probe and not store.run_freshness_probe_cached(
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
    with hidden_datasources(executor.environment, pending_sql_ds_ids):
        try:
            sql = executor.update_datasource(
                datasource,
                keys=asset.filters,
                dry_run=dry_run,
                partitions=asset.partitions or None,
            )
        except Exception as e:
            raise RefreshAssetError(asset.datasource_id, asset.reason, e) from e
        if on_refresh_query and sql is not None:
            on_refresh_query(asset.datasource_id, sql)
    # Invalidate so any downstream re-eval queries the post-refresh state.
    store.invalidate_address(executor.environment, datasource.safe_address)


def execute_refresh_plan(
    executor: "Executor",
    plan: RefreshPlan,
    on_refresh: Callable[[str, str], None] | None = None,
    on_refresh_query: Callable[[str, str], None] | None = None,
    dry_run: bool = False,
    state_store: "StateStore | None" = None,
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
        store = new_state_store()
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
        # for assets the caller asked for by name — re-deciding those would
        # discard the very intent that put them in the plan.
        if (
            asset.kind != RefreshKind.SCRIPT
            and not asset.explicit
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
        # Materialized: is_stale's partition probe mutates this dict (see
        # get_stale_assets).
        for ds_id in list(executor.environment.datasources):
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
    policy: RefreshPolicy | None = None,
    on_refresh_query: Callable[[str, str], None] | None = None,
    dry_run: bool = False,
    cache: ColumnStatsCache | None = None,
    state_store: "StateStore | None" = None,
) -> RefreshResult:
    """Find and refresh stale assets.

    Args:
        executor: The executor with parsed environment
        on_stale_found: Optional callback(stale_count, root_assets, all_assets)
        on_refresh: Optional callback(asset_id, reason) called before each refresh
        on_watermarks: Optional callback(watermarks_dict) called after collecting watermarks
        on_approval: Optional callback(stale_assets, watermarks) called before refresh.
                     Return True to proceed, False to skip.
        policy: What to refresh — see :class:`RefreshPolicy`.
        cache: Optional column stats cache to avoid redundant metadata DB queries
    """
    plan = create_refresh_plan(
        executor,
        policy=policy,
        cache=cache,
        state_store=state_store,
    )

    if on_watermarks:
        on_watermarks(plan.watermarks, plan.concept_max_watermarks)

    if on_stale_found:
        on_stale_found(plan.stale_count, plan.root_assets, plan.all_assets)

    if (
        on_approval
        and plan.refresh_assets
        and not on_approval(plan.refresh_assets, plan.watermarks)
    ):
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
        state_store=state_store,
    )
