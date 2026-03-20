from dataclasses import dataclass
from typing import Callable

from trilogy import Executor
from trilogy.core.enums import Purpose
from trilogy.core.models.datasource import UpdateKey, UpdateKeys, UpdateKeyType
from trilogy.core.models.environment import Environment
from trilogy.execution.state.watermarks import (
    DatasourceWatermark,
    StaleAsset,
    _compare_watermark_values,
    get_concept_max_watermark_abstract,
    get_concept_max_watermarks,
    get_freshness_watermarks,
    get_incremental_key_watermarks,
    get_last_update_time_watermarks,
    get_unique_key_hash_watermarks,
    has_schema_mismatch,
    run_freshness_probe,
)


class BaseStateStore:

    def __init__(self) -> None:
        self.watermarks: dict[str, DatasourceWatermark] = {}

    def watermark_asset(self, datasource, executor: Executor) -> DatasourceWatermark:
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
            if ds.is_root:
                if ds.freshness_by or ds.incremental_by:
                    self.watermark_asset(ds, executor)
                elif needed_concepts:
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
        stale: list[StaleAsset] = []

        self.watermark_all_assets(env, executor, skip_datasources=skip_datasources)

        for ds in env.datasources.values():
            if (
                ds.freshness_probe
                and ds.identifier not in root_assets
                and ds.identifier not in skip_datasources
            ):
                if not run_freshness_probe(ds.freshness_probe):
                    stale.append(
                        StaleAsset(
                            datasource_id=ds.identifier,
                            reason=f"freshness probe '{ds.freshness_probe}' returned false",
                            filters=UpdateKeys(),
                        )
                    )

        already_stale = {a.datasource_id for a in stale}
        for ds in env.datasources.values():
            if (
                ds.identifier not in root_assets
                and ds.identifier not in skip_datasources
                and ds.identifier not in already_stale
                and has_schema_mismatch(ds, executor)
            ):
                stale.append(
                    StaleAsset(
                        datasource_id=ds.identifier,
                        reason="schema changed: column mismatch",
                        filters=UpdateKeys(),
                    )
                )

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

        # For derived concepts (e.g. `auto x <- greatest(a, b)`) that don't appear
        # directly on any root datasource, query their expected value using only roots.
        missing_derived: dict[str, str] = {}  # concept name -> address
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

        for ds_id, watermark in self.watermarks.items():
            if ds_id in root_assets:
                continue

            for key, val in watermark.keys.items():
                if val.type == UpdateKeyType.INCREMENTAL_KEY:
                    max_val = concept_max_watermarks.get(key)
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
                            stale.append(
                                StaleAsset(
                                    datasource_id=ds_id,
                                    reason=f"incremental key '{key}' behind: {val.value} < {max_val.value}",
                                    filters=filters,
                                )
                            )
                            break

                elif val.type == UpdateKeyType.UPDATE_TIME:
                    max_val = concept_max_watermarks.get(key)
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
                            stale.append(
                                StaleAsset(
                                    datasource_id=ds_id,
                                    reason=f"freshness '{key}' behind: {val.value} < {max_val.value}",
                                    filters=UpdateKeys(),
                                )
                            )
                            break

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


def refresh_stale_assets(
    executor: "Executor",
    on_stale_found: Callable[[int, int, int], None] | None = None,
    on_refresh: Callable[[str, str], None] | None = None,
    on_watermarks: Callable[[dict[str, DatasourceWatermark]], None] | None = None,
    on_approval: (
        Callable[[list[StaleAsset], dict[str, DatasourceWatermark]], bool] | None
    ) = None,
    force_sources: set[str] | None = None,
    on_refresh_query: Callable[[str, str], None] | None = None,
    dry_run: bool = False,
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
    """
    state_store = BaseStateStore()
    force_sources = force_sources or set()

    stale_assets = state_store.get_stale_assets(
        executor.environment, executor, skip_datasources=force_sources
    )

    stale_ids = {a.datasource_id for a in stale_assets}
    forced_assets: list[StaleAsset] = []
    for ds in executor.environment.datasources.values():
        if ds.identifier in force_sources and ds.identifier not in stale_ids:
            forced_assets.append(
                StaleAsset(datasource_id=ds.identifier, reason="forced rebuild")
            )

    if on_watermarks:
        on_watermarks(state_store.watermarks)
    root_assets = sum(
        1 for asset in executor.environment.datasources.values() if asset.is_root
    )
    all_assets = len(executor.environment.datasources)

    all_refresh_assets = stale_assets + forced_assets

    if on_stale_found:
        on_stale_found(len(all_refresh_assets), root_assets, all_assets)

    if on_approval and all_refresh_assets:
        if not on_approval(all_refresh_assets, state_store.watermarks):
            return RefreshResult(
                stale_count=len(all_refresh_assets),
                refreshed_count=0,
                root_assets=root_assets,
                all_assets=all_assets,
            )

    refreshed = 0
    remaining_stale = {a.datasource_id for a in all_refresh_assets}
    for asset in all_refresh_assets:
        remaining_stale.discard(asset.datasource_id)
        hidden = {
            ds_id: executor.environment.datasources.pop(ds_id)
            for ds_id in remaining_stale
            if ds_id in executor.environment.datasources
        }
        try:
            if on_refresh:
                on_refresh(asset.datasource_id, asset.reason)
            datasource = executor.environment.datasources[asset.datasource_id]
            sql = executor.update_datasource(datasource, dry_run=dry_run)
            if on_refresh_query and sql is not None:
                on_refresh_query(asset.datasource_id, sql)
            refreshed += 1
        finally:
            executor.environment.datasources.update(hidden)

    return RefreshResult(
        stale_count=len(all_refresh_assets),
        refreshed_count=refreshed,
        root_assets=root_assets,
        all_assets=all_assets,
    )
