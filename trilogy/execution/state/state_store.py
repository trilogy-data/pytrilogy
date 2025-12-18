from dataclasses import dataclass, field

from trilogy import Executor
from trilogy.core.enums import Purpose
from trilogy.core.models.datasource import (
    Address,
    Datasource,
    UpdateKey,
    UpdateKeys,
    UpdateKeyType,
)
from trilogy.core.models.environment import Environment


@dataclass
class DatasourceWatermark:
    keys: dict[str, UpdateKey]


@dataclass
class StaleAsset:
    """Represents an asset that needs to be refreshed."""

    datasource_id: str
    reason: str
    filters: UpdateKeys = field(default_factory=UpdateKeys)


def _compare_watermark_values(a: str | int | float, b: str | int | float) -> int:
    """Compare two watermark values, returning -1, 0, or 1.

    Handles type mismatches by comparing string representations.
    """
    if type(a) is type(b):
        if a < b:  # type: ignore[operator]
            return -1
        elif a > b:  # type: ignore[operator]
            return 1
        return 0
    # Different types: compare as strings
    sa, sb = str(a), str(b)
    if sa < sb:
        return -1
    elif sa > sb:
        return 1
    return 0


def get_last_update_time_watermarks(
    datasource: Datasource, executor: Executor
) -> DatasourceWatermark:
    update_time = executor.generator.get_table_last_modified(
        executor, datasource.safe_address
    )
    return DatasourceWatermark(
        keys={
            "update_time": UpdateKey(
                concept_name="update_time",
                type=UpdateKeyType.UPDATE_TIME,
                value=update_time,
            )
        }
    )


def get_unique_key_hash_watermarks(
    datasource: Datasource, executor: Executor
) -> DatasourceWatermark:
    key_columns = []
    for col_assignment in datasource.columns:
        concrete = executor.environment.concepts[col_assignment.concept.address]
        if concrete.purpose == Purpose.KEY:
            key_columns.append(concrete)

    if not key_columns:
        return DatasourceWatermark(keys={})

    if isinstance(datasource.address, Address) and datasource.address.is_query:
        table_ref = f"({datasource.safe_address})"
    else:
        table_ref = datasource.safe_address

    watermarks = {}
    for col in key_columns:
        hash_expr = executor.generator.hash_column_value(col.name)
        checksum_expr = executor.generator.aggregate_checksum(hash_expr)
        query = f"SELECT {checksum_expr} as checksum FROM {table_ref}"
        result = executor.execute_raw_sql(query).fetchone()

        checksum_value = result[0] if result else None
        watermarks[col.name] = UpdateKey(
            concept_name=col.name,
            type=UpdateKeyType.KEY_HASH,
            value=checksum_value,
        )

    return DatasourceWatermark(keys=watermarks)


def get_incremental_key_watermarks(
    datasource: Datasource, executor: Executor
) -> DatasourceWatermark:
    if not datasource.incremental_by:
        return DatasourceWatermark(keys={})

    if isinstance(datasource.address, Address) and datasource.address.is_query:
        table_ref = f"({datasource.safe_address})"
    else:
        table_ref = datasource.safe_address

    watermarks = {}
    for concept_ref in datasource.incremental_by:
        concept = executor.environment.concepts[concept_ref.address]
        query = f"SELECT MAX({concept.name}) as max_value FROM {table_ref}"
        result = executor.execute_raw_sql(query).fetchone()

        max_value = result[0] if result else None
        watermarks[concept.name] = UpdateKey(
            concept_name=concept.name,
            type=UpdateKeyType.INCREMENTAL_KEY,
            value=max_value,
        )

    return DatasourceWatermark(keys=watermarks)


class BaseStateStore:

    def __init__(self):
        self.watermarks: dict[str, DatasourceWatermark] = {}

    def watermark_asset(
        self, datasource: Datasource, executor: Executor
    ) -> DatasourceWatermark:
        if datasource.incremental_by:
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

    def get_datasource_watermarks(
        self, datasource: Datasource
    ) -> DatasourceWatermark | None:
        return self.watermarks.get(datasource.identifier)

    def check_datasource_state(self, datasource: Datasource) -> bool:
        return datasource.identifier in self.watermarks

    def watermark_all_assets(
        self, env: Environment, executor: Executor
    ) -> dict[str, DatasourceWatermark]:
        """Watermark all datasources in the environment."""
        for ds in env.datasources.values():
            self.watermark_asset(ds, executor)
        return self.watermarks

    def get_stale_assets(
        self,
        env: Environment,
        executor: Executor,
        root_assets: set[str] | None = None,
    ) -> list[StaleAsset]:
        """Find all assets that are stale and need refresh.

        Args:
            env: The environment containing datasources
            executor: Executor for querying current state
            root_assets: Set of datasource identifiers that are "source of truth"
                         and should not be marked stale. If None, defaults to empty.

        Returns:
            List of StaleAsset objects describing what needs refresh and why.
        """
        root_assets = root_assets or set()
        stale: list[StaleAsset] = []

        # First pass: watermark all assets to get current state
        self.watermark_all_assets(env, executor)

        # Build map of concept -> max watermark across all assets
        concept_max_watermarks: dict[str, UpdateKey] = {}
        for ds_id, watermark in self.watermarks.items():
            if ds_id in root_assets:
                # Root assets define the "truth" for incremental keys
                for key, val in watermark.keys.items():
                    if (
                        val.type == UpdateKeyType.INCREMENTAL_KEY
                        and val.value is not None
                    ):
                        existing = concept_max_watermarks.get(key)
                        if existing is None or (
                            existing.value is not None
                            and _compare_watermark_values(val.value, existing.value) > 0
                        ):
                            concept_max_watermarks[key] = val

        # Second pass: check non-root assets against max watermarks
        for ds_id, watermark in self.watermarks.items():
            if ds_id in root_assets:
                continue

            for key, val in watermark.keys.items():
                if val.type == UpdateKeyType.INCREMENTAL_KEY:
                    max_val = concept_max_watermarks.get(key)
                    if max_val and max_val.value is not None:
                        if (
                            val.value is None
                            or _compare_watermark_values(val.value, max_val.value) < 0
                        ):
                            # Create UpdateKeys with the filter for incremental update
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
                    # For update_time, we'd need root asset update times to compare
                    # This is tricky without explicit dependency tracking
                    pass

                elif val.type == UpdateKeyType.KEY_HASH:
                    # Hash changes indicate data changed, but we need a reference
                    # to compare against - requires dependency graph
                    pass

        return stale
