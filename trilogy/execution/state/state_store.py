from dataclasses import dataclass, field
from datetime import date

from sqlalchemy.exc import ProgrammingError

from trilogy import Executor
from trilogy.core.enums import Purpose
from trilogy.core.models.build import Factory
from trilogy.core.models.datasource import (
    Address,
    ColumnAssignment,
    Datasource,
    RawColumnExpr,
    UpdateKey,
    UpdateKeys,
    UpdateKeyType,
)
from trilogy.core.models.environment import Environment
from trilogy.core.models.execute import CTE


def _is_table_not_found_error(exc: ProgrammingError, dialect) -> bool:
    """Check if exception is a table-not-found error for the given dialect."""
    pattern = dialect.TABLE_NOT_FOUND_PATTERN
    if pattern is None:
        return False
    error_msg = str(exc.orig) if exc.orig else str(exc)
    return pattern in error_msg


@dataclass
class DatasourceWatermark:
    keys: dict[str, UpdateKey]


@dataclass
class StaleAsset:
    """Represents an asset that needs to be refreshed."""

    datasource_id: str
    reason: str
    filters: UpdateKeys = field(default_factory=UpdateKeys)


def _compare_watermark_values(
    a: str | int | float | date, b: str | int | float | date
) -> int:
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
    key_columns: list[ColumnAssignment] = []
    for col_assignment in datasource.columns:
        concrete = executor.environment.concepts[col_assignment.concept.address]
        if concrete.purpose == Purpose.KEY:
            key_columns.append(col_assignment)

    if not key_columns:
        return DatasourceWatermark(keys={})

    if isinstance(datasource.address, Address):
        table_ref = executor.generator.render_source(datasource.address)
    else:
        table_ref = datasource.safe_address

    dialect = executor.generator
    watermarks = {}
    for col in key_columns:
        if isinstance(col.alias, str):
            column_name = col.alias
        elif isinstance(col.alias, RawColumnExpr):
            column_name = col.alias.text
        else:
            # Function - use rendered expression
            column_name = str(col.alias)
        hash_expr = dialect.hash_column_value(column_name)
        checksum_expr = dialect.aggregate_checksum(hash_expr)
        query = f"SELECT {checksum_expr} as checksum FROM {table_ref}"

        try:
            result = executor.execute_raw_sql(query).fetchone()
            checksum_value = result[0] if result else None
        except ProgrammingError as e:
            if _is_table_not_found_error(e, dialect):
                checksum_value = None
            else:
                raise

        watermarks[col.concept.address] = UpdateKey(
            concept_name=col.concept.address,
            type=UpdateKeyType.KEY_HASH,
            value=checksum_value,
        )

    return DatasourceWatermark(keys=watermarks)


def get_incremental_key_watermarks(
    datasource: Datasource, executor: Executor
) -> DatasourceWatermark:
    if not datasource.incremental_by:
        return DatasourceWatermark(keys={})

    if isinstance(datasource.address, Address):
        table_ref = executor.generator.render_source(datasource.address)
    else:
        table_ref = datasource.safe_address

    watermarks = {}
    factory = Factory(environment=executor.environment)

    dialect = executor.generator
    for concept_ref in datasource.incremental_by:
        concept = executor.environment.concepts[concept_ref.address]
        build_concept = factory.build(concept)
        build_datasource = factory.build(datasource)
        cte: CTE = CTE.from_datasource(build_datasource)
        # Check if concept is in output_concepts by comparing addresses
        output_addresses = {c.address for c in datasource.output_concepts}
        if concept.address in output_addresses:
            query = f"SELECT MAX({dialect.render_concept_sql(build_concept, cte=cte, alias=False)}) as max_value FROM {table_ref} as {dialect.quote(cte.base_alias)}"
        else:
            query = f"SELECT MAX({dialect.render_expr(build_concept.lineage, cte=cte)}) as max_value FROM {table_ref} as {dialect.quote(cte.base_alias)}"

        try:
            result = executor.execute_raw_sql(query).fetchone()
            max_value = result[0] if result else None
        except ProgrammingError as e:
            if _is_table_not_found_error(e, dialect):
                max_value = None
            else:
                raise

        watermarks[concept.name] = UpdateKey(
            concept_name=concept.name,
            type=UpdateKeyType.INCREMENTAL_KEY,
            value=max_value,
        )

    return DatasourceWatermark(keys=watermarks)


class BaseStateStore:

    def __init__(self) -> None:
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
            root_assets: Optional set of datasource identifiers that are "source of truth"
                         and should not be marked stale. If None, uses datasources marked
                         with is_root=True in the model.

        Returns:
            List of StaleAsset objects describing what needs refresh and why.
        """
        if root_assets is None:
            root_assets = {
                ds.identifier for ds in env.datasources.values() if ds.is_root
            }
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
