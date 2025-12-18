from dataclasses import dataclass
from enum import Enum

from trilogy import Executor
from trilogy.core.enums import Purpose
from trilogy.core.models.datasource import Address, Datasource


class WatermarkType(Enum):
    INCREMENTAL_KEY = "incremental_key"
    UPDATE_TIME = "update_time"
    KEY_HASH = "key_hash"


@dataclass
class WatermarkValue:
    type: WatermarkType
    value: str | int | float | None


@dataclass
class DatasourceWatermark:
    keys: dict[str, WatermarkValue]


def get_last_update_time_watermarks(
    datasource: Datasource, executor: Executor
) -> DatasourceWatermark:
    update_time = executor.generator.get_table_last_modified(
        executor, datasource.safe_address
    )
    return DatasourceWatermark(
        keys={
            "update_time": WatermarkValue(
                type=WatermarkType.UPDATE_TIME, value=update_time
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
        watermarks[col.name] = WatermarkValue(
            type=WatermarkType.KEY_HASH, value=checksum_value
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
        watermarks[concept.name] = WatermarkValue(
            type=WatermarkType.INCREMENTAL_KEY, value=max_value
        )

    return DatasourceWatermark(keys=watermarks)


class BaseStateStore:

    def __init__(self):
        self.watermarks: dict[str, DatasourceWatermark] = {}

    def watermark_root_asset(
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
