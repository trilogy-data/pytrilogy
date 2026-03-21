import subprocess
from dataclasses import dataclass, field
from datetime import date, datetime

from trilogy import Executor
from trilogy.core.enums import Purpose
from trilogy.core.models.author import ConceptRef
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
from trilogy.core.models.execute import CTE
from trilogy.execution.state.cache import ColumnStatsCache
from trilogy.execution.state.exceptions import is_missing_source_error


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
        if isinstance(a, datetime):
            a_aware = a.tzinfo is not None
            b_aware = b.tzinfo is not None  # type: ignore[union-attr]
            if a_aware != b_aware:
                raise TypeError(
                    f"offset-naive and offset-aware datetimes: {a!r} vs {b!r}"
                )
        if a < b:  # type: ignore[operator]
            return -1
        elif a > b:  # type: ignore[operator]
            return 1
        return 0
    sa, sb = str(a), str(b)
    if sa < sb:
        return -1
    elif sa > sb:
        return 1
    return 0


def _execute_raw_sql_scalar(
    query: str, executor: Executor
) -> str | int | float | datetime | date | None:
    """Execute a raw SQL query and return the first column of the first row.

    Returns None if the source is missing; rolls back and suppresses the error.
    Re-raises all other exceptions.
    """
    dialect = executor.generator
    try:
        result = executor.execute_raw_sql(query).fetchone()
        return result[0] if result else None
    except Exception as e:
        if is_missing_source_error(e, dialect):
            executor.connection.rollback()
            return None
        raise


def _resolve_table_ref(datasource: Datasource, executor: Executor) -> str:
    if isinstance(datasource.address, Address):
        return executor.generator.render_source(datasource.address)
    return datasource.safe_address


def has_schema_mismatch(
    datasource: Datasource,
    executor: Executor,
    cache: ColumnStatsCache | None = None,
) -> bool:
    """Return True if the existing table's columns (names or types) differ from the definition."""
    if isinstance(datasource.address, Address) and (
        datasource.address.is_file or datasource.address.is_query
    ):
        return False
    table_name = datasource.safe_address
    if cache is not None:
        hit, actual = cache.get_columns(table_name)
        if not hit:
            actual = executor.generator.get_table_columns(executor, table_name)
            cache.set_columns(table_name, actual)
    else:
        actual = executor.generator.get_table_columns(executor, table_name)
    if actual is None:
        return False
    expected = {
        executor.environment.concepts[col.concept.address]
        .safe_address.lower(): executor.environment.concepts[col.concept.address]
        .datatype.data_type
        for col in datasource.columns
    }
    if set(actual) != set(expected):
        return True
    # Check types where the dialect can resolve them (skip UNKNOWN — can't map the type)
    from trilogy.core.models.core import DataType

    return any(
        actual[name] != expected[name]
        for name in expected
        if actual.get(name, DataType.UNKNOWN) != DataType.UNKNOWN
    )


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
    key_columns: list[ColumnAssignment] = [
        col
        for col in datasource.columns
        if executor.environment.concepts[col.concept.address].purpose == Purpose.KEY
    ]

    if not key_columns:
        return DatasourceWatermark(keys={})

    table_ref = _resolve_table_ref(datasource, executor)
    dialect = executor.generator
    watermarks = {}

    for col in key_columns:
        if isinstance(col.alias, str):
            column_name = col.alias
        elif isinstance(col.alias, RawColumnExpr):
            column_name = col.alias.text
        else:
            column_name = str(col.alias)
        hash_expr = dialect.hash_column_value(column_name)
        checksum_expr = dialect.aggregate_checksum(hash_expr)
        query = f"SELECT {checksum_expr} as checksum FROM {table_ref}"
        checksum_value = _execute_raw_sql_scalar(query, executor)

        watermarks[col.concept.address] = UpdateKey(
            concept_name=col.concept.address,
            type=UpdateKeyType.KEY_HASH,
            value=checksum_value,
        )

    return DatasourceWatermark(keys=watermarks)


def _get_max_watermarks(
    concept_refs: list[ConceptRef],
    datasource: Datasource,
    executor: Executor,
    key_type: UpdateKeyType,
) -> DatasourceWatermark:
    """Fetch MAX watermarks for concept refs using the appropriate query expression."""
    if not concept_refs:
        return DatasourceWatermark(keys={})

    table_ref = _resolve_table_ref(datasource, executor)
    factory = Factory(environment=executor.environment)
    dialect = executor.generator
    output_addresses = {c.address for c in datasource.output_concepts}
    watermarks = {}

    for concept_ref in concept_refs:
        concept = executor.environment.concepts[concept_ref.address]
        build_concept = factory.build(concept)
        build_datasource = factory.build(datasource)
        cte: CTE = CTE.from_datasource(build_datasource)

        if concept.address in output_addresses:
            query = f"SELECT MAX({dialect.render_concept_sql(build_concept, cte=cte, alias=False)}) as max_value FROM {table_ref} as {dialect.quote(cte.base_alias)}"
        elif build_concept.lineage is None:
            raise ValueError(
                f"Concept '{concept.address}' is set as a freshness field but does not"
                f" exist on datasource '{datasource.identifier}' and cannot be derived"
                f" from other datasource fields. Add it to the datasource column list"
                f" or change the freshness field."
            )
        else:
            query = f"SELECT MAX({dialect.render_expr(build_concept.lineage, cte=cte)}) as max_value FROM {table_ref} as {dialect.quote(cte.base_alias)}"

        max_value = _execute_raw_sql_scalar(query, executor)

        watermarks[concept.name] = UpdateKey(
            concept_name=concept.name,
            type=key_type,
            value=max_value,
        )

    return DatasourceWatermark(keys=watermarks)


def get_incremental_key_watermarks(
    datasource: Datasource, executor: Executor
) -> DatasourceWatermark:
    return _get_max_watermarks(
        datasource.incremental_by, datasource, executor, UpdateKeyType.INCREMENTAL_KEY
    )


def get_freshness_watermarks(
    datasource: Datasource, executor: Executor
) -> DatasourceWatermark:
    return _get_max_watermarks(
        datasource.freshness_by, datasource, executor, UpdateKeyType.UPDATE_TIME
    )


def get_concept_max_watermark_abstract(
    concept_address: str,
    executor: Executor,
    root_assets: set[str],
) -> UpdateKey:
    """Compute MAX watermark for a derived concept using only root datasources.

    Temporarily hides non-root datasources so the query planner is forced to
    resolve the concept exclusively from authoritative root sources.
    """
    hidden = {
        ds_id: executor.environment.datasources.pop(ds_id)
        for ds_id in list(executor.environment.datasources)
        if ds_id not in root_assets
    }
    try:
        result = executor.execute_query(f"SELECT MAX({concept_address}) as max_value;")
        row = result.fetchone() if result else None
        value = row[0] if row else None
    except Exception:
        value = None
    finally:
        executor.environment.datasources.update(hidden)
    return UpdateKey(
        concept_name=concept_address,
        type=UpdateKeyType.INCREMENTAL_KEY,
        value=value,
    )


def get_concept_max_watermarks(
    datasource: Datasource,
    concept_refs: list[ConceptRef],
    executor: Executor,
) -> DatasourceWatermark:
    """Fetch MAX watermarks for given concept refs from a root datasource.

    Used to auto-watermark roots when non-root datasources reference those concepts
    in their freshness_by/incremental_by without requiring explicit root declarations.
    """
    table_ref = _resolve_table_ref(datasource, executor)
    output_addresses = {c.address for c in datasource.output_concepts}
    factory = Factory(environment=executor.environment)
    dialect = executor.generator
    watermarks = {}

    for concept_ref in concept_refs:
        if concept_ref.address not in output_addresses:
            continue
        concept = executor.environment.concepts[concept_ref.address]
        build_concept = factory.build(concept)
        build_datasource = factory.build(datasource)
        cte: CTE = CTE.from_datasource(build_datasource)
        query = f"SELECT MAX({dialect.render_concept_sql(build_concept, cte=cte, alias=False)}) as max_value FROM {table_ref} as {dialect.quote(cte.base_alias)}"

        max_value = _execute_raw_sql_scalar(query, executor)

        watermarks[concept.name] = UpdateKey(
            concept_name=concept.name,
            type=UpdateKeyType.INCREMENTAL_KEY,
            value=max_value,
        )

    return DatasourceWatermark(keys=watermarks)


def run_freshness_probe(probe_path: str) -> bool:
    """Run a probe script to check datasource freshness.

    The script should exit 0 and print a truthy value (true/1/yes) if up-to-date,
    or a falsy value (false/0/no) if stale. A non-zero exit code raises RuntimeError.
    """
    result = subprocess.run(
        ["uv", "run", "--no-project", "--quiet", probe_path],
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        raise RuntimeError(
            f"Freshness probe '{probe_path}' failed (exit {result.returncode}): {result.stderr.strip()}"
        )
    return result.stdout.strip().lower() in ("true", "1", "yes")
