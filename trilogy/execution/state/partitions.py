"""Per-partition observation for partitioned datasources.

A partitioned datasource is not one asset with one freshness verdict — it is N
independently refreshable slices behind a single address. This module produces
the two sides of the per-slice comparison:

- :func:`probe_observed_partitions` — what slices the physical table actually
  holds, by grouping it on its declared ``partition by`` columns.
- :func:`probe_expected_partitions` — what slices the *roots* say should exist,
  planned with every non-root hidden so the answer can only come from
  authoritative sources (the same trick
  :func:`~trilogy.execution.state.watermarks.get_concept_max_watermark_abstract`
  uses for derived concepts).

Both sides carry the same per-slice watermark keys as
:class:`~trilogy.execution.state.watermarks.DatasourceWatermark`, so a partition
is judged by exactly the rule a whole datasource is: behind on a key, or absent.
Partition columns are excluded from those keys — ``MAX(order_date)`` inside the
``order_date=2024-01-03`` slice is the slice's own name, never a signal.

The identity of a slice is :func:`partition_id`, a hive-style
``col=value/col2=value2`` rendering in declared column order. It is the merge
unit for state deltas (see ``snapshot.py``), so it must be stable across
processes: values are rendered canonically, never via ``repr``.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime

from trilogy import Executor
from trilogy.constants import logger
from trilogy.core.models.author import ConceptRef
from trilogy.core.models.build import Factory
from trilogy.core.models.datasource import (
    ColumnAssignment,
    Datasource,
    RawColumnExpr,
    UpdateKey,
    UpdateKeyType,
)
from trilogy.core.models.execute import CTE
from trilogy.execution.state.exceptions import (
    is_missing_source_error,
    is_schema_mismatch_error,
)
from trilogy.execution.state.watermarks import _resolve_table_ref

LOGGER_PREFIX = "[PARTITIONS]"

#: Stands in for a NULL partition value in a partition id. A NULL slice is a
#: real slice (rows whose partition key is unset) and needs a stable name.
NULL_PARTITION_TOKEN = "__NULL__"

#: Separates the components of a multi-column partition id.
PARTITION_ID_SEPARATOR = "/"

PartitionValue = str | int | float | bool | datetime | date | None


@dataclass
class PartitionObservation:
    """One partition slice as observed on one side of the comparison."""

    values: dict[str, PartitionValue]  # partition column -> value, declared order
    row_count: int | None = None
    keys: dict[str, UpdateKey] = field(default_factory=dict)

    @property
    def id(self) -> str:
        return partition_id(self.values)


def render_partition_value(value: PartitionValue) -> str:
    """Canonical string form of a partition value for a partition id.

    Temporal values render ISO so a ``date`` and the ``datetime.date`` a driver
    hands back for the same day produce the same id — the id is compared across
    processes, and a ``str()`` that varies by driver would split one slice in
    two."""
    if value is None:
        return NULL_PARTITION_TOKEN
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, bool):
        return "true" if value else "false"
    return str(value)


def partition_id(values: dict[str, PartitionValue]) -> str:
    """Hive-style identity for a slice, in the mapping's (declared) order."""
    return PARTITION_ID_SEPARATOR.join(
        f"{column}={render_partition_value(value)}" for column, value in values.items()
    )


def partition_assignments(ds: Datasource) -> list[ColumnAssignment]:
    """The datasource's ``partition by`` columns, in declared order.

    A partition ref that names no concrete column cannot be grouped on, and
    silently dropping it would report a coarser partitioning than the model
    declares — so it raises, matching the DDL path in ``table_processor``."""
    by_address = {col.concept.address: col for col in ds.columns}
    resolved: list[ColumnAssignment] = []
    for ref in ds.partition_by:
        col = by_address.get(ref.address)
        if col is None or not col.is_concrete:
            raise ValueError(
                f"Datasource {ds.identifier} partitions by '{ref.address}', which is"
                " not a concrete column on it; partition state cannot be probed."
            )
        resolved.append(col)
    return resolved


def is_partitioned(ds: Datasource) -> bool:
    return bool(ds.partition_by) and not ds.is_root


def partition_watermark_refs(ds: Datasource) -> list[ConceptRef]:
    """Freshness refs that say something about a slice.

    Mirrors ``watermark_asset``'s precedence (freshness over incremental), minus
    the partition columns themselves."""
    partition_addresses = {ref.address for ref in ds.partition_by}
    refs = ds.freshness_by or ds.incremental_by
    return [ref for ref in refs if ref.address not in partition_addresses]


def _watermark_key_type(ds: Datasource) -> UpdateKeyType:
    return (
        UpdateKeyType.UPDATE_TIME if ds.freshness_by else UpdateKeyType.INCREMENTAL_KEY
    )


def _execute_raw_sql_rows(query: str, executor: Executor) -> list[tuple]:
    """Rows for a probe query, or none when the source does not exist yet.

    An unbuilt or reshaped target is the normal case for a partition probe — it
    is exactly the "no slices yet" answer the caller wants, not an error."""
    dialect = executor.generator
    try:
        result = executor.execute_raw_sql(query)
        return list(result.fetchall())
    except Exception as e:
        if is_missing_source_error(e, dialect) or is_schema_mismatch_error(e, dialect):
            executor.connection.rollback()
            return []
        raise


def partition_column_name(col: ColumnAssignment) -> str:
    """The PHYSICAL column a partition key is stored in.

    Partition ids are keyed on this, never on the concept address: concept
    addresses are namespaced per script and are deliberately never reconciled
    across scripts, while the column is the same in every model pointing at the
    table — the same rule the snapshot's asset keys follow."""
    if isinstance(col.alias, RawColumnExpr):
        return col.alias.text
    return str(col.alias)


def probe_observed_partitions(
    ds: Datasource, executor: Executor
) -> list[PartitionObservation]:
    """Group the physical table on its partition columns.

    One query per datasource regardless of slice count — the whole point of
    partition state is that it costs a GROUP BY, not N probes."""
    assignments = partition_assignments(ds)
    if not assignments:
        return []

    table_ref = _resolve_table_ref(ds, executor)
    dialect = executor.generator
    factory = Factory(environment=executor.environment)
    cte: CTE = CTE.from_datasource(factory.build(ds))
    alias = dialect.quote(cte.base_alias)

    def rendered(address: str) -> str:
        build_concept = factory.build(executor.environment.concepts[address])
        return dialect.render_concept_sql(build_concept, cte=cte, alias=False)

    group_exprs = [rendered(col.concept.address) for col in assignments]
    wm_refs = partition_watermark_refs(ds)
    wm_exprs = [f"MAX({rendered(ref.address)})" for ref in wm_refs]

    selected = ", ".join([*group_exprs, "COUNT(*)", *wm_exprs])
    query = (
        f"SELECT {selected} FROM {table_ref} as {alias}"
        f" GROUP BY {', '.join(group_exprs)}"
    )
    rows = _execute_raw_sql_rows(query, executor)

    key_type = _watermark_key_type(ds)
    names = [executor.environment.concepts[ref.address].name for ref in wm_refs]
    offset = len(assignments)
    return [
        PartitionObservation(
            values={
                partition_column_name(col): row[idx]
                for idx, col in enumerate(assignments)
            },
            row_count=row[offset],
            keys={
                name: UpdateKey(
                    concept_name=name, type=key_type, value=row[offset + 1 + i]
                )
                for i, name in enumerate(names)
            },
        )
        for row in rows
    ]


def probe_expected_partitions(
    ds: Datasource, executor: Executor, root_assets: set[str]
) -> list[PartitionObservation]:
    """The slices the roots say should exist, and each one's expected watermark.

    Non-root datasources are hidden for the duration so the planner can only
    answer from authoritative sources — otherwise the target would supply its
    own expectation and every partition would look complete.
    """
    assignments = partition_assignments(ds)
    if not assignments:
        return []

    key_type = _watermark_key_type(ds)
    wm_refs = partition_watermark_refs(ds)
    wm_names = [executor.environment.concepts[ref.address].name for ref in wm_refs]
    selected = [ref.address for ref in ds.partition_by] + [
        f"MAX({ref.address}) -> _expected_{name}"
        for ref, name in zip(wm_refs, wm_names)
    ]

    hidden = {
        ds_id: executor.environment.datasources.pop(ds_id)
        for ds_id in list(executor.environment.datasources)
        if ds_id not in root_assets
    }
    try:
        result = executor.execute_query(f"SELECT {', '.join(selected)};")
        rows = list(result.fetchall()) if result else []
    except Exception as e:
        # An unresolvable expectation is a real answer here: the partition key
        # may not be derivable from roots alone. Report no expectation rather
        # than failing the whole snapshot.
        logger.debug(
            "%s expected-partition probe for %s failed: %s",
            LOGGER_PREFIX,
            ds.identifier,
            e,
        )
        return []
    finally:
        executor.environment.datasources.update(hidden)

    offset = len(assignments)
    return [
        PartitionObservation(
            values={
                partition_column_name(col): row[idx]
                for idx, col in enumerate(assignments)
            },
            keys={
                name: UpdateKey(concept_name=name, type=key_type, value=row[offset + i])
                for i, name in enumerate(wm_names)
            },
        )
        for row in rows
    ]


__all__ = [
    "NULL_PARTITION_TOKEN",
    "PARTITION_ID_SEPARATOR",
    "PartitionObservation",
    "PartitionValue",
    "is_partitioned",
    "partition_assignments",
    "partition_column_name",
    "partition_id",
    "partition_watermark_refs",
    "probe_expected_partitions",
    "probe_observed_partitions",
    "render_partition_value",
]
