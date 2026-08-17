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

from collections.abc import Iterable
from dataclasses import dataclass, field
from datetime import date, datetime
from typing import TYPE_CHECKING

from trilogy import Executor
from trilogy.constants import MagicConstants, logger
from trilogy.core.enums import BooleanOperator, ComparisonOperator
from trilogy.core.models.author import (
    Comparison,
    ConceptRef,
    Conditional,
    SubselectComparison,
    WhereClause,
)
from trilogy.core.models.build import Factory
from trilogy.core.models.core import DataType, ListWrapper
from trilogy.core.models.datasource import (
    ColumnAssignment,
    Datasource,
    RawColumnExpr,
    UpdateKey,
    UpdateKeyType,
)
from trilogy.core.models.execute import CTE
from trilogy.execution.state.exceptions import (
    UNRESOLVABLE_ERRORS,
    is_missing_source_error,
    is_schema_mismatch_error,
)
from trilogy.execution.state.isolation import hidden_datasources
from trilogy.execution.state.watermarks import (
    _compare_watermark_values,
    _resolve_table_ref,
)

if TYPE_CHECKING:
    from trilogy.core.models.environment import Environment

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


def parse_partition_value(rendered: str, datatype: DataType | None) -> PartitionValue:
    """Inverse of :func:`render_partition_value`, typed by the READER's model.

    Values travel as their canonical rendering — in a snapshot, and on a
    ``--partition`` flag — so the type comes from the datasource reading them,
    not the process that wrote them. Unparseable degrades to the string, which
    still compares consistently.
    """
    if rendered == NULL_PARTITION_TOKEN:
        return None
    try:
        if datatype == DataType.DATE:
            return date.fromisoformat(rendered)
        if datatype in (DataType.DATETIME, DataType.TIMESTAMP):
            return datetime.fromisoformat(rendered)
        if datatype == DataType.INTEGER:
            return int(rendered)
        if datatype == DataType.FLOAT:
            return float(rendered)
        if datatype == DataType.BOOL:
            return rendered == "true"
    except ValueError:
        pass
    return rendered


def parse_partition_selector(pairs: Iterable[str]) -> dict[str, str]:
    """``concept.address=value`` pairs into a selector map.

    Addressed by concept, not physical column; :func:`selected_slice` bridges
    the two per-datasource.

    Commas separate pairs, but a fragment with no ``=`` is read as the rest of a
    value that contained one, so a comma-bearing value survives instead of being
    truncated. A value holding both a comma and an ``=`` reads as two pairs.

    Two different values for one concept raise: that names a range, not the one
    slice a run owns.
    """
    selector: dict[str, str] = {}
    for pair in pairs:
        # Continuation is only meaningful inside one flag's text — otherwise a
        # malformed second flag would be swallowed by the first flag's value.
        last: str | None = None
        for item in pair.split(","):
            item = item.strip()
            if not item:
                continue
            address, sep, value = item.partition("=")
            address = address.strip()
            if not sep or not address:
                if last is None:
                    raise ValueError(
                        f"Invalid --partition {item!r};"
                        " expected <concept.address>=<value>"
                    )
                selector[last] = f"{selector[last]},{item}"
                continue
            value = value.strip()
            if selector.get(address, value) != value:
                raise ValueError(
                    f"--partition names {address} twice, as {selector[address]!r} and"
                    f" {value!r}; two values name a range, not the slice a run owns"
                )
            selector[address] = value
            last = address
    return selector


def partition_key_addresses(datasources: Iterable[Datasource]) -> set[str]:
    """Concept addresses these datasources declare as partition keys."""
    return {ref.address for ds in datasources for ref in ds.partition_by}


def selected_slice(
    ds: Datasource, environment: Environment, selector: dict[str, str]
) -> PartitionObservation | None:
    """The one slice ``selector`` names on ``ds``, or None if it names none.

    Not applying is normal — a directory holds many datasources and a selector
    speaks for those partitioned on the concept it names. Naming *some* of a
    multi-column key raises: that is a range, and silently widening a targeted
    refresh is the failure this flag exists to avoid. (Naming nothing *anywhere*
    is caught up front by ``validate_partition_selector``.)
    """
    if not ds.partition_by:
        return None
    matched = [ref for ref in ds.partition_by if ref.address in selector]
    if not matched:
        return None
    if len(matched) != len(ds.partition_by):
        missing = sorted(
            ref.address for ref in ds.partition_by if ref.address not in selector
        )
        raise ValueError(
            f"--partition names only part of {ds.identifier}'s partition key;"
            f" missing {', '.join(missing)}"
        )
    values: dict[str, PartitionValue] = {}
    for col in partition_assignments(ds):
        concept = environment.concepts[col.concept.address]
        values[partition_column_name(col)] = parse_partition_value(
            selector[col.concept.address], concept.datatype.data_type
        )
    return PartitionObservation(values=values)


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


@dataclass(frozen=True)
class PartitionVerdict:
    """Whether one slice needs refreshing, and why."""

    stale: bool
    reason: str | None = None


def partition_verdict(
    observed: PartitionObservation | None,
    expected: PartitionObservation | None,
) -> PartitionVerdict:
    """THE rule for judging one slice. Both consumers must call this.

    ``trilogy state`` renders it into a :class:`PartitionState`; ``trilogy
    refresh`` filters on it to build its work list. Deriving the verdict twice
    is how the two commands come to disagree about the same asset — which is
    exactly the bug that made slice-aware refresh necessary in the first place.

    A slice the roots demand but the table lacks is stale; that missing-slice
    case is the one an unpartitioned watermark can never express. A slice
    present but absent upstream is NOT stale — nothing is asking for it.
    """
    if observed is None:
        return PartitionVerdict(True, "partition missing")
    if expected is None:
        return PartitionVerdict(False)
    if observed.row_count == 0:
        return PartitionVerdict(True, "partition empty")
    for key, expected_key in expected.keys.items():
        if expected_key.value is None:
            continue
        observed_key = observed.keys.get(key)
        current = observed_key.value if observed_key else None
        if current is None:
            return PartitionVerdict(
                True, f"'{key}' missing (expected {expected_key.value})"
            )
        if _compare_watermark_values(current, expected_key.value) < 0:
            return PartitionVerdict(
                True, f"'{key}' behind: {current} < {expected_key.value}"
            )
    return PartitionVerdict(False)


def stale_slices(
    observed: list[PartitionObservation], expected: list[PartitionObservation]
) -> list[PartitionObservation]:
    """The refresh work list: expected slices whose verdict is stale.

    Returned as observations rather than rendered states because a refresh needs
    the real values to build its filter.
    """
    observed_by_id = {obs.id: obs for obs in observed}
    return [
        exp
        for exp in expected
        if partition_verdict(observed_by_id.get(exp.id), exp).stale
    ]


def partition_filter(
    ds: Datasource,
    environment: Environment,
    slices: list[PartitionObservation],
) -> WhereClause | None:
    """A WHERE restricting a refresh to exactly ``slices``.

    One ``IN`` list for single-key partitioning; an OR of AND-ed equalities when
    several columns make up the key, because row-value ``IN`` is not portable.
    Returns None for an empty slice list — "no slices" must never render as "no
    filter", which would rebuild the whole table.
    """
    if not slices:
        return None
    assignments = partition_assignments(ds)
    concepts = [environment.concepts[col.concept.address] for col in assignments]
    columns = [partition_column_name(col) for col in assignments]

    if len(assignments) == 1:
        concept, column = concepts[0], columns[0]
        values = [obs.values.get(column) for obs in slices]
        present = [value for value in values if value is not None]
        arms: list[Comparison | Conditional] = []
        if present:
            arms.append(
                # Membership, not a scalar comparison: the RHS is a set, and only
                # a SubselectComparison carries the planner's existence semantics
                # for one.
                SubselectComparison(
                    left=concept.reference,
                    right=ListWrapper(present, type=concept.datatype.data_type),
                    operator=ComparisonOperator.IN,
                )
            )
        if len(present) != len(values):
            arms.append(_is_null(concept))
        return WhereClause(conditional=_any_of(arms))

    def one_slice(obs: PartitionObservation) -> Comparison | Conditional:
        return _all_of(
            [
                _equals(concept, obs.values.get(column))
                for concept, column in zip(concepts, columns)
            ]
        )

    return WhereClause(conditional=_any_of([one_slice(obs) for obs in slices]))


def _is_null(concept) -> Comparison:
    """``IS NULL``, not ``= NULL``.

    A NULL partition key is a real slice. Selecting it with ``=`` matches
    nothing, so the slice would be reported stale forever and never written —
    the read-side twin of the null-safe partition delete."""
    return Comparison(
        left=concept.reference,
        right=MagicConstants.NULL,
        operator=ComparisonOperator.IS,
    )


def _equals(concept, value: PartitionValue) -> Comparison:
    if value is None:
        return _is_null(concept)
    return Comparison(
        left=concept.reference, right=value, operator=ComparisonOperator.EQ
    )


def _all_of(parts: list) -> Comparison | Conditional:
    return _fold(parts, BooleanOperator.AND)


def _any_of(parts: list) -> Comparison | Conditional:
    return _fold(parts, BooleanOperator.OR)


def _fold(parts: list, operator: BooleanOperator):
    combined = parts[0]
    for part in parts[1:]:
        combined = Conditional(left=combined, right=part, operator=operator)
    return combined


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
    addresses = [executor.environment.concepts[ref.address].address for ref in wm_refs]
    offset = len(assignments)
    return [
        PartitionObservation(
            values={
                partition_column_name(col): row[idx]
                for idx, col in enumerate(assignments)
            },
            row_count=row[offset],
            keys={
                address: UpdateKey(
                    concept_name=address, type=key_type, value=row[offset + 1 + i]
                )
                for i, address in enumerate(addresses)
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
    wm_addresses = [
        executor.environment.concepts[ref.address].address for ref in wm_refs
    ]
    # Positional aliases: an address contains dots, which no alias can carry.
    selected = [ref.address for ref in ds.partition_by] + [
        f"MAX({ref.address}) -> _expected_{i}" for i, ref in enumerate(wm_refs)
    ]

    non_roots = [
        ds_id
        for ds_id in list(executor.environment.datasources)
        if ds_id not in root_assets
    ]
    with hidden_datasources(executor.environment, non_roots):
        try:
            result = executor.execute_ephemeral(f"SELECT {', '.join(selected)};")
            rows = list(result.fetchall()) if result else []
        except UNRESOLVABLE_ERRORS as e:
            # A real answer, not a failure: the partition key may not be
            # derivable from roots alone, and a rootless project has nothing to
            # derive it from. Narrow on purpose — see UNRESOLVABLE_ERRORS.
            logger.debug(
                "%s no root-derived expectation for %s: %s",
                LOGGER_PREFIX,
                ds.identifier,
                e,
            )
            return []

    offset = len(assignments)
    return [
        PartitionObservation(
            values={
                partition_column_name(col): row[idx]
                for idx, col in enumerate(assignments)
            },
            keys={
                address: UpdateKey(
                    concept_name=address, type=key_type, value=row[offset + i]
                )
                for i, address in enumerate(wm_addresses)
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
    "parse_partition_selector",
    "parse_partition_value",
    "partition_assignments",
    "partition_column_name",
    "partition_id",
    "partition_key_addresses",
    "partition_watermark_refs",
    "probe_expected_partitions",
    "probe_observed_partitions",
    "render_partition_value",
    "selected_slice",
]
