"""State snapshot contract: a serializable, project-scoped view of asset state.

This is THE interchange format for asset state — shared by the ``trilogy state``
command, ``run/refresh --state-file``, ``trilogy serve``'s ``/state``, the studio
UI, and the cloud service. Every producer goes through one computation
(:func:`~trilogy.scripts.state.snapshot_for_parsed_script` for a single script,
the directory probe for many), and every consumer receives this shape verbatim.
A format is only an interchange while one implementation defines it, so there is
deliberately no per-surface variant to render into.

Keying rules (load-bearing — see ``trilogy/scripts/AGENTS.md``):

- The PRIMARY key of an asset is its **physical address**. Logical concept
  addresses are namespaced per script and deliberately never reconciled
  across scripts; deduplication happens at the physical layer.
- Keys dispatch on the **address type**, never on the shape of the address
  string, and contain **nothing logical** — no script, no datasource name. Two
  scripts writing the same file key to the same asset, because it is the same
  asset. External addresses (remote URLs like ``gs://``, warehouse tables) are
  ``ds.safe_address`` verbatim. **Project-local file assets are keyed by their
  project-relative path**, never by the absolute path of the checkout (which
  changes per machine — and per run, under an orchestrator's scratch dir). The
  project root is ``trilogy.toml``'s directory, so a script in a subdirectory
  keys identically to one at the top. See :func:`stable_asset_key`.
- Two address types are **not plain data artifacts** and carry a type label: a
  Python datasource script is a procedure (``script::<project-relative
  path>``), and an inline query has no artifact at all, so its SQL is its
  identity (``query::<digest>`` — the raw text is multi-line and churns on
  reformatting). Note ``AddressType.SQL`` is a ``.sql`` *file*, keyed as a
  file; only inline ``query '''...'''`` is ``AddressType.QUERY``.
- The owning script rides as ``PhysicalAssetState.owner_script`` — attribute
  data, deliberately not part of the key.
- **Every datasource is an asset, roots included.** A root is the *expected*
  side of staleness and is never seeded from a snapshot (see
  :func:`managed_states_by_address`) — but it is still state worth reporting.
  Unmanaged shows up as ``PhysicalAssetState.managed = False``, not as an
  omission.
- Watermark keys are recorded **as emitted** by the state store: bare concept
  names for MAX-based probes, full concept addresses for KEY_HASH checksums,
  and the literal ``"update_time"`` for table-mtime watermarks. Do NOT
  normalize them — the emitted key is what refresh comparisons use. The
  ``concept_address`` field is best-effort attribute data.

Observations are recorded **in phases** (``DatasourceState.observations``):
``begin`` — state as the run found it, before any refresh executed; ``end`` —
state after execution, with the freshest expected values the run can attest
to. Each phase carries BOTH sides of the staleness comparison (the
datasource's own ``observed_watermarks`` and the root-derived
``expected_watermarks``), so a reader can re-derive that phase's verdict
itself. ``DatasourceState.plan`` carries the refresh plan's verdict as an input
the reader can audit, not a classification it must trust; the top-level
``status``/``stale_reason``/``observed_watermarks``/``expected_watermarks``
remain the end-phase merged view for existing consumers. Non-refresh
operations emit a single ``end`` observation — begin/plan only exist where a
refresh plan exists. All ``probed_at`` stamps are the emitting process's
clock: fine for display, not for ordering across machines.

Partitioned datasources (those declaring ``partition by``) additionally record
**per-partition state** (``DatasourceState.partitions``), because a partitioned
asset is N independently refreshable slices behind one address, not one thing
with one verdict. A slice's identity is its hive-style ``partition_id``
(``order_date=2024-01-03``), keyed on the **physical column** for the same
reason asset keys are keyed on the physical address. Each slice carries both
sides of its own comparison, so an orchestrator can read the snapshot, take the
stale slices as its work list, and fan out one refresh per slice.

``DatasourceState.partitions_complete`` is what makes that fan-out safe to merge
back. A whole-asset probe enumerates every slice and sets it True; a run scoped
to the partitions it owns (``--state-partition``) sets it False, declaring "these
slices, and nothing about the others". :func:`merge_snapshots` overlays a scoped
delta slice-by-slice and lets a complete one replace the list — so N concurrent
workers writing N delta files never clobber each other's slices, and no worker
has to observe (or lock) state it does not own.

The slice list is **capped** (:data:`MAX_REPORTED_PARTITIONS`) and
``DatasourceState.partition_summary`` carries the counts over the whole probed
set, so trimming changes what a reader can *enumerate* and never what it can
*conclude*. This split is why a snapshot can be put on a wire at all: partition
counts are unbounded (``partition by`` an id, or hourly over five years) while
every consumer has a size budget, and a producer that emits 44,000 slices just
gets truncated by something further downstream with less context about which
ones mattered. :func:`cap_partitions` keeps the stale slices — the backfill
queue — and :class:`PartitionSummary` keeps "2,583 of 2,700 stale" true
regardless. ``PartitionSummary.level`` says how the observation was obtained, so
"did not look" never reads as "nothing to report".

``schema_version`` bumps only on breaking changes to existing fields; new
fields/values are added without a bump. Consumers must ignore unknown fields.
"""

from __future__ import annotations

import hashlib
from datetime import date, datetime
from pathlib import PurePath
from typing import Literal

from pydantic import BaseModel, Field

from trilogy.core.enums import AddressType
from trilogy.core.models.datasource import (
    Address,
    Datasource,
    UpdateKey,
    UpdateKeyType,
)
from trilogy.execution.state.partitions import (
    PartitionObservation,
    PartitionValue,
    parse_partition_value,
    partition_column_name,
    partition_id,
    partition_verdict,
    render_partition_value,
)
from trilogy.execution.state.phases import get_phase_recorder
from trilogy.execution.state.watermarks import (
    DatasourceWatermark,
    StaleAsset,
)
from trilogy.utility import utc_now_iso

SNAPSHOT_SCHEMA_VERSION = 1

AssetStatus = Literal["fresh", "stale", "unknown"]

#: How many slices a single datasource may carry in ``DatasourceState``.
#:
#: A rendering limit, not a scheduling one. Partition counts are unbounded in
#: principle and consumers are not: a status event, a message payload, a
#: database column. A producer that emits 44,000 slices does not inform anyone
#: — it gets truncated further downstream, by something with less context about
#: what mattered. Capping here, where the verdicts are known, means the slices
#: that survive are the ones a reader would have picked (see
#: :func:`cap_partitions`), and :class:`PartitionSummary` keeps the counts exact
#: regardless. Raise it via ``build_partition_states(limit=...)``.
MAX_REPORTED_PARTITIONS = 200

#: Separates a key's type scheme from its body (``script::``, ``query::``).
KEY_SCHEME_SEPARATOR = "::"

#: Type labels for addresses that are not plain data artifacts: a procedure
#: that produces rows, and an inline query with no artifact at all.
SCRIPT_KEY_SCHEME = "script"
QUERY_KEY_SCHEME = "query"

#: Characters of SHA-256 hex kept in a query digest. 16 hex chars = 64 bits;
#: collision odds stay negligible for any plausible number of queries.
QUERY_DIGEST_CHARS = 16

#: Address types that name a file on disk (or a remote object store path).
#: Mirrors ``Address.is_file`` — note ``SQL`` is a ``.sql`` *file*, while
#: inline ``query '''...'''`` text is ``QUERY``.
FILE_ADDRESS_TYPES = frozenset(
    {
        AddressType.CSV,
        AddressType.TSV,
        AddressType.PARQUET,
        AddressType.SQL,
        AddressType.PYTHON_SCRIPT,
    }
)


class WatermarkValue(BaseModel):
    """One watermark entry, keyed as the state store emitted it."""

    key: str
    type: str  # "incremental_key" | "update_time" | "key_hash"
    value_raw: str | int | float | bool | None = None  # JSON-native when possible
    value: str | None = None  # always-stringified rendering
    value_type: str | None = None  # python type name of the original value
    concept_address: str | None = None  # best-effort logical mapping
    # Physical column the concept was bound to. The stable bridge when a reader
    # renamed its concepts — see ``_rekey_for``.
    column: str | None = None
    # When this value was read (emitting process's clock; display only).
    probed_at: str | None = None


class ColumnMapping(BaseModel):
    """Physical column -> logical concept binding from the datasource model."""

    column: str
    concrete: bool  # False when the "column" is a raw expression or function
    concept_address: str
    modifiers: list[str] = Field(default_factory=list)


class PartitionColumn(BaseModel):
    """One declared partition key: physical column plus its logical binding."""

    column: str  # physical column — what partition ids are keyed on
    concept_address: str


class PartitionState(BaseModel):
    """State of one partition slice of a partitioned datasource."""

    partition_id: str  # hive-style ``col=value[/col2=value2]`` — the merge unit
    values: dict[str, str] = Field(default_factory=dict)  # column -> rendered value
    observed: bool = False  # the slice exists in the physical table
    expected: bool = False  # the roots say this slice should exist
    status: AssetStatus = "unknown"
    stale_reason: str | None = None
    row_count: int | None = None
    observed_watermarks: list[WatermarkValue] = Field(default_factory=list)
    expected_watermarks: list[WatermarkValue] = Field(default_factory=list)
    probed_at: str | None = None
    # Which run last wrote this slice. Provenance for a merged file, whose
    # slices come from different processes at different times.
    run_id: str | None = None


class PartitionSummary(BaseModel):
    """Aggregate truth about a datasource's slices, independent of how many of
    them ``partitions`` actually carries.

    Partition counts are unbounded in principle — ``partition by`` an id, or
    hourly over five years — while every consumer downstream has a size budget,
    so ``partitions`` is capped (:func:`cap_partitions`). The counts here are
    computed over the WHOLE probed set before that cap, so trimming the list
    changes what a reader can *enumerate* and never what it can *conclude*:
    "2,583 of 2,700 slices stale" survives a 200-slice payload. Without that a
    consumer cannot tell a clean table from a truncated one, which is the single
    distinction per-partition state exists to make.

    ``level`` is the other half of that: a reader must never mistake "did not
    look" for "nothing to report". When the expected side could not be resolved
    the probe still reports every observed slice, but ``missing`` is then
    trivially zero because nothing was asked for — ``scan`` says so out loud.
    """

    # How the observation was obtained. ``scan`` groups the physical table but
    # has no expectation to compare against, so ``missing`` and ``stale``
    # understate. ``reconciled`` probed the roots too and its counts are
    # meaningful. ``metadata`` is reserved for the cheap catalog route
    # (INFORMATION_SCHEMA.PARTITIONS and friends), which is not implemented yet.
    level: Literal["metadata", "scan", "reconciled"] = "scan"
    total: int = 0  # slices in the probed set, before the cap
    reported: int = 0  # how many of them ``partitions`` carries
    stale: int = 0  # stale across ``total``, not across ``reported``
    missing: int = 0  # expected but not observed — the backfill count
    first: str | None = None  # lowest partition id in the whole set
    last: str | None = None  # highest partition id in the whole set
    truncated: bool = False  # total > reported


class PhaseObservation(BaseModel):
    """Watermark state observed at one phase of a run.

    ``begin`` is the state as the run found it — collected by the refresh
    plan's probe before anything executed. ``end`` is the state after
    execution, its ``expected_watermarks`` probed as late as the run can
    manage so the expected side is the freshest it can attest to.
    """

    phase: Literal["begin", "end"]
    probed_at: str | None = None  # emitting process's clock; display only
    observed_watermarks: list[WatermarkValue] = Field(default_factory=list)
    expected_watermarks: list[WatermarkValue] = Field(default_factory=list)


class PlanVerdict(BaseModel):
    """What the refresh plan judged about one datasource, and why.

    An input the reader can audit — recorded verbatim from the plan, never
    re-derived. Absent when no refresh plan existed (``run``/``state`` ops).
    """

    judged_stale: bool = False
    reason: str | None = None
    kind: Literal["sql", "script"] | None = None
    forced: bool = False


class DatasourceState(BaseModel):
    """State of one logical datasource (script-scoped view of an address)."""

    datasource_id: str  # ds.identifier — attribute data, NOT the key
    script: str | None = None  # defining script path, project-relative
    is_root: bool = False
    refresh_kind: Literal["sql", "script"] | None = None
    # The end-phase merged view, kept for existing consumers. Consumers that
    # can, derive their own verdict from ``observations`` instead.
    status: AssetStatus = "unknown"
    stale_reason: str | None = None
    observed_watermarks: list[WatermarkValue] = Field(default_factory=list)
    expected_watermarks: list[WatermarkValue] = Field(default_factory=list)
    columns: list[ColumnMapping] = Field(default_factory=list)
    # Phased observations (begin/end) + the plan's advisory verdict.
    observations: list[PhaseObservation] = Field(default_factory=list)
    plan: PlanVerdict | None = None
    # Declared partitioning, empty for an unpartitioned datasource. Present even
    # when the table holds no slices yet — "partitioned and empty" and "not
    # partitioned" are different states.
    partition_by: list[PartitionColumn] = Field(default_factory=list)
    partitions: list[PartitionState] = Field(default_factory=list)
    # Whether ``partitions`` enumerates every slice. False marks a delta scoped
    # to the slices one worker owns — see ``merge_snapshots``.
    partitions_complete: bool = True
    # Counts over the whole probed set, which outlive the cap on ``partitions``.
    # None on a scoped delta: a worker that owns three slices has no standing to
    # describe the table.
    partition_summary: PartitionSummary | None = None


class PhysicalAssetState(BaseModel):
    """State of one physical address — the snapshot's unit of identity."""

    address: str  # stable asset key — see ``stable_asset_key``
    managed: bool = False  # trilogy owns refresh for this address
    # The script that builds this asset, project-relative. Attribute data, NOT
    # part of the key: which script owns an address is a logical fact about the
    # project, while the address itself is physical.
    owner_script: str | None = None
    status: AssetStatus = "unknown"
    datasources: list[DatasourceState] = Field(default_factory=list)


class StateSnapshotSummary(BaseModel):
    total: int = 0
    managed: int = 0
    stale: int = 0
    fresh: int = 0
    unknown: int = 0


class StateSnapshot(BaseModel):
    schema_version: int = SNAPSHOT_SCHEMA_VERSION
    snapshot_ts: str = ""
    run_id: str | None = None
    project: str | None = None
    target: str = ""
    dialect: str | None = None
    assets: list[PhysicalAssetState] = Field(default_factory=list)
    summary: StateSnapshotSummary = Field(default_factory=StateSnapshotSummary)


def is_remote_address(address: str) -> bool:
    """Whether an address lives behind a URL scheme (``gs://``, ``s3://``,
    ``http://``) rather than on the local filesystem. Remote addresses are
    already stable, so they are never relativized."""
    return "://" in address


def address_type_of(ds: Datasource) -> AddressType:
    """The datasource's address type. A bare string address is a table."""
    return ds.address.type if isinstance(ds.address, Address) else AddressType.TABLE


def query_digest(sql: str) -> str:
    """A short stable identity for an inline query address.

    The SQL text *is* the physical identity of a query datasource, but it is
    unusable as a key verbatim — multi-line, and it churns on reformatting.
    Whitespace is collapsed before hashing so reindenting the query does not
    change its identity (this can in principle merge two queries differing
    only inside a string literal; immaterial for ingest queries).
    """
    normalized = " ".join(sql.split())
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()[:QUERY_DIGEST_CHARS]


def project_relative_path(path: str, project_root: PurePath | None) -> str:
    """A project-local path relative to the project root, POSIX-rendered.

    Stable across runs and across checkouts, unlike the absolute path (which
    changes per machine — and per run under an orchestrator's scratch dir).
    Paths outside the project root keep their absolute form: they at least
    stay stable on one machine, and relativizing against nothing would
    manufacture collisions.
    """
    if project_root is None:
        return path
    try:
        return PurePath(path).relative_to(project_root).as_posix()
    except ValueError:
        return path


def stable_asset_key(
    address: str,
    address_type: AddressType,
    project_root: PurePath | None,
) -> str:
    """The snapshot's identity for an asset (load-bearing — keying rules).

    A pure function of the *physical* pointer, dispatching on address type and
    never on the shape of the address string. Nothing logical enters the key:
    two scripts writing the same file produce the same key, because it is the
    same asset. Every datasource gets one — roots included; being unmanaged
    changes ``PhysicalAssetState.managed``, not whether the asset exists.

    - Warehouse tables and remote URLs pass through verbatim: already stable,
      and shared across models pointing at the same object.
    - Project-local files are keyed by their project-relative path. The
      absolute path can never be the identity — it changes per machine, and
      per run under an orchestrator's scratch dir.
    - A Python datasource script is a procedure rather than a data artifact,
      so its path is labeled: ``script::<project-relative path>``.
    - An inline query has no artifact at all; its SQL *is* the identity, keyed
      ``query::<digest>`` (see :func:`query_digest`) because the raw text is
      multi-line and churns on reformatting.
    """
    if address_type == AddressType.QUERY:
        return f"{QUERY_KEY_SCHEME}{KEY_SCHEME_SEPARATOR}{query_digest(address)}"
    if address_type not in FILE_ADDRESS_TYPES or is_remote_address(address):
        return address
    relative = project_relative_path(address, project_root)
    if address_type == AddressType.PYTHON_SCRIPT:
        return f"{SCRIPT_KEY_SCHEME}{KEY_SCHEME_SEPARATOR}{relative}"
    return relative


def _watermark_value(
    key: str, update_key: UpdateKey, ds: Datasource | None, probed_at: str | None = None
) -> WatermarkValue:
    value = update_key.value
    raw: str | int | float | bool | None = (
        value if isinstance(value, (bool, int, float, str)) else None
    )
    concept_address, column = _match_column_binding(key, ds)
    return WatermarkValue(
        key=key,
        type=update_key.type.value,
        value_raw=raw,
        value=str(value) if value is not None else None,
        value_type=type(value).__name__ if value is not None else None,
        concept_address=concept_address,
        column=column,
        probed_at=probed_at,
    )


def observed_and_expected(
    watermark: DatasourceWatermark | None,
    concept_max: dict[str, UpdateKey] | None,
    ds: Datasource,
    probed_at: str | None = None,
) -> tuple[list[WatermarkValue], list[WatermarkValue]]:
    """One probe's observed values, plus the expected value for each key that
    has one.

    The single place the observed/expected pairing is derived, so both the
    end-phase view (:func:`build_datasource_state`) and the begin phase
    (:func:`_phase_observations`) render a probe identically.
    """
    observed: list[WatermarkValue] = []
    expected: list[WatermarkValue] = []
    if watermark is None:
        return observed, expected
    for key, update_key in watermark.keys.items():
        observed.append(_watermark_value(key, update_key, ds, probed_at))
        if concept_max and key in concept_max:
            expected.append(_watermark_value(key, concept_max[key], ds, probed_at))
    return observed, expected


def restore_watermark_value(
    entry: WatermarkValue,
) -> str | int | float | datetime | date | None:
    """Inverse of :func:`_watermark_value` — rebuild the comparable python value.

    ``value_raw`` covers the JSON-native cases. Temporal values survive only as
    their ``str()`` rendering, so ``value_type`` drives the parse back; anything
    unrecognized (or unparseable) degrades to the string, which
    ``_compare_watermark_values`` still orders consistently."""
    if entry.value_raw is not None:
        return entry.value_raw
    if entry.value is None:
        return None
    try:
        if entry.value_type == "datetime":
            return datetime.fromisoformat(entry.value)
        if entry.value_type == "date":
            return date.fromisoformat(entry.value)
    except ValueError:
        pass
    return entry.value


def managed_states_by_address(snapshot: StateSnapshot) -> dict[str, DatasourceState]:
    """Managed (non-root) datasource states from a snapshot, keyed by PHYSICAL
    ADDRESS.

    Root entries are excluded on purpose: a root's watermark is the *expected*
    side of every staleness comparison and must always be re-read live, or a
    moved upstream would never mark its dependents stale.

    Physical-address keying is what makes a snapshot reusable by a *different*
    model file: logical concept addresses are namespaced per script and never
    reconciled across scripts, but the table a datasource points at is stable.
    """
    by_address: dict[str, DatasourceState] = {}
    for asset in snapshot.assets:
        for ds_state in asset.datasources:
            # Either kind of observation makes an entry worth seeding: a
            # partitioned datasource carries its whole state per slice and may
            # have no table-level watermark at all.
            if ds_state.is_root or not (
                ds_state.observed_watermarks or ds_state.partitions
            ):
                continue
            by_address.setdefault(asset.address, ds_state)
    return by_address


def _rekey_for(entry: WatermarkValue, ds: Datasource) -> str:
    """Translate a recorded watermark key into the key ``ds`` would emit.

    Watermark keys are logical concept names, so a model that renamed its
    concepts cannot match the writer's keys directly. The physical column is the
    stable bridge: the snapshot records which column each concept was bound to,
    and ``ds`` binds that same column to its own concept. Key conventions mirror
    ``watermarks.py`` — full address for KEY_HASH, bare name otherwise, and the
    literal ``update_time`` (no concept) passes through.
    """
    if entry.key == "update_time" or entry.column is None:
        return entry.key
    for col in ds.columns:
        if col.alias == entry.column:
            if entry.type == UpdateKeyType.KEY_HASH.value:
                return col.concept.address
            return col.concept.address.rsplit(".", 1)[-1]
    return entry.key


def watermarks_for_datasource(
    ds_state: DatasourceState, ds: Datasource
) -> DatasourceWatermark:
    """Rehydrate a recorded datasource's observations for ``ds``, re-keyed onto
    its concept names."""
    return DatasourceWatermark(
        keys={
            # concept_name mirrors the dict key, as every producer in
            # watermarks.py emits it.
            key: UpdateKey(
                concept_name=key,
                type=UpdateKeyType(entry.type),
                value=restore_watermark_value(entry),
            )
            for entry, key in (
                (entry, _rekey_for(entry, ds)) for entry in ds_state.observed_watermarks
            )
        }
    )


#: Typed inverse of ``render_partition_value``. Lives in ``partitions`` because
#: a ``--partition`` selector parses values the same way a recorded slice does,
#: and two spellings of that would let a flag and a snapshot disagree about what
#: ``2024-01-03`` means.
_restore_partition_value = parse_partition_value


def _restore_partition_observation(
    partition: PartitionState,
    datatypes: dict[str, object],
    watermarks: list[WatermarkValue],
    row_count: int | None,
) -> PartitionObservation:
    return PartitionObservation(
        values={
            column: _restore_partition_value(rendered, datatypes.get(column))
            for column, rendered in partition.values.items()
        },
        row_count=row_count,
        keys={
            entry.key: UpdateKey(
                concept_name=entry.key,
                type=UpdateKeyType(entry.type),
                value=restore_watermark_value(entry),
            )
            for entry in watermarks
        },
    )


def partitions_for_datasource(
    ds_state: DatasourceState, ds: Datasource, environment
) -> tuple[list[PartitionObservation], list[PartitionObservation]] | None:
    """Rehydrate a recorded datasource's slices as ``(observed, expected)``.

    The partition twin of :func:`watermarks_for_datasource`. Both sides round
    trip: each recorded slice carries ``observed``/``expected`` flags and its own
    two watermark lists, so the pair a live probe would have produced is
    reconstructible without touching the warehouse.

    Returns None — meaning "probe normally" — when there is nothing trustworthy
    to seed: the reader declares no partitioning, the writer recorded none (an
    older snapshot), or the record is a partition-scoped delta, which speaks for
    only some slices and would understate the rest as absent.
    """
    if not ds.partition_by or not ds_state.partition_by:
        return None
    if not ds_state.partitions_complete:
        return None
    datatypes: dict[str, object] = {}
    for col in ds.columns:
        concept = environment.concepts.get(col.concept.address)
        if concept is not None and isinstance(col.alias, str):
            datatypes[col.alias] = concept.datatype.data_type
    observed = [
        _restore_partition_observation(p, datatypes, p.observed_watermarks, p.row_count)
        for p in ds_state.partitions
        if p.observed
    ]
    expected = [
        _restore_partition_observation(p, datatypes, p.expected_watermarks, None)
        for p in ds_state.partitions
        if p.expected
    ]
    return observed, expected


def _match_column_binding(
    key: str, ds: Datasource | None
) -> tuple[str | None, str | None]:
    """Best-effort mapping of an as-emitted watermark key back to its
    ``(concept_address, physical column)`` binding on the datasource.

    Full-address keys match exactly; bare-name keys match on the trailing name
    component; ``update_time`` is a table-level watermark with no concept. The
    column is reported only when it is a real column name — a raw expression
    binding is not a stable identifier for a reader to match on."""
    if ds is None or key == "update_time":
        return None, None
    for col in ds.columns:
        address = col.concept.address
        if key == address or ("." not in key and address.rsplit(".", 1)[-1] == key):
            return address, col.alias if isinstance(col.alias, str) else None
    return None, None


def _column_mappings(ds: Datasource) -> list[ColumnMapping]:
    return [
        ColumnMapping(
            column=str(col.alias),
            concrete=isinstance(col.alias, str),
            concept_address=col.concept.address,
            modifiers=[str(m.value) for m in col.modifiers],
        )
        for col in ds.columns
    ]


def _partition_columns(ds: Datasource) -> list[PartitionColumn]:
    by_address = {col.concept.address: col for col in ds.columns}
    return [
        PartitionColumn(
            column=partition_column_name(by_address[ref.address]),
            concept_address=ref.address,
        )
        for ref in ds.partition_by
        if ref.address in by_address
    ]


def summarize_partitions(
    states: list[PartitionState], level: Literal["metadata", "scan", "reconciled"]
) -> PartitionSummary:
    """Counts over the whole probed set, taken before any cap is applied.

    ``reported`` is filled in by :func:`cap_partitions`, which is the only thing
    that knows how many survived.
    """
    return PartitionSummary(
        level=level,
        total=len(states),
        reported=len(states),
        stale=sum(1 for p in states if p.status == "stale"),
        missing=sum(1 for p in states if p.expected and not p.observed),
        first=states[0].partition_id if states else None,
        last=states[-1].partition_id if states else None,
        truncated=False,
    )


def cap_partitions(
    states: list[PartitionState], limit: int, summary: PartitionSummary
) -> tuple[list[PartitionState], PartitionSummary]:
    """Trim the slice list to ``limit``, keeping the ones worth carrying.

    Stale slices first, oldest id first — that ordering *is* the backfill queue,
    so the head of the list is what a consumer would act on. The remaining
    budget goes to the most recent healthy slices, which are what a freshness
    view renders; older healthy slices are the least informative thing in the
    set and are the first to go.

    The kept list is re-sorted by id, because the merge unit is the id and a
    reader diffing two snapshots should not see order churn.
    """
    if limit < 0 or len(states) <= limit:
        return states, summary
    stale = [p for p in states if p.status == "stale"]
    healthy = [p for p in states if p.status != "stale"]
    kept = stale[:limit]
    kept += healthy[len(healthy) - (limit - len(kept)) :] if len(kept) < limit else []
    kept.sort(key=lambda p: p.partition_id)
    return kept, summary.model_copy(
        update={"reported": len(kept), "truncated": len(kept) < summary.total}
    )


def build_partition_states(
    ds: Datasource,
    observed: list[PartitionObservation],
    expected: list[PartitionObservation],
    probed_at: str | None = None,
    run_id: str | None = None,
    limit: int | None = MAX_REPORTED_PARTITIONS,
) -> tuple[list[PartitionState], PartitionSummary]:
    """Pair up the two probed sides into one state entry per slice.

    Returns the (possibly capped) slice list alongside a summary whose counts
    cover the full set — see :class:`PartitionSummary`. ``limit=None`` disables
    the cap, which is right for a local file store and wrong for anything that
    has to put the result on a wire.
    """
    observed_by_id = {obs.id: obs for obs in observed}
    expected_by_id = {exp.id: exp for exp in expected}

    states: list[PartitionState] = []
    for pid in sorted(observed_by_id.keys() | expected_by_id.keys()):
        obs = observed_by_id.get(pid)
        exp = expected_by_id.get(pid)
        source = obs or exp
        assert source is not None  # pid came from one of the two maps
        # The single verdict rule, shared with the refresh work list — see
        # ``partitions.partition_verdict``.
        verdict = partition_verdict(obs, exp)
        states.append(
            PartitionState(
                partition_id=pid,
                values={
                    column: render_partition_value(value)
                    for column, value in source.values.items()
                },
                observed=obs is not None,
                expected=exp is not None,
                status="stale" if verdict.stale else "fresh",
                stale_reason=verdict.reason,
                row_count=obs.row_count if obs else None,
                observed_watermarks=[
                    _watermark_value(key, update_key, ds, probed_at)
                    for key, update_key in (obs.keys.items() if obs else ())
                ],
                expected_watermarks=[
                    _watermark_value(key, update_key, ds, probed_at)
                    for key, update_key in (exp.keys.items() if exp else ())
                ],
                probed_at=probed_at,
                run_id=run_id,
            )
        )
    # An empty expected side is "could not resolve an expectation", not "nothing
    # is expected" — probe_expected_partitions swallows an unresolvable plan and
    # returns []. Saying `scan` there is what stops a consumer reading
    # `missing: 0` as a clean bill of health.
    level = "reconciled" if expected_by_id else "scan"
    summary = summarize_partitions(states, level)
    if limit is None:
        return states, summary
    return cap_partitions(states, limit, summary)


def _partition_rollup_reason(
    partitions: list[PartitionState], summary: PartitionSummary | None = None
) -> str | None:
    """How a datasource explains itself when its slices are what made it stale.

    One phrasing, used both when a snapshot is built and when deltas are merged
    — the merged file must not describe the same condition differently. Counted
    off the summary when there is one, because ``partitions`` may have been
    capped and "3 of 200 stale" would understate a table that is 2,583 behind.
    """
    if summary is not None:
        if not summary.stale:
            return None
        return f"{summary.stale} of {summary.total} partitions stale"
    stale = [p for p in partitions if p.status == "stale"]
    if not stale:
        return None
    return f"{len(stale)} of {len(partitions)} partitions stale"


def _rollup_status(statuses: list[AssetStatus]) -> AssetStatus:
    if any(s == "stale" for s in statuses):
        return "stale"
    if any(s == "unknown" for s in statuses):
        return "unknown"
    return "fresh"


def build_datasource_state(
    ds: Datasource,
    watermark: DatasourceWatermark | None,
    stale: StaleAsset | None,
    concept_max: dict[str, UpdateKey] | None = None,
    script: str | None = None,
    partitions: list[PartitionState] | None = None,
    partition_summary: PartitionSummary | None = None,
) -> DatasourceState:
    """Build the per-datasource state entry from probe results.

    ``concept_max`` is the expected-side map (root-derived max per concept);
    only entries matching this datasource's observed watermark keys are
    attached, so a UI can render "behind by how much".

    ``partitions`` is the per-slice state for a partitioned datasource (None
    when unpartitioned or not probed). A stale slice makes the datasource stale
    even when its whole-table watermark looks caught up — a missing slice is
    exactly what a table-level MAX cannot see."""
    if ds.is_refreshable_root:
        refresh_kind: Literal["sql", "script"] | None = "script"
    elif not ds.is_root:
        refresh_kind = "sql"
    else:
        refresh_kind = None

    probed_at = utc_now_iso()
    observed, expected = observed_and_expected(watermark, concept_max, ds, probed_at)

    if stale is not None:
        status: AssetStatus = "stale"
    elif watermark is not None:
        status = "fresh"
    else:
        status = "unknown"

    stale_reason = stale.reason if stale is not None else None
    # The summary is authoritative over the list, which the cap may have trimmed
    # — a table with 2,583 stale slices must not read as fresh because none of
    # them fit in the payload.
    if partition_summary is not None:
        any_stale = partition_summary.stale > 0
    else:
        any_stale = any(p.status == "stale" for p in partitions or [])
    if any_stale:
        status = "stale"
        stale_reason = stale_reason or _partition_rollup_reason(
            partitions or [], partition_summary
        )

    observations, plan = _phase_observations(ds, observed, expected, probed_at)

    return DatasourceState(
        datasource_id=ds.identifier,
        script=script,
        is_root=ds.is_root,
        refresh_kind=refresh_kind,
        status=status,
        stale_reason=stale_reason,
        observed_watermarks=observed,
        expected_watermarks=expected,
        columns=_column_mappings(ds),
        observations=observations,
        plan=plan,
        partition_by=_partition_columns(ds),
        partitions=partitions or [],
        partitions_complete=not ds.partition_by or partitions is not None,
        partition_summary=partition_summary,
    )


def _phase_observations(
    ds: Datasource,
    observed: list[WatermarkValue],
    expected: list[WatermarkValue],
    probed_at: str,
) -> tuple[list[PhaseObservation], PlanVerdict | None]:
    """The phased record for one datasource.

    The ``end`` phase is always present: the same values this snapshot was
    built from, probed post-execution (the snapshot probe runs after the
    work). The ``begin`` phase and the plan verdict come from the ambient
    :class:`~trilogy.execution.state.phases.PhaseRecorder`, which the refresh
    command populates from its own planning probe — absent (run/state ops, or
    an unrecorded refresh), only the end observation is emitted.
    """
    observations = [
        PhaseObservation(
            phase="end",
            probed_at=probed_at,
            observed_watermarks=observed,
            expected_watermarks=expected,
        )
    ]
    plan_verdict: PlanVerdict | None = None

    recorder = get_phase_recorder()
    if recorder is None:
        return observations, plan_verdict

    begin = recorder.begin_for(ds.identifier)
    if begin is not None:
        begin_observed, begin_expected = observed_and_expected(
            begin.watermark, begin.concept_max, ds, begin.probed_at
        )
        observations.insert(
            0,
            PhaseObservation(
                phase="begin",
                probed_at=begin.probed_at,
                observed_watermarks=begin_observed,
                expected_watermarks=begin_expected,
            ),
        )
    verdict = recorder.plan_for(ds.identifier)
    if verdict is not None:
        plan_verdict = PlanVerdict(
            judged_stale=verdict.judged_stale,
            reason=verdict.reason,
            kind=verdict.kind,
            forced=verdict.forced,
        )
    return observations, plan_verdict


def merge_into_snapshot(
    entries: list[tuple[str, DatasourceState]],
    managed_addresses: set[str] | None = None,
    run_id: str | None = None,
    project: str | None = None,
    target: str = "",
    dialect: str | None = None,
    owner_scripts: dict[str, str] | None = None,
) -> StateSnapshot:
    """Group per-datasource entries by physical address and roll up status.

    ``entries``: (physical_address, DatasourceState) pairs. Duplicate
    (script, datasource_id) pairs at an address are dropped. Address status is
    stale if any datasource is stale, else unknown if any is unknown, else
    fresh."""
    managed_addresses = managed_addresses or set()
    by_address: dict[str, list[DatasourceState]] = {}
    for address, ds_state in entries:
        bucket = by_address.setdefault(address, [])
        if any(
            existing.datasource_id == ds_state.datasource_id
            and existing.script == ds_state.script
            for existing in bucket
        ):
            continue
        bucket.append(ds_state)

    assets: list[PhysicalAssetState] = []
    for address in sorted(by_address):
        datasources = sorted(by_address[address], key=lambda d: d.datasource_id)
        assets.append(
            PhysicalAssetState(
                address=address,
                managed=address in managed_addresses,
                owner_script=(owner_scripts or {}).get(address),
                status=_rollup_status([d.status for d in datasources]),
                datasources=datasources,
            )
        )

    return StateSnapshot(
        snapshot_ts=utc_now_iso(),
        run_id=run_id,
        project=project,
        target=target,
        dialect=dialect,
        assets=assets,
        summary=summarize(assets),
    )


def summarize(assets: list[PhysicalAssetState]) -> StateSnapshotSummary:
    return StateSnapshotSummary(
        total=len(assets),
        managed=sum(1 for a in assets if a.managed),
        stale=sum(1 for a in assets if a.status == "stale"),
        fresh=sum(1 for a in assets if a.status == "fresh"),
        unknown=sum(1 for a in assets if a.status == "unknown"),
    )


def stale_partitions(snapshot: StateSnapshot) -> list[tuple[str, str, PartitionState]]:
    """Every stale slice in a snapshot as ``(asset key, datasource id, slice)``.

    The orchestrator's work list: read the state file, take this, fan out one
    run per entry."""
    return [
        (asset.address, ds_state.datasource_id, partition)
        for asset in snapshot.assets
        for ds_state in asset.datasources
        for partition in ds_state.partitions
        if partition.status == "stale"
    ]


def scope_to_partitions(
    snapshot: StateSnapshot, partition_ids: set[str]
) -> StateSnapshot:
    """Narrow a snapshot to the slices a worker owns, as a mergeable delta.

    A worker that refreshed one partition observed the whole table on its way
    out — including slices other workers were concurrently rewriting. Publishing
    those observations would let the last writer win on data it never owned, so
    a scoped delta keeps only its own slices and flags
    ``partitions_complete=False``; :func:`merge_snapshots` then overlays it
    rather than replacing. Datasources with no matching slice keep an empty
    scoped list, contributing nothing.

    The **summary is kept**, and that is not a contradiction of the above. What
    a partial writer cannot be trusted about is a per-slice *claim* — asserting
    a state for a slice another worker is mid-write on. An aggregate is a
    different kind of statement: "this table has 2,700 slices and 2,583 are
    behind" is true of the table at the moment it was probed, by anyone who
    probed it. :class:`PartitionSummary` already distinguishes the two —
    ``reported`` drops to the scoped count while ``total`` stays whole — so the
    delta says "1 of 2,700" rather than pretending to 2,700.

    Dropping it instead would be quietly disastrous for the case this exists to
    serve: where every run is partition-targeted, no run ever emits a complete
    probe, and the totals would never be reported at all.
    """
    scoped = snapshot.model_copy(deep=True)
    for asset in scoped.assets:
        for ds_state in asset.datasources:
            if not ds_state.partition_by:
                continue
            ds_state.partitions = [
                p for p in ds_state.partitions if p.partition_id in partition_ids
            ]
            ds_state.partitions_complete = False
            if ds_state.partition_summary is not None:
                ds_state.partition_summary = ds_state.partition_summary.model_copy(
                    update={
                        "reported": len(ds_state.partitions),
                        "truncated": len(ds_state.partitions)
                        < ds_state.partition_summary.total,
                    }
                )
    return scoped


def selector_partition_ids(snapshot: StateSnapshot, selector: dict[str, str]) -> set[str]:
    """Partition ids a concept-addressed selector names, read off the snapshot.

    The bridge between ``refresh --partition`` (which speaks concept addresses,
    because that is what a caller working from the model has) and
    ``scope_to_partitions`` (which speaks partition ids, because that is the
    merge unit). ``DatasourceState.partition_by`` carries both halves of the
    mapping, so no environment is needed here.

    Values go through the same canonical rendering a recorded slice used, so the
    ISO spellings of a date (``2024-01-03``, ``20240103``) name the same slice.
    Anything that does not parse as a temporal value is taken verbatim, since
    every other partition type is already its own canonical string.

    A value that matches no recorded slice simply selects nothing. That is the
    safe direction to be wrong in: a scoped delta only ever *adds* claims on
    merge, so under-matching costs reporting detail and can never overwrite
    another worker's slice.
    """
    ids: set[str] = set()
    for asset in snapshot.assets:
        for ds_state in asset.datasources:
            columns = ds_state.partition_by
            if not columns or any(c.concept_address not in selector for c in columns):
                continue
            ids.add(
                partition_id(
                    {
                        c.column: _normalize_selector_value(
                            selector[c.concept_address]
                        )
                        for c in columns
                    }
                )
            )
    return ids


def _normalize_selector_value(raw: str) -> PartitionValue:
    """Best-effort canonical form for a selector value.

    Temporal values are the ones that actually vary in spelling, and they are
    also almost every real partition key; anything else is already its own
    canonical string."""
    for parse in (date.fromisoformat, datetime.fromisoformat):
        try:
            return parse(raw)
        except ValueError:
            continue
    return raw


def _merge_partitions(
    base: DatasourceState, delta: DatasourceState
) -> list[PartitionState]:
    if delta.partitions_complete:
        return list(delta.partitions)
    merged = {p.partition_id: p for p in base.partitions}
    for partition in delta.partitions:
        merged[partition.partition_id] = partition
    return [merged[pid] for pid in sorted(merged)]


def _is_missing(partition: PartitionState) -> bool:
    return partition.expected and not partition.observed


def _merge_partition_summary(
    base: DatasourceState,
    delta: DatasourceState,
    merged_partitions: list[PartitionState],
) -> PartitionSummary | None:
    """Carry the whole-set counts across a merge.

    A complete delta re-probed everything, so its summary replaces the base's. A
    scoped delta only moved the slices it owned, so the base's counts stand and
    are adjusted slice by slice — which is the whole point of keeping counts
    separate from the list: a backfill that fixes one of 2,583 stale slices must
    leave a readable 2,582 behind, not a recount of whatever 200 slices happened
    to fit in the payload.

    One inexactness, bounded and deliberate: if the base was itself truncated, a
    delta slice whose id is not in the base's retained list is counted as new,
    when it may have been one of the slices the cap dropped. The drift is at most
    the number of such slices, and it self-corrects on the next complete probe.
    Making it exact would mean carrying every id the cap exists to not carry.
    """
    if delta.partitions_complete:
        return delta.partition_summary
    if delta.partition_summary is not None:
        # A scoped delta still probed the whole table on its way out, so its
        # aggregate is the more recent one — and unlike its per-slice claims, an
        # aggregate is not something it needed to own to observe. It can lag by
        # the slices other workers finished after it looked; that self-corrects
        # on the next probe and is why counts are a summary, not a ledger.
        return delta.partition_summary.model_copy(
            update={
                "reported": len(merged_partitions),
                "truncated": len(merged_partitions) < delta.partition_summary.total,
            }
        )
    if base.partition_summary is None:
        return None

    summary = base.partition_summary.model_copy(deep=True)
    base_by_id = {p.partition_id: p for p in base.partitions}
    for partition in delta.partitions:
        was = base_by_id.get(partition.partition_id)
        if was is None:
            summary.total += 1
            summary.stale += 1 if partition.status == "stale" else 0
            summary.missing += 1 if _is_missing(partition) else 0
            continue
        summary.stale += (partition.status == "stale") - (was.status == "stale")
        summary.missing += _is_missing(partition) - _is_missing(was)

    summary.reported = len(merged_partitions)
    summary.truncated = summary.reported < summary.total
    summary.first = merged_partitions[0].partition_id if merged_partitions else None
    summary.last = merged_partitions[-1].partition_id if merged_partitions else None
    return summary


def _merge_datasource_state(
    base: DatasourceState, delta: DatasourceState
) -> DatasourceState:
    """Fold one datasource record into another.

    A complete delta is a newer whole-asset probe and simply wins. A scoped
    delta speaks only for its slices: its asset-level verdict was computed
    against a table other workers were mid-write on, so the base's fields stand
    and the status is re-derived from the merged slice set instead.
    """
    if delta.partitions_complete and not delta.partition_by:
        return delta.model_copy(deep=True)
    summary = _merge_partition_summary(base, delta, _merge_partitions(base, delta))
    if delta.partitions_complete:
        merged = delta.model_copy(deep=True)
    else:
        merged = base.model_copy(deep=True)
        merged.partitions = _merge_partitions(base, delta)
    merged.partition_summary = summary
    if summary is not None and summary.total:
        merged.status = "stale" if summary.stale else "fresh"
        merged.stale_reason = _partition_rollup_reason(merged.partitions, summary)
    elif merged.partitions:
        merged.status = _rollup_status([p.status for p in merged.partitions])
        merged.stale_reason = _partition_rollup_reason(merged.partitions)
    merged.partitions_complete = base.partitions_complete or delta.partitions_complete
    return merged


def merge_snapshots(base: StateSnapshot, *deltas: StateSnapshot) -> StateSnapshot:
    """Fold partition deltas into a base snapshot.

    The write side of fan-out: N workers each publish a snapshot scoped to the
    slices they refreshed (see :func:`scope_to_partitions`), and this merges
    them into one file. Because the merge unit is the ``partition_id`` and each
    worker only speaks for slices it owns, the result is independent of merge
    order — which is what lets a file-backed store support parallelism with a
    single-writer coordinator instead of a lock per asset.

    Assets and datasources present only in a delta are added; everything else
    keeps the base's record unless the delta claims a complete probe.
    """
    assets: dict[str, PhysicalAssetState] = {
        asset.address: asset.model_copy(deep=True) for asset in base.assets
    }
    latest = base
    for delta in deltas:
        if delta.snapshot_ts > latest.snapshot_ts:
            latest = delta
        for incoming in delta.assets:
            existing = assets.get(incoming.address)
            if existing is None:
                assets[incoming.address] = incoming.model_copy(deep=True)
                continue
            # Keyed by datasource id alone, never by script: the defining script
            # is attribute data, and a delta legitimately comes from a different
            # one (the per-partition build script rather than the model). Keying
            # on the pair would file the same asset twice.
            by_key = {d.datasource_id: d for d in existing.datasources}
            for ds_state in incoming.datasources:
                current = by_key.get(ds_state.datasource_id)
                by_key[ds_state.datasource_id] = (
                    ds_state.model_copy(deep=True)
                    if current is None
                    else _merge_datasource_state(current, ds_state)
                )
            existing.datasources = [by_key[k] for k in sorted(by_key)]
            existing.status = _rollup_status([d.status for d in existing.datasources])
            existing.managed = existing.managed or incoming.managed
            existing.owner_script = existing.owner_script or incoming.owner_script

    ordered = [assets[address] for address in sorted(assets)]
    return StateSnapshot(
        snapshot_ts=utc_now_iso(),
        run_id=latest.run_id,
        project=base.project or latest.project,
        target=base.target or latest.target,
        dialect=base.dialect or latest.dialect,
        assets=ordered,
        summary=summarize(ordered),
    )


__all__ = [
    "KEY_SCHEME_SEPARATOR",
    "MAX_REPORTED_PARTITIONS",
    "QUERY_KEY_SCHEME",
    "SCRIPT_KEY_SCHEME",
    "SNAPSHOT_SCHEMA_VERSION",
    "AssetStatus",
    "ColumnMapping",
    "DatasourceState",
    "PartitionColumn",
    "PartitionState",
    "PartitionSummary",
    "PhaseObservation",
    "PhysicalAssetState",
    "PlanVerdict",
    "StateSnapshot",
    "StateSnapshotSummary",
    "WatermarkValue",
    "address_type_of",
    "build_datasource_state",
    "build_partition_states",
    "cap_partitions",
    "is_remote_address",
    "managed_states_by_address",
    "merge_into_snapshot",
    "merge_snapshots",
    "observed_and_expected",
    "partitions_for_datasource",
    "project_relative_path",
    "query_digest",
    "restore_watermark_value",
    "scope_to_partitions",
    "selector_partition_ids",
    "stable_asset_key",
    "stale_partitions",
    "summarize",
    "summarize_partitions",
    "watermarks_for_datasource",
]
