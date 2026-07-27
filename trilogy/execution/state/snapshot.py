"""State snapshot contract: a serializable, project-scoped view of asset state.

This is the machine-facing state contract for orchestrators (the ``trilogy
state`` command, ``run/refresh --state-file``, and any future remote state
store). It is a superset of the single-file ``/state`` response served by
``trilogy serve`` (``serve_helpers/models.py``), which stays pinned for its
existing consumers.

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
from trilogy.execution.state.phases import get_phase_recorder
from trilogy.execution.state.watermarks import DatasourceWatermark, StaleAsset
from trilogy.utility import utc_now_iso

SNAPSHOT_SCHEMA_VERSION = 1

AssetStatus = Literal["fresh", "stale", "unknown"]

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
            if ds_state.is_root or not ds_state.observed_watermarks:
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


def build_datasource_state(
    ds: Datasource,
    watermark: DatasourceWatermark | None,
    stale: StaleAsset | None,
    concept_max: dict[str, UpdateKey] | None = None,
    script: str | None = None,
) -> DatasourceState:
    """Build the per-datasource state entry from probe results.

    ``concept_max`` is the expected-side map (root-derived max per concept);
    only entries matching this datasource's observed watermark keys are
    attached, so a UI can render "behind by how much"."""
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

    observations, plan = _phase_observations(ds, observed, expected, probed_at)

    return DatasourceState(
        datasource_id=ds.identifier,
        script=script,
        is_root=ds.is_root,
        refresh_kind=refresh_kind,
        status=status,
        stale_reason=stale.reason if stale is not None else None,
        observed_watermarks=observed,
        expected_watermarks=expected,
        columns=_column_mappings(ds),
        observations=observations,
        plan=plan,
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
        if any(d.status == "stale" for d in datasources):
            status: AssetStatus = "stale"
        elif any(d.status == "unknown" for d in datasources):
            status = "unknown"
        else:
            status = "fresh"
        assets.append(
            PhysicalAssetState(
                address=address,
                managed=address in managed_addresses,
                owner_script=(owner_scripts or {}).get(address),
                status=status,
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
        summary=StateSnapshotSummary(
            total=len(assets),
            managed=sum(1 for a in assets if a.managed),
            stale=sum(1 for a in assets if a.status == "stale"),
            fresh=sum(1 for a in assets if a.status == "fresh"),
            unknown=sum(1 for a in assets if a.status == "unknown"),
        ),
    )


__all__ = [
    "KEY_SCHEME_SEPARATOR",
    "QUERY_KEY_SCHEME",
    "SCRIPT_KEY_SCHEME",
    "SNAPSHOT_SCHEMA_VERSION",
    "AssetStatus",
    "ColumnMapping",
    "DatasourceState",
    "PhaseObservation",
    "PhysicalAssetState",
    "PlanVerdict",
    "StateSnapshot",
    "StateSnapshotSummary",
    "WatermarkValue",
    "address_type_of",
    "build_datasource_state",
    "is_remote_address",
    "managed_states_by_address",
    "merge_into_snapshot",
    "observed_and_expected",
    "project_relative_path",
    "query_digest",
    "restore_watermark_value",
    "stable_asset_key",
    "watermarks_for_datasource",
]
