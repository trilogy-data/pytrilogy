"""State snapshot contract: a serializable, project-scoped view of asset state.

This is the machine-facing state contract for orchestrators (the ``trilogy
state`` command, ``run/refresh --state-file``, and any future remote state
store). It is a superset of the single-file ``/state`` response served by
``trilogy serve`` (``serve_helpers/models.py``), which stays pinned for its
existing consumers.

Keying rules (load-bearing — see ``trilogy/scripts/AGENTS.md``):

- The PRIMARY key of an asset is its **physical address** (``ds.safe_address``).
  Logical concept addresses are namespaced per script and deliberately never
  reconciled across scripts; deduplication happens at the physical layer.
- Watermark keys are recorded **as emitted** by the state store: bare concept
  names for MAX-based probes, full concept addresses for KEY_HASH checksums,
  and the literal ``"update_time"`` for table-mtime watermarks. Do NOT
  normalize them — the emitted key is what refresh comparisons use. The
  ``concept_address`` field is best-effort attribute data.

``schema_version`` bumps only on breaking changes to existing fields; new
fields/values are added without a bump. Consumers must ignore unknown fields.
"""

from __future__ import annotations

from datetime import date, datetime, timezone
from typing import Literal

from pydantic import BaseModel, Field

from trilogy.core.models.datasource import Datasource, UpdateKey, UpdateKeyType
from trilogy.execution.state.watermarks import DatasourceWatermark, StaleAsset

SNAPSHOT_SCHEMA_VERSION = 1

AssetStatus = Literal["fresh", "stale", "unknown"]


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


class ColumnMapping(BaseModel):
    """Physical column -> logical concept binding from the datasource model."""

    column: str
    concrete: bool  # False when the "column" is a raw expression or function
    concept_address: str
    modifiers: list[str] = Field(default_factory=list)


class DatasourceState(BaseModel):
    """State of one logical datasource (script-scoped view of an address)."""

    datasource_id: str  # ds.identifier — attribute data, NOT the key
    script: str | None = None  # defining script path
    is_root: bool = False
    refresh_kind: Literal["sql", "script"] | None = None
    status: AssetStatus = "unknown"
    stale_reason: str | None = None
    observed_watermarks: list[WatermarkValue] = Field(default_factory=list)
    expected_watermarks: list[WatermarkValue] = Field(default_factory=list)
    columns: list[ColumnMapping] = Field(default_factory=list)


class PhysicalAssetState(BaseModel):
    """State of one physical address — the snapshot's unit of identity."""

    address: str  # ds.safe_address
    managed: bool = False  # trilogy owns refresh for this address
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


def _watermark_value(
    key: str, update_key: UpdateKey, ds: Datasource | None
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
    )


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
    if ds.is_root and ds.refresh_script and ds.freshness_probe:
        refresh_kind: Literal["sql", "script"] | None = "script"
    elif not ds.is_root:
        refresh_kind = "sql"
    else:
        refresh_kind = None

    observed: list[WatermarkValue] = []
    expected: list[WatermarkValue] = []
    if watermark is not None:
        for key, update_key in watermark.keys.items():
            observed.append(_watermark_value(key, update_key, ds))
            if concept_max and key in concept_max:
                expected.append(_watermark_value(key, concept_max[key], ds))

    if stale is not None:
        status: AssetStatus = "stale"
    elif watermark is not None:
        status = "fresh"
    else:
        status = "unknown"

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
    )


def merge_into_snapshot(
    entries: list[tuple[str, DatasourceState]],
    managed_addresses: set[str] | None = None,
    run_id: str | None = None,
    project: str | None = None,
    target: str = "",
    dialect: str | None = None,
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
                status=status,
                datasources=datasources,
            )
        )

    return StateSnapshot(
        snapshot_ts=datetime.now(timezone.utc).isoformat(),
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
    "SNAPSHOT_SCHEMA_VERSION",
    "AssetStatus",
    "ColumnMapping",
    "DatasourceState",
    "PhysicalAssetState",
    "StateSnapshot",
    "StateSnapshotSummary",
    "WatermarkValue",
    "build_datasource_state",
    "managed_states_by_address",
    "merge_into_snapshot",
    "restore_watermark_value",
    "watermarks_for_datasource",
]
