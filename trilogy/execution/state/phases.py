"""Phased state observations: what a refresh found before it changed anything.

The refresh pipeline probes every datasource to build its plan
(:func:`~trilogy.execution.state.state_store.create_refresh_plan`) and then
discards those observations once the plan is built; the post-run snapshot
re-probes and can only ever see the *end* state. The :class:`PhaseRecorder`
keeps the planning probe's observations (the ``begin`` phase — both the
observed watermarks and the expected values they were compared against) and
the plan's per-datasource verdicts so the snapshot can emit them — see
``DatasourceState.observations`` / ``DatasourceState.plan`` in
``snapshot.py``.

Everything here is keyed by ``ds.identifier``, matching
``DirectoryProbeResult.ds_objects`` ("ds_id -> first seen"), NOT by physical
address as ``scripts/AGENTS.md`` requires elsewhere. Two scripts defining
same-named datasources therefore share one begin/verdict — the same
collapsing the directory probe already applies upstream.

The recorder is ambient, installed by the refresh command around its whole
execution. A plain module global rather than a ContextVar, for the same
reason as ``state_store.py``'s factory: refresh evaluates managed nodes on
worker threads that a ContextVar set in the entrypoint would not reach.

Recording is first-wins per datasource id: the earliest probe of a
datasource is its true begin state, and later re-probes (post-refresh
re-evaluation, the final snapshot's own planning pass) must not overwrite
it. The refresh command additionally freezes the recorder before the final
snapshot probe so datasources never probed during execution do not acquire a
fake begin phase from the end-of-run pass.
"""

from __future__ import annotations

import threading
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from typing import TYPE_CHECKING, Literal

from trilogy.core.models.datasource import UpdateKey
from trilogy.execution.state.watermarks import DatasourceWatermark
from trilogy.utility import utc_now_iso

if TYPE_CHECKING:
    from trilogy.core.models.environment import Environment
    from trilogy.execution.state.state_store import RefreshPlan


@dataclass(frozen=True)
class PlanRecord:
    """One datasource's plan verdict, recorded verbatim from the plan."""

    judged_stale: bool
    reason: str | None
    kind: Literal["sql", "script"] | None
    forced: bool


@dataclass(frozen=True)
class BeginObservation:
    """One datasource's state as the planning probe found it.

    Carries both sides of the comparison — the datasource's own watermark and
    the root-derived expected values — so a reader can re-derive the
    begin-phase verdict instead of taking :class:`PlanRecord` on faith.
    """

    watermark: DatasourceWatermark
    concept_max: dict[str, UpdateKey]
    probed_at: str


class PhaseRecorder:
    """Collects begin-phase observations and plan verdicts, first-wins."""

    def __init__(self) -> None:
        self._begin: dict[str, BeginObservation] = {}
        self._plans: dict[str, PlanRecord] = {}
        self._frozen = False
        self._lock = threading.Lock()

    def freeze(self) -> None:
        """Stop accepting records (call before any post-execution re-probe)."""
        with self._lock:
            self._frozen = True

    def record_plan(
        self,
        environment: Environment,
        plan: RefreshPlan,
        skipped: set[str] | None = None,
    ) -> None:
        """Record a planning probe: observed watermarks + per-ds verdicts.

        ``skipped`` datasources were excluded from the plan entirely (owned
        by another script) and get no verdict here — their own owner's plan
        records them. Roots that are not script-refreshable get watermarks
        (they are the expected side) but no verdict: the plan never judges
        them.
        """
        if self._frozen:
            return
        skipped = skipped or set()
        probed_at = utc_now_iso()
        forced_ids = {asset.datasource_id for asset in plan.forced_assets}
        verdicts: dict[str, PlanRecord] = {}
        for asset in plan.refresh_assets:
            verdicts[asset.datasource_id] = PlanRecord(
                judged_stale=True,
                reason=asset.reason,
                kind=asset.kind.value,
                forced=asset.datasource_id in forced_ids,
            )
        # The expected side is snapshotted per plan: a later plan in the same
        # run rebuilds it, and begin must keep what THIS probe compared against.
        concept_max = dict(plan.concept_max_watermarks)
        with self._lock:
            if self._frozen:  # froze while this plan was being built
                return
            for ds_id, watermark in plan.watermarks.items():
                self._begin.setdefault(
                    ds_id, BeginObservation(watermark, concept_max, probed_at)
                )
            for ds in environment.datasources.values():
                ds_id = ds.identifier
                if ds_id in skipped:
                    continue
                if ds_id in verdicts:
                    self._plans.setdefault(ds_id, verdicts[ds_id])
                elif ds.is_managed:
                    kind: Literal["sql", "script"] = "script" if ds.is_root else "sql"
                    self._plans.setdefault(
                        ds_id,
                        PlanRecord(
                            judged_stale=False, reason=None, kind=kind, forced=False
                        ),
                    )

    def begin_for(self, ds_id: str) -> BeginObservation | None:
        return self._begin.get(ds_id)

    def plan_for(self, ds_id: str) -> PlanRecord | None:
        return self._plans.get(ds_id)


_ACTIVE_RECORDER: PhaseRecorder | None = None


def get_phase_recorder() -> PhaseRecorder | None:
    return _ACTIVE_RECORDER


@contextmanager
def phase_recording() -> Iterator[PhaseRecorder]:
    """Scope an ambient recorder to a block, restoring the previous one."""
    global _ACTIVE_RECORDER
    previous = _ACTIVE_RECORDER
    recorder = PhaseRecorder()
    _ACTIVE_RECORDER = recorder
    try:
        yield recorder
    finally:
        _ACTIVE_RECORDER = previous


__all__ = [
    "PhaseRecorder",
    "PlanRecord",
    "get_phase_recorder",
    "phase_recording",
]
