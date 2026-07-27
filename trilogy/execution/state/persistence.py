"""Load and store asset state as a file.

The write side is the ``--state-file`` output of ``run``/``refresh`` and
``trilogy state -o``; the read side (``--state-input``) rehydrates that file
into a :class:`SnapshotStateStore` so a later invocation trusts the recorded
observations instead of re-probing the warehouse for them. Together they let an
external orchestrator own trilogy's refresh state across processes and machines
without trilogy knowing anything about the orchestrator.

The file is a :class:`~trilogy.execution.state.snapshot.StateSnapshot`. Its unit
of identity is the PHYSICAL ADDRESS, so a snapshot written by one model file is
consumable by a different model that points at the same tables.

The ambient factory seam lives in ``state_store.py``
(:func:`~trilogy.execution.state.state_store.new_state_store`): refresh builds a
state store per script / per managed node, on parallel threads, so what gets
injected is a *factory*, not a shared instance.
"""

from __future__ import annotations

import os
from pathlib import Path

from trilogy import Executor
from trilogy.core.models.datasource import Datasource
from trilogy.core.models.environment import Environment
from trilogy.execution.state.cache import ColumnStatsCache
from trilogy.execution.state.snapshot import (
    DatasourceState,
    StateSnapshot,
    address_type_of,
    managed_states_by_address,
    stable_asset_key,
    watermarks_for_datasource,
)
from trilogy.execution.state.state_store import BaseStateStore, StateStoreFactory
from trilogy.execution.state.watermarks import DatasourceWatermark, StaleAsset

ENV_STATE_FILE = "TRILOGY_STATE_FILE"
ENV_STATE_INPUT = "TRILOGY_STATE_INPUT"


def read_state_snapshot(path: Path | str) -> StateSnapshot:
    """Parse a snapshot file. Unknown fields are ignored by the model, so a
    file written by a newer trilogy still loads."""
    return StateSnapshot.model_validate_json(Path(path).read_text(encoding="utf-8"))


def resolve_state_input(state_input: str | None) -> Path | None:
    """Flag > TRILOGY_STATE_INPUT env > None (no seeding)."""
    if state_input:
        return Path(state_input)
    env_value = os.environ.get(ENV_STATE_INPUT, "").strip()
    return Path(env_value) if env_value else None


class SnapshotStateStore(BaseStateStore):
    """In-memory store pre-seeded from a persisted snapshot.

    Seeding is by physical address and covers only the snapshot's managed
    (non-root) observations — see
    :func:`~trilogy.execution.state.snapshot.managed_states_by_address`. Roots
    stay live, so a moved upstream still marks its dependents stale; assets
    absent from the snapshot fall back to a normal warehouse probe. Recorded
    watermarks are re-keyed onto the reading model's own concept names via the
    shared physical column, so a renamed model still lines up.

    Seeding happens once, on first environment-aware call. It deliberately does
    NOT re-apply afterwards: ``invalidate_address`` drops entries so that
    post-refresh evaluations re-read the warehouse, and re-seeding there would
    resurrect the pre-refresh values.
    """

    def __init__(
        self,
        snapshot: StateSnapshot,
        cache: ColumnStatsCache | None = None,
        project_root: Path | None = None,
    ) -> None:
        super().__init__(cache=cache)
        self.snapshot = snapshot
        # The root recorded keys are relative to (``trilogy.toml``'s directory).
        # Falls back to each environment's working path, which equals the
        # project root only for a script at the top level.
        self.project_root = project_root.resolve() if project_root else None
        self._by_address = managed_states_by_address(snapshot)
        self._seeded = False

    def _recorded_state(
        self, ds: Datasource, project_root: Path
    ) -> DatasourceState | None:
        """The snapshot entry for a datasource, if recorded.

        The key is a pure function of the physical address, so the reader
        simply recomputes it against its own project root. The raw address is
        tried first for snapshots written before stable keys existed.
        """
        direct = self._by_address.get(ds.safe_address)
        if direct is not None:
            return direct
        key = stable_asset_key(ds.safe_address, address_type_of(ds), project_root)
        return self._by_address.get(key)

    def seeded_watermarks(self, env: Environment) -> dict[str, DatasourceWatermark]:
        """Snapshot observations re-keyed onto this environment's datasource
        ids and concept names."""
        project_root = self.project_root or Path(env.working_path).resolve()
        seeded: dict[str, DatasourceWatermark] = {}
        for ds in env.datasources.values():
            if ds.is_root:
                continue
            recorded = self._recorded_state(ds, project_root)
            if recorded is not None:
                seeded[ds.identifier] = watermarks_for_datasource(recorded, ds)
        return seeded

    def _seed(self, env: Environment) -> None:
        if self._seeded:
            return
        self._seeded = True
        seeded = self.seeded_watermarks(env)
        with self._lock:
            for ds_id, watermark in seeded.items():
                self.watermarks.setdefault(ds_id, watermark)

    def watermark_all_assets(
        self,
        env: Environment,
        executor: Executor,
        skip_datasources: set[str] | None = None,
    ) -> dict[str, DatasourceWatermark]:
        self._seed(env)
        return super().watermark_all_assets(
            env, executor, skip_datasources=skip_datasources
        )

    def is_stale(
        self,
        env: Environment,
        executor: Executor,
        ds_id: str,
        root_assets: set[str] | None = None,
        force: bool = False,
    ) -> StaleAsset | None:
        self._seed(env)
        return super().is_stale(
            env, executor, ds_id, root_assets=root_assets, force=force
        )


def snapshot_store_factory(
    snapshot: StateSnapshot, project_root: Path | None = None
) -> StateStoreFactory:
    """A factory producing a fresh :class:`SnapshotStateStore` per call.

    Each script / managed node gets its own store (they mutate independently on
    parallel threads) seeded from the same immutable snapshot and resolving
    recorded keys against the same project root.
    """
    return lambda cache: SnapshotStateStore(
        snapshot, cache=cache, project_root=project_root
    )


__all__ = [
    "ENV_STATE_FILE",
    "ENV_STATE_INPUT",
    "SnapshotStateStore",
    "read_state_snapshot",
    "resolve_state_input",
    "snapshot_store_factory",
]
