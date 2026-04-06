"""JSON file-backed state store — thin wrapper used by the refresh pipeline."""

from pathlib import Path

from trilogy.execution.state.env_manager import EnvironmentManager
from trilogy.execution.state.watermarks import DatasourceWatermark


class FileStateStore:
    """Persists and loads datasource watermarks from a JSON file via EnvironmentManager."""

    def __init__(self, manager: EnvironmentManager, env_name: str) -> None:
        self._manager = manager
        self._env_name = env_name

    def load(self) -> dict[str, DatasourceWatermark]:
        return self._manager.load_state(self._env_name)

    def save(self, watermarks: dict[str, DatasourceWatermark]) -> None:
        self._manager.save_state(self._env_name, watermarks)

    def track_asset(self, physical_address: str) -> None:
        self._manager.track_asset(self._env_name, physical_address)


def get_state_store_for_project(
    project_name: str,
    env_name: str,
    state_home: Path | None = None,
) -> FileStateStore:
    manager = EnvironmentManager(project_name=project_name, state_home=state_home)
    return FileStateStore(manager=manager, env_name=env_name)
