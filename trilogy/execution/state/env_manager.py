"""Environment management: create, activate, delete environments and persist watermark state."""

import json
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from trilogy.core.models.datasource import UpdateKey, UpdateKeyType
from trilogy.execution.state.watermarks import DatasourceWatermark

DEFAULT_STATE_HOME = Path.home() / ".trilogy"


def _state_root(state_home: Path | None = None) -> Path:
    return state_home or DEFAULT_STATE_HOME


def _project_dir(project_name: str, state_home: Path | None = None) -> Path:
    return _state_root(state_home) / project_name


def _env_dir(project_name: str, env_name: str, state_home: Path | None = None) -> Path:
    return _project_dir(project_name, state_home) / "envs" / env_name


def _active_file(project_name: str, state_home: Path | None = None) -> Path:
    return _project_dir(project_name, state_home) / "active"


def _main_state_file(project_name: str, state_home: Path | None = None) -> Path:
    return _project_dir(project_name, state_home) / "state.json"


@dataclass
class EnvMeta:
    name: str
    created_at: str
    tracked_assets: list[str] = field(default_factory=list)

    def to_dict(self) -> dict:
        return {
            "name": self.name,
            "created_at": self.created_at,
            "tracked_assets": self.tracked_assets,
        }

    @classmethod
    def from_dict(cls, d: dict) -> "EnvMeta":
        return cls(
            name=d["name"],
            created_at=d["created_at"],
            tracked_assets=d.get("tracked_assets", []),
        )


def _serialize_watermarks(
    watermarks: dict[str, DatasourceWatermark],
) -> dict:
    out: dict = {}
    for ds_id, wm in watermarks.items():
        keys_out = {}
        for k, v in wm.keys.items():
            raw_value = v.value
            if isinstance(raw_value, datetime):
                raw_value = raw_value.isoformat()
            elif raw_value is not None and hasattr(raw_value, "isoformat"):
                raw_value = raw_value.isoformat()  # type: ignore[union-attr]
            keys_out[k] = {
                "concept_name": v.concept_name,
                "type": v.type.value,
                "value": raw_value,
            }
        out[ds_id] = {
            "keys": keys_out,
            "refreshed_at": datetime.now(timezone.utc).isoformat(),
        }
    return out


def _deserialize_watermarks(
    data: dict,
) -> dict[str, DatasourceWatermark]:
    result: dict[str, DatasourceWatermark] = {}
    for ds_id, entry in data.items():
        keys: dict[str, UpdateKey] = {}
        for k, v in entry.get("keys", {}).items():
            raw_val = v.get("value")
            try:
                key_type = UpdateKeyType(v["type"])
            except ValueError:
                continue
            if raw_val is not None and key_type == UpdateKeyType.UPDATE_TIME:
                try:
                    raw_val = datetime.fromisoformat(raw_val)
                except (ValueError, TypeError):
                    pass
            elif raw_val is not None and key_type == UpdateKeyType.INCREMENTAL_KEY:
                try:
                    raw_val = int(raw_val)
                except (ValueError, TypeError):
                    try:
                        raw_val = float(raw_val)
                    except (ValueError, TypeError):
                        pass
            keys[k] = UpdateKey(
                concept_name=v["concept_name"],
                type=key_type,
                value=raw_val,
            )
        result[ds_id] = DatasourceWatermark(keys=keys)
    return result


class EnvironmentManager:
    """Manages trilogy environments: create, activate, list, delete, and state persistence."""

    def __init__(self, project_name: str, state_home: Path | None = None) -> None:
        self.project_name = project_name
        self.state_home = state_home
        self._project_dir = _project_dir(project_name, state_home)

    def _env_dir(self, env_name: str) -> Path:
        return _env_dir(self.project_name, env_name, self.state_home)

    def _meta_file(self, env_name: str) -> Path:
        return self._env_dir(env_name) / "meta.json"

    def _state_file(self, env_name: str) -> Path:
        return self._env_dir(env_name) / "state.json"

    def _active_file(self) -> Path:
        return _active_file(self.project_name, self.state_home)

    def _main_state_file(self) -> Path:
        return _main_state_file(self.project_name, self.state_home)

    def create(self, env_name: str) -> EnvMeta:
        """Create a new environment. Raises ValueError if it already exists."""
        env_path = self._env_dir(env_name)
        if env_path.exists():
            raise ValueError(f"Environment '{env_name}' already exists.")
        env_path.mkdir(parents=True, exist_ok=True)
        meta = EnvMeta(
            name=env_name,
            created_at=datetime.now(timezone.utc).isoformat(),
        )
        with open(self._meta_file(env_name), "w") as f:
            json.dump(meta.to_dict(), f, indent=2)
        # Create empty state file
        with open(self._state_file(env_name), "w") as f:
            json.dump({}, f)
        return meta

    def activate(self, env_name: str) -> None:
        """Set the active environment. Raises ValueError if env doesn't exist."""
        if not self._env_dir(env_name).exists():
            raise ValueError(
                f"Environment '{env_name}' does not exist. Run 'trilogy env create {env_name}' first."
            )
        self._project_dir.mkdir(parents=True, exist_ok=True)
        with open(self._active_file(), "w") as f:
            f.write(env_name)

    def deactivate(self) -> None:
        """Clear the active environment."""
        active = self._active_file()
        if active.exists():
            active.unlink()

    def get_active(self) -> Optional[str]:
        """Return the active environment name, or None if no env is active."""
        active = self._active_file()
        if not active.exists():
            return None
        name = active.read_text().strip()
        return name if name else None

    def list_envs(self) -> list[EnvMeta]:
        """Return all environments."""
        envs_dir = self._project_dir / "envs"
        if not envs_dir.exists():
            return []
        metas: list[EnvMeta] = []
        for env_path in sorted(envs_dir.iterdir()):
            meta_file = env_path / "meta.json"
            if meta_file.exists():
                with open(meta_file) as f:
                    metas.append(EnvMeta.from_dict(json.load(f)))
        return metas

    def get_meta(self, env_name: str) -> EnvMeta:
        meta_file = self._meta_file(env_name)
        if not meta_file.exists():
            raise ValueError(f"Environment '{env_name}' does not exist.")
        with open(meta_file) as f:
            return EnvMeta.from_dict(json.load(f))

    def _save_meta(self, meta: EnvMeta) -> None:
        with open(self._meta_file(meta.name), "w") as f:
            json.dump(meta.to_dict(), f, indent=2)

    def track_asset(self, env_name: str, physical_address: str) -> None:
        """Record a physical asset address as created by this environment."""
        meta = self.get_meta(env_name)
        if physical_address not in meta.tracked_assets:
            meta.tracked_assets.append(physical_address)
            self._save_meta(meta)

    def delete(self, env_name: str, drop_assets_fn=None) -> None:
        """Delete an environment.

        drop_assets_fn: optional callable(list[str]) to drop tracked physical assets
            from the remote before removing local state.
        """
        import shutil

        if not self._env_dir(env_name).exists():
            raise ValueError(f"Environment '{env_name}' does not exist.")

        # Drop remote assets if requested
        if drop_assets_fn is not None:
            try:
                meta = self.get_meta(env_name)
                if meta.tracked_assets:
                    drop_assets_fn(meta.tracked_assets)
            except Exception:
                pass

        # Deactivate if this was the active env
        if self.get_active() == env_name:
            self.deactivate()

        shutil.rmtree(self._env_dir(env_name))

    # ── State store read/write ───────────────────────────────────────────────

    def load_state(self, env_name: str) -> dict[str, DatasourceWatermark]:
        """Load persisted watermarks for an environment."""
        state_file = self._state_file(env_name)
        if not state_file.exists():
            return {}
        with open(state_file) as f:
            try:
                raw = json.load(f)
            except json.JSONDecodeError:
                return {}
        return _deserialize_watermarks(raw)

    def save_state(
        self,
        env_name: str,
        watermarks: dict[str, DatasourceWatermark],
    ) -> None:
        """Persist watermarks for an environment (merges with existing state)."""
        env_path = self._env_dir(env_name)
        env_path.mkdir(parents=True, exist_ok=True)
        existing: dict = {}
        state_file = self._state_file(env_name)
        if state_file.exists():
            with open(state_file) as f:
                try:
                    existing = json.load(f)
                except json.JSONDecodeError:
                    existing = {}
        existing.update(_serialize_watermarks(watermarks))
        with open(state_file, "w") as f:
            json.dump(existing, f, indent=2, default=str)

    def load_main_state(self) -> dict[str, DatasourceWatermark]:
        """Load the main (no-env) state store."""
        main_file = self._main_state_file()
        if not main_file.exists():
            return {}
        with open(main_file) as f:
            try:
                raw = json.load(f)
            except json.JSONDecodeError:
                return {}
        return _deserialize_watermarks(raw)

    def save_main_state(
        self,
        watermarks: dict[str, DatasourceWatermark],
    ) -> None:
        """Persist watermarks to the main (no-env) state store."""
        self._project_dir.mkdir(parents=True, exist_ok=True)
        existing: dict = {}
        main_file = self._main_state_file()
        if main_file.exists():
            with open(main_file) as f:
                try:
                    existing = json.load(f)
                except json.JSONDecodeError:
                    existing = {}
        existing.update(_serialize_watermarks(watermarks))
        with open(main_file, "w") as f:
            json.dump(existing, f, indent=2, default=str)
