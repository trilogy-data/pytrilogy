"""Deployment environments: namespaced physical builds with rename-based cutover.

A deployment environment (distinct from the semantic-model
:class:`~trilogy.core.models.environment.Environment`) prefixes every managed
(non-root) physical table with ``{env}_``, so a full parallel build of a project
can coexist with production, be verified, and then be promoted atomically by
rename (``trilogy env publish``). Root datasources are never rewritten — an
environment shares its sources of truth with production.

The rewrite is installed as an ``Executor.datasource_transform`` and runs after
parsing but before statement processing, so generated SQL (including persists)
targets the prefixed addresses. ``Address.env_label`` marks an address as
already rewritten, making the transform idempotent across repeated parses on
one executor (startup scripts, multi-call flows).

The ambient activation follows the same pattern as
``state_store.state_store_factory`` — a plain module global with a context
manager, shared across refresh worker threads.
"""

import json
import posixpath
import re
import shutil
import threading
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path

from trilogy.core.models.datasource import Address, AddressType, Datasource
from trilogy.core.models.environment import Environment

DEFAULT_ENV_HOME = Path.home() / ".trilogy"

#: Address types the SQL rename-based publish cutover can promote; local
#: file-backed assets cut over via filesystem rename instead.
RENAMEABLE_TYPES = {AddressType.TABLE}

#: Kind tags for registry tracked-asset entries.
TRACKED_TABLE_PREFIX = "table:"
TRACKED_FILE_PREFIX = "file:"

#: Environment names become unquoted SQL identifier prefixes (probes, renames,
#: drops), so they must be identifier-safe on every dialect. A hyphen would
#: build fine (generated DDL quotes) and then break every raw publish
#: statement — reject it up front.
_VALID_ENV_NAME = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def validate_env_name(env_name: str) -> None:
    if not _VALID_ENV_NAME.match(env_name):
        raise ValueError(
            f"Invalid environment name '{env_name}': use letters, digits, and "
            f"underscores only (e.g. '{env_name.replace('-', '_')}')."
        )


def _prefix_table(location: str, env_name: str) -> str:
    """``schema.table`` -> ``schema.{env}_table``; bare ``table`` -> ``{env}_table``."""
    parts = location.split(".")
    parts[-1] = f"{env_name}_{parts[-1]}"
    return ".".join(parts)


def _suffix_file(location: str, env_name: str) -> str:
    """``dir/name.parquet`` -> ``dir/name_{env}.parquet`` (scheme-preserving)."""
    scheme = ""
    rest = location
    if "://" in location:
        scheme_end = location.index("://") + 3
        scheme, rest = location[:scheme_end], location[scheme_end:]
    rest = rest.replace("\\", "/")
    parent = posixpath.dirname(rest)
    filename = posixpath.basename(rest)
    if not filename:
        return location
    stem, dot, ext = filename.rpartition(".")
    new_filename = f"{stem}_{env_name}{dot}{ext}" if stem else f"{filename}_{env_name}"
    new_rest = posixpath.join(parent, new_filename) if parent else new_filename
    return f"{scheme}{new_rest}"


def apply_env_prefix(location: str, env_name: str, is_file: bool = False) -> str:
    """The environment-namespaced version of a physical address."""
    if is_file:
        return _suffix_file(location, env_name)
    return _prefix_table(location, env_name)


def strip_env_prefix(location: str, env_name: str, is_file: bool = False) -> str:
    """Inverse of :func:`apply_env_prefix`, for an address whose ``env_label``
    records that the transform ran. Kept adjacent to the forward transform so
    the two cannot diverge. Returns the location unchanged when the expected
    prefix/suffix is absent (e.g. a file address the transform left alone)."""
    if not is_file:
        parts = location.split(".")
        prefix = f"{env_name}_"
        parts[-1] = parts[-1].removeprefix(prefix)
        return ".".join(parts)
    scheme = ""
    rest = location
    if "://" in location:
        scheme_end = location.index("://") + 3
        scheme, rest = location[:scheme_end], location[scheme_end:]
    rest = rest.replace("\\", "/")
    parent = posixpath.dirname(rest)
    filename = posixpath.basename(rest)
    stem, dot, ext = filename.rpartition(".")
    suffix = f"_{env_name}"
    if stem:
        if not stem.endswith(suffix):
            return location
        new_filename = f"{stem[: -len(suffix)]}{dot}{ext}"
    else:
        if not filename.endswith(suffix):
            return location
        new_filename = filename[: -len(suffix)]
    new_rest = posixpath.join(parent, new_filename) if parent else new_filename
    return f"{scheme}{new_rest}"


def env_backup_address(location: str) -> str:
    """Holding address for the current production asset during a publish."""
    return f"{location}__pub_backup"


def rewritable_address(ds: Datasource) -> Address | None:
    """The Address the environment transform namespaces, or None if exempt.

    The single source of the skip rules: roots, query addresses, and script
    addresses are never rewritten. Publish MUST classify through this same
    predicate — anything it skips was never prefixed by a build, so publish
    skipping anything else (or less) silently diverges from what exists.
    """
    if ds.is_root:
        return None
    address = ds.address
    if isinstance(address, str):
        address = Address(location=address)
        ds.address = address
    if address.is_query or address.type in (AddressType.PYTHON_SCRIPT, AddressType.SQL):
        return None
    return address


def is_remote_location(location: str) -> bool:
    return "://" in location


def transform_datasource(ds: Datasource, env_name: str) -> Address | None:
    """Rewrite one datasource's physical address into the environment namespace.

    Returns the rewritten Address when a rewrite happened, else None.
    ``env_label`` guards against double application.
    """
    address = rewritable_address(ds)
    if address is None or address.env_label is not None:
        return None
    is_file = address.is_file
    address.location = apply_env_prefix(address.location, env_name, is_file=is_file)
    if address.write_location:
        address.write_location = apply_env_prefix(
            address.write_location, env_name, is_file=is_file
        )
    if address.additional_locations:
        address.additional_locations = [
            apply_env_prefix(loc, env_name, is_file=is_file)
            for loc in address.additional_locations
        ]
    address.env_label = env_name
    return address


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


class EnvironmentManager:
    """Registry of a project's deployment environments.

    On-disk layout: ``<home>/<project>/envs/<name>/meta.json`` plus an
    ``<home>/<project>/active`` file naming the active environment.
    """

    # Meta writes are read-merge-write; parallel refresh threads share one
    # process, so a process lock is sufficient.
    _lock = threading.Lock()

    def __init__(self, project_name: str, home: Path | None = None) -> None:
        self.project_name = project_name
        self.home = home or DEFAULT_ENV_HOME
        self.project_dir = self.home / project_name

    def _env_dir(self, env_name: str) -> Path:
        return self.project_dir / "envs" / env_name

    def _meta_file(self, env_name: str) -> Path:
        return self._env_dir(env_name) / "meta.json"

    def _active_file(self) -> Path:
        return self.project_dir / "active"

    def exists(self, env_name: str) -> bool:
        return self._meta_file(env_name).exists()

    def create(self, env_name: str) -> EnvMeta:
        if self.exists(env_name):
            raise ValueError(f"Environment '{env_name}' already exists.")
        return self._write_new(env_name)

    def ensure(self, env_name: str) -> EnvMeta:
        """Fetch the environment, registering it first if unknown."""
        if self.exists(env_name):
            return self.get_meta(env_name)
        return self._write_new(env_name)

    def _write_new(self, env_name: str) -> EnvMeta:
        validate_env_name(env_name)
        self._env_dir(env_name).mkdir(parents=True, exist_ok=True)
        meta = EnvMeta(name=env_name, created_at=datetime.now(timezone.utc).isoformat())
        self._save_meta(meta)
        return meta

    def get_meta(self, env_name: str) -> EnvMeta:
        meta_file = self._meta_file(env_name)
        if not meta_file.exists():
            raise ValueError(f"Environment '{env_name}' does not exist.")
        return EnvMeta.from_dict(json.loads(meta_file.read_text(encoding="utf-8")))

    def _save_meta(self, meta: EnvMeta) -> None:
        self._meta_file(meta.name).write_text(
            json.dumps(meta.to_dict(), indent=2), encoding="utf-8"
        )

    def list_envs(self) -> list[EnvMeta]:
        envs_dir = self.project_dir / "envs"
        if not envs_dir.exists():
            return []
        return [
            self.get_meta(p.name)
            for p in sorted(envs_dir.iterdir())
            if (p / "meta.json").exists()
        ]

    def activate(self, env_name: str) -> None:
        if not self.exists(env_name):
            raise ValueError(
                f"Environment '{env_name}' does not exist. "
                f"Run 'trilogy env create {env_name}' first."
            )
        self.project_dir.mkdir(parents=True, exist_ok=True)
        self._active_file().write_text(env_name, encoding="utf-8")

    def deactivate(self) -> None:
        active = self._active_file()
        if active.exists():
            active.unlink()

    def get_active(self) -> str | None:
        active = self._active_file()
        if not active.exists():
            return None
        name = active.read_text(encoding="utf-8").strip()
        return name or None

    def _fingerprint_file(self, env_name: str) -> Path:
        return self._env_dir(env_name) / "fingerprint.json"

    def save_fingerprint(self, env_name: str, data: dict) -> None:
        """Persist the model fingerprint the environment was last built with.

        Stored as a plain dict; (de)serialization to typed models lives in
        ``trilogy.execution.model_fingerprint``.
        """
        with self._lock:
            self.ensure(env_name)
            self._fingerprint_file(env_name).write_text(
                json.dumps(data, indent=2), encoding="utf-8"
            )

    def load_fingerprint(self, env_name: str) -> dict | None:
        fingerprint_file = self._fingerprint_file(env_name)
        if not fingerprint_file.exists():
            return None
        return json.loads(fingerprint_file.read_text(encoding="utf-8"))

    def track_assets(self, env_name: str, addresses: list[str]) -> None:
        """Union physical addresses into the environment's tracked-asset list."""
        if not addresses:
            return
        with self._lock:
            meta = self.ensure(env_name)
            new = [a for a in addresses if a not in meta.tracked_assets]
            if new:
                meta.tracked_assets.extend(new)
                self._save_meta(meta)

    def clear_tracked_assets(self, env_name: str) -> None:
        with self._lock:
            meta = self.get_meta(env_name)
            meta.tracked_assets = []
            self._save_meta(meta)

    def delete(self, env_name: str) -> None:
        """Remove the environment's registration (not its warehouse assets)."""
        if not self._env_dir(env_name).exists():
            raise ValueError(f"Environment '{env_name}' does not exist.")
        if self.get_active() == env_name:
            self.deactivate()
        shutil.rmtree(self._env_dir(env_name))


@dataclass
class EnvActivation:
    """A resolved active environment: the label plus its registry handle.

    Accumulates the physical addresses it rewrites; ``flush_tracked`` persists
    them so ``env delete --drop-assets`` can clean up later.
    """

    name: str
    manager: EnvironmentManager
    _tracked: set[str] = field(default_factory=set)
    _tracked_lock: threading.Lock = field(default_factory=threading.Lock)

    def transform(self, ds: Datasource, working_dir: Path | None = None) -> None:
        address = transform_datasource(ds, self.name)
        if address is not None:
            entry = tracked_entry(address, working_dir)
            with self._tracked_lock:
                self._tracked.add(entry)

    def transform_for(self, working_dir: Path) -> Callable[[Datasource], None]:
        """The transform bound to a script directory, so file assets track as
        absolute paths (file addresses are script-relative; tables are not)."""

        def bound(ds: Datasource) -> None:
            self.transform(ds, working_dir=working_dir)

        return bound

    def flush_tracked(self) -> None:
        with self._tracked_lock:
            tracked = sorted(self._tracked)
        self.manager.track_assets(self.name, tracked)


def tracked_entry(address: Address, working_dir: Path | None) -> str:
    """Registry entry for a rewritten address: ``file:<abs path>`` or
    ``table:<location>``. The kind travels with the entry because delete-time
    cleanup drops tables via SQL but files via unlink."""
    if address.is_file:
        location = address.write_location or address.location
        path = Path(location)
        if not path.is_absolute() and not is_remote_location(location):
            path = (working_dir or Path(".")) / path
        return f"{TRACKED_FILE_PREFIX}{path.resolve()}"
    return f"{TRACKED_TABLE_PREFIX}{address.location}"


def parse_tracked_entry(entry: str) -> tuple[str, str]:
    """(kind, address) for a registry entry; bare entries default to table."""
    if entry.startswith(TRACKED_FILE_PREFIX):
        return "file", entry[len(TRACKED_FILE_PREFIX) :]
    if entry.startswith(TRACKED_TABLE_PREFIX):
        return "table", entry[len(TRACKED_TABLE_PREFIX) :]
    return "table", entry


_ACTIVE_ACTIVATION: EnvActivation | None = None


def active_env() -> EnvActivation | None:
    return _ACTIVE_ACTIVATION


def datasource_transform_from_active(
    working_dir: Path,
) -> Callable[[Datasource], None] | None:
    """The transform new executors should install, or None outside a scope."""
    activation = _ACTIVE_ACTIVATION
    return activation.transform_for(working_dir) if activation else None


@contextmanager
def env_activation_scope(activation: EnvActivation | None) -> Iterator[None]:
    """Install an activation for the duration of a CLI command.

    Tracked assets are flushed to the registry on exit, success or failure —
    a failed run may still have created tables that need cleanup.
    """
    global _ACTIVE_ACTIVATION
    if activation is None:
        yield
        return
    previous = _ACTIVE_ACTIVATION
    _ACTIVE_ACTIVATION = activation
    try:
        yield
    finally:
        _ACTIVE_ACTIVATION = previous
        activation.flush_tracked()


def apply_env_to_environment(
    environment: Environment, activation: EnvActivation
) -> None:
    """Rewrite every datasource of an already-parsed model environment.

    Used where parsing bypasses the Executor (the directory probe's
    lightweight phase-1 parse)."""
    working_dir = Path(environment.working_path)
    for ds in environment.datasources.values():
        activation.transform(ds, working_dir=working_dir)
