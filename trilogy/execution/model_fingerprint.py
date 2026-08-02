"""Per-deployment-environment model fingerprints.

Records what code an environment's tables were last built with, so
``trilogy env diff`` can answer "are these environments different, and which
objects changed" without touching a warehouse.

A project fingerprint is per-script: concept namespaces are deliberately
never reconciled across scripts (see ``execution/state/AGENTS.md``), so each
script's parsed Environment fingerprints independently, keyed by its
project-relative path — the same anchor the state contract uses for asset
keys. Datasource identity across scripts is physical (the address), also
mirroring the state contract.

Scripts are parsed lightweight (no executor, engine, or startup scripts) —
fingerprinting never needs a database. Fingerprints are env-invariant (see
``trilogy.core.fingerprint``), so a transformed executor environment and an
unscoped parse of the same code produce the same manifest.
"""

from pathlib import Path
from typing import Any

from pydantic import BaseModel, Field

from trilogy.core.fingerprint import (
    FINGERPRINT_VERSION,
    ChangeKind,
    EnvironmentFingerprint,
    FingerprintDiff,
    FingerprintError,
    _h,
    build_environment_fingerprint,
    diff_fingerprints,
)
from trilogy.core.models.environment import Environment
from trilogy.execution.envs import EnvironmentManager


class ProjectFingerprint(BaseModel):
    fingerprint_version: int = FINGERPRINT_VERSION
    scripts: dict[str, EnvironmentFingerprint] = Field(default_factory=dict)

    @property
    def root(self) -> str:
        return _h(
            "project",
            *sorted(f"{path}={fp.root}" for path, fp in self.scripts.items()),
        )


class InvalidatedDatasource(BaseModel):
    datasource_id: str
    script: str
    location: str | None = None
    kind: ChangeKind


class ProjectFingerprintDiff(BaseModel):
    identical: bool
    added_scripts: list[str] = Field(default_factory=list)
    removed_scripts: list[str] = Field(default_factory=list)
    # scripts whose fingerprints differ, with the per-object diff
    changed_scripts: dict[str, FingerprintDiff] = Field(default_factory=dict)
    # datasources across all changed scripts whose contents are stale on
    # rebuild (effective hash changed); refactor-only changes are excluded
    invalidated_datasources: list[InvalidatedDatasource] = Field(default_factory=list)

    @property
    def invalidated_locations(self) -> list[str]:
        """Unique physical addresses needing rebuild, across all scripts."""
        return sorted({d.location for d in self.invalidated_datasources if d.location})


def script_key(path: Path, project_root: Path) -> str:
    """Project-relative POSIX key for a script, absolute when outside the root
    (mirrors the state contract's asset-key anchoring)."""
    resolved = path.resolve()
    try:
        return resolved.relative_to(project_root.resolve()).as_posix()
    except ValueError:
        return resolved.as_posix()


def fingerprint_script(
    path: Path, env_params: dict[str, Any] | None = None
) -> EnvironmentFingerprint:
    from trilogy.core.statements.author import PersistStatement
    from trilogy.parsing.parse_engine_v2 import parse_text as lightweight_parse

    environment = Environment(working_path=str(path.parent))
    if env_params:
        environment.set_parameters(**env_params)
    text = path.read_text(encoding="utf-8")
    parsed, statements = lightweight_parse(text, environment=environment, root=path)
    # Persist targets are not registered in environment.datasources at parse
    # time (same reason the env transform walks PersistStatements separately);
    # their select decides the table's contents, so it rides along as context.
    persists: list[tuple[Any, Any]] = []
    for statement in statements:
        if isinstance(statement, PersistStatement):
            persists.append(
                (
                    statement.datasource,
                    (
                        statement.select.as_lineage(parsed),
                        statement.persist_mode,
                        statement.partition_by,
                    ),
                )
            )
    return build_environment_fingerprint(parsed, extra_datasources=persists)


def build_project_fingerprint(
    files: list[Path],
    project_root: Path,
    env_params: dict[str, Any] | None = None,
) -> ProjectFingerprint:
    scripts = {
        script_key(f, project_root): fingerprint_script(f, env_params) for f in files
    }
    return ProjectFingerprint(scripts=scripts)


def diff_project_fingerprints(
    base: ProjectFingerprint, other: ProjectFingerprint
) -> ProjectFingerprintDiff:
    if base.fingerprint_version != other.fingerprint_version:
        raise FingerprintError(
            f"Cannot diff fingerprints with different versions: "
            f"{base.fingerprint_version} vs {other.fingerprint_version}; "
            "re-record with 'trilogy env fingerprint'"
        )
    added = sorted(set(other.scripts) - set(base.scripts))
    removed = sorted(set(base.scripts) - set(other.scripts))
    changed: dict[str, FingerprintDiff] = {}
    invalidated: list[InvalidatedDatasource] = []
    for key in sorted(set(base.scripts) & set(other.scripts)):
        diff = diff_fingerprints(base.scripts[key], other.scripts[key])
        if diff.identical:
            continue
        changed[key] = diff
        for ds_key in diff.invalidated_datasources:
            entry = other.scripts[key].datasources[ds_key]
            invalidated.append(
                InvalidatedDatasource(
                    datasource_id=ds_key,
                    script=key,
                    location=entry.location,
                    kind=diff.datasources.changed[ds_key],
                )
            )
    return ProjectFingerprintDiff(
        identical=not (added or removed or changed),
        added_scripts=added,
        removed_scripts=removed,
        changed_scripts=changed,
        invalidated_datasources=invalidated,
    )


def fingerprint_baseline(fingerprint: ProjectFingerprint) -> dict[str, str]:
    """Logical location -> effective hash across all scripts, the shape the
    state store's model-staleness check consumes."""
    baseline: dict[str, str] = {}
    for script_fp in fingerprint.scripts.values():
        for entry in script_fp.datasources.values():
            if entry.location:
                baseline[entry.location] = entry.effective
    return baseline


def load_project_fingerprint(
    manager: EnvironmentManager, env_name: str
) -> ProjectFingerprint | None:
    """The environment's recorded fingerprint, or None when never recorded.

    Raises FingerprintError when a record exists but was written by an
    incompatible fingerprint algorithm version.
    """
    data = manager.load_fingerprint(env_name)
    if data is None:
        return None
    if data.get("fingerprint_version") != FINGERPRINT_VERSION:
        raise FingerprintError(
            f"Environment '{env_name}' has a fingerprint from an incompatible "
            f"version ({data.get('fingerprint_version')}); re-record with "
            "'trilogy env fingerprint' or a run/refresh under the environment"
        )
    return ProjectFingerprint.model_validate(data)


def update_project_fingerprint(
    manager: EnvironmentManager, env_name: str, updates: ProjectFingerprint
) -> ProjectFingerprint:
    """Merge per-script entries into the environment's recorded fingerprint.

    Only the scripts in ``updates`` are replaced — a run that touched one
    script must not claim the whole project was built with current code. An
    incompatible-version record is discarded wholesale rather than merged.
    """
    try:
        existing = load_project_fingerprint(manager, env_name)
    except FingerprintError:
        existing = None
    scripts = dict(existing.scripts) if existing else {}
    scripts.update(updates.scripts)
    merged = ProjectFingerprint(scripts=scripts)
    manager.save_fingerprint(env_name, merged.model_dump())
    return merged
