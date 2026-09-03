"""Locating a project's ``trilogy.toml``.

Deliberately stdlib-only. ``common`` pulls in the executor and dialect stack —
roughly a quarter-second — which commands that only need to find the config
file (``trilogy cloud``) should not pay for. ``common`` re-exports both names,
so its importers are unaffected.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import tomllib

TRILOGY_CONFIG_NAME = "trilogy.toml"

#: When a declared dependent runs, given what its `after` scripts did.
#: `completed` is the derived-edge rule (run after they succeed, skip if one
#: failed); `failed` runs only when at least one of them failed — a repair
#: script; `always` runs once they are all finished, whatever happened.
DEPENDENCY_CONDITIONS = ("completed", "failed", "always")
DEFAULT_DEPENDENCY_CONDITION = "completed"

# Directory holding root datasource definitions, relative to the project root.
# `init` scaffolds it and `ingest` writes generated models into it by default —
# one constant so the two cannot drift apart.
MODEL_ROOT_DIR = "root"


def find_trilogy_config(start_path: Path | None = None) -> Path | None:
    """The nearest ``trilogy.toml`` at or above *start_path* (default: cwd).

    A file argument searches from its directory, so callers can pass the script
    they are about to run rather than having to strip the name first.
    """
    search_path = start_path if start_path else Path.cwd()
    if not search_path.is_dir():
        search_path = search_path.parent

    for parent in [search_path] + list(search_path.parents):
        candidate = parent / TRILOGY_CONFIG_NAME
        if candidate.exists():
            return candidate
    return None


@dataclass(frozen=True)
class DeclaredDependency:
    """One `[dependencies]` entry: `script` runs after every path in `after`,
    on the condition `when`. Paths are absolute, resolved from the config."""

    script: Path
    after: tuple[Path, ...]
    when: str = DEFAULT_DEPENDENCY_CONDITION


def load_declared_dependencies(config_path: Path) -> list[DeclaredDependency]:
    """The `[dependencies]` table of a trilogy.toml.

    Edges between scripts are normally derived from what they import, declare
    and persist; this is the one place to declare an edge the content cannot
    express — chiefly a script that must run *because* another failed::

        [dependencies]
        "repair.preql" = { after = ["refresh.preql"], when = "failed" }

    Keys and `after` entries are paths relative to the config's directory.
    Consumers (the local directory runner, and the platform ordering a
    schedule tick) share this reader so they cannot disagree about the shape.
    """
    parsed = tomllib.loads(config_path.read_text(encoding="utf-8"))
    section = parsed.get("dependencies")
    if section is None:
        return []
    if not isinstance(section, dict):
        raise TypeError("[dependencies] must be a table of script -> settings")
    base = config_path.parent
    declared: list[DeclaredDependency] = []
    for script, spec in section.items():
        if not isinstance(spec, dict):
            raise TypeError(
                f"[dependencies] {script!r} must be a table like "
                '{ after = [...], when = "failed" }'
            )
        unknown = set(spec) - {"after", "when"}
        if unknown:
            raise ValueError(
                f"[dependencies] {script!r} has unknown keys: {sorted(unknown)}"
            )
        after = spec.get("after")
        if isinstance(after, str):
            after = [after]
        if (
            not isinstance(after, list)
            or not after
            or not all(isinstance(a, str) for a in after)
        ):
            raise ValueError(
                f"[dependencies] {script!r} needs `after`: a script path or a list of them"
            )
        when = spec.get("when", DEFAULT_DEPENDENCY_CONDITION)
        if when not in DEPENDENCY_CONDITIONS:
            raise ValueError(
                f"[dependencies] {script!r}: `when` must be one of "
                f"{', '.join(DEPENDENCY_CONDITIONS)}, not {when!r}"
            )
        declared.append(
            DeclaredDependency(
                script=(base / script).resolve(),
                after=tuple((base / a).resolve() for a in after),
                when=when,
            )
        )
    return declared
