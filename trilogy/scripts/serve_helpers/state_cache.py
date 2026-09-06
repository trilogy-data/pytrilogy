"""On-disk cache for the ``/state`` endpoint.

``GET /state`` recomputes from scratch on every call: it re-parses the target,
builds an executor (running any ``[setup]`` scripts), and re-probes the
warehouse. That is seconds per request, and on a billed warehouse it is money
per request — which is why a consumer cannot show state passively (sidebar
badges, a staleness column, load-on-select) without a cache somewhere. The
server is the only place that can hold one correctly, because it is the only
party that knows when a refresh ran.

**The cached value is a `StateSnapshot`** — the same interchange format the
endpoint returns and ``trilogy state -o`` writes. Serve deliberately has no
state shape of its own (see ``execution/state/AGENTS.md``), so only the
bookkeeping needed to judge an entry's validity lives beside it, in a sidecar
file. That also makes the cache directory directly readable by anything that
already speaks snapshots, including ``--state-input``.

Validity is a **fingerprint, not a TTL**: an entry is reused until something the
server can actually observe changes — an edit to a model file, or a completed
run/refresh job. What no server-side cache can observe is a table loaded outside
trilogy, so a cached response stays self-describing (``snapshot_ts`` says how
old the observation is) and the client keeps a force path (``?refresh=true``).
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from pathlib import Path

from trilogy.constants import logger
from trilogy.execution.staged_write import write_text_staged
from trilogy.execution.state.persistence import read_state_snapshot
from trilogy.execution.state.snapshot import StateSnapshot
from trilogy.scripts.serve_helpers.file_discovery import find_all_model_files
from trilogy.utility import utc_now_iso

#: Relative to the served directory. Kept inside the project so restarting the
#: server in the same place reloads it, and so a human can inspect or delete it.
CACHE_SUBPATH = Path(".trilogy") / "state"

LOGGER_PREFIX = "[STATE CACHE]"

_KEY_CHARS = 16


@dataclass
class CachedSnapshot:
    snapshot: StateSnapshot
    #: When the underlying probe actually ran — distinct from when the response
    #: was assembled, which is what a client would otherwise infer.
    computed_at: str


def fingerprint_directory(directory: Path) -> str:
    """A digest of every model file's size and mtime under ``directory``.

    Stat rather than content: this recomputes on every cache read, and re-probing
    a warehouse because an editor rewrote identical bytes is the expensive
    mistake, not the cheap one. Scoped to the whole served directory rather than
    the target's own imports because a target's state depends on whatever it
    imports, and resolving that here would mean parsing — which is part of the
    cost being avoided.
    """
    parts: list[str] = []
    for path in sorted(find_all_model_files(directory)):
        try:
            stat = path.stat()
        except OSError:
            # Raced with a delete: leaving it out is itself a fingerprint change.
            continue
        parts.append(
            f"{path.relative_to(directory).as_posix()}:{stat.st_size}:{stat.st_mtime_ns}"
        )
    return hashlib.sha256("\n".join(parts).encode("utf-8")).hexdigest()


class StateSnapshotCache:
    """Snapshot cache rooted at ``<served directory>/.trilogy/state``.

    Every failure mode degrades to "no cache" rather than to an error: an
    unwritable project directory, a corrupt entry, or a snapshot written by an
    incompatible version all read as a miss. A state endpoint that 500s because
    its cache is broken would be strictly worse than the uncached one it
    replaced.
    """

    def __init__(self, directory: Path) -> None:
        self.directory = directory
        self.root = directory / CACHE_SUBPATH

    def _paths(self, target: str) -> tuple[Path, Path]:
        key = hashlib.sha256(target.encode("utf-8")).hexdigest()[:_KEY_CHARS]
        return (
            self.root / f"{key}.snapshot.json",
            self.root / f"{key}.meta.json",
        )

    def get(self, target: str, fingerprint: str) -> CachedSnapshot | None:
        """The stored snapshot for ``target``, or None if absent or stale."""
        snapshot_path, meta_path = self._paths(target)
        try:
            meta = json.loads(meta_path.read_text(encoding="utf-8"))
            if meta.get("fingerprint") != fingerprint:
                return None
            snapshot = StateSnapshot.model_validate_json(
                snapshot_path.read_text(encoding="utf-8")
            )
        except (OSError, ValueError) as e:
            logger.debug("%s miss for %r: %s", LOGGER_PREFIX, target, e)
            return None
        return CachedSnapshot(
            snapshot=snapshot, computed_at=str(meta.get("computed_at", ""))
        )

    def put(
        self,
        target: str,
        snapshot: StateSnapshot,
        fingerprint: str,
        computed_at: str,
    ) -> None:
        snapshot_path, meta_path = self._paths(target)
        try:
            self.root.mkdir(parents=True, exist_ok=True)
            # Snapshot first: the meta file is what makes an entry readable, so
            # writing it last means a crash between the two reads as a miss
            # rather than as a stale hit.
            write_text_staged(snapshot_path, snapshot.model_dump_json())
            write_text_staged(
                meta_path,
                json.dumps(
                    {
                        "target": target,
                        "fingerprint": fingerprint,
                        "computed_at": computed_at,
                    }
                ),
            )
        except OSError as e:
            logger.warning(
                "%s could not write entry for %r: %s", LOGGER_PREFIX, target, e
            )

    def state_input_path(self, target: str, fingerprint: str) -> Path | None:
        """The stored snapshot file to seed a job with, if it is still valid.

        This is the cache acting as the *state store* rather than as a response
        cache: a run or refresh started from the server reads the same
        observations the server already showed the user, instead of re-probing
        the warehouse for them. Returns None unless a live entry exists, so a
        job never seeds from a snapshot the server itself would not serve.
        """
        if self.get(target, fingerprint) is None:
            return None
        return self._paths(target)[0]

    def adopt(self, target: str, written: Path, fingerprint: str) -> bool:
        """Take a snapshot a finished job wrote as the entry for ``target``.

        This closes the loop: a job's own ``--state-file`` output *is* a fresh
        probe of the assets it just rewrote, so the next ``/state`` is served
        from it rather than paying for an identical probe. Its ``target`` field
        is renormalized to the cache key because the subprocess records the path
        it was invoked with, which is absolute.
        """
        try:
            snapshot = read_state_snapshot(written)
        except (OSError, ValueError) as e:
            logger.warning("%s could not adopt %s: %s", LOGGER_PREFIX, written, e)
            return False
        self.put(
            target,
            snapshot.model_copy(update={"target": target}),
            fingerprint,
            snapshot.snapshot_ts or utc_now_iso(),
        )
        return True

    def clear(self) -> None:
        """Drop every entry.

        Called when a run or refresh job finishes. Deliberately not narrowed to
        the job's own target: a refresh rewrites assets, a target's state
        depends on every asset upstream of it, and targets overlap (a directory
        contains its files). Anything narrower would have to reason about the
        dependency graph to be correct, and getting that wrong shows a stale
        "fresh" — the one answer this endpoint must never invent.
        """
        try:
            for path in self.root.glob("*.json"):
                path.unlink(missing_ok=True)
        except OSError as e:
            logger.warning("%s could not clear: %s", LOGGER_PREFIX, e)
