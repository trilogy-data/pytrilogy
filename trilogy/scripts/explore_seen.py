"""Session-scoped dedup of repeated ``explore`` JSON payload entries.

An agent conversation explores several models whose imported dimension trees
overlap heavily (every TPC-DS fact chains in customer/item/date/...), and often
re-explores the same file. Each repeat re-transmits schemas the conversation
already holds — and every transmitted char is re-billed on all later turns.

When ``TRILOGY_EXPLORE_SESSION`` is set (the agent loop sets one id per
conversation), entries whose rendered content is byte-identical to something
already emitted this session collapse to an ``{"already_shown": <path>}`` stub.
Safety is by construction: the hash covers the exact rendered entry, so any
change to the source model — or a different regex filter, role set, or
canonical prefix — renders differently, hashes differently, and prints in
full. Only true repeats are suppressed, and ``--reshow`` reprints regardless.

``TRILOGY_EXPLORE_RECORD_LIMIT`` guards the other direction: the agent wrapper
truncates oversized tool output, so it passes its cap down and a payload bigger
than the cap is never *recorded* as seen — content the agent never received
cannot be suppressed later. Store I/O is best-effort: any failure disables
dedup for the call rather than breaking explore.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import tempfile
from pathlib import Path

SESSION_ENV = "TRILOGY_EXPLORE_SESSION"
RECORD_LIMIT_ENV = "TRILOGY_EXPLORE_RECORD_LIMIT"

_NOTE = (
    "already_shown entries are byte-identical to an earlier explore output in "
    "this session (first shown exploring the named file); pass --reshow to "
    "reprint them in full."
)


def active_session() -> str | None:
    session = os.environ.get(SESSION_ENV, "").strip()
    return session or None


def record_limit() -> int | None:
    raw = os.environ.get(RECORD_LIMIT_ENV, "").strip()
    if not raw:
        return None
    try:
        return int(raw)
    except ValueError:
        return None


def _store_path(session: str) -> Path:
    safe = re.sub(r"[^A-Za-z0-9_.-]", "_", session)[:64]
    return Path(tempfile.gettempdir()) / "trilogy_explore_seen" / f"{safe}.json"


def _load(session: str) -> dict[str, str]:
    try:
        raw = json.loads(_store_path(session).read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return {}
    if not isinstance(raw, dict):
        return {}
    return {k: v for k, v in raw.items() if isinstance(k, str) and isinstance(v, str)}


def _save(session: str, store: dict[str, str]) -> None:
    path = _store_path(session)
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_suffix(f".{os.getpid()}.tmp")
        tmp.write_text(json.dumps(store), encoding="utf-8")
        tmp.replace(path)
    except OSError:
        pass


def _entry_hash(section: str, key: str, entry: object) -> str:
    canon = json.dumps([section, key, entry], sort_keys=True, ensure_ascii=False)
    return hashlib.sha256(canon.encode("utf-8")).hexdigest()[:20]


def apply_seen_dedup(
    payload: dict, explored: str, session: str, reshow: bool = False
) -> dict:
    """Collapse already-seen entries of a concepts payload and record the new
    ones. Suppression replaces an entry's body with a pointer at the file
    whose explore first showed it; the entry key (which carries the namespace
    and role names) stays visible. Recording is skipped when the deduped
    payload would still exceed the agent wrapper's truncation cap — a
    truncated payload was never fully seen."""
    store = _load(session)
    fresh: dict[str, str] = {}
    suppressed = False
    out = dict(payload)
    for section in ("namespaces", "namespaced"):
        entries = payload.get(section)
        if not isinstance(entries, dict):
            continue
        section_out: dict = {}
        for key, entry in entries.items():
            digest = _entry_hash(section, key, entry)
            first = store.get(digest)
            if first is not None and not reshow:
                section_out[key] = {"already_shown": first}
                suppressed = True
            else:
                section_out[key] = entry
                if first is None:
                    fresh[digest] = explored
        out[section] = section_out
    if suppressed:
        out["already_shown_note"] = _NOTE
    if fresh:
        limit = record_limit()
        if limit is None or len(json.dumps(out, ensure_ascii=False)) <= limit:
            store.update(fresh)
            _save(session, store)
    return out
