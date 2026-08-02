"""Persistent conversation sessions for `trilogy agent`.

A session is an append-only JSONL transcript of the conversation messages plus
a small `.meta.json` sidecar. Listing reads only the sidecars — transcripts run
to megabytes once tool output is in them, and `--list-sessions` must not pay
that cost. Sessions live under a per-project directory keyed by the working
directory, mirroring how other per-user trilogy state lands in ``~/.trilogy``.
"""

from __future__ import annotations

import json
import os
import re
import uuid
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path

from trilogy.ai.models import LLMMessage

SESSION_HOME_ENV = "TRILOGY_AGENT_SESSION_HOME"
FORMAT_VERSION = 1
LATEST_ALIASES = frozenset({"last", "latest"})


class SessionError(Exception):
    """Raised when a requested session cannot be resolved."""


def session_home() -> Path:
    raw = os.environ.get(SESSION_HOME_ENV)
    if raw:
        return Path(raw).expanduser()
    return Path.home() / ".trilogy" / "agent_sessions"


def project_slug(cwd: Path) -> str:
    slug = re.sub(r"[^A-Za-z0-9]+", "-", str(Path(cwd).resolve())).strip("-")
    return slug or "root"


def project_dir(cwd: Path) -> Path:
    return session_home() / project_slug(cwd)


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


@dataclass
class SessionMeta:
    id: str
    created_at: str
    updated_at: str
    cwd: str
    provider: str
    model: str
    toolset: str
    first_command: str
    last_command: str
    turns: int = 0
    message_count: int = 0
    version: int = FORMAT_VERSION


@dataclass
class AgentSession:
    """Append-only writer over one session transcript."""

    path: Path
    meta: SessionMeta
    flushed: int = 0

    @property
    def id(self) -> str:
        return self.meta.id

    @property
    def meta_path(self) -> Path:
        return self.path.with_suffix(".meta.json")

    @classmethod
    def start(
        cls,
        cwd: Path,
        provider: str,
        model: str,
        toolset: str,
        command: str,
    ) -> AgentSession:
        directory = project_dir(cwd)
        directory.mkdir(parents=True, exist_ok=True)
        session_id = uuid.uuid4().hex[:8]
        ts = _now()
        meta = SessionMeta(
            id=session_id,
            created_at=ts,
            updated_at=ts,
            cwd=str(Path(cwd).resolve()),
            provider=provider,
            model=model,
            toolset=toolset,
            first_command=command,
            last_command=command,
            turns=1,
        )
        session = cls(path=directory / f"{session_id}.jsonl", meta=meta)
        session.path.write_text("", encoding="utf-8")
        session._append_record({"type": "command", "ts": ts, "text": command})
        session._write_meta()
        return session

    @classmethod
    def load(cls, path: Path) -> tuple[AgentSession, list[LLMMessage]]:
        meta = read_meta(path.with_suffix(".meta.json"))
        messages = read_messages(path)
        session = cls(path=path, meta=meta, flushed=len(messages))
        return session, messages

    def record_command(self, command: str) -> None:
        ts = _now()
        self.meta.last_command = command
        self.meta.turns += 1
        self.meta.updated_at = ts
        self._append_record({"type": "command", "ts": ts, "text": command})
        self._write_meta()

    def flush(self, messages: list[LLMMessage]) -> None:
        """Append every message not yet written. Safe to call repeatedly."""
        pending = messages[self.flushed :]
        if not pending:
            return
        with self.path.open("a", encoding="utf-8") as handle:
            for msg in pending:
                handle.write(json.dumps(_message_to_record(msg), default=str) + "\n")
        self.flushed = len(messages)
        self.meta.message_count = self.flushed
        self.meta.updated_at = _now()
        self._write_meta()

    def _append_record(self, record: dict) -> None:
        with self.path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(record, default=str) + "\n")

    def _write_meta(self) -> None:
        self.meta_path.write_text(
            json.dumps(asdict(self.meta), indent=2), encoding="utf-8"
        )


def _message_to_record(msg: LLMMessage) -> dict:
    return {
        "type": "message",
        "role": msg.role,
        "content": msg.content,
        "model_info": msg.model_info or {},
        "hidden": msg.hidden,
    }


def _record_to_message(record: dict) -> LLMMessage:
    return LLMMessage(
        role=record["role"],
        content=record.get("content") or "",
        model_info=record.get("model_info") or {},
        hidden=bool(record.get("hidden")),
    )


def read_messages(path: Path) -> list[LLMMessage]:
    if not path.exists():
        raise SessionError(f"Session transcript not found: {path}")
    messages: list[LLMMessage] = []
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if not line:
                continue
            try:
                record = json.loads(line)
            except json.JSONDecodeError as exc:
                raise SessionError(f"Corrupt session transcript {path}: {exc}") from exc
            if record.get("type") == "message":
                messages.append(_record_to_message(record))
    return messages


def read_meta(path: Path) -> SessionMeta:
    if not path.exists():
        raise SessionError(f"Session metadata not found: {path}")
    data = json.loads(path.read_text(encoding="utf-8"))
    known = {f for f in SessionMeta.__dataclass_fields__}
    return SessionMeta(**{k: v for k, v in data.items() if k in known})


def list_sessions(cwd: Path, all_projects: bool = False) -> list[SessionMeta]:
    """Sessions newest-first. Sidecars that fail to parse are skipped rather
    than failing the listing — one bad file should not hide the rest."""
    roots = (
        sorted(p for p in session_home().glob("*") if p.is_dir())
        if all_projects
        else [project_dir(cwd)]
    )
    found: list[SessionMeta] = []
    for root in roots:
        if not root.is_dir():
            continue
        for meta_file in root.glob("*.meta.json"):
            try:
                found.append(read_meta(meta_file))
            except (SessionError, json.JSONDecodeError, TypeError):
                continue
    return sorted(found, key=lambda m: m.updated_at, reverse=True)


def session_path(meta: SessionMeta) -> Path:
    return project_dir(Path(meta.cwd)) / f"{meta.id}.jsonl"


def resolve_session(cwd: Path, ref: str) -> Path:
    """Resolve a `--resume` reference to a transcript path. ``last``/``latest``
    picks the newest session for this project; anything else matches a session
    id exactly or by unique prefix, falling back to a search across projects so
    an id copied from another directory still resolves."""
    ref = ref.strip()
    local = list_sessions(cwd)
    if ref.lower() in LATEST_ALIASES:
        if not local:
            raise SessionError(
                f"No saved agent sessions for {Path(cwd).resolve()}. "
                "Run `trilogy agent` once to start one."
            )
        return session_path(local[0])
    matches = [m for m in local if m.id.startswith(ref)]
    if not matches:
        matches = [
            m for m in list_sessions(cwd, all_projects=True) if m.id.startswith(ref)
        ]
    if not matches:
        raise SessionError(
            f"No agent session matching '{ref}'. "
            "Run `trilogy agent --list-sessions` to see saved sessions."
        )
    if len(matches) > 1:
        ids = ", ".join(m.id for m in matches)
        raise SessionError(f"Session id '{ref}' is ambiguous: {ids}.")
    return session_path(matches[0])
