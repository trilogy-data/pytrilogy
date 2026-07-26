"""Structured execution report: a true-JSONL sink for orchestrators.

This module is the machine-facing execution contract. Unlike the ``--format
json`` stdout stream (pretty-printed, human/agent-facing, display-owned), the
report file is strict JSONL: exactly one JSON object per line, appended as
execution progresses, safe to tail line-by-line.

Envelope — every record carries:

- ``ts``: UTC ISO-8601 timestamp
- ``type``: record type tag (vocabulary below)
- ``schema_version``: integer, currently 1. Bumps ONLY on breaking changes to
  existing fields; new record types and new fields are added without a bump.
- ``run_id``: correlation id. Supplied by the orchestrator via ``--run-id`` /
  ``TRILOGY_RUN_ID``; a uuid4 hex is generated when absent.
- ``seq``: monotonic per-process sequence number (ordering across threads).

Consumers MUST ignore unknown record types and unknown fields.

Record vocabulary (fields with ``None`` values are omitted):

- ``run_start``: command, target, dialect, trilogy_version, parallelism,
  config_path, mode ("single" | "directory")
- ``file_start``: file (script path) — or address + owner_script for managed
  refresh nodes; node_kind ("script" | "managed_address")
- ``statement_end``: file (omitted on the single-file path — implied by the
  sole ``file_start``), index, total, statement_type, duration_s, success,
  error_type, error
- ``file_end``: same attribution as ``file_start``, success, skipped
  (dependency-propagated failure), duration_s, error_type, error, stats
  {persist_count, update_count, validate_count, refresh_query_count}
- ``refresh_plan``: scope (file/dir label), stale_count, forced_count,
  root_assets, all_assets, assets [{datasource_id, address, reason, kind}]
- ``asset_refresh``: datasource_id, address, reason (emitted as each asset's
  refresh begins)
- ``asset_refresh_query``: datasource_id, sql_bytes (length only)
- ``plan_graph``: nodes, edges (dependency graph of a ``plan`` invocation)
- ``state_snapshot``: a full StateSnapshot payload (see
  ``trilogy.execution.state.snapshot``), or {path} when written to a file
- ``error``: error_type, message, file (fatal errors outside the file loop)
- ``summary``: terminal record — success, exit_code, total, succeeded,
  failed, skipped, partial_failure, total_duration_s, refreshed_assets

One report file per invocation: the sink serializes writers within this
process only. Orchestrators must supply a distinct path per run.
"""

from __future__ import annotations

import json
import os
import threading
import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from trilogy.constants import logger

REPORT_SCHEMA_VERSION = 1

ENV_REPORT_FILE = "TRILOGY_REPORT_FILE"
ENV_RUN_ID = "TRILOGY_RUN_ID"


class ReportSink:
    """Append-only JSONL writer. Never raises from ``emit`` — a telemetry
    failure must not break a run (mirrors the on_script_complete contract in
    parallel execution)."""

    def __init__(self, path: Path, run_id: str, command: str) -> None:
        self.path = Path(path)
        self.run_id = run_id
        self.command = command
        self._lock = threading.Lock()
        self._seq = 0
        self._summary_emitted = False

    def emit(self, record_type: str, **fields: Any) -> None:
        try:
            with self._lock:
                self._seq += 1
                record: dict[str, Any] = {
                    "ts": datetime.now(timezone.utc).isoformat(),
                    "type": record_type,
                    "schema_version": REPORT_SCHEMA_VERSION,
                    "run_id": self.run_id,
                    "seq": self._seq,
                }
                for key, value in fields.items():
                    if value is not None:
                        record[key] = value
                if record_type == "summary":
                    self._summary_emitted = True
                # Open-per-write append (the agent --log-file precedent):
                # each record is durable immediately, and there is no handle
                # to leak if the process dies mid-run.
                with self.path.open("a", encoding="utf-8") as f:
                    f.write(json.dumps(record, default=str) + "\n")
        except Exception as e:  # pragma: no cover - defensive
            logger.debug("report sink emit failed: %s", e)

    @property
    def summary_emitted(self) -> bool:
        return self._summary_emitted


# Module-level active sink. A plain global (not a ContextVar) on purpose:
# parallel execution emits from worker threads, and ContextVars set in the
# CLI entrypoint do not propagate into threads started elsewhere. Matches the
# display_core.OUTPUT_FORMAT precedent; the CLI is one invocation per process.
_ACTIVE_SINK: ReportSink | None = None


def set_report_sink(sink: ReportSink | None) -> None:
    global _ACTIVE_SINK
    _ACTIVE_SINK = sink


def get_report_sink() -> ReportSink | None:
    return _ACTIVE_SINK


def emit_report(record_type: str, **fields: Any) -> None:
    """Emit a record to the active sink; no-op when reporting is off."""
    sink = _ACTIVE_SINK
    if sink is not None:
        sink.emit(record_type, **fields)


def resolve_run_id(run_id: str | None) -> str:
    """Flag > TRILOGY_RUN_ID env > generated uuid4 hex."""
    if run_id:
        return run_id
    env_value = os.environ.get(ENV_RUN_ID, "").strip()
    if env_value:
        return env_value
    return uuid.uuid4().hex


def resolve_report_file(report_file: str | None) -> Path | None:
    """Flag > TRILOGY_REPORT_FILE env > None (reporting off)."""
    if report_file:
        return Path(report_file)
    env_value = os.environ.get(ENV_REPORT_FILE, "").strip()
    if env_value:
        return Path(env_value)
    return None


@contextmanager
def report_run(
    command: str,
    report_file: str | None,
    run_id: str | None,
    **run_fields: Any,
) -> Iterator[ReportSink | None]:
    """Activate a report sink for the duration of a CLI command.

    Emits ``run_start`` on entry. Guarantees a terminal ``summary`` record:
    the execution path emits the detailed one; if the command dies before
    reaching it (config errors, parse failures outside the file loop), a
    fallback failure summary is written so consumers always see a terminal
    record. Deactivates the sink on exit.
    """
    path = resolve_report_file(report_file)
    if path is None:
        yield None
        return

    from trilogy import __version__

    sink = ReportSink(path, resolve_run_id(run_id), command)
    set_report_sink(sink)
    sink.emit(
        "run_start",
        command=command,
        trilogy_version=__version__,
        **run_fields,
    )
    try:
        yield sink
    except BaseException as e:
        if not sink.summary_emitted:
            exit_code = getattr(e, "exit_code", None)
            sink.emit(
                "summary",
                success=False,
                exit_code=exit_code if isinstance(exit_code, int) else 1,
                error_type=type(e).__name__,
                error=str(e) or None,
            )
        raise
    finally:
        if not sink.summary_emitted:
            sink.emit("summary", success=True, exit_code=0)
        set_report_sink(None)
