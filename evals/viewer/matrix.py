"""The question-by-run grid behind the Debug page.

One row per run (newest first), one cell per question, so a column of red is a
problem question and a row of red is a bad run. Built from report.json only -
no log parsing, no transpilation - and cached per run dir, so a rebuild after
the first costs a stat() per dir.

The grid spans BOTH what is still on disk and everything in the history db
(``common.archive``): building it syncs each changed run dir into the archive
first, then unions the archived runs in. Run dirs are hundreds of GB and get
reclaimed; the archive is a few KB per run and doesn't, so a cleaned-up run
keeps its row here instead of vanishing from the record.

Statuses travel as one character per cell (uppercase = spliced in from an
earlier run rather than actually run here), which keeps a 94-run x 99-question
grid around 10KB instead of megabytes of JSON objects.
"""

from __future__ import annotations

import re
import threading
import time
from pathlib import Path

from . import logs
from .suites import Suite

STATUS_CHARS = {
    "pass": "p",
    "fail": "f",
    "error": "e",
    "missing": "m",
    "timeout": "t",
    "exhausted": "x",
    "crashed": "c",
    "running": "r",
    "unscored": "u",
    "partial": "w",
}
CHAR_STATUS = {v: k for k, v in STATUS_CHARS.items()}
ABSENT = "."

_TS_RE = re.compile(r"\d{8}[-_]\d{6}")


def _ts_from(*candidates: str | None) -> str:
    """First candidate carrying a sortable ``YYYYMMDD-HHMMSS``."""
    for candidate in candidates:
        found = _TS_RE.search(candidate or "")
        if found:
            return found.group(0).replace("_", "-")
    return ""


def _timestamp(run_dir: Path, meta: dict) -> str:
    """Sortable stamp: the run name's own, else the report's, else mtime."""
    found = _ts_from(run_dir.name, meta.get("timestamp"))
    return found or time.strftime(
        "%Y%m%d-%H%M%S", time.localtime(run_dir.stat().st_mtime)
    )


def _dir_stamp(run_dir: Path) -> tuple:
    """Change token for a run dir: the report's identity when it has one, else
    the log set (a live run grows logs while its report doesn't exist yet)."""
    report = run_dir / "report.json"
    repeat = run_dir / "repeat_report.json"
    for path in (report, repeat):
        try:
            st = path.stat()
        except OSError:
            continue
        return (path.name, st.st_mtime_ns, st.st_size)
    logs = sorted(p.name for p in run_dir.glob("agent_log.*.jsonl"))
    try:
        newest = max((run_dir / n).stat().st_mtime_ns for n in logs) if logs else 0
    except OSError:
        newest = 0
    return ("live", len(logs), newest)


def _log_qids(run_dir: Path) -> list[int]:
    out = []
    for path in run_dir.glob("agent_log.q*.jsonl"):
        found = re.search(r"\bq(\d+)\b", path.name)
        if found:
            out.append(int(found.group(1)))
    return sorted(set(out))


def _eval_row(run_dir: Path, report: dict) -> dict:
    meta = report.get("meta", {})
    cells: dict[int, str] = {}
    for row in report.get("queries", []):
        char = STATUS_CHARS.get(row.get("status", ""), "u")
        spliced = row.get("source") not in (None, "this_run")
        cells[row["id"]] = char.upper() if spliced else char
    tokens = sum(
        m.get("prompt_tokens") or 0 for m in report.get("per_query_metrics", [])
    )
    return {
        "kind": "eval",
        "category": meta.get("category") or meta.get("mode") or "unknown",
        "provider": meta.get("provider"),
        "model": meta.get("model"),
        "effort": meta.get("reasoning_effort"),
        "scale_factor": meta.get("scale_factor"),
        "prompt_tokens": tokens or None,
        "cells": cells,
        "ts": _timestamp(run_dir, meta),
    }


def _repeat_row(run_dir: Path, report: dict) -> dict:
    """A 10x run is one question many times; its single cell reports the spread."""
    meta = report.get("meta", {})
    reps = report.get("runs", [])
    passed = sum(1 for r in reps if r.get("status") == "pass")
    qid = meta.get("query_id")
    status = "pass" if passed == len(reps) else "fail" if not passed else "partial"
    return {
        "kind": "repeat",
        "category": meta.get("mode") or "repeat",
        "provider": meta.get("provider"),
        "model": meta.get("model"),
        "effort": None,
        "scale_factor": meta.get("scale_factor"),
        "prompt_tokens": sum(r.get("prompt_tokens") or 0 for r in reps) or None,
        "cells": {qid: STATUS_CHARS[status]} if qid is not None else {},
        "reps": {"passed": passed, "total": len(reps)},
        # One question, N reps: the headline is the spread, not "1 of 1".
        "passed": passed,
        "total": len(reps),
        "ts": _timestamp(run_dir, meta),
    }


def _live_row(run_dir: Path) -> dict | None:
    qids = _log_qids(run_dir)
    if not qids:
        return None
    return {
        "kind": "live",
        "category": "in progress",
        "provider": None,
        "model": None,
        "effort": None,
        "scale_factor": None,
        "prompt_tokens": None,
        "cells": {qid: STATUS_CHARS["running"] for qid in qids},
        "ts": _timestamp(run_dir, {}),
    }


def _build_row(run_dir: Path) -> dict | None:
    row: dict | None
    repeat = logs.read_json(run_dir / "repeat_report.json")
    if repeat:
        row = _repeat_row(run_dir, repeat)
    else:
        report = logs.read_json(run_dir / "report.json")
        row = (
            _eval_row(run_dir, report) if report.get("queries") else _live_row(run_dir)
        )
    if row is None:
        return None
    row["name"] = run_dir.name
    row["has_logs"] = any(run_dir.glob("agent_log.*.jsonl"))
    cells: dict[int, str] = row["cells"]
    if "passed" not in row:
        row["passed"] = sum(1 for c in cells.values() if c.lower() == "p")
        row["total"] = len(cells)
    return row


# Row cache: {run dir -> (stamp, row)}. A rebuild only re-reads the dirs whose
# report (or log set) moved, so polling the grid stays cheap.
_ROW_CACHE: dict[str, tuple[tuple, dict | None]] = {}


def _row_for(run_dir: Path) -> dict | None:
    key = str(run_dir)
    stamp = _dir_stamp(run_dir)
    cached = _ROW_CACHE.get(key)
    if cached is None or cached[0] != stamp:
        _ROW_CACHE[key] = (stamp, _build_row(run_dir))
    return _ROW_CACHE[key][1]


def _archived_row(run: dict) -> dict:
    """An archived run as a grid row. It has no files left, so no drilldown."""
    return {
        "name": run["name"],
        "kind": run["kind"],
        "category": run["variant"] or "unknown",
        "provider": run["provider"],
        "model": run["model"],
        "effort": None,
        "scale_factor": run["scale_factor"],
        "prompt_tokens": run["prompt_tokens"],
        "cells": {
            qid: STATUS_CHARS.get(status or "", "u")
            for qid, status in run["cells"].items()
        },
        "ts": _ts_from(run["run_timestamp"], run["name"], run["archived_at"]),
        "passed": run["passed"],
        "total": run["total"],
        "has_logs": False,
        "on_disk": False,
        "archived": True,
        "curated": run["curated"],
    }


def build(suite: Suite, on_progress=None, sync: bool = True) -> dict:
    """The grid payload: every run on disk, unioned with everything archived.
    ``on_progress(done, total)`` fires per run dir. With ``sync`` (the default)
    each run dir whose report has moved since it was last read is folded into
    the archive first - the sweep that makes a later cleanup lossless."""
    from common import archive

    root = suite.results_dir
    dirs = (
        sorted((p for p in root.iterdir() if p.is_dir()), reverse=True)
        if root.is_dir()
        else []
    )
    conn = archive.connect()
    try:
        stamps = archive.run_stamps(conn, suite.key)
        rows: list[dict] = []
        on_disk: set[str] = set()
        for i, run_dir in enumerate(dirs, 1):
            row = _row_for(run_dir)
            if row is not None:
                stamp = str(_dir_stamp(run_dir))
                # A live run is still being written: archive it once it settles.
                if sync and row["kind"] != "live" and stamps.get(row["name"]) != stamp:
                    archive.archive_run(conn, run_dir, suite.key, stamp=stamp)
                    stamps[row["name"]] = stamp
                rows.append({**row, "on_disk": True, "archived": row["name"] in stamps})
                on_disk.add(row["name"])
            if on_progress is not None:
                on_progress(i, len(dirs))
        rows.extend(
            _archived_row(run)
            for run in archive.archived_runs(conn, suite.key)
            if run["name"] not in on_disk
        )
    finally:
        conn.close()
    rows.sort(key=lambda r: (r["ts"], r["name"]), reverse=True)
    questions = sorted({qid for r in rows for qid in r["cells"]})
    index = {qid: i for i, qid in enumerate(questions)}
    encoded = []
    for row in rows:
        line = [ABSENT] * len(questions)
        for qid, char in row["cells"].items():
            line[index[qid]] = char
        # A copy: rows are cached by run dir, and the question axis they encode
        # against changes as soon as another run adds a question.
        encoded.append({**row, "cells": "".join(line)})
    return {
        "suite": suite.key,
        "label": suite.label,
        "questions": questions,
        "legend": CHAR_STATUS,
        "runs": encoded,
    }


class MatrixCache:
    """Serves the grid, with progress while it is being read.

    A warm build is milliseconds, so the request waits briefly and usually
    answers with the payload outright; a cold read of hundreds of runs hands
    back ``{ready: false, progress}`` instead and the page shows a bar.
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._builds: dict[str, dict] = {}

    def get(self, suite: Suite, wait: float = 0.35) -> dict:
        with self._lock:
            build_state = self._builds.get(suite.key)
            if build_state is None or build_state["done"].is_set():
                build_state = {
                    "done": threading.Event(),
                    "progress": {"done": 0, "total": 0},
                    "payload": None,
                    "error": None,
                }
                self._builds[suite.key] = build_state
                threading.Thread(
                    target=self._run, args=(suite, build_state), daemon=True
                ).start()
        build_state["done"].wait(wait)
        if not build_state["done"].is_set():
            return {
                "suite": suite.key,
                "ready": False,
                "progress": dict(build_state["progress"]),
            }
        if build_state["error"]:
            return {"suite": suite.key, "ready": False, "error": build_state["error"]}
        return {**build_state["payload"], "ready": True}

    def _run(self, suite: Suite, state: dict) -> None:
        def progress(done: int, total: int) -> None:
            state["progress"] = {"done": done, "total": total}

        try:
            state["payload"] = build(suite, progress)
        except Exception as exc:
            state["error"] = f"{type(exc).__name__}: {exc}"
        finally:
            state["done"].set()
