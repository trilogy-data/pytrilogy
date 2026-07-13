"""Live HTTP server behind the viewer page.

Serves the baked ``viewer.html`` plus JSON endpoints the page polls:
``suites.json`` (eval + run pickers), ``data.json`` (one run's trajectories),
``summary.json`` (cross-eval performance), ``replay_status.json`` — and POST
endpoints for replay / rerun-all / archive. Every endpoint that touches a run
takes an explicit suite so one server drives all benchmarks.
"""

from __future__ import annotations

import functools
import http.server
import json
import threading
import urllib.parse
from pathlib import Path

from . import suites as suites_mod
from .collect import collect
from .suites import Suite
from .summary import summary_payload

_REPLAY_LOG_LINES = 200
_MAX_AGENTS = 4  # concurrent agent processes across all replay jobs
_MAX_FINISHED = 12  # finished jobs kept so the page can still show their results


class ReplayJobs:
    """Replay jobs run off the HTTP thread, several at once (capped at
    ``_MAX_AGENTS`` concurrent agents). The page starts them with POST /replay
    or /replay_all and polls GET /replay_status.json. Per-run-dir safety
    (worker copies, report.json splices) lives in ``common.replay``; this layer
    only refuses overlaps that can't be made safe — the same query twice, or
    touching a run while a full rerun forks it."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._next_id = 1
        self._jobs: list[dict] = []

    def snapshot(self) -> dict:
        with self._lock:
            jobs = [
                {
                    k: (list(v) if k == "log" else v)
                    for k, v in j.items()
                    if not k.startswith("_")
                }
                for j in self._jobs
            ]
        return {"running": any(j["running"] for j in jobs), "jobs": jobs}

    def _append_log(self, job: dict, message: str) -> None:
        print(f"[replay #{job['id']}] {message}", flush=True)
        with self._lock:
            lines = job["log"]
            lines.append(message)
            del lines[:-_REPLAY_LOG_LINES]

    def _new_job(
        self,
        mode: str,
        suite: Suite,
        run_dir: Path,
        qid: int | None,
        worker: int | None,
        slots: int,
    ) -> dict:
        job: dict = {
            "id": self._next_id,
            "running": True,
            "mode": mode,
            "suite": suite.key,
            "run": run_dir.name,
            "qid": qid,
            "worker": worker,
            "progress": (
                {"done": 0, "total": 0, "qid": None, "active": []}
                if mode == "all"
                else None
            ),
            "new_run": None,
            "log": [],
            "result": None,
            "error": None,
            "_cancel": False,
            "_slots": slots,
        }
        self._next_id += 1
        finished = [j for j in self._jobs if not j["running"]]
        stale = finished[:-_MAX_FINISHED]
        self._jobs = [j for j in self._jobs if not any(j is s for s in stale)]
        self._jobs.append(job)
        return job

    def start(self, suite: Suite, run_dir: Path, qid: int) -> str | None:
        """Accept the job, or return why it was rejected."""
        with self._lock:
            running = [j for j in self._jobs if j["running"]]
            if sum(j["_slots"] for j in running) >= _MAX_AGENTS:
                return f"{_MAX_AGENTS} agents already running - wait for one to finish"
            used: set[int] = set()
            for j in running:
                if j["suite"] != suite.key or run_dir.name not in (
                    j["run"],
                    j["new_run"],
                ):
                    continue
                if j["mode"] == "all":
                    return "a full rerun is using this run"
                if j["qid"] == qid:
                    return f"q{qid:02d} is already replaying"
                if j["worker"] is not None:
                    used.add(j["worker"])
            # Lowest free _worker_N copy in this run (0 is the run's own).
            worker = next(i for i in range(len(used) + 1) if i not in used)
            job = self._new_job("one", suite, run_dir, qid, worker, 1)
        threading.Thread(
            target=self._run, args=(job, suite, run_dir, qid), daemon=True
        ).start()
        return None

    def start_all(self, suite: Suite, run_dir: Path, concurrency: int) -> str | None:
        """Fork the run, then replay every query in the fork — original untouched."""
        concurrency = max(1, min(concurrency, _MAX_AGENTS))
        with self._lock:
            running = [j for j in self._jobs if j["running"]]
            if any(j["mode"] == "all" for j in running):
                return "a full rerun is already running"
            if any(
                j["suite"] == suite.key and j["run"] == run_dir.name for j in running
            ):
                return "queries are replaying in this run - wait for them to finish"
            if sum(j["_slots"] for j in running) + concurrency > _MAX_AGENTS:
                return f"not enough capacity (max {_MAX_AGENTS} agents) - wait for a replay to finish"
            job = self._new_job("all", suite, run_dir, None, None, concurrency)
        threading.Thread(
            target=self._run_all,
            args=(job, suite, run_dir, concurrency),
            daemon=True,
        ).start()
        return None

    def cancel(self, job_id: int | None = None) -> None:
        with self._lock:
            for j in self._jobs:
                if j["running"] and (job_id is None or j["id"] == job_id):
                    j["_cancel"] = True

    def _run(self, job: dict, suite: Suite, run_dir: Path, qid: int) -> None:
        result: dict | None = None
        error: str | None = None
        log = functools.partial(self._append_log, job)
        try:
            # Lazy: pulls in the full eval pipeline (trilogy, scoring, ...).
            from common import replay

            result = replay.replay_query(
                run_dir, suite.spec, qid, worker=job["worker"], log=log
            )
        except Exception as exc:
            error = f"{type(exc).__name__}: {exc}"
            log(error)
        with self._lock:
            job.update(running=False, result=result, error=error)

    def _run_all(
        self, job: dict, suite: Suite, run_dir: Path, concurrency: int
    ) -> None:
        result: dict | None = None
        error: str | None = None
        new_run: str | None = None
        log = functools.partial(self._append_log, job)
        try:
            from common import replay

            fork = replay.fork_run(run_dir, log=log)
            new_run = fork.name
            with self._lock:
                job.update(run=new_run, new_run=new_run)
            result = replay.replay_all(
                fork,
                suite.spec,
                concurrency=concurrency,
                log=log,
                on_progress=functools.partial(self._progress, job),
                cancelled=lambda: job["_cancel"],
            )
        except Exception as exc:
            error = f"{type(exc).__name__}: {exc}"
            log(error)
        with self._lock:
            job.update(running=False, result=result, error=error, new_run=new_run)

    def _progress(self, job: dict, done: int, total: int, active: list[int]) -> None:
        with self._lock:
            job["progress"] = {
                "done": done,
                "total": total,
                "qid": active[0] if active else None,
                "active": list(active),
            }


# A cold collect() transpiles every query (tens of seconds). Serialise it so
# overlapping polls queue behind one build instead of each doing the work.
_COLLECT_LOCK = threading.Lock()


class ViewerHandler(http.server.SimpleHTTPRequestHandler):
    """Static viewer.html plus the live-data and replay endpoints. ``suites``,
    ``default_suite``, ``default_dir``, ``jobs`` and ``log_requests`` are
    injected by :func:`serve`."""

    suites: dict[str, Suite]
    default_suite: Suite
    default_dir: Path
    jobs: ReplayJobs
    log_requests: bool = False

    def log_message(self, format: str, *args) -> None:
        # The page polls every few seconds forever; the default per-request line
        # grows the console without bound until it falls over.
        if self.log_requests:
            super().log_message(format, *args)

    def _json(self, obj, status: int = 200) -> None:
        payload = json.dumps(obj).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Cache-Control", "no-store")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def _suite(self, key: str | None) -> Suite:
        return self.suites.get(key or "") or self.default_suite

    def _run_names(self, suite: Suite) -> list[str]:
        """Advertised run dirs — includes the startup dir even if it sits
        outside the suite's results dir (e.g. an explicit CLI path)."""
        names = suites_mod.list_run_dirs(suite)
        if suite.key == self.default_suite.key and self.default_dir.name not in names:
            names.insert(0, self.default_dir.name)
        return names

    def _resolve_run(self, suite: Suite, name: str | None) -> Path | None:
        """Name → run dir, constrained to the run dirs we advertise."""
        if name is None:
            return None
        if suite.key == self.default_suite.key and name == self.default_dir.name:
            return self.default_dir
        if name in suites_mod.list_run_dirs(suite):
            return suite.results_dir / name
        return None

    def _target_dir(self, suite: Suite, name: str | None) -> Path | None:
        run_dir = self._resolve_run(suite, name)
        if run_dir is not None:
            return run_dir
        if suite.key == self.default_suite.key:
            return self.default_dir
        names = suites_mod.list_run_dirs(suite)
        return suite.results_dir / names[0] if names else None

    def _archive(self, suite: Suite, run_dir: Path) -> None:
        """Write this run's summary stats into the longitudinal history db
        (non-destructive; the files stay). Idempotent per run."""
        try:
            from common import archive

            conn = archive.connect()
            try:
                count = archive.archive_run(conn, run_dir, suite.spec.short_name)
            finally:
                conn.close()
        except Exception as exc:
            self._json({"error": f"{type(exc).__name__}: {exc}"}, 500)
            return
        self._json({"ok": True, "count": count, "db": archive.default_db_path().name})

    def do_GET(self):
        parsed = urllib.parse.urlparse(self.path)
        if parsed.path == "/suites.json":
            self._json(
                {
                    "suites": [
                        {"key": s.key, "label": s.label, "runs": self._run_names(s)}
                        for s in self.suites.values()
                    ],
                    "current": {
                        "suite": self.default_suite.key,
                        "run": self.default_dir.name,
                    },
                }
            )
            return
        if parsed.path == "/summary.json":
            self._json(summary_payload(self.suites))
            return
        if parsed.path == "/replay_status.json":
            self._json(self.jobs.snapshot())
            return
        if parsed.path == "/data.json":
            # Re-read the chosen run's logs per request so a live (or
            # just-finished) run streams in — the page polls this.
            qs = urllib.parse.parse_qs(parsed.query)
            suite = self._suite((qs.get("suite") or [None])[0])
            target = self._target_dir(suite, (qs.get("run") or [None])[0])
            if target is None:
                self._json([])
                return
            with _COLLECT_LOCK:
                payload = collect(target, suite.spec)
            self._json(payload)
            return
        super().do_GET()

    def do_POST(self):
        path = urllib.parse.urlparse(self.path).path
        if path not in ("/replay", "/replay_all", "/replay_cancel", "/archive"):
            self.send_error(404)
            return
        try:
            length = int(self.headers.get("Content-Length") or 0)
            body = json.loads(self.rfile.read(length) or b"{}")
        except (ValueError, json.JSONDecodeError):
            self._json({"error": "expected a JSON body"}, 400)
            return
        if path == "/replay_cancel":
            try:
                job_id = int(body["id"]) if body.get("id") is not None else None
            except (TypeError, ValueError):
                job_id = None
            self.jobs.cancel(job_id)
            self._json(self.jobs.snapshot())
            return
        suite = self._suite(body.get("suite"))
        run_dir = self._resolve_run(suite, body.get("run"))
        if run_dir is None:
            self._json({"error": f"unknown run {body.get('run')!r}"}, 400)
            return
        if path == "/archive":
            self._archive(suite, run_dir)
            return
        if path == "/replay_all":
            try:
                concurrency = int(body.get("concurrency") or 1)
            except (TypeError, ValueError):
                concurrency = 1
            rejected = self.jobs.start_all(suite, run_dir, concurrency)
        else:
            try:
                rejected = self.jobs.start(suite, run_dir, int(body["qid"]))
            except (TypeError, ValueError, KeyError):
                self._json({"error": "expected JSON body {suite, run, qid}"}, 400)
                return
        if rejected:
            self._json({"error": rejected}, 409)
            return
        self._json(self.jobs.snapshot())


def serve(
    suites: dict[str, Suite],
    default_suite: Suite,
    results_dir: Path,
    port: int,
    log_requests: bool,
) -> None:
    ViewerHandler.suites = suites
    ViewerHandler.default_suite = default_suite
    ViewerHandler.default_dir = results_dir
    ViewerHandler.jobs = ReplayJobs()
    ViewerHandler.log_requests = log_requests
    handler = functools.partial(ViewerHandler, directory=str(results_dir))
    # Threading: a replay runs off-thread, but the browser still opens parallel
    # connections for data.json + replay_status.json while it works.
    with http.server.ThreadingHTTPServer(("127.0.0.1", port), handler) as httpd:
        httpd.daemon_threads = True
        print(f"serving http://127.0.0.1:{port}/viewer.html  (ctrl-c to stop)")
        httpd.serve_forever()
