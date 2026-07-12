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


class ReplayJob:
    """One replay at a time, run off the HTTP thread. The page starts it with
    POST /replay and polls GET /replay_status.json until ``running`` clears."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._cancel = False
        self._state: dict = {
            "running": False,
            "mode": None,
            "suite": None,
            "run": None,
            "qid": None,
            "progress": None,
            "new_run": None,
            "log": [],
            "result": None,
            "error": None,
        }

    def snapshot(self) -> dict:
        with self._lock:
            return {**self._state, "log": list(self._state["log"])}

    def _log(self, message: str) -> None:
        print(f"[replay] {message}", flush=True)
        with self._lock:
            lines = self._state["log"]
            lines.append(message)
            del lines[:-_REPLAY_LOG_LINES]

    def start(self, suite: Suite, run_dir: Path, qid: int) -> str | None:
        """Accept the job, or return why it was rejected."""
        with self._lock:
            if self._state["running"]:
                return self._busy_reason()
            self._cancel = False
            self._state = {
                "running": True,
                "mode": "one",
                "suite": suite.key,
                "run": run_dir.name,
                "qid": qid,
                "progress": None,
                "new_run": None,
                "log": [],
                "result": None,
                "error": None,
            }
        threading.Thread(
            target=self._run, args=(suite, run_dir, qid), daemon=True
        ).start()
        return None

    def start_all(self, suite: Suite, run_dir: Path) -> str | None:
        """Fork the run, then replay every query in the fork — original untouched."""
        with self._lock:
            if self._state["running"]:
                return self._busy_reason()
            self._cancel = False
            self._state = {
                "running": True,
                "mode": "all",
                "suite": suite.key,
                "run": run_dir.name,
                "qid": None,
                "progress": {"done": 0, "total": 0, "qid": None},
                "new_run": None,
                "log": [],
                "result": None,
                "error": None,
            }
        threading.Thread(
            target=self._run_all, args=(suite, run_dir), daemon=True
        ).start()
        return None

    def cancel(self) -> None:
        with self._lock:
            self._cancel = True

    def _busy_reason(self) -> str:
        if self._state["mode"] == "all":
            return "a full rerun is already running"
        return f"a replay is already running (q{self._state['qid']:02d})"

    def _run(self, suite: Suite, run_dir: Path, qid: int) -> None:
        result: dict | None = None
        error: str | None = None
        try:
            # Lazy: pulls in the full eval pipeline (trilogy, scoring, ...).
            from common import replay

            result = replay.replay_query(run_dir, suite.spec, qid, log=self._log)
        except Exception as exc:
            error = f"{type(exc).__name__}: {exc}"
            self._log(error)
        with self._lock:
            self._state.update(running=False, result=result, error=error)

    def _run_all(self, suite: Suite, run_dir: Path) -> None:
        result: dict | None = None
        error: str | None = None
        new_run: str | None = None
        try:
            from common import replay

            fork = replay.fork_run(run_dir, log=self._log)
            new_run = fork.name
            with self._lock:
                self._state.update(run=new_run, new_run=new_run)
            result = replay.replay_all(
                fork,
                suite.spec,
                log=self._log,
                on_progress=self._progress,
                cancelled=lambda: self._cancel,
            )
        except Exception as exc:
            error = f"{type(exc).__name__}: {exc}"
            self._log(error)
        with self._lock:
            self._state.update(
                running=False, result=result, error=error, new_run=new_run
            )

    def _progress(self, done: int, total: int, qid: int) -> None:
        with self._lock:
            self._state["progress"] = {"done": done, "total": total, "qid": qid}


# A cold collect() transpiles every query (tens of seconds). Serialise it so
# overlapping polls queue behind one build instead of each doing the work.
_COLLECT_LOCK = threading.Lock()


class ViewerHandler(http.server.SimpleHTTPRequestHandler):
    """Static viewer.html plus the live-data and replay endpoints. ``suites``,
    ``default_suite``, ``default_dir``, ``job`` and ``log_requests`` are
    injected by :func:`serve`."""

    suites: dict[str, Suite]
    default_suite: Suite
    default_dir: Path
    job: ReplayJob
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
            self._json(self.job.snapshot())
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
            self.job.cancel()
            self._json(self.job.snapshot())
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
            rejected = self.job.start_all(suite, run_dir)
        else:
            try:
                rejected = self.job.start(suite, run_dir, int(body["qid"]))
            except (TypeError, ValueError, KeyError):
                self._json({"error": "expected JSON body {suite, run, qid}"}, 400)
                return
        if rejected:
            self._json({"error": rejected}, 409)
            return
        self._json(self.job.snapshot())


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
    ViewerHandler.job = ReplayJob()
    ViewerHandler.log_requests = log_requests
    handler = functools.partial(ViewerHandler, directory=str(results_dir))
    # Threading: a replay runs off-thread, but the browser still opens parallel
    # connections for data.json + replay_status.json while it works.
    with http.server.ThreadingHTTPServer(("127.0.0.1", port), handler) as httpd:
        httpd.daemon_threads = True
        print(f"serving http://127.0.0.1:{port}/viewer.html  (ctrl-c to stop)")
        httpd.serve_forever()
