"""Kick eval runs off from the viewer.

The page's Launch screen posts a form (suite, categories, question scope,
model, knobs); this module turns it into a ``run_eval.py`` command line and
runs it as a subprocess off the HTTP thread. ONE run executes at a time - a
run already fans out (legs x ``--concurrency``) agents against one provider -
so extra submissions queue and start when the one ahead finishes.

Everything the form can set is a ``run_eval.py`` flag: the CLI stays the single
source of truth for what an eval run is, and the page shows the exact command
before it starts one.
"""

from __future__ import annotations

import os
import re
import sqlite3
import subprocess
import sys
import threading
import time
from pathlib import Path

from .suites import Suite

REPO_ROOT = Path(__file__).resolve().parents[2]

_LOG_LINES = 400
_MAX_ACTIVE = 1  # concurrent run_eval.py processes (each fans out its own legs)
_MAX_FINISHED = 12  # finished jobs kept so the page can still show their logs
_SCAN_INTERVAL = 3.0  # seconds between "did a new run dir appear" checks

# Knobs the form may override; anything else stays at the run_eval.py default.
_DEFAULT_CONCURRENCY = 2  # per leg - 4 legs x 2 is the sustainable provider load
_DEFAULT_MAX_ITERATIONS = 75
_DEFAULT_TIMEOUT = 900


# ---------------------------------------------------------------- options


def _known_models() -> list[dict]:
    """(provider, model) pairs seen in the history db - picker suggestions."""
    from common import archive

    if not archive.default_db_path().exists():
        return []
    try:
        conn = archive.connect()
        try:
            rows = conn.execute(
                "SELECT provider, model, MAX(run_timestamp) FROM questions "
                "WHERE model IS NOT NULL GROUP BY provider, model ORDER BY 3 DESC"
            ).fetchall()
        finally:
            conn.close()
    except sqlite3.Error:
        return []
    return [{"provider": provider or "", "model": model} for provider, model, _ in rows]


_ENV_KEY_RE = re.compile(r"^\s*(?:export\s+)?([A-Za-z_][A-Za-z0-9_]*)\s*=")


def _configured_api_keys(env_file: Path) -> list[str]:
    """Names - never values - of the ``*_API_KEY`` vars a run would find, so the
    page can grey out a provider before it burns a database build on it."""
    names = {name for name in os.environ if name.endswith("_API_KEY")}
    if env_file.exists():
        try:
            for line in env_file.read_text(encoding="utf-8", errors="replace").split(
                "\n"
            ):
                match = _ENV_KEY_RE.match(line)
                if match and match.group(1).endswith("_API_KEY"):
                    names.add(match.group(1))
        except OSError:
            pass
    return sorted(names)


def _suite_payload(suite: Suite) -> dict:
    from common import prompts
    from common.categories import FUNNEL_ORDER, categories_for, funnel_order_for
    from common.main import DEFAULT_MODEL, DEFAULT_PROVIDER

    spec = suite.spec
    known = categories_for(spec)
    order = [key for key in funnel_order_for(spec) if key in known]
    ordered = order + [key for key in known if key not in order]
    # A spec's funnel_order is a DISPLAY order - TPC-DS lists 25 warehouse
    # variants in it. The form's default selection is the shared funnel legs.
    base = [key for key in FUNNEL_ORDER if key in known]
    try:
        query_ids = [p["id"] for p in prompts.active_prompts(spec)]
    except (OSError, ValueError, KeyError):
        query_ids = []
    return {
        "key": suite.key,
        "label": suite.label,
        "categories": [
            {
                "key": key,
                "label": known[key].label,
                "harness": known[key].harness,
                "base": key in base,
            }
            for key in ordered
        ],
        "base": base,
        "query_ids": query_ids,
        "runnable": (spec.eval_dir / "run_eval.py").exists(),
        "enriched_dir": (
            str(spec.default_enriched_dir) if spec.default_enriched_dir else None
        ),
        "defaults": {
            "provider": DEFAULT_PROVIDER,
            "model": DEFAULT_MODEL,
            "scale_factor": spec.default_scale_factor,
            "num_queries": spec.default_num_queries,
            "concurrency": _DEFAULT_CONCURRENCY,
            "max_iterations": _DEFAULT_MAX_ITERATIONS,
            "timeout": _DEFAULT_TIMEOUT,
        },
    }


def launch_options(suites: dict[str, Suite]) -> dict:
    """Everything the Launch form needs: per-suite categories/questions plus the
    provider list and which API keys are actually configured."""
    from common.main import PROVIDER_ENV

    env_file = REPO_ROOT / ".env.secrets"
    configured = set(_configured_api_keys(env_file))
    return {
        "suites": [_suite_payload(s) for s in suites.values()],
        "providers": [
            {"key": key, "env": env, "configured": env in configured}
            for key, env in sorted(PROVIDER_ENV.items())
        ],
        "models": _known_models(),
        "efforts": ["", "none", "low", "medium", "high", "xhigh", "max"],
        "env_file": str(env_file),
        "max_active": _MAX_ACTIVE,
    }


# ---------------------------------------------------------------- command


def _int(form: dict, key: str, default: int, lo: int, hi: int) -> int:
    raw = form.get(key)
    if raw is None or raw == "":
        return default
    try:
        value = int(float(raw))
    except (TypeError, ValueError):
        raise ValueError(f"{key} must be a number") from None
    return max(lo, min(hi, value))


def _float(form: dict, key: str, default: float, lo: float, hi: float) -> float:
    raw = form.get(key)
    if raw is None or raw == "":
        return default
    try:
        value = float(raw)
    except (TypeError, ValueError):
        raise ValueError(f"{key} must be a number") from None
    return max(lo, min(hi, value))


def _parse_ids(raw: str) -> list[int]:
    """``5,13,18`` / ``1-20`` / whitespace - deduped and sorted."""
    out: list[int] = []
    for part in re.split(r"[,\s]+", raw.strip()):
        if not part:
            continue
        span = re.fullmatch(r"(\d+)\s*-\s*(\d+)", part)
        if span:
            first, last = int(span.group(1)), int(span.group(2))
            if last < first:
                raise ValueError(f"bad id range {part!r}")
            out.extend(range(first, last + 1))
        elif part.isdigit():
            out.append(int(part))
        else:
            raise ValueError(f"bad query id {part!r}")
    return sorted(dict.fromkeys(out))


def _scope_args(suite: Suite, form: dict) -> tuple[list[str], str]:
    """(argv fragment, human scope label) for which questions to run."""
    spec = suite.spec
    ids = _parse_ids(str(form.get("query_ids") or ""))
    if not ids:
        count = _int(form, "num_queries", spec.default_num_queries, 1, 10_000)
        return ["--num-queries", str(count)], f"first {count}"
    from common import prompts

    available = {p["id"] for p in prompts.active_prompts(spec)}
    unknown = [i for i in ids if i not in available]
    if unknown:
        raise ValueError(f"{suite.key} has no question(s) {unknown}")
    args = ["--query-ids", ",".join(str(i) for i in ids)]
    # A targeted rerun splices the rest of the benchmark in from the latest run
    # by default; that is wrong when the point is a standalone subset run.
    if not form.get("splice"):
        args += ["--splice-from", "none"]
    return args, f"{len(ids)} question{'' if len(ids) == 1 else 's'}"


def build_command(suite: Suite, form: dict) -> tuple[list[str], str]:
    """(argv, label) for this form. Raises ValueError on anything unrunnable."""
    from common.categories import categories_for

    spec = suite.spec
    script = spec.eval_dir / "run_eval.py"
    if not script.exists():
        raise ValueError(f"{suite.key} has no run_eval.py")
    known = categories_for(spec)
    categories = [c for c in (form.get("categories") or []) if isinstance(c, str)]
    if not categories:
        raise ValueError("pick at least one category")
    unknown = [c for c in categories if c not in known]
    if unknown:
        raise ValueError(f"unknown categories: {', '.join(unknown)}")
    if spec.default_enriched_dir is None and any(
        c.startswith("enriched") for c in categories
    ):
        raise ValueError(
            f"{suite.key} has no default enriched model dir - that leg cannot run here"
        )

    argv = [sys.executable, str(script)]
    if len(categories) == 1:
        argv += ["--category", categories[0]]
    else:
        argv += ["--categories", ",".join(categories)]
    scope_args, scope_label = _scope_args(suite, form)
    argv += scope_args
    argv += [
        "--scale-factor",
        f"{_float(form, 'scale_factor', spec.default_scale_factor, 0.001, 1000):g}",
    ]
    provider = str(form.get("provider") or "").strip()
    model = str(form.get("model") or "").strip()
    if provider:
        argv += ["--provider", provider]
    if model:
        argv += ["--model", model]
    effort = str(form.get("reasoning_effort") or "").strip()
    if effort:
        argv += ["--reasoning-effort", effort]
    argv += [
        "--concurrency",
        str(_int(form, "concurrency", _DEFAULT_CONCURRENCY, 1, 16)),
        "--max-iterations",
        str(_int(form, "max_iterations", _DEFAULT_MAX_ITERATIONS, 1, 1000)),
        "--timeout",
        str(_int(form, "timeout", _DEFAULT_TIMEOUT, 30, 86_400)),
    ]
    if form.get("enable_todo"):
        argv.append("--enable-todo")
    label = (
        f"{suite.key} · {'+'.join(categories)} · {scope_label} · "
        f"{provider or 'default'}/{model or 'default'}"
    )
    return argv, label


def command_text(argv: list[str]) -> str:
    """The argv as a copy-pasteable line (quote only what needs it)."""
    parts = []
    for arg in argv:
        parts.append(f'"{arg}"' if re.search(r"[\s\"]", arg) else arg)
    return " ".join(parts)


# ---------------------------------------------------------------- jobs


def _process_group_kwargs() -> dict:
    if sys.platform == "win32":
        return {"creationflags": subprocess.CREATE_NEW_PROCESS_GROUP}
    return {"start_new_session": True}


def _terminate(proc: subprocess.Popen) -> None:
    """Kill the whole tree: run_eval spawns one leg process per category and each
    leg spawns an agent process per worker, so killing the parent alone would
    leave the real work running."""
    if sys.platform == "win32":
        subprocess.run(
            ["taskkill", "/F", "/T", "/PID", str(proc.pid)],
            capture_output=True,
            check=False,
        )
        return
    import signal

    try:
        os.killpg(os.getpgid(proc.pid), signal.SIGTERM)
    except (ProcessLookupError, PermissionError, OSError):
        proc.terminate()


def _dir_names(root: Path) -> set[str]:
    try:
        return {p.name for p in root.iterdir() if p.is_dir()}
    except OSError:
        return set()


class LaunchJobs:
    """Queue of ``run_eval.py`` invocations. The page starts them with POST
    /launch and polls GET /launch_status.json. At most ``_MAX_ACTIVE`` run at
    once; the rest sit in ``queued`` until a slot frees, so a stack of
    combinations can be fired off in one sitting."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._next_id = 1
        self._jobs: list[dict] = []

    # -- public -----------------------------------------------------------

    def submit(self, suite: Suite, form: dict) -> dict:
        argv, label = build_command(suite, form)
        with self._lock:
            job: dict = {
                "id": self._next_id,
                "state": "queued",
                "suite": suite.key,
                "suite_label": suite.label,
                "label": label,
                "command": command_text(argv),
                "created_at": time.time(),
                "started_at": None,
                "ended_at": None,
                "exit_code": None,
                "error": None,
                "log": [],
                "runs": [],
                "_argv": argv,
                "_results_dir": suite.results_dir,
                "_cancel": False,
                "_proc": None,
            }
            self._next_id += 1
            finished = [
                j for j in self._jobs if j["state"] not in ("queued", "running")
            ]
            stale = finished[:-_MAX_FINISHED]
            self._jobs = [j for j in self._jobs if not any(j is s for s in stale)]
            self._jobs.append(job)
        self._start_next()
        return self.snapshot()

    def snapshot(self) -> dict:
        now = time.time()
        with self._lock:
            jobs = []
            for j in self._jobs:
                public = {
                    k: (list(v) if isinstance(v, list) else v)
                    for k, v in j.items()
                    if not k.startswith("_")
                }
                started = j["started_at"]
                public["elapsed"] = round(
                    ((j["ended_at"] or now) - started) if started else 0.0, 1
                )
                jobs.append(public)
        return {
            "running": any(j["state"] == "running" for j in jobs),
            "queued": sum(1 for j in jobs if j["state"] == "queued"),
            "jobs": jobs,
        }

    def cancel(self, job_id: int | None) -> dict:
        with self._lock:
            targets = [
                j
                for j in self._jobs
                if j["state"] in ("queued", "running")
                and (job_id is None or j["id"] == job_id)
            ]
            procs = []
            for job in targets:
                job["_cancel"] = True
                if job["state"] == "queued":
                    job.update(state="cancelled", ended_at=time.time())
                elif job["_proc"] is not None:
                    procs.append(job["_proc"])
        for proc in procs:
            _terminate(proc)
        self._start_next()
        return self.snapshot()

    # -- internals --------------------------------------------------------

    def _append_log(self, job: dict, message: str) -> None:
        print(f"[launch #{job['id']}] {message}", flush=True)
        with self._lock:
            lines = job["log"]
            lines.append(message)
            del lines[:-_LOG_LINES]

    def _start_next(self) -> None:
        with self._lock:
            active = sum(1 for j in self._jobs if j["state"] == "running")
            if active >= _MAX_ACTIVE:
                return
            job = next((j for j in self._jobs if j["state"] == "queued"), None)
            if job is None:
                return
            job.update(state="running", started_at=time.time())
        threading.Thread(target=self._run, args=(job,), daemon=True).start()

    def _run(self, job: dict) -> None:
        results_dir: Path = job["_results_dir"]
        before = _dir_names(results_dir)
        self._append_log(job, job["command"])
        env = {
            **os.environ,
            "PYTHONUNBUFFERED": "1",
            "PYTHONIOENCODING": "utf-8",
        }
        try:
            proc = subprocess.Popen(
                job["_argv"],
                cwd=str(REPO_ROOT),
                env=env,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                encoding="utf-8",
                errors="replace",
                bufsize=1,
                **_process_group_kwargs(),
            )
        except OSError as exc:
            with self._lock:
                job.update(
                    state="error",
                    error=f"{type(exc).__name__}: {exc}",
                    ended_at=time.time(),
                )
            self._append_log(job, f"failed to start: {exc}")
            self._start_next()
            return
        with self._lock:
            job["_proc"] = proc
        pump = threading.Thread(
            target=self._pump_output, args=(job, proc.stdout), daemon=True
        )
        pump.start()
        while proc.poll() is None:
            self._scan_runs(job, before)
            time.sleep(_SCAN_INTERVAL)
        pump.join(timeout=10)
        self._scan_runs(job, before)
        code = proc.returncode
        with self._lock:
            cancelled = job["_cancel"]
            job.update(
                state=("cancelled" if cancelled else "done" if code == 0 else "error"),
                exit_code=code,
                ended_at=time.time(),
                error=(
                    None
                    if code == 0 or cancelled
                    else f"run_eval.py exited {code} - see the log"
                ),
            )
        self._append_log(job, f"exit {code}")
        self._start_next()

    def _pump_output(self, job: dict, stream) -> None:
        for line in stream:
            self._append_log(job, line.rstrip("\n"))
        stream.close()

    def _scan_runs(self, job: dict, before: set[str]) -> None:
        """Result dirs that appeared since this job started - the run(s) it is
        writing. ``ready`` flips once a dir has trajectories the viewer can open."""
        results_dir: Path = job["_results_dir"]
        found = [
            {
                "name": name,
                "ready": any((results_dir / name).glob("agent_log.*.jsonl")),
            }
            for name in sorted(_dir_names(results_dir) - before)
        ]
        with self._lock:
            if found != job["runs"]:
                job["runs"] = found
