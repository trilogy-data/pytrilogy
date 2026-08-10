"""Live `trilogy cloud` integration: deploy (``jobs push``), trigger
(``jobs run``), monitor (``runs show``) against a real trilogy-cloud API.

Opt-in — skipped unless ``TRILOGY_CLOUD_TOKEN`` and ``TRILOGY_CLOUD_API`` are
both set, and refused outright against production, because it triggers real
job runs and bills real warehouse time.

Commands run as **subprocesses with an isolated home directory**, so
``~/.trilogy/cloud_credentials.json`` cannot exist. That is what makes this a
test of the CI path rather than of the developer's stored login: the token
reaches the CLI only through the environment. ``CREDENTIALS_PATH`` is computed
at import from ``Path.home()``, so pointing HOME/USERPROFILE at a temp dir is
enough to guarantee it.

A **scoped deploy token** is sufficient. The flow deliberately touches only
job-pinned routes — org-wide ones like ``runs list`` return 403 for such a
token by design, so reaching for one here would make the test pass only for
unscoped credentials.
"""

from __future__ import annotations

import json
import os
import re
import subprocess
import sys
from pathlib import Path
from typing import Any

import pytest

from trilogy.scripts.cloud import DEFAULT_API_URL

pytestmark = pytest.mark.cloud_live

REPO_ROOT = Path(__file__).resolve().parents[2]


def _dotenv() -> dict[str, str]:
    """The repo's gitignored dotenv files, first definition winning.

    Same source the bigquery live tests read, so credentials live in one
    place. ``utf-8-sig`` because these files are hand-edited on Windows and a
    BOM would otherwise ride along inside the first key's name.
    """
    values: dict[str, str] = {}
    for candidate in (".env.secrets", ".env"):
        path = REPO_ROOT / candidate
        if not path.is_file():
            continue
        for raw in path.read_text(encoding="utf-8-sig").splitlines():
            line = raw.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, _, value = line.partition("=")
            values.setdefault(key.strip(), value.strip().strip('"').strip("'"))
    return values


_DOTENV = _dotenv()


def _setting(*names: str) -> str:
    """First non-empty of these names, real environment ahead of the dotenv."""
    for name in names:
        value = os.environ.get(name) or _DOTENV.get(name)
        if value:
            return value
    return ""


#: TRILOGY_DEV_JOB_TOKEN is what the repo's .env.secrets calls the scoped
#: deploy token; TRILOGY_CLOUD_TOKEN is what the CLI reads. Accept either.
TOKEN = _setting("TRILOGY_CLOUD_TOKEN", "TRILOGY_DEV_JOB_TOKEN")
API = _setting("TRILOGY_CLOUD_API", "TRILOGY_CLOUD_DEV_API")
JOB = _setting("TRILOGY_CLOUD_LIVE_JOB") or "hello-trilogy"
# A cold executor claims a dispatched run minutes after it is published.
RUN_TIMEOUT = int(_setting("TRILOGY_CLOUD_LIVE_TIMEOUT") or "900")
POLL_SECONDS = 10

BOGUS = "tri_not_a_real_token"

if not TOKEN or not API:
    pytest.skip(
        "live cloud tests need a token (TRILOGY_CLOUD_TOKEN or "
        "TRILOGY_DEV_JOB_TOKEN) and TRILOGY_CLOUD_API, in the environment or "
        f"in {REPO_ROOT / '.env.secrets'}",
        allow_module_level=True,
    )
if API.rstrip("/") == DEFAULT_API_URL:
    pytest.skip(
        "refusing to run live cloud tests against production; point "
        "TRILOGY_CLOUD_API at a dev environment",
        allow_module_level=True,
    )


def _run(
    *args: str, token: str | None = TOKEN, home: Path, expect_ok: bool = True
) -> subprocess.CompletedProcess[str]:
    """Invoke the real CLI with no credentials file reachable."""
    env = dict(os.environ)
    env.pop("HOMEDRIVE", None)
    env.pop("HOMEPATH", None)
    env["HOME"] = env["USERPROFILE"] = str(home)
    env["TRILOGY_CLOUD_API"] = API
    if token is None:
        env.pop("TRILOGY_CLOUD_TOKEN", None)
    else:
        env["TRILOGY_CLOUD_TOKEN"] = token
    proc = subprocess.run(
        [sys.executable, "-m", "trilogy.scripts.trilogy", *args],
        capture_output=True,
        text=True,
        env=env,
        timeout=RUN_TIMEOUT + 120,
        check=False,
    )
    if expect_ok and proc.returncode != 0:
        pytest.fail(
            f"{' '.join(args)} failed ({proc.returncode}):\n"
            f"{proc.stdout}\n{proc.stderr}"
        )
    return proc


def _said(proc: subprocess.CompletedProcess[str]) -> str:
    """Both streams: click writes errors to stderr, events to stdout."""
    return proc.stdout + proc.stderr


def _events(stdout: str) -> list[dict[str, Any]]:
    """`--format json` emits a stream of concatenated objects, not one doc."""
    decoder = json.JSONDecoder()
    out: list[dict[str, Any]] = []
    idx = 0
    while (idx := stdout.find("{", idx)) != -1:
        obj, end = decoder.raw_decode(stdout, idx)
        out.append(obj)
        idx = end
    return out


def _event(stdout: str, name: str) -> dict[str, Any]:
    matches = [e for e in _events(stdout) if e.get("event") == name]
    assert matches, f"no {name!r} event in:\n{stdout}"
    return matches[0]


@pytest.fixture(scope="module")
def home(tmp_path_factory) -> Path:
    """A home with no ~/.trilogy — the state a CI runner starts in."""
    path = tmp_path_factory.mktemp("runner-home")
    assert not (path / ".trilogy" / "cloud_credentials.json").exists()
    return path


class TestTokenFromTheEnvironment:
    def test_it_authenticates_with_no_credentials_file(self, home):
        out = _run("--format", "json", "cloud", "whoami", home=home).stdout
        assert "@" in _event(out, "whoami")["email"]

    def test_no_token_anywhere_names_every_source(self, home):
        proc = _run("cloud", "whoami", token=None, home=home, expect_ok=False)
        assert proc.returncode != 0
        assert "TRILOGY_CLOUD_TOKEN" in _said(proc)
        assert "--token" in _said(proc)

    def test_the_flag_wins_over_a_broken_env_var(self, home):
        out = _run(
            "--format",
            "json",
            "cloud",
            "--token",
            TOKEN,
            "whoami",
            token=BOGUS,
            home=home,
        ).stdout
        assert "@" in _event(out, "whoami")["email"]

    def test_the_flag_wins_over_a_working_env_var(self, home):
        proc = _run("cloud", "--token", BOGUS, "whoami", home=home, expect_ok=False)
        assert proc.returncode != 0
        assert "401" in _said(proc)

    def test_a_rejected_env_token_points_at_the_env_var(self, home):
        proc = _run("cloud", "whoami", token=BOGUS, home=home, expect_ok=False)
        assert "check $TRILOGY_CLOUD_TOKEN" in _said(proc)
        assert "cloud login" not in _said(proc)


@pytest.fixture(scope="module")
def deployed(home, tmp_path_factory) -> dict[str, Any]:
    """Fetch the job's own content and push it back — a real deploy whose
    payload is byte-identical to what is already live, so the suite never
    rewrites the job it borrows.

    That identity holds only while every file in the job matches ``jobs
    push``'s default include globs; one that does not would be dropped by the
    round-trip and the push would mint a version. The ``unchanged`` assertion
    below is what catches that, so do not relax it to "any successful push".
    """
    source = tmp_path_factory.mktemp("project")
    _run("cloud", "jobs", "fetch", JOB, "--dest", str(source), home=home)
    assert (source / "trilogy.toml").is_file()
    out = _run(
        "--format",
        "json",
        "cloud",
        "jobs",
        "push",
        "--source",
        str(source),
        "--name",
        JOB,
        home=home,
    ).stdout
    events = [e for e in _events(out) if str(e.get("event")).startswith("job_")]
    assert events, f"push emitted no job_* event:\n{out}"
    return events[0]


class TestDeploy:
    def test_pushing_identical_content_mints_no_version(self, deployed):
        assert deployed["outcome"] == "unchanged"
        assert deployed["job"]["name"] == JOB

    def test_the_push_reports_where_the_bytes_came_from(self, deployed):
        fingerprint = deployed["source_fingerprint"]
        assert re.fullmatch(r"[0-9a-f]{64}", fingerprint["content"])
        assert fingerprint["origin"] and fingerprint["origin_kind"]


@pytest.fixture(scope="module")
def waited_run(home, deployed) -> str:
    """Trigger once and block — every assertion below reads this same run,
    because each extra trigger is another cold start and another real query."""
    out = _run(
        "--format",
        "json",
        "cloud",
        "jobs",
        "run",
        JOB,
        "--wait",
        "--timeout",
        str(RUN_TIMEOUT),
        "--poll-seconds",
        str(POLL_SECONDS),
        home=home,
    ).stdout
    triggered = _event(out, "run_triggered")["run"]
    finished = _event(out, "run_finished")["run"]
    assert finished["id"] == triggered["id"]
    return out


class TestTriggerAndMonitor:
    def test_wait_reports_a_completed_run(self, waited_run):
        run = _event(waited_run, "run_finished")["run"]
        assert re.fullmatch(r"[0-9a-f-]{36}", run["id"])
        # A zero exit from --wait already implies this; asserting the payload
        # catches a wait that returned early on a still-pending run.
        assert run["status"] == "completed"
        assert run["exit_code"] == 0
        assert run["started_at"] and run["finished_at"]

    def test_the_timeline_shows_the_whole_lifecycle(self, home, waited_run):
        run_id = _event(waited_run, "run_finished")["run"]["id"]
        shown = _run("cloud", "runs", "show", run_id, home=home).stdout
        assert run_id in shown
        for phase in ("queued", "dispatched", "claimed", "running", "completed"):
            assert phase in shown, f"{phase!r} missing from timeline:\n{shown}"

    def test_the_job_output_reaches_the_client(self, home, waited_run):
        run_id = _event(waited_run, "run_finished")["run"]["id"]
        shown = _run("cloud", "runs", "show", run_id, home=home).stdout
        assert "stdout" in shown

    def test_waiting_on_a_finished_run_returns_immediately(self, home, waited_run):
        """CI retries re-issue this; it must be a cheap no-op, not a hang."""
        run_id = _event(waited_run, "run_finished")["run"]["id"]
        again = _run(
            "--format", "json", "cloud", "runs", "wait", run_id, home=home
        ).stdout
        assert _event(again, "run_finished")["run"]["status"] == "completed"
