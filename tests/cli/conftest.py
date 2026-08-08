"""A lightweight in-process stand-in for the trilogy-cloud API.

``trilogy cloud`` talks to the platform through exactly one seam — the
``urlopen`` in ``trilogy.scripts.cloud`` — so replacing that seam exercises the
whole stack a real invocation would: request construction, auth headers, status
handling, JSON decoding, pydantic validation, and the command's own rendering.
The alternative (stubbing ``CloudClient``) would skip most of the code the
commands actually depend on.

Routes are seeded with realistic payloads for every endpoint the CLI calls, so
a test only declares the part it cares about::

    def test_x(cloud_api, run_cloud):
        cloud_api.set("GET", "/auth/tokens", [])
        assert "No API tokens" in run_cloud("tokens", "list").output
"""

from __future__ import annotations

import io
import json
from dataclasses import dataclass, field
from typing import Any, Self
from urllib.error import HTTPError, URLError
from urllib.parse import parse_qs
from urllib.request import Request

import pytest
from click.testing import CliRunner, Result

from trilogy.scripts import cloud as cloud_mod
from trilogy.scripts import display_core

TS = "2026-07-28T12:00:00Z"
API_URL = "https://api.test"
ORG = "acme"


class _FakeResponse:
    def __init__(self, body: bytes) -> None:
        self._body = body

    def read(self) -> bytes:
        return self._body

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *exc: object) -> bool:
        return False


@dataclass
class Failure:
    """A route that answers with an HTTP error status."""

    status: int
    detail: str = ""


@dataclass
class Raw:
    """A route that answers with bytes verbatim, bypassing JSON encoding."""

    body: bytes


@dataclass
class RecordedCall:
    method: str
    #: Path only. Routes are registered and matched without a query string —
    #: the server distinguishes endpoints by path, and a route table keyed by
    #: full URL would need re-registering for every filter combination.
    path: str
    body: Any
    headers: dict[str, str]
    data: bytes | None
    #: Parsed query string, so a test can assert a filter was sent server-side
    #: rather than applied after the fact.
    query: dict[str, list[str]] = field(default_factory=dict)


_MISSING = object()


def _org_summary(slug: str = ORG, role: str = "admin") -> dict:
    return {
        "id": f"org-{slug}",
        "name": slug.title(),
        "slug": slug,
        "role": role,
        "created_at": TS,
    }


def _job(job_id: str = "job-1", name: str = "nightly", **over: Any) -> dict:
    return {
        "id": job_id,
        "org_id": f"org-{ORG}",
        "name": name,
        "operation": "run",
        "timeout_seconds": 600,
        "current_version_id": f"{job_id}-v1",
        "created_at": TS,
        "updated_at": TS,
        **over,
    }


#: What a server that records push provenance answers with. Every deployed one
#: drops the field today, so the CLI must read it as absent — the seed carries
#: it so the rendering path is covered either way.
_SOURCE_FINGERPRINT = {
    "version": 1,
    "content": "a" * 64,
    "origin": "github.com/acme/models",
    "origin_kind": "git",
    "path": "etl",
    "revision": "e05bdfb7" + "0" * 32,
    "branch": "main",
}


def _job_version(number: int = 1, job_id: str = "job-1", **over: Any) -> dict:
    return {
        "id": f"{job_id}-v{number}",
        "job_id": job_id,
        "version_number": number,
        "operation": "run",
        "timeout_seconds": 600,
        "created_at": TS,
        **over,
    }


def _run(run_id: str = "run-1", **over: Any) -> dict:
    return {
        "id": run_id,
        "job_id": "job-1",
        "job_name": "nightly",
        "status": "succeeded",
        "created_at": TS,
        "finished_at": TS,
        "exit_code": 0,
        **over,
    }


def _schedule(schedule_id: str = "sched-1", **over: Any) -> dict:
    return {
        "id": schedule_id,
        "org_id": f"org-{ORG}",
        "name": "nightly-schedule",
        "cron_expr": "0 3 * * *",
        "is_active": True,
        "next_run_at": TS,
        "created_at": TS,
        "updated_at": TS,
        **over,
    }


@dataclass
class FakeCloudAPI:
    """Route table plus call log for the cloud API.

    ``*`` in a registered path matches one segment, so id-bearing routes
    (``/orgs/acme/jobs/*/run``) need no per-test registration.
    """

    url: str = API_URL
    org: str = ORG
    routes: dict[tuple[str, str], Any] = field(default_factory=dict)
    calls: list[RecordedCall] = field(default_factory=list)
    offline: bool = False

    def __post_init__(self) -> None:
        self.seed()

    def seed(self) -> None:
        self.routes.update(
            {
                ("GET", "/auth/me"): {
                    "id": "user-1",
                    "email": "dev@example.com",
                    "name": "Dev",
                    "provider": "google",
                    "orgs": [_org_summary()],
                },
                ("GET", "/auth/tokens"): [
                    {
                        "id": "tok-1",
                        "name": "laptop",
                        "token_prefix": "tri_abc",
                        "created_at": TS,
                        "last_used_at": TS,
                        "expires_at": None,
                    }
                ],
                ("POST", "/auth/tokens"): {
                    "id": "tok-2",
                    "name": "ci",
                    "token": "tri_secret_value",
                    "token_prefix": "tri_sec",
                    "created_at": TS,
                },
                ("DELETE", "/auth/tokens/*"): Raw(b""),
                ("GET", "/orgs"): [_org_summary()],
                ("GET", f"/orgs/{ORG}/jobs"): [_job()],
                ("POST", f"/orgs/{ORG}/jobs"): _job("job-new", "fresh"),
                # A PUT that moved the job on: same id, a version id the
                # caller did not already hold, which is how push tells an
                # update from a content no-op.
                ("PUT", f"/orgs/{ORG}/jobs/*"): _job(
                    current_version_id="job-1-v2", updated_at=TS
                ),
                ("GET", f"/orgs/{ORG}/jobs/*/versions"): [
                    _job_version(2, source_fingerprint=_SOURCE_FINGERPRINT),
                    _job_version(1),
                ],
                ("POST", f"/orgs/{ORG}/jobs/*/run"): _run("run-new", status="queued"),
                ("GET", f"/orgs/{ORG}/jobs/runs"): [_run(), _run("run-2")],
                ("GET", f"/orgs/{ORG}/jobs/runs/*"): _run(
                    events=[
                        {
                            "id": "ev-1",
                            "run_id": "run-1",
                            "type": "started",
                            "message": "worker picked up the run",
                            "created_at": TS,
                        }
                    ],
                    files=[
                        {
                            "id": "step-1",
                            "run_id": "run-1",
                            "name": "model.preql",
                            "order": 0,
                            "status": "succeeded",
                        }
                    ],
                    stdout="hello from the worker\n",
                    stderr="  ",
                ),
                ("GET", f"/orgs/{ORG}/schedules"): [_schedule(job_names=["nightly"])],
                ("POST", f"/orgs/{ORG}/schedules"): _schedule("sched-new"),
                ("DELETE", f"/orgs/{ORG}/schedules/*"): Raw(b""),
                ("GET", f"/orgs/{ORG}/secrets"): [
                    {"name": "SNOWFLAKE_PASSWORD", "created_at": TS, "updated_at": TS}
                ],
                ("POST", f"/orgs/{ORG}/secrets"): Raw(b""),
                ("DELETE", f"/orgs/{ORG}/secrets/*"): Raw(b""),
            }
        )

    def set(self, method: str, path: str, payload: Any) -> None:
        self.routes[(method, path)] = payload

    def fail(self, method: str, path: str, status: int, detail: str = "") -> None:
        self.routes[(method, path)] = Failure(status, detail)

    def set_raw(self, method: str, path: str, body: bytes) -> None:
        """Answer with bytes verbatim — an empty or malformed body."""
        self.routes[(method, path)] = Raw(body)

    def call_for(self, method: str, path: str) -> RecordedCall:
        """The last matching call, for asserting on its body or headers."""
        for call in reversed(self.calls):
            if call.method == method and call.path == path:
                return call
        raise AssertionError(f"no {method} {path} in {[c.path for c in self.calls]}")

    def body_for(self, method: str, path: str) -> Any:
        return self.call_for(method, path).body

    def requests_for(self, method: str, path: str) -> list[Any]:
        """Every matching call's body, oldest first — empty when there were
        none. The plural of ``body_for``, and the only way to assert a call did
        *not* happen (a sync that should have updated must not also create)."""
        return [c.body for c in self.calls if c.method == method and c.path == path]

    def _lookup(self, method: str, path: str) -> Any:
        exact = self.routes.get((method, path), _MISSING)
        if exact is not _MISSING:
            return exact
        segments = path.split("/")
        for (route_method, route_path), payload in self.routes.items():
            if route_method != method or "*" not in route_path:
                continue
            parts = route_path.split("/")
            if len(parts) == len(segments) and all(
                p == "*" or p == s for p, s in zip(parts, segments)
            ):
                return payload
        return _MISSING

    def urlopen(self, req: Request, timeout: float | None = None) -> _FakeResponse:
        if self.offline:
            raise URLError("connection refused")
        path, _, query = req.full_url[len(self.url) :].partition("?")
        try:
            body = json.loads(req.data.decode("utf-8")) if req.data else None
        except (UnicodeDecodeError, json.JSONDecodeError):
            body = None
        self.calls.append(
            RecordedCall(
                req.get_method(),
                path,
                body,
                {k.lower(): v for k, v in req.header_items()},
                req.data,
                parse_qs(query),
            )
        )
        payload = self._lookup(req.get_method(), path)
        if payload is _MISSING:
            payload = Failure(404, f"no route for {req.get_method()} {path}")
        if isinstance(payload, Failure):
            raise HTTPError(
                req.full_url,
                payload.status,
                "Error",
                {},  # type: ignore[arg-type]
                io.BytesIO(payload.detail.encode("utf-8")),
            )
        if isinstance(payload, Raw):
            return _FakeResponse(payload.body)
        return _FakeResponse(json.dumps(payload).encode("utf-8"))


@pytest.fixture
def cloud_api(tmp_path, monkeypatch) -> FakeCloudAPI:
    """A seeded fake API, wired in as the cloud module's transport.

    Also isolates the two pieces of ambient state a cloud command reads:
    the credentials file and the working directory's trilogy.toml.
    """
    api = FakeCloudAPI()
    monkeypatch.setattr(cloud_mod, "urlopen", api.urlopen)
    monkeypatch.setattr(cloud_mod, "CREDENTIALS_PATH", tmp_path / "creds.json")
    monkeypatch.setenv(cloud_mod.ENV_API_URL, api.url)
    workdir = tmp_path / "work"
    workdir.mkdir()
    monkeypatch.chdir(workdir)
    cloud_mod._cloud_table.cache_clear()
    yield api
    cloud_mod._cloud_table.cache_clear()


@pytest.fixture
def logged_in(cloud_api: FakeCloudAPI) -> FakeCloudAPI:
    cloud_mod.store_token(cloud_api.url, "tri_stored", "dev@example.com")
    return cloud_api


@pytest.fixture
def run_cloud():
    """Invoke a ``trilogy cloud`` subcommand in-process."""
    runner = CliRunner()

    def invoke(*args: str, **kwargs: Any) -> Result:
        return runner.invoke(cloud_mod.cloud, list(args), obj={}, **kwargs)

    return invoke


@pytest.fixture
def json_mode():
    display_core.set_output_format("json")
    yield
    display_core.set_output_format("rich")
