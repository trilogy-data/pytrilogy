"""Cloud command for Trilogy CLI - interact with a trilogy-cloud environment.

Authentication is an API token (``tri_...``): ``trilogy cloud login`` runs a
browser OAuth loopback against the environment's API, which mints one and
hands it back on a localhost redirect; ``--token`` accepts a pre-issued value
instead. Tokens are stored per API URL in ``~/.trilogy/cloud_credentials.json``.

The target environment defaults to the production API. Point elsewhere with,
in order of precedence: ``--api``, ``TRILOGY_CLOUD_API``, or a ``[cloud]``
section in the working directory's ``trilogy.toml``::

    [cloud]
    api_url = "https://trilogy-cloud-api-dev.fly.dev"
    org = "trilogy"

Responses are parsed into the models in ``cloud_models``, which mirror the
API's own response types — an unexpected shape is a clean error naming the
field, not a ``KeyError`` three frames deep.

``jobs push`` bundles a local project directory into the job's inline files —
paths relative to ``--source``, so the directory shape (relative imports,
``sys.path`` tricks in Python datasources) survives the trip. This mirrors
trilogy-cloud's ``development/scripts/bundle_project.py``, including the
PubSub size budget: a bundle that cannot fit in a queue message is refused at
push time, not an hour later by a broker error.
"""

from __future__ import annotations

import fnmatch
import hmac
import json
import os
import secrets as pysecrets
import threading
import webbrowser
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import datetime
from functools import cache
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from typing import Any, TypeVar
from urllib.error import HTTPError, URLError
from urllib.parse import parse_qs, quote, urlparse
from urllib.request import Request, urlopen

import click
import tomllib
from pydantic import BaseModel, ValidationError

from trilogy.scripts.cloud_models import (
    IssuedToken,
    Job,
    JobRun,
    JobRunExt,
    Me,
    OrgSummary,
    Schedule,
    ScheduleExt,
    SecretMeta,
    TokenSummary,
)
from trilogy.scripts.display import (
    emit_event,
    is_json_mode,
    print_info,
    print_success,
    print_warning,
)
from trilogy.scripts.project_config import find_trilogy_config

DEFAULT_API_URL = "https://trilogy-cloud-api.fly.dev"
ENV_API_URL = "TRILOGY_CLOUD_API"
CREDENTIALS_PATH = Path.home() / ".trilogy" / "cloud_credentials.json"

# PubSub caps a published message at 10MB and the job's files ride inside one;
# same margin as trilogy-cloud's bundler so the two agree on what fits.
PUBSUB_MAX_BYTES = 10 * 1024 * 1024
SAFETY_MARGIN = 0.80

DEFAULT_INCLUDE = ("*.preql", "*.py", "*.sql", "*.toml", "*.json", "*.csv")
DEFAULT_EXCLUDE = (
    "*/__pycache__/*",
    "*/.venv/*",
    "*/tests/*",
    "*/test_*",
    "*.pyc",
    "*.parquet",
)

LOGIN_TIMEOUT_SECONDS = 300
# How much of a run's stdout/stderr to echo. Full logs are an artifact
# download, not a terminal dump.
RUN_LOG_TAIL_CHARS = 4000

M = TypeVar("M", bound=BaseModel)


class CloudError(click.ClickException):
    """An API or configuration failure, rendered as a clean CLI error."""


# ============================================================================
# Configuration and credentials
# ============================================================================


@cache
def _cloud_table(directory: Path) -> dict:
    """The ``[cloud]`` table of the nearest trilogy.toml at or above
    *directory*. Cached because org-scoped commands resolve both the API URL
    and the org, and neither should re-read the file. Never mutated."""
    config_path = find_trilogy_config(directory)
    if config_path is None:
        return {}
    try:
        parsed = tomllib.loads(config_path.read_text(encoding="utf-8"))
    except (OSError, tomllib.TOMLDecodeError) as exc:
        print_warning(f"Could not parse {config_path}: {exc}")
        return {}
    cloud = parsed.get("cloud", {})
    return cloud if isinstance(cloud, dict) else {}


def _project_cloud_config(start: Path | None = None) -> dict:
    return _cloud_table((start or Path.cwd()).resolve())


def resolve_api_url(explicit: str | None) -> str:
    """--api flag > TRILOGY_CLOUD_API env > trilogy.toml [cloud] > production."""
    if explicit:
        return explicit.rstrip("/")
    env = os.environ.get(ENV_API_URL)
    if env:
        return env.rstrip("/")
    from_config = _project_cloud_config().get("api_url")
    if from_config:
        return str(from_config).rstrip("/")
    return DEFAULT_API_URL


def _load_credentials() -> dict:
    if not CREDENTIALS_PATH.is_file():
        return {}
    try:
        return json.loads(CREDENTIALS_PATH.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}


def _save_credentials(creds: dict) -> None:
    CREDENTIALS_PATH.parent.mkdir(parents=True, exist_ok=True)
    CREDENTIALS_PATH.write_text(json.dumps(creds, indent=2), encoding="utf-8")
    try:
        CREDENTIALS_PATH.chmod(0o600)
    except OSError:
        pass  # best-effort on platforms without POSIX modes


def stored_token(api_url: str) -> str | None:
    entry = _load_credentials().get(api_url)
    return entry.get("token") if isinstance(entry, dict) else None


def store_token(api_url: str, token: str, email: str | None) -> None:
    creds = _load_credentials()
    creds[api_url] = {"token": token, "email": email}
    _save_credentials(creds)


def forget_token(api_url: str) -> bool:
    creds = _load_credentials()
    if api_url in creds:
        del creds[api_url]
        _save_credentials(creds)
        return True
    return False


# ============================================================================
# HTTP client
# ============================================================================


@dataclass
class CloudClient:
    api_url: str
    token: str | None

    def request(
        self,
        method: str,
        path: str,
        body: dict | list | bytes | None = None,
        timeout: float = 120.0,
    ) -> Any:
        """The decoded JSON body, or ``None`` when the response is empty.

        ``body`` may be pre-encoded bytes: ``jobs push`` measures its bundle
        against the queue limit and sends exactly what it measured, rather than
        serializing a multi-megabyte payload a second time.
        """
        url = f"{self.api_url}{path}"
        if isinstance(body, bytes):
            data: bytes | None = body
        elif body is not None:
            data = json.dumps(body).encode("utf-8")
        else:
            data = None
        headers = {"User-Agent": "trilogy-cli", "Accept": "application/json"}
        if self.token:
            headers["Authorization"] = f"Bearer {self.token}"
        if data is not None:
            headers["Content-Type"] = "application/json"
        req = Request(url, data=data, headers=headers, method=method)
        try:
            with urlopen(req, timeout=timeout) as resp:
                raw = resp.read()
        except HTTPError as exc:
            detail = exc.read().decode("utf-8", errors="replace").strip()
            hint = ""
            if exc.code == 401:
                hint = " (run `trilogy cloud login`)"
            raise CloudError(
                f"{method} {path} failed: {exc.code} {detail or exc.reason}{hint}"
            ) from exc
        except (URLError, TimeoutError) as exc:
            raise CloudError(f"Could not reach {self.api_url}: {exc}") from exc
        if not raw:
            return None
        try:
            return json.loads(raw.decode("utf-8"))
        except json.JSONDecodeError as exc:
            raise CloudError(f"{method} {path}: response was not JSON") from exc

    def get(self, path: str) -> Any:
        return self.request("GET", path)

    def post(self, path: str, body: dict | list | bytes | None = None) -> Any:
        return self.request("POST", path, body)

    def delete(self, path: str) -> None:
        self.request("DELETE", path)

    def get_one(self, path: str, model: type[M]) -> M:
        return self._validate("GET", path, model, self.get(path))

    def get_many(self, path: str, model: type[M]) -> list[M]:
        payload = self.get(path) or []
        if not isinstance(payload, list):
            raise CloudError(f"GET {path}: expected a list from {self.api_url}.")
        return [self._validate("GET", path, model, item) for item in payload]

    def post_one(
        self, path: str, model: type[M], body: dict | list | bytes | None = None
    ) -> M:
        return self._validate("POST", path, model, self.post(path, body))

    def _validate(self, method: str, path: str, model: type[M], payload: Any) -> M:
        try:
            return model.model_validate(payload)
        except ValidationError as exc:
            detail = "; ".join(
                f"{'.'.join(str(p) for p in err['loc']) or '<root>'}: {err['msg']}"
                for err in exc.errors()[:3]
            )
            raise CloudError(
                f"{method} {path}: unexpected response from {self.api_url} "
                f"({detail}). This CLI may be older than the API."
            ) from exc


def _client(ctx: click.Context, require_auth: bool = True) -> CloudClient:
    api_url = resolve_api_url(ctx.obj.get("CLOUD_API"))
    token = ctx.obj.get("CLOUD_TOKEN") or stored_token(api_url)
    if require_auth and not token:
        raise CloudError(
            f"Not logged in to {api_url}. Run `trilogy cloud login` "
            f"(or pass --token)."
        )
    return CloudClient(api_url=api_url, token=token)


def _resolve_org(ctx: click.Context, client: CloudClient) -> str:
    """--org flag > trilogy.toml [cloud].org > the sole membership."""
    explicit = ctx.obj.get("CLOUD_ORG")
    if explicit:
        return str(explicit)
    from_config = _project_cloud_config().get("org")
    if from_config:
        return str(from_config)
    me = client.get_one("/auth/me", Me)
    if len(me.orgs) == 1:
        return me.orgs[0].slug
    if not me.orgs:
        raise CloudError("You are not a member of any org on this environment.")
    slugs = ", ".join(o.slug for o in me.orgs)
    raise CloudError(f"Multiple orgs ({slugs}); pass --org to choose one.")


def _org_client(ctx: click.Context) -> tuple[CloudClient, str]:
    """Every org-scoped command's preamble: an authenticated client, and the
    org it should act on."""
    client = _client(ctx)
    return client, _resolve_org(ctx, client)


# ============================================================================
# Output helpers
# ============================================================================


def _ts(value: datetime | None, fallback: str = "-") -> str:
    return value.isoformat(sep=" ", timespec="seconds") if value else fallback


def _show_rows(
    event: str,
    key: str,
    rows: Sequence[M],
    empty: str,
    render: Callable[[M], str],
    org: str | None = None,
) -> None:
    """The shape every ``list`` subcommand shares: one JSON event, or a line
    per row with a dedicated message for the empty case."""
    if is_json_mode():
        # Annotated dict[str, Any]: emit_event's **fields sits alongside a typed
        # `discriminator` keyword, which a narrower value type collides with.
        fields: dict[str, Any] = {key: [row.model_dump(mode="json") for row in rows]}
        emit_event(event, org=org, **fields)
        return
    if not rows:
        print_info(empty)
        return
    for row in rows:
        print_info(render(row))


def _fmt_org(org: OrgSummary) -> str:
    return f"{org.slug}  {org.name!r}  role: {org.role}"


def _fmt_token(token: TokenSummary) -> str:
    return (
        f"{token.id}  {token.token_prefix}…  {token.name!r}  "
        f"expires: {_ts(token.expires_at, 'never')}  "
        f"last used: {_ts(token.last_used_at, 'never')}"
    )


def _fmt_job(job: Job) -> str:
    return (
        f"{job.id}  {job.name!r}  op: {job.operation}  "
        f"timeout: {job.timeout_seconds or 'default'}"
    )


def _fmt_run(run: JobRunExt) -> str:
    return (
        f"{run.id}  {run.job_name!r}  {run.status}  "
        f"created: {_ts(run.created_at)}  finished: {_ts(run.finished_at)}"
    )


def _fmt_schedule(schedule: ScheduleExt) -> str:
    active = "active" if schedule.is_active else "paused"
    return (
        f"{schedule.id}  {schedule.name!r}  {schedule.cron_expr!r}  {active}  "
        f"next: {_ts(schedule.next_run_at)}  "
        f"jobs: {', '.join(schedule.job_names) or '-'}"
    )


def _fmt_secret(secret: SecretMeta) -> str:
    return f"{secret.name}  updated: {_ts(secret.updated_at)}"


def _report_schedule(org: str, schedule: Schedule, prefix: str) -> None:
    if is_json_mode():
        emit_event(
            "schedule_created", org=org, schedule=schedule.model_dump(mode="json")
        )
        return
    print_success(
        f"{prefix} {schedule.name!r} at {schedule.cron_expr!r} "
        f"(next run {_ts(schedule.next_run_at)})"
    )


# ============================================================================
# Project bundling (mirrors trilogy-cloud/development/scripts/bundle_project.py)
# ============================================================================


def _matches(path: str, patterns: tuple[str, ...]) -> bool:
    return any(fnmatch.fnmatch(path, pat) for pat in patterns)


def collect_files(
    source: Path,
    include: tuple[str, ...] = DEFAULT_INCLUDE,
    exclude: tuple[str, ...] = DEFAULT_EXCLUDE,
) -> list[dict]:
    """``{name, content}`` entries for every bundled file, paths relative to
    *source* so the directory shape survives."""
    files: list[dict] = []
    for path in sorted(source.rglob("*")):
        if not path.is_file():
            continue
        rel = path.relative_to(source).as_posix()
        # Leading-slash form so "*/tests/*" also catches tests/ at the root.
        if _matches(f"/{rel}", exclude) or _matches(rel, exclude):
            continue
        if not _matches(rel, include):
            continue
        try:
            content = path.read_text(encoding="utf-8")
        except UnicodeDecodeError:
            print_warning(f"skip (not utf-8): {rel}")
            continue
        files.append({"name": rel, "content": content})
    return files


def apply_rewrites(text: str, rewrites: list[tuple[str, str]]) -> str:
    """Literal substring replacement — the strings being redirected (URLs,
    table addresses) are full of regex metacharacters."""
    for old, new in rewrites:
        text = text.replace(old, new)
    return text


def parse_rewrite(
    _ctx: click.Context, _param: click.Parameter, values: tuple[str, ...]
) -> list[tuple[str, str]]:
    parsed: list[tuple[str, str]] = []
    for raw in values:
        if "=" not in raw:
            raise click.BadParameter(f"--rewrite expects OLD=NEW, got {raw!r}")
        old, new = raw.split("=", 1)
        if not old:
            raise click.BadParameter("--rewrite OLD must not be empty")
        parsed.append((old, new))
    return parsed


def check_bundle_size(payload: dict) -> bytes:
    """The encoded payload, refused if it cannot fit in a queue message.

    Returns the bytes so the caller sends exactly what was measured, instead of
    serializing a multi-megabyte bundle twice.
    """
    encoded = json.dumps(payload).encode("utf-8")
    budget = int(PUBSUB_MAX_BYTES * SAFETY_MARGIN)
    if len(encoded) > budget:
        raise CloudError(
            f"Bundle is {len(encoded):,}B, over the {budget:,}B budget "
            f"({SAFETY_MARGIN:.0%} of PubSub's {PUBSUB_MAX_BYTES:,}B message "
            "limit). Narrow --include, add --exclude, or split the job."
        )
    return encoded


# ============================================================================
# Login loopback listener
# ============================================================================


class _LoginResult:
    token: str | None = None
    event: threading.Event

    def __init__(self) -> None:
        self.event = threading.Event()


def _make_login_handler(
    result: _LoginResult, expected_state: str
) -> type[BaseHTTPRequestHandler]:
    class Handler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:
            parsed = urlparse(self.path)
            # Anything but the callback (favicon probes, port scans) is a 404
            # — only the redirect we asked the API for is expected here.
            if parsed.path != "/callback":
                self.send_response(404)
                self.end_headers()
                return
            query = parse_qs(parsed.query)
            token = next(iter(query.get("token") or []), "")
            state = next(iter(query.get("state") or []), "")
            # The callback must echo the nonce we sent out in redirect_to.
            # Without this check, any local process (or any web page the user
            # has open, via a cross-origin GET to 127.0.0.1) could plant an
            # attacker-controlled credential while a login is pending.
            # Constant-time compare; a mismatch leaves the listener armed for
            # the genuine redirect rather than aborting the login.
            if not token or not hmac.compare_digest(state, expected_state):
                self.send_response(403)
                self.send_header("Content-Type", "text/html; charset=utf-8")
                self.end_headers()
                self.wfile.write(
                    b"<html><body><h2>Sign-in rejected.</h2>"
                    b"<p>Missing or mismatched login state.</p></body></html>"
                )
                return
            result.token = token
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.end_headers()
            self.wfile.write(
                b"<html><body><h2>Signed in.</h2>"
                b"<p>You can close this tab and return to your terminal.</p>"
                b"</body></html>"
            )
            result.event.set()

        def log_message(self, *args: Any) -> None:  # silence request logging
            pass

    return Handler


def browser_login(api_url: str) -> str:
    """Run the OAuth loopback: local listener, browser to the API, token back.

    A fresh random nonce rides out in ``redirect_to`` (``cli:{port}:{nonce}``)
    and must come back as ``state`` on the callback — the standard OAuth
    loopback CSRF guard, enforced in the handler above.
    """
    result = _LoginResult()
    nonce = pysecrets.token_urlsafe(32)
    server = HTTPServer(("127.0.0.1", 0), _make_login_handler(result, nonce))
    port = server.server_address[1]
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        login_url = (
            f"{api_url}/auth/google/login?redirect_to={quote(f'cli:{port}:{nonce}')}"
        )
        print_info(f"Opening browser for sign-in: {login_url}")
        print_info("Complete the sign-in there; waiting for the redirect...")
        if not webbrowser.open(login_url):
            print_warning("Could not open a browser; visit the URL above manually.")
        if not result.event.wait(timeout=LOGIN_TIMEOUT_SECONDS):
            raise CloudError("Timed out waiting for the browser sign-in.")
    finally:
        server.shutdown()
        thread.join(timeout=5)
        server.server_close()
    if result.token is None:
        raise CloudError("Browser sign-in completed without returning a token.")
    return result.token


# ============================================================================
# Commands
# ============================================================================


@click.group()
@click.option(
    "--api",
    "api_url",
    default=None,
    help="Cloud API base URL (default: production, or trilogy.toml [cloud].api_url).",
)
@click.option(
    "--org",
    "org_slug",
    default=None,
    help="Org slug (default: trilogy.toml [cloud].org, or your only membership).",
)
@click.option(
    "--token",
    default=None,
    help="API token to use for this invocation instead of stored credentials.",
)
@click.pass_context
def cloud(
    ctx: click.Context, api_url: str | None, org_slug: str | None, token: str | None
) -> None:
    """Interact with a trilogy-cloud environment (jobs, runs, schedules)."""
    ctx.ensure_object(dict)
    ctx.obj["CLOUD_API"] = api_url
    ctx.obj["CLOUD_ORG"] = org_slug
    ctx.obj["CLOUD_TOKEN"] = token


@cloud.command()
@click.option(
    "--token",
    "token_value",
    default=None,
    help="Store this pre-issued API token instead of signing in via browser.",
)
@click.pass_context
def login(ctx: click.Context, token_value: str | None) -> None:
    """Sign in and store an API token for this environment."""
    api_url = resolve_api_url(ctx.obj.get("CLOUD_API"))
    token = token_value or ctx.obj.get("CLOUD_TOKEN") or browser_login(api_url)

    client = CloudClient(api_url=api_url, token=token)
    me = client.get_one("/auth/me", Me)
    store_token(api_url, token, me.email)

    orgs = ", ".join(o.slug for o in me.orgs) or "none"
    if is_json_mode():
        emit_event("login", api=api_url, email=me.email, orgs=orgs)
    else:
        print_success(f"Logged in to {api_url} as {me.email} (orgs: {orgs})")


@cloud.command()
@click.pass_context
def logout(ctx: click.Context) -> None:
    """Forget the stored token for this environment."""
    api_url = resolve_api_url(ctx.obj.get("CLOUD_API"))
    if forget_token(api_url):
        print_success(f"Logged out of {api_url}")
    else:
        print_info(f"No stored credentials for {api_url}")


@cloud.command()
@click.pass_context
def whoami(ctx: click.Context) -> None:
    """Show the signed-in user and their orgs."""
    client = _client(ctx)
    me = client.get_one("/auth/me", Me)
    if is_json_mode():
        emit_event("whoami", **me.model_dump(mode="json"))
        return
    print_info(f"API:   {client.api_url}")
    print_info(f"User:  {me.email} ({me.name or 'no name'})")
    for org in me.orgs:
        print_info(f"Org:   {org.slug} ({org.role})")


@cloud.group()
def tokens() -> None:
    """Manage API tokens for the signed-in user."""


@tokens.command("list")
@click.pass_context
def tokens_list(ctx: click.Context) -> None:
    """List your API tokens (values are never shown)."""
    client = _client(ctx)
    _show_rows(
        "tokens",
        "tokens",
        client.get_many("/auth/tokens", TokenSummary),
        "No API tokens.",
        _fmt_token,
    )


@tokens.command("create")
@click.argument("name")
@click.option(
    "--expires-in-days",
    type=int,
    default=None,
    help="Lifetime; omit for non-expiring.",
)
@click.pass_context
def tokens_create(ctx: click.Context, name: str, expires_in_days: int | None) -> None:
    """Create a token. The value is printed once and never again."""
    client = _client(ctx)
    body: dict = {"name": name}
    if expires_in_days is not None:
        body["expires_in_days"] = expires_in_days
    issued = client.post_one("/auth/tokens", IssuedToken, body)
    if is_json_mode():
        emit_event("token_created", **issued.model_dump(mode="json"))
        return
    print_success(f"Created token {issued.name!r} ({issued.id})")
    print_info("Store this value now; it cannot be retrieved later:")
    click.echo(issued.token)


@tokens.command("revoke")
@click.argument("token_id")
@click.pass_context
def tokens_revoke(ctx: click.Context, token_id: str) -> None:
    """Revoke (delete) a token by id."""
    client = _client(ctx)
    client.delete(f"/auth/tokens/{token_id}")
    print_success(f"Revoked token {token_id}")


@cloud.command()
@click.pass_context
def orgs(ctx: click.Context) -> None:
    """List orgs you belong to on this environment."""
    client = _client(ctx)
    _show_rows(
        "orgs",
        "orgs",
        client.get_many("/orgs", OrgSummary),
        "You are not a member of any org on this environment.",
        _fmt_org,
    )


@cloud.group()
def jobs() -> None:
    """Manage jobs in an org."""


@jobs.command("list")
@click.pass_context
def jobs_list(ctx: click.Context) -> None:
    """List the org's jobs."""
    client, org = _org_client(ctx)
    _show_rows(
        "jobs",
        "jobs",
        client.get_many(f"/orgs/{org}/jobs", Job),
        f"No jobs in org {org!r}.",
        _fmt_job,
        org=org,
    )


def _find_job(jobs_: Sequence[Job], org: str, name_or_id: str) -> Job:
    """Resolve a job reference against an already-fetched list, so a command
    resolving several of them (``schedules create``) fetches once rather than
    once per name."""
    for job in jobs_:
        if job.id == name_or_id:
            return job
    by_name = [j for j in jobs_ if j.name == name_or_id]
    if len(by_name) == 1:
        return by_name[0]
    if len(by_name) > 1:
        raise CloudError(
            f"Multiple jobs named {name_or_id!r}; use an id: "
            + ", ".join(j.id for j in by_name)
        )
    raise CloudError(f"No job named {name_or_id!r} in org {org!r}.")


@jobs.command("push")
@click.option(
    "--source",
    type=click.Path(exists=True, file_okay=False, path_type=Path),
    required=True,
    help="Project directory to bundle.",
)
@click.option(
    "--config",
    "config_path",
    type=click.Path(exists=True, dir_okay=False, path_type=Path),
    default=None,
    help="trilogy.toml for the job (default: <source>/trilogy.toml).",
)
@click.option("--name", required=True, help="Job name.")
@click.option("--description", default=None)
@click.option(
    "--operation", default="run", type=click.Choice(["run", "refresh", "plan", "state"])
)
@click.option("--timeout-seconds", type=int, default=None)
@click.option("--memory-mb", type=int, default=None)
@click.option("--cpus", type=float, default=None)
@click.option(
    "--secret-env",
    multiple=True,
    help="Org secret names to inject as env vars (repeatable).",
)
@click.option(
    "--include",
    multiple=True,
    help=f"Glob to include (default: {', '.join(DEFAULT_INCLUDE)}).",
)
@click.option("--exclude", multiple=True, help="Glob to exclude (adds to defaults).")
@click.option(
    "--rewrite",
    multiple=True,
    callback=parse_rewrite,
    help="Literal OLD=NEW substitution applied to file contents and the config (repeatable).",
)
@click.option(
    "--rewrite-glob",
    multiple=True,
    help="Restrict --rewrite to files matching these globs (default: all).",
)
@click.option(
    "--cron",
    default=None,
    help="Also create a schedule with this cron expression (UTC).",
)
@click.pass_context
def jobs_push(
    ctx: click.Context,
    source: Path,
    config_path: Path | None,
    name: str,
    description: str | None,
    operation: str,
    timeout_seconds: int | None,
    memory_mb: int | None,
    cpus: float | None,
    secret_env: tuple[str, ...],
    include: tuple[str, ...],
    exclude: tuple[str, ...],
    rewrite: list[tuple[str, str]],
    rewrite_glob: tuple[str, ...],
    cron: str | None,
) -> None:
    """Create a job from a local project directory."""
    client, org = _org_client(ctx)

    config_file = config_path or (source / "trilogy.toml")
    if not config_file.is_file():
        raise CloudError(
            f"No config at {config_file}; pass --config or add a trilogy.toml "
            "to the source directory."
        )

    include_pats = tuple(include) if include else DEFAULT_INCLUDE
    exclude_pats = DEFAULT_EXCLUDE + tuple(exclude)
    files = collect_files(source.resolve(), include_pats, exclude_pats)
    if not files:
        raise CloudError(f"No files matched under {source}.")
    # The job config is authoritative on the platform side; keep a copy out of
    # the file list so the bundle root does not shadow it with a second one.
    files = [f for f in files if f["name"] != "trilogy.toml"]

    config_text = config_file.read_text(encoding="utf-8")
    if rewrite:
        globs = rewrite_glob or ("*",)
        touched = 0
        for f in files:
            if _matches(f["name"], globs):
                rewritten = apply_rewrites(f["content"], rewrite)
                if rewritten != f["content"]:
                    f["content"] = rewritten
                    touched += 1
        config_text = apply_rewrites(config_text, rewrite)
        print_info(f"Applied rewrites to {touched} file(s)")

    if any(j.name == name for j in client.get_many(f"/orgs/{org}/jobs", Job)):
        print_warning(
            f"A job named {name!r} already exists in {org!r}; this push "
            "creates another with the same name."
        )

    payload: dict = {
        "name": name,
        "config": config_text,
        "files": files,
        "operation": operation,
    }
    if description:
        payload["description"] = description
    if timeout_seconds is not None:
        payload["timeout_seconds"] = timeout_seconds
    if memory_mb is not None:
        payload["memory_mb"] = memory_mb
    if cpus is not None:
        payload["cpus"] = cpus
    if secret_env:
        payload["secret_env"] = list(secret_env)

    encoded = check_bundle_size(payload)
    print_info(f"Bundled {len(files)} files ({len(encoded):,} bytes) from {source}")

    job = client.post_one(f"/orgs/{org}/jobs", Job, encoded)
    if is_json_mode():
        emit_event("job_created", org=org, job=job.model_dump(mode="json"))
    else:
        print_success(f"Created job {job.name!r} ({job.id}) in org {org!r}")

    if cron:
        schedule = client.post_one(
            f"/orgs/{org}/schedules",
            Schedule,
            {"name": f"{name}-schedule", "cron_expr": cron, "job_ids": [job.id]},
        )
        _report_schedule(org, schedule, "Scheduled")


@jobs.command("run")
@click.argument("job")
@click.pass_context
def jobs_run(ctx: click.Context, job: str) -> None:
    """Trigger a run of a job (by name or id)."""
    client, org = _org_client(ctx)
    found = _find_job(client.get_many(f"/orgs/{org}/jobs", Job), org, job)
    run = client.post_one(f"/orgs/{org}/jobs/{found.id}/run", JobRun)
    if is_json_mode():
        emit_event("run_triggered", org=org, run=run.model_dump(mode="json"))
    else:
        print_success(
            f"Triggered run {run.id} of {found.name!r} (status: {run.status})"
        )


@cloud.group()
def runs() -> None:
    """Inspect job runs."""


@runs.command("list")
@click.option(
    "--limit",
    type=int,
    default=15,
    help="How many recent runs to show. The API returns at most 50, so a "
    "larger value cannot show more.",
)
@click.pass_context
def runs_list(ctx: click.Context, limit: int) -> None:
    """Recent runs across the org's jobs."""
    client, org = _org_client(ctx)
    # Sliced here rather than server-side: the endpoint takes no limit
    # parameter, and already caps its own result set.
    rows = client.get_many(f"/orgs/{org}/jobs/runs", JobRunExt)[:limit]
    _show_rows("runs", "runs", rows, f"No runs in org {org!r}.", _fmt_run, org=org)


@runs.command("show")
@click.argument("run_id")
@click.pass_context
def runs_show(ctx: click.Context, run_id: str) -> None:
    """Run detail: timeline events and per-file results."""
    client, org = _org_client(ctx)
    run = client.get_one(f"/orgs/{org}/jobs/runs/{run_id}", JobRunExt)
    if is_json_mode():
        emit_event("run", org=org, run=run.model_dump(mode="json"))
        return
    print_info(f"Run {run.id} of {run.job_name!r}: {run.status}")
    if run.exit_code is not None:
        print_info(f"Exit code: {run.exit_code}")
    for event in run.events:
        print_info(f"  [{_ts(event.created_at)}] {event.type}: {event.message}")
    if run.files:
        print_info("Files:")
        for step in run.files:
            print_info(f"  {step.status}: {step.name}")
    for stream, body in (("stdout", run.stdout), ("stderr", run.stderr)):
        text = (body or "").strip()
        if text:
            print_info(f"--- {stream} (tail) ---")
            click.echo(text[-RUN_LOG_TAIL_CHARS:])


@cloud.group()
def schedules() -> None:
    """Manage schedules in an org."""


@schedules.command("list")
@click.pass_context
def schedules_list(ctx: click.Context) -> None:
    """List the org's schedules."""
    client, org = _org_client(ctx)
    _show_rows(
        "schedules",
        "schedules",
        client.get_many(f"/orgs/{org}/schedules", ScheduleExt),
        f"No schedules in org {org!r}.",
        _fmt_schedule,
        org=org,
    )


@schedules.command("create")
@click.argument("jobs_args", metavar="JOB...", nargs=-1, required=True)
@click.option("--name", required=True, help="Schedule name.")
@click.option("--cron", required=True, help="Cron expression (UTC; 5- or 6-field).")
@click.pass_context
def schedules_create(
    ctx: click.Context, jobs_args: tuple[str, ...], name: str, cron: str
) -> None:
    """Create a schedule binding one or more jobs (by name or id)."""
    client, org = _org_client(ctx)
    known = client.get_many(f"/orgs/{org}/jobs", Job)
    job_ids = [_find_job(known, org, j).id for j in jobs_args]
    schedule = client.post_one(
        f"/orgs/{org}/schedules",
        Schedule,
        {"name": name, "cron_expr": cron, "job_ids": job_ids},
    )
    _report_schedule(org, schedule, "Created schedule")


@schedules.command("delete")
@click.argument("schedule_id")
@click.pass_context
def schedules_delete(ctx: click.Context, schedule_id: str) -> None:
    """Delete a schedule by id."""
    client, org = _org_client(ctx)
    client.delete(f"/orgs/{org}/schedules/{schedule_id}")
    print_success(f"Deleted schedule {schedule_id}")


@cloud.group()
def secrets() -> None:
    """Manage org secrets (values are write-only)."""


@secrets.command("list")
@click.pass_context
def secrets_list(ctx: click.Context) -> None:
    """List secret names and timestamps — never values."""
    client, org = _org_client(ctx)
    _show_rows(
        "secrets",
        "secrets",
        client.get_many(f"/orgs/{org}/secrets", SecretMeta),
        f"No secrets in org {org!r}.",
        _fmt_secret,
        org=org,
    )


@secrets.command("set")
@click.argument("name")
@click.option("--value", default=None, help="Secret value (prompted for if omitted).")
@click.pass_context
def secrets_set(ctx: click.Context, name: str, value: str | None) -> None:
    """Create or update a secret."""
    client, org = _org_client(ctx)
    if value is None:
        value = click.prompt(f"Value for {name}", hide_input=True)
    client.post(f"/orgs/{org}/secrets", {"name": name, "value": value})
    print_success(f"Set secret {name!r} in org {org!r}")


@secrets.command("delete")
@click.argument("name")
@click.pass_context
def secrets_delete(ctx: click.Context, name: str) -> None:
    """Delete a secret."""
    client, org = _org_client(ctx)
    client.delete(f"/orgs/{org}/secrets/{name}")
    print_success(f"Deleted secret {name!r} from org {org!r}")


if __name__ == "__main__":
    cloud()  # pragma: no cover - direct-module entrypoint
