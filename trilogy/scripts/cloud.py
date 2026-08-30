"""Cloud command for Trilogy CLI - interact with a trilogy-cloud environment.

Authentication is an API token (``tri_...``): ``trilogy cloud login`` runs a
browser OAuth loopback against the environment's API, which mints one and
hands it back on a localhost redirect; ``--token`` accepts a pre-issued value
instead. Tokens are stored per API URL in ``~/.trilogy/cloud_credentials.json``.

Commands resolve which token to send in order of precedence: ``--token``,
``TRILOGY_CLOUD_TOKEN``, then the credentials file. ``login`` and ``logout``
are the exceptions: they manage the credentials file and never read the
variable, but warn when it is set, since it would shadow what they just wrote.

The target environment defaults to the production API. Point elsewhere with,
in order of precedence: ``--api``, ``TRILOGY_CLOUD_API``, or a ``[cloud]``
section in the working directory's ``trilogy.toml``::

    [cloud]
    api_url = "https://trilogy-cloud-api-dev.fly.dev"
    org = "trilogy"

Responses are parsed into the models in ``cloud_models``, which mirror the
API's own response types, so an unexpected shape is an error naming the field.

``jobs push`` bundles a local project directory into the job's inline files —
paths relative to ``--source``, so the directory shape (relative imports,
``sys.path`` tricks in Python datasources) survives the trip. A bundle over the
PubSub size budget is refused at push time.

**Push is an upsert, keyed by job name.** Jobs are versioned platform-side —
the row is stable identity and its content is a copy of the newest version — so
re-pushing a job ``PUT``s new content under the same id: run history and
schedule bindings survive, and content the job already has mints no version.
``--create`` forces a new job instead.

Every push carries a ``source_fingerprint`` (see ``cloud_models``): the digest
of the bytes sent, plus where they came from — the git remote and commit when
the source directory is in a repository, and an opaque local token when it is
not. Absolute paths never leave the machine. A server that does not record the
field ignores it.

``jobs fetch`` and ``workspaces fetch`` are the inverse, and the read half of a
fetch-edit-push loop: an entity's config and inline files written back to a
directory at the paths they were bundled from, ready to be edited and sent back
with ``jobs push`` / ``workspaces push``.

**A fetch writes what a push would send back.** Each command exports exactly
the entity's *own* content, so the round trip is lossless and re-pushing an
unedited fetch mints no version. ``--resolved`` is the exception: it also
materializes the files a job or workspace *inherits* from the workspace chain
above it, which makes the directory runnable but no longer a bundle to push
back — pushing it would install a copy of the shared tree into the thing that
was inheriting it. Both commands say so.

A job in a workspace carries **no files of its own** — the workspace holds the
whole tree and the job names the one script it runs — so fetching such a job
without ``--resolved`` writes a config and nothing else, and says where its
files live.

``jobs run`` returns as soon as the run is queued, so on its own a zero exit
means "accepted", not "succeeded". ``--wait`` (and ``runs wait <id>``) blocks
until the server stamps the run finished and then exits non-zero unless it
ended ``completed``.

``cloud sync`` is the declarative form of the same push. It walks a directory
tree, treats every ``trilogy.toml`` whose ``[cloud]`` block declares *how the
job runs* as one deployable project, and upserts each against its
``source_key`` — the repository-and-subdirectory it lives in — rather than
against its name. Which environment it deploys into comes from the branch, so
one command in CI sends main to production and a feature branch to a namespace
of its own; ``cloud env`` manages those namespaces.

**Production is the absence of an environment**, and each command spells it
rather than naming it: ``sync`` reaches production by ``--production`` (or the
empty ``--environment`` a CI step templates), and ``env delete`` on an
environment holding jobs takes ``--with-jobs`` or ``--keep-jobs``, since
deleting the record alone moves them into production with their schedules.
"""

from __future__ import annotations

import fnmatch
import hmac
import json
import os
import secrets as pysecrets
import threading
import time
import webbrowser
from collections import Counter
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from dataclasses import fields as dataclass_fields
from datetime import datetime
from functools import cache
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path, PurePosixPath, PureWindowsPath
from typing import Any, TypeVar
from urllib.error import HTTPError, URLError
from urllib.parse import parse_qs, quote, urlencode, urlparse
from urllib.request import Request, urlopen

import click
import tomllib
from pydantic import BaseModel, ValidationError

from trilogy.scripts.click_utils import dry_run_option
from trilogy.scripts.cloud_models import (
    Environment,
    EnvironmentExt,
    IssuedToken,
    Job,
    JobRun,
    JobRunExt,
    JobVersion,
    Me,
    OrgSummary,
    Schedule,
    ScheduleExt,
    SecretMeta,
    SourceFingerprint,
    TokenSummary,
    Workspace,
)
from trilogy.scripts.display import (
    emit_event,
    is_json_mode,
    print_info,
    print_success,
    print_warning,
)
from trilogy.scripts.project_config import find_trilogy_config
from trilogy.scripts.source_identity import (
    SourceOrigin,
    content_digest,
    environment_label,
    is_valid_environment_name,
    resolve_origin,
)

DEFAULT_API_URL = "https://trilogy-cloud-api.fly.dev"
ENV_API_URL = "TRILOGY_CLOUD_API"
ENV_TOKEN = "TRILOGY_CLOUD_TOKEN"
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

#: A run is over when the server stamps ``finished_at``. The status set is a
#: belt-and-braces second signal, so a terminal state that somehow carries no
#: timestamp still ends the wait instead of hanging until the timeout.
TERMINAL_RUN_STATUSES = frozenset(
    {"completed", "failed", "cancelled", "canceled", "timed_out", "errored", "error"}
)
SUCCESS_RUN_STATUS = "completed"
WAIT_POLL_SECONDS = 5
#: Generous by default: a cold executor can take minutes just to claim a run,
#: and the job's own timeout is the real upper bound on the work itself.
WAIT_TIMEOUT_SECONDS = 1800
# The runs route clamps to this itself; mirrored so `--limit 500` is reported
# as the cap it will actually get rather than silently truncated.
RUNS_MAX_LIMIT = 200
# What the platform assumes when a job names no verb; spelled out here because
# push has to distinguish "unspecified" from "explicitly run" to preserve an
# existing job's operation across an update.
DEFAULT_OPERATION = "run"
# Mirrors the platform's `job_limits::MAX_WORKSPACE_DEPTH`. A chain walk here
# needs a bound of its own regardless: the platform refuses a cycle at the
# write, but a client that trusted that would hang on a database someone
# repaired by hand.
MAX_WORKSPACE_DEPTH = 5

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


# ============================================================================
# Declarative deployment settings — the [cloud] block of a project's toml
# ============================================================================

ALLOWED_OPERATIONS = ("run", "refresh", "plan", "state")
ALLOWED_VM_CLASSES = ("shared", "exclusive")
#: 0 realtime .. 4 background; mirrors the platform's `job_limits`.
MAX_PRIORITY = 4


def _setting_error(source: Path, key: str, detail: str) -> CloudError:
    """Every ``[cloud]`` complaint, naming the file that holds it."""
    return CloudError(f"{source}: [cloud].{key} {detail}")


def _typed_setting(
    table: Mapping[str, Any], source: Path, key: str, kind: type, label: str
) -> Any:
    value = table.get(key)
    if value is None:
        return None
    # bool is an int subclass, and `timeout_seconds = true` is a mistake rather
    # than 1 second.
    if isinstance(value, bool) or not isinstance(value, kind):
        raise _setting_error(source, key, f"must be {label}, got {value!r}")
    return value


def _positive_int_setting(
    table: Mapping[str, Any], source: Path, key: str
) -> int | None:
    value = _typed_setting(table, source, key, int, "an integer")
    if value is not None and value <= 0:
        raise _setting_error(source, key, "must be positive")
    return value


def _choice_setting(
    table: Mapping[str, Any], source: Path, key: str, allowed: tuple[str, ...]
) -> str | None:
    value = _typed_setting(table, source, key, str, "a string")
    if value is not None and value not in allowed:
        raise _setting_error(source, key, f"must be one of {', '.join(allowed)}")
    return value


def _secret_env_setting(
    table: Mapping[str, Any], source: Path
) -> tuple[str, ...] | None:
    value = table.get("secret_env")
    if value is None:
        return None
    if not isinstance(value, list) or not all(isinstance(e, str) for e in value):
        raise _setting_error(source, "secret_env", "must be a list of secret names")
    return tuple(value)


def _glob_setting(
    table: Mapping[str, Any], source: Path, key: str
) -> tuple[str, ...] | None:
    value = table.get(key)
    if value is None:
        return None
    if not isinstance(value, list) or not all(isinstance(e, str) for e in value):
        raise _setting_error(source, key, "must be a list of glob patterns")
    return tuple(value)


def _cpus_setting(table: Mapping[str, Any], source: Path) -> float | None:
    value = table.get("cpus")
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise _setting_error(source, "cpus", f"must be a number, got {value!r}")
    if value <= 0:
        raise _setting_error(source, "cpus", "must be positive")
    return float(value)


@dataclass(frozen=True)
class DeploySettings:
    """One project's declared job settings, parsed out of its ``[cloud]`` block.

    Every field is optional and ``None`` means *unspecified*: a sync carries
    whatever the job already has rather than applying a platform default, since
    a ``PUT`` replaces content wholesale.

    The field list is the source of truth for :data:`DEPLOY_KEYS`, which is what
    ``execution.config`` audits a ``[cloud]`` block against.
    """

    #: What to call the job. Optional, and unlike everything else here it is
    #: not part of the job's *content*: it feeds the ``name`` argument, never
    #: :meth:`job_fields`. Unset means the name derives from the path under the
    #: sync root, which is the right answer for a models repository and the
    #: wrong one for a project that is its own repository — where the path is
    #: the bare directory name and half of them are called ``data``.
    name: str | None = None

    #: Identity fragment for one entry of a ``[[cloud.job]]`` array, appended
    #: to the directory's `source_key` as ``::{key}``. **Immutable**: it is
    #: what a sync upserts against, so editing it deploys a new job and
    #: orphans the old one. Rejected on a bare ``[cloud]`` block, which
    #: declares only one job.
    key: str | None = None

    #: The one script this job runs, relative to the project root. Required
    #: in the ``[[cloud.job]]`` form and meaningless outside it: a project
    #: deployed as one job runs its whole directory, which is what it has
    #: always done. It is what makes several jobs over one *shared* tree
    #: possible at all — see `_ensure_workspace`.
    entrypoint: str | None = None

    #: The workspace a multi-job project deploys into. Block-level only: there
    #: is exactly one per toml, holding the whole tree its jobs share. Unset,
    #: it derives from the path like a job name does.
    workspace: str | None = None

    #: Per-job bundle filters, the same glob semantics ``jobs push``
    #: ``--include``/``--exclude`` use. They exist because several jobs over
    #: one directory is the whole point of ``[[cloud.job]]``, and shipping
    #: every file to every job is how a stray ``debug.preql`` ends up running
    #: in production. ``exclude`` adds to the defaults; ``include`` replaces
    #: them.
    include: tuple[str, ...] | None = None
    exclude: tuple[str, ...] | None = None

    schedule: str | None = None
    operation: str | None = None
    timeout_seconds: int | None = None
    memory_mb: int | None = None
    cpus: float | None = None
    secret_env: tuple[str, ...] | None = None
    vm_class: str | None = None
    priority: int | None = None
    deadline_seconds: int | None = None

    def merged_over(self, defaults: DeploySettings) -> DeploySettings:
        """This entry's settings with *defaults* filling every unset field.

        ``[cloud]`` holds the shared defaults and each ``[[cloud.job]]`` says
        only what differs. `name` and `key` are never inherited.
        """
        merged = {
            field.name: getattr(self, field.name) or getattr(defaults, field.name)
            for field in dataclass_fields(self)
        }
        merged["name"] = self.name
        merged["key"] = self.key
        return DeploySettings(**merged)

    @classmethod
    def from_table(cls, table: Mapping[str, Any], source: Path) -> DeploySettings:
        """Parse and validate, naming *source* in every error."""
        priority = _typed_setting(table, source, "priority", int, "an integer")
        if priority is not None and not 0 <= priority <= MAX_PRIORITY:
            raise _setting_error(
                source, "priority", f"must be between 0 and {MAX_PRIORITY}"
            )
        name = _typed_setting(table, source, "name", str, "a string")
        if name is not None and not name.strip():
            raise _setting_error(source, "name", "must not be empty")
        key = _typed_setting(table, source, "key", str, "a string")
        if key is not None and not key.strip():
            raise _setting_error(source, "key", "must not be empty")
        return cls(
            name=name,
            key=key,
            entrypoint=_typed_setting(table, source, "entrypoint", str, "a string"),
            workspace=_typed_setting(table, source, "workspace", str, "a string"),
            include=_glob_setting(table, source, "include"),
            exclude=_glob_setting(table, source, "exclude"),
            schedule=_typed_setting(table, source, "schedule", str, "a string"),
            operation=_choice_setting(table, source, "operation", ALLOWED_OPERATIONS),
            timeout_seconds=_positive_int_setting(table, source, "timeout_seconds"),
            memory_mb=_positive_int_setting(table, source, "memory_mb"),
            cpus=_cpus_setting(table, source),
            secret_env=_secret_env_setting(table, source),
            vm_class=_choice_setting(table, source, "vm_class", ALLOWED_VM_CLASSES),
            priority=priority,
            deadline_seconds=_positive_int_setting(table, source, "deadline_seconds"),
        )

    def declared_fields(self) -> dict[str, Any]:
        """Only what the toml actually said, JSON-ready — ``None`` dropped and
        tuples flattened.

        ``name`` is absent: it feeds the ``name=`` argument rather than the
        content, and it does not make a directory deployable. ``key``,
        ``include`` and ``exclude`` are absent too — they say which job this is
        and which files it gets, neither of which the platform stores.
        """
        declared = {
            "entrypoint": self.entrypoint,
            "schedule": self.schedule,
            "operation": self.operation,
            "timeout_seconds": self.timeout_seconds,
            "memory_mb": self.memory_mb,
            "cpus": self.cpus,
            "secret_env": None if self.secret_env is None else list(self.secret_env),
            "vm_class": self.vm_class,
            "priority": self.priority,
            "deadline_seconds": self.deadline_seconds,
        }
        return {key: value for key, value in declared.items() if value is not None}

    @property
    def declared(self) -> bool:
        """Whether this ``[cloud]`` block describes a *job* at all.

        A toml that only points at an API (``api_url``/``org``) is
        configuration, and is not deployed.
        """
        return bool(self.declared_fields())

    def job_fields(self) -> dict[str, Any]:
        """The declared settings as job-payload fields — ``schedule`` excluded,
        since that binds a *schedule* to the job rather than being part of it."""
        return {k: v for k, v in self.declared_fields().items() if k != "schedule"}


#: Keys that describe *how a job runs*, as opposed to which environment to talk
#: to (``api_url``/``org``). Derived from `DeploySettings` rather than written
#: out, so the parser, the deployability test and `execution.config`'s section
#: audit cannot disagree about what a `[cloud]` block may contain.
#:
#: ``name`` is among them but is not one of them in the sense that matters: it
#: says what to *call* the job, not how it runs, so it is the one key that does
#: not make a directory deployable (see :data:`JOB_DECLARING_KEYS`). Unset, the
#: name derives from the path under the sync root. Note that identity
#: (`SourceOrigin.source_key`) is path-derived and *not* declarable — moving a
#: directory deploys a new job and orphans the old one whatever it is called,
#: which is what ``--prune`` is for.
DEPLOY_KEYS: tuple[str, ...] = tuple(f.name for f in dataclass_fields(DeploySettings))

#: Keys that say *which* job this is, or which files it gets, rather than how
#: it runs. None of them reaches the job payload, and none of them makes a
#: directory deployable on its own.
_NON_DECLARING_KEYS = ("name", "key", "include", "exclude", "workspace")

#: The subset that makes a directory a job. Split out so the "nothing
#: deployable" error lists what would actually have helped: a toml declaring
#: only a name still describes nothing to run.
JOB_DECLARING_KEYS: tuple[str, ...] = tuple(
    k for k in DEPLOY_KEYS if k not in _NON_DECLARING_KEYS
)

#: The key that turns one directory into several jobs. Its entries are
#: `DeploySettings` tables in their own right, merged over the block they sit
#: in — so ``[cloud]`` keeps holding the org, the API and whatever every job
#: shares, and each ``[[cloud.job]]`` says only what makes it different.
JOB_ARRAY_KEY = "job"


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


def resolve_token(explicit: str | None, api_url: str) -> str | None:
    """--token flag > TRILOGY_CLOUD_TOKEN env > stored credentials for this API.

    The env var is the CI path: a runner has no credentials file, and a token
    in argv is readable from process listings.
    """
    if explicit:
        return explicit
    env = os.environ.get(ENV_TOKEN)
    if env:
        return env
    return stored_token(api_url)


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

        ``body`` may be pre-encoded bytes, so a caller that measured a bundle
        against the queue limit sends exactly what it measured.
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
                # `cloud login` is useless advice on a runner that got its
                # token from the environment.
                if self.token and self.token == os.environ.get(ENV_TOKEN):
                    hint = f" (check ${ENV_TOKEN})"
                else:
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

    def put(self, path: str, body: dict | list | bytes | None = None) -> Any:
        return self.request("PUT", path, body)

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

    def put_one(
        self, path: str, model: type[M], body: dict | list | bytes | None = None
    ) -> M:
        return self._validate("PUT", path, model, self.put(path, body))

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
    token = resolve_token(ctx.obj.get("CLOUD_TOKEN"), api_url)
    if require_auth and not token:
        raise CloudError(
            f"Not logged in to {api_url}. Run `trilogy cloud login` "
            f"(or pass --token, or set {ENV_TOKEN})."
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
    # `--org` belongs to the `cloud` GROUP, so it only parses before the
    # subcommand -- spell the position out. Trailing it (`jobs run x --org y`)
    # gets "No such option: --org", and click then suggests a same-prefix flag
    # from the subcommand, which reads as if the org flag never existed.
    raise CloudError(
        f"Multiple orgs ({slugs}); pass --org before the subcommand"
        f" (trilogy cloud --org {me.orgs[0].slug} ...), or set org under"
        " [cloud] in trilogy.toml."
    )


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


def _fmt_version(version: JobVersion, current: bool) -> str:
    source = version.source_fingerprint
    # Only ever populated by a server that records push provenance; the field
    # is dropped by every one that does not.
    origin = f"  from: {source.origin}" if source else ""
    return (
        f"v{version.version_number}{'  (current)' if current else ''}  "
        f"{_ts(version.created_at)}  op: {version.operation}  "
        f"timeout: {version.timeout_seconds or 'default'}{origin}"
    )


def _fmt_run(run: JobRunExt) -> str:
    version = f"  v{run.job_version_number}" if run.job_version_number else ""
    return (
        f"{run.id}  {run.job_name!r}{version}  {run.status}  "
        f"created: {_ts(run.created_at)}  finished: {_ts(run.finished_at)}"
    )


def _fmt_schedule(schedule: ScheduleExt) -> str:
    active = "active" if schedule.is_active else "paused"
    return (
        f"{schedule.id}  {schedule.name!r}  {schedule.cron_expr!r}  {active}  "
        f"next: {_ts(schedule.next_run_at)}  "
        f"jobs: {', '.join(schedule.job_names) or '-'}"
    )


def _file_count(files: Any) -> int:
    """How many inline files an entity carries.

    ``files`` is untyped JSON on the wire, so ``null`` (every job in a
    workspace) and a non-list both answer 0 rather than raising.
    """
    return len(files) if isinstance(files, list) else 0


def _fmt_workspace(workspace: Workspace) -> str:
    parent = (
        f"  extends: {workspace.parent_workspace_id}"
        if workspace.parent_workspace_id
        else ""
    )
    return (
        f"{workspace.id}  {workspace.name!r}  "
        f"files: {_file_count(workspace.files)}{parent}"
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

    Returns the bytes, so the caller sends exactly what was measured.
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
    and must come back as ``state`` on the callback; the handler above enforces
    that.
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
    help=(
        "API token for this invocation, ahead of $TRILOGY_CLOUD_TOKEN and stored "
        "credentials. Prefer the env var in CI to keep it out of argv."
    ),
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

    # Login stores; every other command resolves the env var ahead of the store,
    # so leaving it set would make this sign-in a no-op for the next command.
    env_token = os.environ.get(ENV_TOKEN)
    if env_token and env_token != token:
        print_warning(
            f"{ENV_TOKEN} is set and takes precedence over stored credentials — "
            f"unset it to use this login."
        )

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
    if os.environ.get(ENV_TOKEN):
        print_warning(f"{ENV_TOKEN} is still set — commands remain authenticated.")


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
# Every job setting defaults to "unspecified" rather than to its platform
# default, so an update can tell "leave this alone" from "set it to run/none".
# A PUT replaces content wholesale; without that distinction, pushing an edited
# file to a job configured with --operation refresh and three secrets would
# quietly reset it to a 300-second `run` with none.
@click.option("--operation", default=None, type=click.Choice(list(ALLOWED_OPERATIONS)))
@click.option("--timeout-seconds", type=int, default=None)
@click.option("--memory-mb", type=int, default=None)
@click.option("--cpus", type=float, default=None)
@click.option(
    "--secret-env",
    multiple=True,
    help="Org secret names to inject as env vars (repeatable). Replaces the "
    "job's existing set; omit to keep it.",
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
    help="Also create a schedule with this cron expression (UTC). Ignored when "
    "updating an existing job, which keeps the schedules it already has.",
)
@click.option(
    "--create",
    "force_create",
    is_flag=True,
    help="Always create a new job, even if one of this name exists — leaving "
    "the existing job, its runs and its schedules untouched.",
)
@dry_run_option(
    "Bundle and size-check the source, report whether the push would create or "
    "update the job, and send nothing."
)
@click.pass_context
def jobs_push(
    ctx: click.Context,
    source: Path,
    config_path: Path | None,
    name: str,
    description: str | None,
    operation: str | None,
    timeout_seconds: int | None,
    memory_mb: int | None,
    cpus: float | None,
    secret_env: tuple[str, ...],
    include: tuple[str, ...],
    exclude: tuple[str, ...],
    rewrite: list[tuple[str, str]],
    rewrite_glob: tuple[str, ...],
    cron: str | None,
    force_create: bool,
    dry_run: bool,
) -> None:
    """Push a local project directory to a job, creating or updating it.

    An existing job of this name is *replaced in place*: same id, new version,
    run history and schedule bindings intact. Pushing content the job already
    has mints nothing, so re-running this is idempotent.

    Settings not named on the command line are carried over from the job being
    updated, so pushing an edited file changes the source only.
    """
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

    existing = _existing_job(client, org, name, force_create)
    if existing is not None and existing.workspace_id and files:
        # The natural way to reach here is `jobs fetch --resolved` followed by
        # a push: the directory holds the workspace's tree, and sending it as
        # the job's own files gives this job a private copy that shadows the
        # shared one — so the edit lands, the siblings never see it, and the
        # two copies drift from the next workspace push onwards.
        print_warning(
            f"{name!r} runs out of a workspace, which holds its files; this "
            f"push adds {len(files)} file(s) to the job itself, shadowing the "
            "shared tree. Push a shared tree with `trilogy cloud workspaces "
            "push` instead."
        )

    origin = resolve_origin(source.resolve())
    fingerprint = SourceFingerprint.build(content_digest(config_text, files), origin)

    payload = _job_payload(
        name=name,
        config_text=config_text,
        files=files,
        fingerprint=fingerprint,
        declared={
            "operation": operation,
            "description": description,
            "timeout_seconds": timeout_seconds,
            "memory_mb": memory_mb,
            "cpus": cpus,
            "secret_env": list(secret_env) or None,
        },
        existing=existing,
    )

    encoded = check_bundle_size(payload)
    print_info(f"Bundled {len(files)} files ({len(encoded):,} bytes) from {source}")
    print_info(f"Source: {origin.describe()} (content {fingerprint.content[:12]}…)")

    if dry_run:
        schedule_note = (
            f" and a schedule on {cron!r}" if cron and existing is None else ""
        )
        _report_push_dry_run("job", name, existing, files, schedule_note)
        return

    job, outcome = _upsert_job(client, org, encoded, existing)
    _report_push(org, job, outcome, fingerprint)

    if cron:
        if existing is not None:
            print_warning(
                f"--cron ignored: {name!r} already exists and keeps its "
                "schedules. Use `trilogy cloud schedules create` to add one."
            )
        else:
            schedule = client.post_one(
                f"/orgs/{org}/schedules",
                Schedule,
                {"name": f"{name}-schedule", "cron_expr": cron, "job_ids": [job.id]},
            )
            _report_schedule(org, schedule, "Scheduled")


def _carried_settings(existing: Job | None) -> dict[str, Any]:
    """The settings a write has to send back when it is updating a job.

    ``PUT`` replaces a job's content wholesale — an omitted field is *cleared*
    — so every setting the caller did not name is read off the job being
    updated. Empty for a create, which leaves everything but ``operation`` to
    the platform.

    **Every content field on `Job` belongs here**, and nothing `Job` does not
    model; `tests/cli/test_cloud.py` pins the two lists against each other.
    """
    if existing is None:
        return {}
    return {
        "operation": existing.operation,
        "description": existing.description,
        "timeout_seconds": existing.timeout_seconds,
        "memory_mb": existing.memory_mb,
        "cpus": existing.cpus,
        "secret_env": existing.secret_env,
        "parameters": existing.parameters,
        "vm_class": existing.vm_class,
        "priority": existing.priority,
        "deadline_seconds": existing.deadline_seconds,
        "entrypoint": existing.entrypoint,
    }


#: Content fields carried across an update, in payload order. ``operation`` is
#: not among them: it is the one field with a platform default rather than a
#: "leave it unset" state, so it is always sent.
CARRIED_FIELDS = (
    "description",
    "entrypoint",
    "timeout_seconds",
    "memory_mb",
    "cpus",
    "secret_env",
    "parameters",
    "vm_class",
    "priority",
    "deadline_seconds",
)


def _apply_carried(
    payload: dict,
    fields: Sequence[str],
    declared: Mapping[str, Any],
    carried: Mapping[str, Any],
) -> None:
    """Fill *payload* from what the caller declared, falling back to what the
    entity being updated already holds.

    A field neither supplies is left out rather than sent as null, which on a
    create leaves it to the platform. Shared by the job and workspace writes.
    """
    for field in fields:
        value = declared.get(field)
        if value is None:
            value = carried.get(field)
        if value is not None:
            payload[field] = value


def _job_payload(
    name: str,
    config_text: str,
    files: list[dict],
    fingerprint: SourceFingerprint,
    declared: Mapping[str, Any],
    existing: Job | None,
    extra: Mapping[str, Any] | None = None,
) -> dict:
    """The body of a job create or update.

    Shared by ``jobs push`` (settings from the command line) and ``cloud sync``
    (settings from a ``[cloud]`` block), so both send the same field set.

    *declared* is what the caller asked for, ``None`` meaning unspecified;
    *extra* is for fields with no carried counterpart (``source_key``,
    ``environment_id``) and is sent verbatim.
    """
    carried = _carried_settings(existing)
    payload: dict = {
        "name": name,
        "config": config_text,
        "files": files,
        "source_fingerprint": fingerprint.model_dump(mode="json", exclude_none=True),
        "operation": declared.get("operation")
        or carried.get("operation")
        or DEFAULT_OPERATION,
        **(extra or {}),
    }
    _apply_carried(payload, CARRIED_FIELDS, declared, carried)
    return payload


def _upsert_job(
    client: CloudClient, org: str, encoded: bytes, existing: Job | None
) -> tuple[Job, str]:
    """Create or replace a job, and say which happened.

    ``PUT`` replaces the job's *content* under its existing id, so its
    schedules stay bound to the job the edit landed on.
    """
    if existing is None:
        return client.post_one(f"/orgs/{org}/jobs", Job, encoded), "created"
    job = client.put_one(f"/orgs/{org}/jobs/{existing.id}", Job, encoded)
    outcome = (
        "unchanged"
        if job.current_version_id == existing.current_version_id
        else "updated"
    )
    return job, outcome


def _existing_job(
    client: CloudClient, org: str, name: str, force_create: bool
) -> Job | None:
    """The one job of this name to update, or ``None`` to create a new one.

    Job names carry no unique constraint platform-side, so several matches is
    an error rather than a pick.

    ``--create`` always creates, but still looks, and says so when the name is
    already taken: the org's schedules keep pointing at the first job.
    """
    matches = [j for j in client.get_many(f"/orgs/{org}/jobs", Job) if j.name == name]
    if force_create:
        if matches:
            print_warning(
                f"{len(matches)} job(s) in {org!r} are already named {name!r}; "
                "--create adds another. Existing schedules stay bound to the "
                "job they already name."
            )
        return None
    if len(matches) > 1:
        raise CloudError(
            f"{len(matches)} jobs in {org!r} are named {name!r} "
            f"({', '.join(j.id for j in matches)}); this push cannot tell which "
            "to update. Delete the duplicates, or pass --create to add another."
        )
    return matches[0] if matches else None


def _report_push_dry_run(
    kind: str, name: str, existing: Any | None, files: list[dict], extra: str = ""
) -> None:
    """What a ``--dry-run`` push reports instead of writing.

    Bundling, rewrites and the size check have all already run by the time this
    is reached -- that work is exactly what a dry run exists to exercise, so the
    only thing skipped is the write itself.
    """
    action = "update" if existing is not None else "create"
    if is_json_mode():
        emit_event(
            f"{kind}_dry_run",
            org_name=name,
            action=action,
            files=len(files),
            existing_id=getattr(existing, "id", None),
            dry_run=True,
        )
        return
    print_success(
        f"Dry run: would {action} {kind} {name!r} with {len(files)} file(s)"
        f"{extra}; nothing was written."
    )


def _report_push(
    org: str, job: Job, outcome: str, fingerprint: SourceFingerprint
) -> None:
    if is_json_mode():
        emit_event(
            f"job_{outcome}",
            org=org,
            outcome=outcome,
            job=job.model_dump(mode="json"),
            source_fingerprint=fingerprint.model_dump(mode="json"),
        )
        return
    if outcome == "unchanged":
        print_success(
            f"Job {job.name!r} ({job.id}) already matches this content; "
            "no new version"
        )
        return
    verb = "Created" if outcome == "created" else "Updated"
    version = job.current_version_id
    suffix = f" as version {version[:8]}…" if version else ""
    print_success(f"{verb} job {job.name!r} ({job.id}) in org {org!r}{suffix}")


def _is_contained_name(name: str) -> bool:
    """Whether *name* is a directory-relative path on **every** platform.

    Judged against both flavours rather than the host's: ``C:/x`` and
    ``a\\..\\..\\x`` are escapes that a POSIX ``resolve()`` reads as ordinary
    filenames.
    """
    flavours = [PureWindowsPath(name), PurePosixPath(name)]
    return not any(p.anchor or ".." in p.parts for p in flavours)


def _resolve_bundle_entries(dest: Path, files: Any) -> list[tuple[Path, str]]:
    """``(target, content)`` for every inline file, resolved against *dest*.

    **Entry names are checked against the destination, not trusted.** A name
    that escapes *dest* — ``../../.bashrc``, an absolute path — is refused
    rather than sanitized.

    Every name is resolved before anything is written, so a bundle carrying one
    bad entry leaves no half-unpacked directory behind.
    """
    root = dest.resolve()
    resolved: list[tuple[Path, str]] = []
    for entry in files or []:
        if not isinstance(entry, dict):
            continue
        name = str(entry.get("name") or "")
        if not name:
            continue
        target = (dest / name).resolve()
        if not _is_contained_name(name) or not target.is_relative_to(root):
            raise CloudError(
                f"Refusing to write {name!r}: it escapes {dest}. The bundle is "
                "corrupt or hostile; fetch it somewhere isolated and inspect it."
            )
        resolved.append((target, str(entry.get("content") or "")))
    return resolved


def _write_bundle(dest: Path, config: Any, files: Any) -> tuple[int, Path | None]:
    """Write a config and inline files under *dest*, mirroring the layout they
    were bundled from. Returns ``(file_count, config_path)``.

    ``config`` of ``None`` writes no trilogy.toml at all, which is the normal
    case for a workspace.

    **Bytes land as stored** (``newline=""``), so a fetch is byte-identical to
    the platform's copy rather than newline-translated on Windows.
    """
    entries = _resolve_bundle_entries(dest, files)

    dest.mkdir(parents=True, exist_ok=True)
    config_path: Path | None = None
    if config is not None:
        config_path = dest / "trilogy.toml"
        # A config is TOML text, but the column is JSON — a server that stored
        # an object hands one back, and writing `{'engine': ...}` to a
        # trilogy.toml would produce a file that parses as neither.
        config_path.write_text(
            config if isinstance(config, str) else json.dumps(config, indent=2),
            encoding="utf-8",
            newline="",
        )
    for target, content in entries:
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(content, encoding="utf-8", newline="")
    return len(entries), config_path


def _guard_destination(dest: Path, force: bool) -> None:
    """Refuse a non-empty destination unless *force*, so a fetch does not
    revert local edits. Shared by both fetch commands."""
    if dest.exists() and any(dest.iterdir()) and not force:
        raise CloudError(
            f"{dest} is not empty; pass --force to write into it. (Fetching over "
            "a checkout replaces its files with the stored copy.)"
        )


def _find_workspace(
    workspaces_: Sequence[Workspace], org: str, name_or_id: str
) -> Workspace:
    """Resolve a workspace reference against an already-fetched list.

    Workspace names are unique per org, so there is no ambiguity to refuse.
    """
    for workspace in workspaces_:
        if workspace.id == name_or_id or workspace.name == name_or_id:
            return workspace
    raise CloudError(f"No workspace named {name_or_id!r} in org {org!r}.")


def _workspace_chain(
    workspaces_: Sequence[Workspace], workspace_id: str | None
) -> list[Workspace]:
    """The chain above a job or workspace, **nearest first**.

    Resolved against one already-fetched list, so a chain costs one request
    however deep it is.

    A parent the caller cannot see ends the walk rather than failing it. Depth
    and a visited set bound the walk against a cycle.
    """
    by_id = {w.id: w for w in workspaces_}
    chain: list[Workspace] = []
    seen: set[str] = set()
    current = workspace_id
    while current and current not in seen and len(chain) < MAX_WORKSPACE_DEPTH:
        seen.add(current)
        workspace = by_id.get(current)
        if workspace is None:
            break
        chain.append(workspace)
        current = workspace.parent_workspace_id
    return chain


def _merge_chain_files(layers: Sequence[Any]) -> list[dict]:
    """Flatten file layers given **nearest first** into one bundle.

    Nearest wins on a path collision, matching the platform's own resolution
    (`db/workspace_resolution.py`): a job's copy of ``model.preql`` shadows its
    workspace's, and a workspace's shadows its parent's.
    """
    merged: dict[str, str] = {}
    for layer in reversed(list(layers)):
        for entry in layer if isinstance(layer, list) else []:
            if isinstance(entry, dict) and entry.get("name"):
                merged[str(entry["name"])] = str(entry.get("content") or "")
    return [{"name": name, "content": merged[name]} for name in sorted(merged)]


@jobs.command("fetch")
@click.argument("job")
@click.option(
    "--dest",
    type=click.Path(file_okay=False, path_type=Path),
    required=True,
    help="Directory to write the job's project into.",
)
@click.option(
    "--force",
    is_flag=True,
    help="Write into a non-empty directory, overwriting files of the same name.",
)
@click.option(
    "--resolved",
    is_flag=True,
    help="Also write the files the job inherits from its workspace chain, so "
    "the directory runs. The result is then not a bundle to push back as the "
    "job — edits to inherited files belong to the workspace.",
)
@click.pass_context
def jobs_fetch(
    ctx: click.Context, job: str, dest: Path, force: bool, resolved: bool
) -> None:
    """Write a cloud job's project to a local directory.

    The inverse of ``jobs push``: the job's ``trilogy.toml`` and every inline
    file land at their bundled paths, so the result is a working project you
    can run, edit and push back. Re-pushing an unedited fetch mints no version.

    A job **in a workspace carries no files of its own**: the workspace holds
    the whole tree and the job names the one script it runs. Fetching one
    without ``--resolved`` writes a config and nothing else, and says where the
    files are. ``--resolved`` materializes the whole chain (nearest wins on a
    collision, as the executor sees it), which makes the directory runnable and
    makes it the *workspace's* content rather than this job's — push edits back
    with ``workspaces push``.

    Refuses a non-empty destination unless ``--force``.
    """
    client, org = _org_client(ctx)
    found = _find_job(client.get_many(f"/orgs/{org}/jobs", Job), org, job)
    _guard_destination(dest, force)

    chain: list[Workspace] = []
    if found.workspace_id:
        chain = _workspace_chain(
            client.get_many(f"/orgs/{org}/workspaces", Workspace), found.workspace_id
        )
    # Layers nearest-first: the job's own files shadow its workspace's, which
    # shadow its parent's — the platform's rule, not this command's.
    files = (
        _merge_chain_files([found.files, *(w.files for w in chain)])
        if resolved
        else found.files
    )

    written, config_path = _write_bundle(dest, found.config, files)
    if is_json_mode():
        emit_event(
            "job_fetched",
            org=org,
            job=found.model_dump(mode="json"),
            dest=str(dest),
            files=written,
            resolved=resolved,
            workspaces=[w.name for w in chain],
        )
        return
    print_success(f"Fetched {found.name!r} ({found.id}) into {dest}")
    config_label = f"{config_path.name} + " if config_path else ""
    print_info(f"  {config_label}{written} file(s)")
    if found.entrypoint:
        print_info(f"  Entrypoint: {found.entrypoint}")
    if found.source_fingerprint is not None:
        print_info(f"  Source: {found.source_fingerprint.origin}")
    _report_chain(chain, resolved)


def _report_chain(chain: Sequence[Workspace], resolved: bool) -> None:
    """Say what the workspace chain contributed, or would have.

    Unresolved, it names where the job's files live; resolved, it names the
    command that accepts edits to them back.
    """
    if not chain:
        return
    names = ", ".join(w.name for w in chain)
    if resolved:
        print_warning(
            f"  Includes inherited files from {names}. Push edits to them with "
            f"`trilogy cloud workspaces push --name {chain[0].name}` — pushing "
            "this directory as the job would install a private copy of the "
            "shared tree."
        )
        return
    inherited = sum(_file_count(w.files) for w in chain)
    print_info(
        f"  Inherits {inherited} file(s) from {names}; pass --resolved to write "
        f"them too, or `trilogy cloud workspaces fetch {chain[0].name}`."
    )


@jobs.command("run")
@click.argument("job")
@click.option(
    "--wait",
    is_flag=True,
    help="Block until the run finishes, and exit non-zero unless it succeeded.",
)
@click.option(
    "--timeout",
    "timeout",
    type=int,
    default=WAIT_TIMEOUT_SECONDS,
    help=f"With --wait, seconds to wait (default: {WAIT_TIMEOUT_SECONDS}).",
)
@click.option(
    "--poll-seconds",
    type=int,
    default=WAIT_POLL_SECONDS,
    help=f"With --wait, seconds between status checks (default: {WAIT_POLL_SECONDS}).",
)
@click.option(
    "--logs",
    is_flag=True,
    help="With --wait, print the log tail even on success (a failure always does).",
)
@click.pass_context
def jobs_run(
    ctx: click.Context,
    job: str,
    wait: bool,
    timeout: int,
    poll_seconds: int,
    logs: bool,
) -> None:
    """Trigger a run of a job (by name or id).

    Without ``--wait`` this returns as soon as the run is queued, so a zero
    exit means "accepted", not "succeeded". CI wants ``--wait``.
    """
    client, org = _org_client(ctx)
    found = _find_job(client.get_many(f"/orgs/{org}/jobs", Job), org, job)
    run = client.post_one(f"/orgs/{org}/jobs/{found.id}/run", JobRun)
    if is_json_mode():
        emit_event("run_triggered", org=org, run=run.model_dump(mode="json"))
    else:
        print_success(
            f"Triggered run {run.id} of {found.name!r} (status: {run.status})"
        )
    if wait:
        finished = _wait_for_run(client, org, run.id, timeout, poll_seconds)
        _report_finished_run(org, finished, logs=logs)


@jobs.command("delete")
@click.argument("jobs_args", metavar="JOB...", nargs=-1, required=True)
@click.option("--yes", is_flag=True, help="Skip the confirmation prompt.")
@click.pass_context
def jobs_delete(ctx: click.Context, jobs_args: tuple[str, ...], yes: bool) -> None:
    """Delete one or more jobs (by name or id), with their run history.

    Names and ids may be mixed. A name matching more than one job is an error
    naming the ids, since a name cannot address either of them.

    Deletes any schedule left bound to nothing but these jobs; a schedule that
    also binds a surviving job is left alone.
    """
    client, org = _org_client(ctx)
    known = client.get_many(f"/orgs/{org}/jobs", Job)
    # By id, so naming one job twice deletes it once rather than 404ing on the
    # second pass.
    targets = {job.id: job for job in (_find_job(known, org, j) for j in jobs_args)}
    if not yes:
        click.confirm(
            f"Delete {len(targets)} job(s): "
            + ", ".join(sorted(job.name for job in targets.values()))
            + "?",
            abort=True,
        )
    for job in targets.values():
        client.delete(f"/orgs/{org}/jobs/{job.id}")
        print_success(f"Deleted job {job.name!r} ({job.id})")
    _delete_emptied_schedules(client, org, set(targets))


def _delete_emptied_schedules(
    client: CloudClient, org: str, deleted_ids: set[str]
) -> None:
    """Delete every schedule whose bound jobs were all just deleted.

    Matched on ``job_ids`` only, so a schedule whose bindings came back empty —
    which is what an API older than that field answers with — is left alone
    rather than read as binding nothing.
    """
    for schedule in client.get_many(f"/orgs/{org}/schedules", ScheduleExt):
        if schedule.job_ids and set(schedule.job_ids) <= deleted_ids:
            client.delete(f"/orgs/{org}/schedules/{schedule.id}")
            print_info(
                f"  removed schedule {schedule.name!r}, which bound only "
                "deleted jobs"
            )


@jobs.command("versions")
@click.argument("job")
@click.option(
    "--limit", type=int, default=10, help="How many versions to show, newest first."
)
@click.pass_context
def jobs_versions(ctx: click.Context, job: str, limit: int) -> None:
    """Version history of a job (by name or id), newest first.

    A version is minted by every content-changing push. Rolling back is pushing
    an old version's content again, which mints a new version.
    """
    client, org = _org_client(ctx)
    found = _find_job(client.get_many(f"/orgs/{org}/jobs", Job), org, job)
    rows = client.get_many(f"/orgs/{org}/jobs/{found.id}/versions", JobVersion)[:limit]
    _show_rows(
        "job_versions",
        "versions",
        rows,
        f"No recorded versions for {found.name!r} (it predates versioning).",
        lambda v: _fmt_version(v, current=v.id == found.current_version_id),
        org=org,
    )


@cloud.group()
def workspaces() -> None:
    """Manage workspaces — the shared project tree a job's files come from."""


@workspaces.command("list")
@click.pass_context
def workspaces_list(ctx: click.Context) -> None:
    """List the org's workspaces."""
    client, org = _org_client(ctx)
    _show_rows(
        "workspaces",
        "workspaces",
        client.get_many(f"/orgs/{org}/workspaces", Workspace),
        f"No workspaces in org {org!r}.",
        _fmt_workspace,
        org=org,
    )


@workspaces.command("jobs")
@click.argument("workspace")
@click.pass_context
def workspaces_jobs(ctx: click.Context, workspace: str) -> None:
    """List the jobs that run out of a workspace, with the entrypoint each
    one executes."""
    client, org = _org_client(ctx)
    found = _find_workspace(
        client.get_many(f"/orgs/{org}/workspaces", Workspace), org, workspace
    )
    _show_rows(
        "workspace_jobs",
        "jobs",
        client.get_many(f"/orgs/{org}/workspaces/{found.id}/jobs", Job),
        f"No jobs use workspace {found.name!r}.",
        lambda j: f"{_fmt_job(j)}  entrypoint: {j.entrypoint or 'whole directory'}",
        org=org,
    )


@workspaces.command("fetch")
@click.argument("workspace")
@click.option(
    "--dest",
    type=click.Path(file_okay=False, path_type=Path),
    required=True,
    help="Directory to write the workspace's tree into.",
)
@click.option(
    "--force",
    is_flag=True,
    help="Write into a non-empty directory, overwriting files of the same name.",
)
@click.option(
    "--resolved",
    is_flag=True,
    help="Also write the files this workspace inherits from the one it extends. "
    "The result is then not a bundle to push back — pushing it would copy the "
    "parent's tree into the child.",
)
@click.pass_context
def workspaces_fetch(
    ctx: click.Context, workspace: str, dest: Path, force: bool, resolved: bool
) -> None:
    """Write a workspace's shared tree to a local directory.

    The inverse of ``workspaces push``, and the read half of editing a
    multi-job project: the whole tree lands at its bundled paths and the jobs
    that run out of it are listed, each with the script it executes.

    Only the workspace's *own* files are written, so pushing an unedited fetch
    mints no version. ``--resolved`` adds what it inherits from its parent,
    which makes the directory runnable and makes it no longer this workspace's
    content.

    A workspace usually has no ``trilogy.toml`` of its own; nothing is written
    for it when there is none.
    """
    client, org = _org_client(ctx)
    all_workspaces = client.get_many(f"/orgs/{org}/workspaces", Workspace)
    found = _find_workspace(all_workspaces, org, workspace)
    _guard_destination(dest, force)

    parents = _workspace_chain(all_workspaces, found.parent_workspace_id)
    files = (
        _merge_chain_files([found.files, *(w.files for w in parents)])
        if resolved
        else found.files
    )

    written, config_path = _write_bundle(dest, found.config, files)
    bound = client.get_many(f"/orgs/{org}/workspaces/{found.id}/jobs", Job)
    if is_json_mode():
        emit_event(
            "workspace_fetched",
            org=org,
            workspace=found.model_dump(mode="json"),
            dest=str(dest),
            files=written,
            resolved=resolved,
            jobs=[j.model_dump(mode="json") for j in bound],
        )
        return
    print_success(f"Fetched workspace {found.name!r} ({found.id}) into {dest}")
    config_label = f"{config_path.name} + " if config_path else ""
    print_info(f"  {config_label}{written} file(s)")
    for job in bound:
        print_info(f"  job {job.name!r}: {job.operation} {job.entrypoint or '.'}")
    _report_chain(parents, resolved)


@workspaces.command("push")
@click.option(
    "--source",
    type=click.Path(exists=True, file_okay=False, path_type=Path),
    required=True,
    help="Directory holding the shared tree.",
)
@click.option("--name", required=True, help="Workspace name (unique per org).")
@click.option("--description", default=None)
# Same "unspecified means carry" rule as `jobs push`, and for the same reason:
# a workspace PUT replaces its content wholesale, so an omitted field is
# cleared rather than left alone.
@click.option(
    "--secret-env",
    multiple=True,
    help="Org secret names its jobs inherit (repeatable). Replaces the "
    "workspace's existing set; omit to keep it.",
)
@click.option("--timeout-seconds", type=int, default=None)
@click.option("--memory-mb", type=int, default=None)
@click.option("--cpus", type=float, default=None)
@click.option("--vm-class", default=None, type=click.Choice(list(ALLOWED_VM_CLASSES)))
@click.option(
    "--config",
    "config_path",
    type=click.Path(exists=True, dir_okay=False, path_type=Path),
    default=None,
    help="Store this trilogy.toml as the workspace's config. Rarely wanted: "
    "workspace config is not layered onto its jobs yet.",
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
    help="Literal OLD=NEW substitution applied to file contents (repeatable).",
)
@click.option(
    "--rewrite-glob",
    multiple=True,
    help="Restrict --rewrite to files matching these globs (default: all).",
)
@dry_run_option(
    "Bundle and size-check the tree, report whether the push would create or "
    "update the workspace, and send nothing."
)
@click.pass_context
def workspaces_push(
    ctx: click.Context,
    source: Path,
    name: str,
    description: str | None,
    secret_env: tuple[str, ...],
    timeout_seconds: int | None,
    memory_mb: int | None,
    cpus: float | None,
    vm_class: str | None,
    config_path: Path | None,
    include: tuple[str, ...],
    exclude: tuple[str, ...],
    rewrite: list[tuple[str, str]],
    rewrite_glob: tuple[str, ...],
    dry_run: bool,
) -> None:
    """Push a local directory to a workspace, creating or updating it.

    The write half of the loop ``workspaces fetch`` opens, and where edits to a
    shared tree belong: the jobs that run out of the workspace carry no files.

    Matched by name, which is unique per org and the same key ``cloud sync``
    deploys against. Settings not named on the command line are carried over
    from the workspace being updated, since a ``PUT`` replaces content
    wholesale.
    """
    client, org = _org_client(ctx)

    include_pats = tuple(include) if include else DEFAULT_INCLUDE
    exclude_pats = DEFAULT_EXCLUDE + tuple(exclude)
    files = collect_files(source.resolve(), include_pats, exclude_pats)
    # Same reason as `jobs push`: the config is a field, not a bundled file,
    # and a copy in the tree would shadow it in the executor's workdir.
    local_config = next((f for f in files if f["name"] == "trilogy.toml"), None)
    files = [f for f in files if f["name"] != "trilogy.toml"]
    if not files:
        raise CloudError(f"No files matched under {source}.")

    if rewrite:
        globs = rewrite_glob or ("*",)
        touched = 0
        for f in files:
            if _matches(f["name"], globs):
                rewritten = apply_rewrites(f["content"], rewrite)
                if rewritten != f["content"]:
                    f["content"] = rewritten
                    touched += 1
        print_info(f"Applied rewrites to {touched} file(s)")

    existing = next(
        (
            w
            for w in client.get_many(f"/orgs/{org}/workspaces", Workspace)
            if w.name == name
        ),
        None,
    )

    # An explicit --config always wins. Otherwise a trilogy.toml in the tree is
    # sent only when the workspace already has a config — that is the file a
    # fetch wrote from it, so dropping the edit would break the round trip —
    # and is left alone when it does not, because giving a workspace a config
    # it never had breaks every job under it on today's CLI (the unknown
    # `--config-overlay` flag).
    config_text: str | None = None
    if config_path is not None:
        config_text = config_path.read_text(encoding="utf-8")
    elif local_config is not None and existing is not None and existing.config:
        config_text = local_config["content"]
    elif local_config is not None:
        print_info(
            "Ignoring trilogy.toml: a workspace's config is not layered onto "
            "its jobs yet, so storing one would break them. Pass --config to "
            "store it anyway."
        )
    if config_text is not None and rewrite:
        config_text = apply_rewrites(config_text, rewrite)

    payload = _workspace_payload(
        name=name,
        files=files,
        config_text=config_text,
        declared={
            "description": description,
            "secret_env": list(secret_env) or None,
            "timeout_seconds": timeout_seconds,
            "memory_mb": memory_mb,
            "cpus": cpus,
            "vm_class": vm_class,
        },
        existing=existing,
    )
    encoded = check_bundle_size(payload)
    print_info(f"Bundled {len(files)} files ({len(encoded):,} bytes) from {source}")

    if dry_run:
        _report_push_dry_run("workspace", name, existing, files)
        return

    workspace, outcome = _upsert_workspace(client, org, encoded, existing)
    if is_json_mode():
        emit_event(
            f"workspace_{outcome}",
            org=org,
            outcome=outcome,
            workspace=workspace.model_dump(mode="json"),
        )
        return
    if outcome == "unchanged":
        print_success(
            f"Workspace {workspace.name!r} ({workspace.id}) already matches this "
            "content; no new version"
        )
        return
    verb = "Created" if outcome == "created" else "Updated"
    print_success(f"{verb} workspace {workspace.name!r} ({workspace.id}) in {org!r}")


#: Content fields a workspace write has to resend to keep. **Every content
#: field on `Workspace` belongs here** — a `PUT` clears what it omits, so one
#: this list forgets is silently reset on every push, the same failure the job
#: side's `CARRIED_FIELDS` exists to prevent. Pinned against the model in
#: `tests/cli/test_cloud.py`.
WORKSPACE_CARRIED_FIELDS = (
    "description",
    "parent_workspace_id",
    "secret_env",
    "parameters",
    "timeout_seconds",
    "memory_mb",
    "cpus",
    "vm_class",
)


def _carried_workspace_settings(existing: Workspace | None) -> dict[str, Any]:
    """The settings a workspace write has to send back when it is updating one.

    The workspace twin of `_carried_settings`, spelled out field by field so a
    key the model does not carry is a mypy error.
    """
    if existing is None:
        return {}
    return {
        "description": existing.description,
        "parent_workspace_id": existing.parent_workspace_id,
        "secret_env": existing.secret_env,
        "parameters": existing.parameters,
        "timeout_seconds": existing.timeout_seconds,
        "memory_mb": existing.memory_mb,
        "cpus": existing.cpus,
        "vm_class": existing.vm_class,
    }


def _workspace_payload(
    name: str,
    files: list[dict],
    config_text: str | None,
    declared: Mapping[str, Any],
    existing: Workspace | None,
) -> dict:
    """The body of a workspace create or update.

    *declared* is what the caller asked for, ``None`` meaning unspecified and
    therefore carried off *existing*. ``config`` is carried the same way.
    """
    payload: dict = {"name": name, "files": files}
    if config_text is not None:
        payload["config"] = config_text
    elif existing is not None and existing.config is not None:
        payload["config"] = existing.config
    _apply_carried(
        payload,
        WORKSPACE_CARRIED_FIELDS,
        declared,
        _carried_workspace_settings(existing),
    )
    return payload


def _upsert_workspace(
    client: CloudClient, org: str, encoded: bytes, existing: Workspace | None
) -> tuple[Workspace, str]:
    """Create or replace a workspace, and say which happened.

    ``current_version_id`` moving is what distinguishes a real update from a
    content no-op, exactly as for a job. A server old enough not to send it
    reports every push as an update rather than claiming a no-op it cannot
    know about.
    """
    if existing is None:
        return (
            client.post_one(f"/orgs/{org}/workspaces", Workspace, encoded),
            "created",
        )
    workspace = client.put_one(
        f"/orgs/{org}/workspaces/{existing.id}", Workspace, encoded
    )
    unchanged = (
        workspace.current_version_id is not None
        and workspace.current_version_id == existing.current_version_id
    )
    return workspace, "unchanged" if unchanged else "updated"


@cloud.group()
def runs() -> None:
    """Inspect job runs."""


@runs.command("list")
@click.option(
    "--limit",
    type=int,
    default=15,
    help=f"How many recent runs to show (server cap: {RUNS_MAX_LIMIT}).",
)
@click.option(
    "--status",
    default=None,
    help="Comma-separated run statuses to keep, e.g. 'failed' or "
    "'queued,dispatched,running'.",
)
@click.option(
    "--source",
    "source_filter",
    type=click.Choice(["manual", "scheduled"]),
    default=None,
    help="Only runs triggered by hand, or only ones born from a schedule tick.",
)
@click.pass_context
def runs_list(
    ctx: click.Context, limit: int, status: str | None, source_filter: str | None
) -> None:
    """Recent runs across the org's jobs.

    Filtered server-side: the route answers with the newest N runs, so a
    client-side filter would only see what fits that window.
    """
    client, org = _org_client(ctx)
    query = {"limit": str(max(1, min(limit, RUNS_MAX_LIMIT)))}
    if status:
        query["status"] = status
    if source_filter:
        query["source"] = source_filter
    path = f"/orgs/{org}/jobs/runs?{urlencode(query)}"
    rows = client.get_many(path, JobRunExt)
    # The empty message names the filters, so "no runs at all" and "none that
    # match" read differently.
    applied = ", ".join(
        part
        for part in (
            f"status {status!r}" if status else "",
            f"source {source_filter!r}" if source_filter else "",
        )
        if part
    )
    empty = (
        f"No runs in org {org!r} matching {applied}."
        if applied
        else f"No runs in org {org!r}."
    )
    _show_rows("runs", "runs", rows, empty, _fmt_run, org=org)


def _run_is_over(run: JobRunExt) -> bool:
    return run.finished_at is not None or run.status in TERMINAL_RUN_STATUSES


def _wait_for_run(
    client: CloudClient, org: str, run_id: str, timeout: int, poll: int
) -> JobRunExt:
    """Poll a run until it is over, or raise naming the state it was stuck in.

    The timeout error is deliberately specific: "still queued after 30m" is a
    saturated or missing executor, while "still running" is a slow job. A bare
    "timed out" would send you to the wrong place.
    """
    deadline = time.monotonic() + timeout
    while True:
        run = client.get_one(f"/orgs/{org}/jobs/runs/{run_id}", JobRunExt)
        if _run_is_over(run):
            return run
        if time.monotonic() >= deadline:
            raise CloudError(
                f"Run {run_id} was still {run.status!r} after {timeout}s. "
                f"It is not cancelled — check `trilogy cloud runs show {run_id}`."
            )
        time.sleep(min(poll, max(0, deadline - time.monotonic())))


def _print_run_logs(run: JobRunExt) -> None:
    for stream, body in (("stdout", run.stdout), ("stderr", run.stderr)):
        text = (body or "").strip()
        if text:
            print_info(f"--- {stream} (tail) ---")
            click.echo(text[-RUN_LOG_TAIL_CHARS:])


def _report_finished_run(org: str, run: JobRunExt, logs: bool = False) -> None:
    """Render a finished run and make a non-success fail the command.

    A failure prints the log tail whether or not it was asked for.
    """
    failed = run.status != SUCCESS_RUN_STATUS
    if is_json_mode():
        emit_event("run_finished", org=org, run=run.model_dump(mode="json"))
    else:
        detail = "" if run.exit_code is None else f" (exit code {run.exit_code})"
        line = f"Run {run.id} of {run.job_name!r}: {run.status}{detail}"
        if failed:
            print_warning(line)
        else:
            print_success(line)
        if run.error:
            print_info(f"  {run.error}")
        if failed or logs:
            _print_run_logs(run)
    if failed:
        raise CloudError(
            f"Run {run.id} finished {run.status!r}. "
            f"See `trilogy cloud runs show {run.id}`."
        )


@runs.command("wait")
@click.argument("run_id")
@click.option(
    "--timeout",
    "timeout",
    type=int,
    default=WAIT_TIMEOUT_SECONDS,
    help=f"Seconds to wait for the run to finish (default: {WAIT_TIMEOUT_SECONDS}).",
)
@click.option(
    "--poll-seconds",
    type=int,
    default=WAIT_POLL_SECONDS,
    help=f"Seconds between status checks (default: {WAIT_POLL_SECONDS}).",
)
@click.option(
    "--logs",
    is_flag=True,
    help="Print the run's log tail even when it succeeded (a failure always does).",
)
@click.pass_context
def runs_wait(
    ctx: click.Context, run_id: str, timeout: int, poll_seconds: int, logs: bool
) -> None:
    """Block until a run finishes; exit non-zero unless it succeeded."""
    client, org = _org_client(ctx)
    run = _wait_for_run(client, org, run_id, timeout, poll_seconds)
    _report_finished_run(org, run, logs=logs)


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
    # What this run executed, which is not necessarily what the job holds now:
    # the version is pinned at creation and the parameters are the rendered
    # values, so both keep answering after the job is edited.
    if run.job_version_number is not None:
        print_info(f"Job version: v{run.job_version_number}")
    if isinstance(run.parameters, dict) and run.parameters:
        pairs = ", ".join(f"{k}={v}" for k, v in sorted(run.parameters.items()))
        print_info(f"Parameters: {pairs}")
    if run.scheduled_for is not None:
        print_info(f"Scheduled for: {_ts(run.scheduled_for)}")
    if run.exit_code is not None:
        print_info(f"Exit code: {run.exit_code}")
    for event in run.events:
        print_info(f"  [{_ts(event.created_at)}] {event.type}: {event.message}")
    if run.files:
        print_info("Files:")
        for step in run.files:
            print_info(f"  {step.status}: {step.name}")
    _print_run_logs(run)


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


#: Directory positions a sync refuses to read as a project even when they
#: carry a declared ``[cloud]`` block: fixtures and scratch, not deployments.
#: Narrower than `DEFAULT_EXCLUDE`, which also filters *files* out of a
#: bundle. A skip here is always reported.
DISCOVERY_EXCLUDE = ("*/__pycache__/*", "*/.venv/*", "*/tests/*", "*/test_*")


@dataclass(frozen=True)
class DeployableProject:
    """One directory that declares itself deployable, ready to become a job."""

    directory: Path
    config_path: Path
    settings: DeploySettings
    #: The ``[cloud] name``, or the path relative to the sync root when the
    #: toml declares none.
    name: str
    #: `{origin}#{subpath}`, plus ``::{key}`` for one entry of a
    #: ``[[cloud.job]]`` array; the identity a sync upserts against.
    source_key: str
    origin: SourceOrigin

    @property
    def include(self) -> tuple[str, ...]:
        return self.settings.include or DEFAULT_INCLUDE

    @property
    def exclude(self) -> tuple[str, ...]:
        """Declared excludes *add* to the defaults, the same way
        ``jobs push --exclude`` does."""
        return DEFAULT_EXCLUDE + (self.settings.exclude or ())


def derive_job_name(root: Path, directory: Path) -> str:
    """A job's name when its ``[cloud]`` block declares none.

    The whole relative path, not the leaf: ``duckdb/covid19_open_data/data`` ->
    ``duckdb-covid19_open_data-data``. A project synced from its own root has
    no path under the root and gets the bare directory name, which is what
    ``[cloud] name`` is for.

    The name is display only; ``source_key`` is what a sync matches on.
    """
    relative = directory.resolve().relative_to(root.resolve())
    parts = [p for p in relative.parts if p not in (".", "")]
    return "-".join(parts) if parts else root.resolve().name


def _excluded_from_discovery(root: Path, directory: Path) -> bool:
    """Whether *directory* sits somewhere a sync never looks for a project.

    Matched on the path *relative to the root*, never the absolute one: the
    patterns describe positions inside a project ("a tests/ directory"), and
    against an absolute path they would also match whatever the checkout
    happens to live under — a repo cloned into ~/test_models would silently
    deploy nothing at all.
    """
    relative = directory.resolve().relative_to(root.resolve()).as_posix()
    # Leading-slash form so "*/tests/*" also catches tests/ at the root.
    return _matches(f"/{relative}", DISCOVERY_EXCLUDE) or _matches(
        relative, DISCOVERY_EXCLUDE
    )


def _reject_nested(found: Sequence[DeployableProject]) -> None:
    """Refuse a project that contains another declared project.

    The outer one's bundle would swallow the inner one's whole tree and both
    would deploy, so the same files run twice under two names. There is no
    reading of that which is obviously intended.
    """
    for outer in found:
        for inner in found:
            if inner is outer:
                continue
            # Several jobs declared by one toml share a directory on purpose,
            # each taking the slice its own include/exclude describes.
            if inner.config_path == outer.config_path:
                continue
            if inner.directory.resolve().is_relative_to(outer.directory.resolve()):
                raise CloudError(
                    f"{inner.config_path} declares a deployable project inside "
                    f"{outer.directory}, which declares one too. The outer "
                    "bundle would contain the inner project and both would "
                    "deploy. Remove the [cloud] deployment keys from one."
                )


def _reject_duplicate_names(found: Sequence[DeployableProject]) -> None:
    """Refuse a sync where two projects would deploy under one name.

    Only a declared name can collide; a path-derived one cannot. Both jobs
    would deploy — identity is `source_key` — but would be indistinguishable in
    every list, run row and schedule binding.
    """
    by_name: dict[str, list[DeployableProject]] = {}
    for project in found:
        by_name.setdefault(project.name, []).append(project)
    for name, projects in by_name.items():
        if len(projects) > 1:
            files = ", ".join(str(p.config_path) for p in projects)
            raise CloudError(
                f"{len(projects)} projects would deploy as {name!r} ({files}). "
                "Job names are how a deploy is read; give each its own "
                "[cloud] name."
            )


def _job_entries(
    table: Mapping[str, Any], defaults: DeploySettings, source: Path
) -> list[DeploySettings]:
    """The ``[[cloud.job]]`` entries of one toml, each merged over *defaults*.

    Empty when the file declares none, which is the single-job form.

    ``key`` and ``name`` are both required per entry, since the path can no
    longer tell two jobs apart: `key` is the identity a sync upserts on
    (immutable — changing it deploys a new job and orphans the old), `name` is
    what the job is called.
    """
    raw = table.get(JOB_ARRAY_KEY)
    if raw is None:
        return []
    if not isinstance(raw, list) or not all(isinstance(e, dict) for e in raw):
        raise _setting_error(
            source, JOB_ARRAY_KEY, "must be an array of tables ([[cloud.job]])"
        )
    if not raw:
        raise _setting_error(
            source, JOB_ARRAY_KEY, "declares no jobs; remove it or add an entry"
        )

    entries: list[DeploySettings] = []
    seen: dict[str, int] = {}
    for index, entry in enumerate(raw):
        settings = DeploySettings.from_table(entry, source).merged_over(defaults)
        if not settings.key:
            raise _setting_error(
                source,
                f"{JOB_ARRAY_KEY}[{index}]",
                "must declare a key — it is this job's identity, and a "
                "directory with several jobs has no path to derive one from",
            )
        if not settings.name:
            raise _setting_error(
                source,
                f"{JOB_ARRAY_KEY}[{index}]",
                f"(key {settings.key!r}) must declare a name",
            )
        if not settings.entrypoint:
            raise _setting_error(
                source,
                f"{JOB_ARRAY_KEY}[{index}]",
                f"(key {settings.key!r}) must declare an entrypoint — several "
                "jobs over one directory share a workspace holding the whole "
                "tree, so the script each one runs is the only thing that "
                "distinguishes them",
            )
        if settings.key in seen:
            raise _setting_error(
                source,
                f"{JOB_ARRAY_KEY}[{index}]",
                f"reuses key {settings.key!r} from entry {seen[settings.key]}; "
                "keys are identities and two jobs sharing one would deploy as one",
            )
        seen[settings.key] = index
        entries.append(settings)
    return entries


def discover_projects(root: Path) -> list[DeployableProject]:
    """Every deployable project under *root*, in a stable order.

    Deployable means "has a trilogy.toml whose ``[cloud]`` block declares at
    least one deployment key". A toml that only names an API and an org is
    configuration and is not deployed.

    A toml may declare **several** jobs over one directory, as
    ``[[cloud.job]]`` entries, each with its own identity, name and bundle
    filters. The entries share the directory and the block's defaults; each
    takes the slice of the tree its `include`/`exclude` describes.
    """
    found: list[DeployableProject] = []
    for config_path in sorted(root.rglob("trilogy.toml")):
        directory = config_path.parent
        try:
            table = tomllib.loads(config_path.read_text(encoding="utf-8")).get(
                "cloud", {}
            )
        except (OSError, tomllib.TOMLDecodeError) as exc:
            raise CloudError(f"Could not parse {config_path}: {exc}") from exc
        if not isinstance(table, dict):
            continue
        defaults = DeploySettings.from_table(table, config_path)
        entries = _job_entries(table, defaults, config_path)
        if not entries and not defaults.declared:
            continue
        if defaults.key and not entries:
            raise _setting_error(
                config_path,
                "key",
                "is only meaningful inside a [[cloud.job]] entry; a directory "
                "declaring one job has its path for an identity",
            )
        if _excluded_from_discovery(root, directory):
            print_warning(
                f"Skipping {config_path}: it is under an excluded path "
                f"({', '.join(DISCOVERY_EXCLUDE)}) but declares a [cloud] "
                "deployment. Move it to deploy it."
            )
            continue
        origin = resolve_origin(directory.resolve())
        for settings in entries or [defaults]:
            found.append(
                DeployableProject(
                    directory=directory,
                    config_path=config_path,
                    settings=settings,
                    name=settings.name or derive_job_name(root, directory),
                    # `::{key}` rather than a deeper `#` segment: the location
                    # is split off `#` by the prune scope check, and a second
                    # one there would make a repository look like a path.
                    source_key=(
                        f"{origin.source_key()}::{settings.key}"
                        if settings.key
                        else origin.source_key()
                    ),
                    origin=origin,
                )
            )
    _reject_nested(found)
    _reject_duplicate_names(found)
    return found


def _resolve_sync_environment(
    client: CloudClient,
    org: str,
    origin: SourceOrigin,
    explicit: str | None,
    create: bool,
) -> tuple[str | None, str | None, bool]:
    """``(environment_id, environment_name, exists)`` for this sync.

    *origin* is the sync **root's**, not any one project's: the environment is
    a property of the checkout being deployed.

    ``--environment`` wins; otherwise the branch decides, and a default branch
    (or no branch at all) means production, which has no row and always
    "exists". The environment is created on demand, through an idempotent
    route, so no separate setup step is needed.

    An empty *explicit* is production, which is what ``--production`` resolves
    to and what ``--environment "$(trilogy cloud env label)"`` resolves to on a
    default branch.

    Every non-empty name is an environment of that name, with no reserved
    words: ``--environment production`` targets an environment called
    `production`. A name that is not a valid identifier is refused rather than
    created, since it prefixes managed tables and suffixes managed files.

    ``create=False`` is what makes ``--dry-run`` truthful: it looks the name up
    instead of creating it, and reports an absent environment.
    """
    label = (explicit if explicit is not None else origin.environment_label()) or ""
    label = label.strip()
    if not label:
        return None, None, True
    if explicit is not None and not is_valid_environment_name(label):
        raise CloudError(
            f"{label!r} cannot be an environment name: it prefixes managed "
            "tables and suffixes managed files, so it must be letters, digits "
            "and underscores, starting with a letter or underscore. "
            "`trilogy cloud env label <branch>` prints the name a branch maps "
            "to."
        )
    if not create:
        for existing in client.get_many(f"/orgs/{org}/environments", EnvironmentExt):
            if existing.name == label:
                return existing.id, existing.name, True
        return None, label, False
    env = client.post_one(
        f"/orgs/{org}/environments",
        Environment,
        {
            "name": label,
            "source_kind": origin.kind,
            "source_location": origin.location,
            "source_ref": origin.branch,
        },
    )
    return env.id, env.name, True


@cloud.command("sync")
@click.argument(
    "root",
    type=click.Path(exists=True, file_okay=False, path_type=Path),
    default=".",
)
@click.option(
    "--environment",
    "environment_flag",
    default=None,
    help="Deployment environment to sync into: an existing one is updated and "
    "a new name is created. Empty ('') means production, so a CI step can pass "
    "`$(trilogy cloud env label)` on every branch. Default: derived from the "
    "git branch, with a default branch (main/master) meaning production.",
)
@click.option(
    "--production",
    is_flag=True,
    help="Sync into production from any checkout, updating production's own "
    "jobs. Production has no environment to name, so this is how a branch "
    "checkout targets it.",
)
@dry_run_option("Report what would change and write nothing.")
@click.option(
    "--prune",
    is_flag=True,
    help="Delete jobs in the target environment whose source directory is gone. "
    "Limited to the repositories ROOT covers, so jobs synced from elsewhere "
    "into the same org are left alone.",
)
@click.pass_context
def cloud_sync(
    ctx: click.Context,
    root: Path,
    environment_flag: str | None,
    production: bool,
    dry_run: bool,
    prune: bool,
) -> None:
    """Deploy every deployable project under ROOT as a job.

    A project is deployable when its ``trilogy.toml`` carries a ``[cloud]``
    block declaring how it runs::

        [cloud]
        operation = "refresh"
        timeout_seconds = 1800
        schedule = "0 0 7 * * *"
        secret_env = ["GOOGLE_HMAC_KEY", "GOOGLE_HMAC_SECRET"]

    A job's name comes from its path under ROOT unless the block declares one::

        [cloud]
        name = "urban-tree-data"

    which is worth doing when the path is not descriptive — a project that is
    its own repository is synced from its own directory, so it inherits a bare
    leaf name like ``data``. Renaming is safe at any time: the name is display
    only, and everything that points at a job (schedules, runs, history, and
    this command's own upsert) goes through its id or its ``source_key``.

    *Identity* is not declarable. It comes from the repository and
    subdirectory the project lives in (``source_key``), stable across branches
    and commits, which is what groups a branch's job under the production job
    it forked from.

    It is **not** stable across a move: relocating a directory deploys a new
    job and leaves the old one behind with its run history, which ``--prune``
    clears out. A declared name renames a job, it does not move it.

    Which environment it syncs into comes from the current branch, so the same
    command in CI deploys main to production and a feature branch to its own
    namespace. ``--environment`` overrides that and targets rather than forks:
    it updates the named environment's jobs, creating the environment if it
    does not exist.

    **Production is reached by ``--production``, not by naming it.** A job with
    no environment is a production job, so production has no environment for
    ``--environment`` to name — `production` there is an environment called
    `production`, like any other name. ``--environment ""`` means production
    too, for a CI step that templates the flag from ``env label``.

    **Existing jobs are not adopted by name.** A job the platform holds with no
    ``source_key`` — anything created by hand or by ``jobs push`` — is invisible
    to this command and will be duplicated rather than updated.
    """
    client, org = _org_client(ctx)
    if production and environment_flag is not None:
        raise CloudError(
            "--production and --environment name different targets; pass one."
        )
    # Resolved to the empty explicit rather than carried separately, so both
    # spellings take one path.
    if production:
        environment_flag = ""
    projects = discover_projects(root)
    if not projects:
        raise CloudError(
            f"No deployable projects under {root}: no trilogy.toml declares a "
            f"[cloud] block with any of {', '.join(JOB_DECLARING_KEYS)}."
        )

    # The environment belongs to the checkout, so it is resolved from ROOT and
    # not from any one project — a root spanning a submodule would otherwise
    # take its branch from whichever project happened to sort first. Nothing
    # here is git-specific: a directory with no repository has no branch, so it
    # resolves to production, which is the same answer main gives.
    # `--environment` is the manual override for a parallel namespace with no
    # branch behind it.
    env_id, env_name, env_exists = _resolve_sync_environment(
        client,
        org,
        resolve_origin(root.resolve()),
        environment_flag,
        create=not dry_run,
    )
    target = env_name or "production"
    print_info(f"Syncing {len(projects)} project(s) to {org!r} / {target}")

    # A branch environment that does not exist yet holds nothing, so nothing can
    # match. Without this an absent environment falls back to `env_id is None`
    # and a dry run compares against *production's* jobs — reporting "would
    # update" for rows it would never touch.
    if env_name is not None and not env_exists:
        by_key: dict[str, Job] = {}
    else:
        by_key = {
            job.source_key: job
            for job in client.get_many(f"/orgs/{org}/jobs", Job)
            if job.source_key and job.environment_id == env_id
        }
    # Fetched once rather than per project: a sync of twenty directories should
    # not be twenty identical list calls. Going stale within the pass is fine —
    # each job owns a distinct binding, so no two projects contend for one row.
    # Only production reconciles schedules at all, so a branch sync skips the
    # call entirely.
    schedules = (
        client.get_many(f"/orgs/{org}/schedules", ScheduleExt)
        if env_name is None
        else []
    )

    results: list[dict] = []
    # (config_path, cron) -> the jobs that declared it. Several jobs from one
    # toml on one cron share a schedule *row*, which is what makes them one
    # tick — and the tick is what the platform orders by dependency. One
    # schedule each would put a refresh and the jobs that publish its output
    # in separate firings with nothing between them but wall clock, which is
    # the arrangement schedule ordering exists to replace.
    # One workspace per multi-job toml, built before its jobs so they can be
    # created already bound to it. A single-job project keeps deploying
    # self-contained: it has no siblings to share a tree with, and giving it a
    # workspace would be ceremony with no reader.
    workspace_ids: dict[Path, str | None] = {}
    for project in projects:
        if project.settings.key is None or project.config_path in workspace_ids:
            continue
        name = project.settings.workspace or derive_job_name(root, project.directory)
        if env_name is not None:
            # A workspace has no environment of its own, so a branch sync
            # deploying into the production workspace would overwrite
            # production's shared tree with the branch's. Namespacing the name
            # keeps the two apart, the same way the environment keeps the jobs
            # apart.
            name = f"{name}-{env_name}"
        # The whole tree, filtered only by the block's own hygiene globs: in
        # workspace mode the per-job filters have nothing left to narrow.
        files = collect_files(
            project.directory.resolve(), project.include, project.exclude
        )
        files = [f for f in files if f["name"] != "trilogy.toml"]
        ws_id, ws_outcome = _ensure_workspace(
            client,
            org,
            name,
            project.config_path.read_text(encoding="utf-8"),
            files,
            dry_run,
        )
        workspace_ids[project.config_path] = ws_id
        print_info(f"  {ws_outcome:>12}  workspace {name} ({len(files)} file(s))")

    groups: dict[tuple[Path, str | None], list[Job]] = {}
    # Every job a given toml declares, across all its groups — what decides
    # whether an existing schedule is this toml's to replace. See
    # `_reconcile_schedule`: matching on the group alone cannot recognize the
    # row it wrote yesterday once the group has gained or lost a job.
    declared_by_toml: dict[Path, list[Job]] = {}
    for project in projects:
        outcome, job = _sync_one(
            client,
            org,
            project,
            by_key,
            env_id,
            schedules,
            dry_run,
            workspace_ids.get(project.config_path),
        )
        results.append(outcome)
        print_info(f"  {outcome['outcome']:>12}  {project.name}")
        if job is not None and env_id is None:
            groups.setdefault(
                (project.config_path, project.settings.schedule), []
            ).append(job)
            declared_by_toml.setdefault(project.config_path, []).append(job)

    for (config_path, cron), jobs in groups.items():
        _reconcile_schedule(
            client, org, jobs, cron, schedules, declared_by_toml[config_path]
        )

    pruned = _prune_stale(client, org, projects, by_key, dry_run) if prune else 0

    if is_json_mode():
        emit_event(
            "sync",
            org=org,
            environment=env_name,
            dry_run=dry_run,
            jobs=results,
            pruned=pruned,
        )
        return
    print_success(_sync_summary(results, target, dry_run, pruned if prune else None))


def _sync_summary(
    results: Sequence[Mapping[str, Any]],
    target: str,
    dry_run: bool,
    pruned: int | None,
) -> str:
    """The closing line, counting what actually happened.

    A dry run reports creates and updates separately and says nothing about
    "unchanged": having sent no ``PUT``, it cannot know whether one would have
    minted a version.
    """
    tally = Counter(str(r["action"] if dry_run else r["outcome"]) for r in results)
    suffix = f", {pruned} pruned" if pruned is not None else ""
    if dry_run:
        return (
            f"Would sync {len(results)} project(s) to {target}: "
            f"{tally['create']} to create, {tally['update']} to update{suffix}"
        )
    return (
        f"Synced {len(results)} project(s) to {target}: {tally['created']} created, "
        f"{tally['updated']} updated, {tally['unchanged']} unchanged{suffix}"
    )


def _prune_stale(
    client: CloudClient,
    org: str,
    projects: Sequence[DeployableProject],
    by_key: Mapping[str, Job],
    dry_run: bool,
) -> int:
    """Delete jobs in this environment whose source directory is gone.

    **Scoped to the repositories ROOT actually covers**, by the location half
    of each `source_key` (``{location}#{subpath}``), so jobs synced into the
    same environment from another repository are left alone.
    """
    live = {p.source_key for p in projects}
    locations = {p.origin.location for p in projects}
    pruned = 0
    for key, job in by_key.items():
        if key in live or key.split("#", 1)[0] not in locations:
            continue
        if not dry_run:
            client.delete(f"/orgs/{org}/jobs/{job.id}")
        pruned += 1
        print_info(f"    {'would prune' if dry_run else 'pruned':>12}  {job.name}")
    return pruned


def _ensure_workspace(
    client: CloudClient,
    org: str,
    name: str,
    config_text: str,
    files: list[dict],
    dry_run: bool,
) -> tuple[str | None, str]:
    """Create or update the workspace a multi-job project deploys into, and
    say which happened.

    **The workspace holds the whole tree.** Its jobs carry no files at all,
    only which script of it they run.

    `config` stays on the *jobs*: workspace config layering needs pytrilogy's
    `--config-overlay`, which has not shipped. Files are the part that moves.

    Matched by name, which is unique per org. A workspace has no `source_key`,
    so a renamed one is a new workspace and the old one is left behind;
    `--prune` does not cover it.

    Everything a sync does not declare is **carried** off the workspace being
    updated, through the payload builder `workspaces push` uses, since a `PUT`
    replaces a workspace wholesale and this body names only the tree.
    """
    existing = next(
        (
            w
            for w in client.get_many(f"/orgs/{org}/workspaces", Workspace)
            if w.name == name
        ),
        None,
    )
    body = _workspace_payload(
        name=name,
        files=files,
        config_text=None,
        declared={"description": f"Shared project tree for {name}"},
        existing=existing,
    )
    if dry_run:
        return (existing.id if existing else None), (
            "would update" if existing else "would create"
        )
    if existing:
        client.put_one(f"/orgs/{org}/workspaces/{existing.id}", Workspace, body)
        return existing.id, "updated"
    created = client.post_one(f"/orgs/{org}/workspaces", Workspace, body)
    return created.id, "created"


def _sync_one(
    client: CloudClient,
    org: str,
    project: DeployableProject,
    by_key: Mapping[str, Job],
    env_id: str | None,
    schedules: Sequence[ScheduleExt],
    dry_run: bool,
    workspace_id: str | None = None,
) -> tuple[dict, Job | None]:
    """Create or update one project's job.

    Returns the result record and the deployed job — the latter so the caller
    can reconcile schedules *by group*, which it cannot do one project at a
    time: several jobs declared by one toml on one cron have to land on one
    schedule row, because a schedule is what the platform co-executes and
    orders. `None` on a dry run, which deploys nothing to schedule.
    """
    # A job in a workspace carries **no files**: the workspace holds the whole
    # tree and the job says which script of it to run. That is the entire
    # point of the arrangement — one copy of the project instead of one per
    # job, and a Workspaces screen that shows what is actually shared.
    if workspace_id is not None:
        files: list[dict] = []
    else:
        files = collect_files(
            project.directory.resolve(), project.include, project.exclude
        )
        files = [f for f in files if f["name"] != "trilogy.toml"]
    config_text = project.config_path.read_text(encoding="utf-8")
    # Digest over what identifies *this* job. In workspace mode its files live
    # elsewhere, so the entrypoint stands in for them: two jobs sharing a
    # workspace differ by which script they run, and a fingerprint that
    # ignored it would report them as the same content pushed twice.
    fingerprint = SourceFingerprint.build(
        content_digest(config_text + (project.settings.entrypoint or ""), files),
        project.origin,
    )

    found = by_key.get(project.source_key)
    action = "update" if found else "create"
    payload = _job_payload(
        name=project.name,
        config_text=config_text,
        files=files,
        fingerprint=fingerprint,
        declared=project.settings.job_fields(),
        existing=found,
        extra={
            "source_key": project.source_key,
            "environment_id": env_id,
            # Sent on create; a content PUT leaves the binding alone, so an
            # existing job is rebound below instead.
            **({"workspace_id": workspace_id} if workspace_id else {}),
        },
    )
    # Measured on a dry run too: a bundle over the queue limit is exactly the
    # kind of thing a dry run exists to find, and skipping the check here would
    # let one pass cleanly and then fail the real sync.
    encoded = check_bundle_size(payload)

    if dry_run:
        return {
            "name": project.name,
            "source_key": project.source_key,
            "action": action,
            "outcome": f"would {action}",
        }, None

    # A job's workspace is *identity*, so a content PUT leaves it alone and an
    # existing job has to be rebound explicitly.
    #
    # **Before** the content write, not after: the API resolves the entrypoint
    # against the job's *current* chain, so an unbound job has no file the
    # entrypoint could name.
    if (
        found is not None
        and workspace_id is not None
        and found.workspace_id != workspace_id
    ):
        if not dry_run:
            client.request(
                "PATCH", f"/orgs/{org}/jobs/{found.id}", {"workspace_id": workspace_id}
            )
        print_info(f"    {'bound':>12}  {project.name} to its project workspace")

    job, outcome = _upsert_job(client, org, encoded, found)

    # The server is authoritative on the name, and an API older than renameable
    # jobs answers with the one it already had. Saying so is the difference
    # between "this deployment cannot do that yet" and a sync that reports
    # `updated` every time while the name never moves.
    if job.name != project.name:
        print_warning(
            f"    {'not renamed':>12}  {project.name}: the API kept {job.name!r}. "
            "It is running a build from before job names could be declared."
        )

    # Schedules belong to production only; a branch job is triggered by hand
    # or by CI. It also keeps the binding match unambiguous, since derived
    # names are identical across environments.
    if env_id is not None and project.settings.schedule:
        print_info(
            f"    {'unscheduled':>12}  {project.name} "
            "(branch build; trigger it by hand)"
        )
    return {
        "name": project.name,
        "id": job.id,
        "source_key": project.source_key,
        "action": action,
        "outcome": outcome,
    }, job


def _reconcile_schedule(
    client: CloudClient,
    org: str,
    jobs: Sequence[Job],
    cron: str | None,
    schedules: Sequence[ScheduleExt],
    owned: Sequence[Job],
) -> None:
    """Bring one group's schedule in line with what its config declares.

    A *group* is the jobs one toml declares on one cron. They share a schedule
    **row**: the platform fires a schedule as a single tick and orders that
    tick by what each job reads and writes, so a shared row is what makes a
    publish wait for the refresh it reads instead of racing it.

    **Matched by binding — by job id, not by name.** Schedules are listed
    before any job is PUT, so a sync that renames a job would not recognize its
    own row by name. Falls back to names for a schedule whose ``job_ids`` came
    back empty, which is what an API older than that field answers with.

    **Ownership is "binds only jobs this toml declares", not "binds exactly
    this group"**, so a schedule still matches after a job is added to or
    removed from the toml. *owned* is every job this toml declares, across all
    its groups; a schedule binding a job from outside it is left alone.

    A declared cron that differs is replaced rather than edited, since there is
    no schedule update route; an unchanged one is left untouched, so a sync
    does not churn `next_run_at`.

    *schedules* is the org's list, fetched once by the caller.
    """
    ids = {job.id for job in jobs}
    names = {job.name for job in jobs}
    owned_ids = {job.id for job in owned}
    owned_names = {job.name for job in owned}
    label = ", ".join(sorted(names))

    def is_mine(s: ScheduleExt) -> bool:
        # `job_ids` is empty against an API older than the field; fall back to
        # names, which is the same answer for every case that existed then.
        bound_ids, bound_names = set(s.job_ids), set(s.job_names)
        if bound_ids:
            return bool(bound_ids) and bound_ids <= owned_ids and bool(bound_ids & ids)
        return (
            bool(bound_names)
            and bound_names <= owned_names
            and bool(bound_names & names)
        )

    mine = [s for s in schedules if is_mine(s)]
    if cron is None:
        for schedule in mine:
            client.delete(f"/orgs/{org}/schedules/{schedule.id}")
            print_info(f"    {'unscheduled':>12}  {label}")
        return

    # "Already correct" means the cadence *and* the bindings match: ownership
    # is broader than the group, so a row can have the right cron and the
    # wrong jobs.
    def binds_this_group(s: ScheduleExt) -> bool:
        return set(s.job_ids) == ids if s.job_ids else set(s.job_names) == names

    current = [s for s in mine if s.cron_expr == cron and binds_this_group(s)]
    stale = [s for s in mine if s not in current]
    if current:
        for schedule in stale:
            client.delete(f"/orgs/{org}/schedules/{schedule.id}")
            print_info(f"    {'unscheduled':>12}  a superseded schedule for {label}")
        return
    for schedule in mine:
        client.delete(f"/orgs/{org}/schedules/{schedule.id}")
    # Named after the group rather than any one member: with several jobs
    # there is no member whose name describes the row, and a schedule called
    # after the first one alphabetically would be a worse lie than a generic
    # name. Alphabetical so the name is stable across syncs.
    first = min(names)
    name = (
        f"{first}-schedule" if len(jobs) == 1 else f"{first}-+{len(jobs) - 1}-schedule"
    )
    client.post_one(
        f"/orgs/{org}/schedules",
        Schedule,
        {"name": name, "cron_expr": cron, "job_ids": sorted(ids)},
    )
    print_info(f"    {'scheduled':>12}  {label} at {cron!r}")


@cloud.group("env")
def cloud_env() -> None:
    """Manage deployment environments (parallel, branch-scoped builds).

    A job in an environment builds into a namespace of its own — pytrilogy
    prefixes its managed tables and suffixes its managed files with the
    environment's name — so a branch can run against the cloud without building
    over production. The default environment is the *absence* of one: jobs with
    no environment build unprefixed production addresses.
    """


def _fmt_environment(env: EnvironmentExt) -> str:
    marks = []
    if env.is_default:
        marks.append("default")
    if env.source_ref:
        marks.append(env.source_ref)
    suffix = f"  ({', '.join(marks)})" if marks else ""
    return f"{env.id}  {env.name!r}  jobs: {env.job_count}{suffix}"


@cloud_env.command("list")
@click.pass_context
def env_list(ctx: click.Context) -> None:
    """List the org's environments and how many jobs each holds."""
    client, org = _org_client(ctx)
    _show_rows(
        "environments",
        "environments",
        client.get_many(f"/orgs/{org}/environments", EnvironmentExt),
        f"No environments in org {org!r} (everything builds in production).",
        _fmt_environment,
        org=org,
    )


@cloud_env.command("label")
@click.argument("branch", required=False, default=None)
def env_label(branch: str | None) -> None:
    """Print the environment label a branch maps to, and nothing for a default
    branch.

    Purely local: it contacts no API and needs no login, so a CI step can run
    it before or instead of a deploy. Reads the current checkout's branch when
    none is given.

    The sanitizing is lossy and digest-suffixed, so anything that needs the
    label should ask for it here rather than derive it again.

    Exits 0 either way; a default branch simply prints nothing.
    """
    if branch is None:
        branch = resolve_origin(Path.cwd()).branch
    label = environment_label(branch)
    if label:
        click.echo(label)


@cloud_env.command("create")
@click.argument("name")
@click.option("--description", default=None)
@click.option("--source-ref", default=None, help="Branch this tracks, for display.")
@click.option(
    "--source-location", default=None, help="Repository this tracks, for display."
)
@click.pass_context
def env_create(
    ctx: click.Context,
    name: str,
    description: str | None,
    source_ref: str | None,
    source_location: str | None,
) -> None:
    """Create an environment (idempotent — an existing one of this name is
    returned unchanged, so CI can call it on every push)."""
    client, org = _org_client(ctx)
    env = client.post_one(
        f"/orgs/{org}/environments",
        Environment,
        {
            "name": name,
            "description": description,
            "source_kind": "git" if source_location or source_ref else None,
            "source_location": source_location,
            "source_ref": source_ref,
        },
    )
    print_success(f"Environment {env.name!r} ({env.id}) ready in org {org!r}")


@cloud_env.command("fork")
@click.argument("source")
@click.argument("name")
@click.option("--source-ref", default=None, help="Branch the fork tracks.")
@click.pass_context
def env_fork(
    ctx: click.Context, source: str, name: str, source_ref: str | None
) -> None:
    """Fork SOURCE's jobs into a new environment NAME.

    SOURCE is an environment id, or ``default`` for production, which has no
    row of its own. Copies jobs only: run history stays with the job that
    produced it, and the fork's jobs start unscheduled.
    """
    client, org = _org_client(ctx)
    result = client.post(
        f"/orgs/{org}/environments/{source}/fork",
        {
            "name": name,
            "source_kind": "git" if source_ref else None,
            "source_ref": source_ref,
        },
    )
    copied = (result or {}).get("jobs_copied", 0)
    print_success(f"Forked {source} into {name!r} ({copied} job(s) copied)")


@cloud_env.command("delete")
@click.argument("name")
@click.option(
    "--with-jobs",
    is_flag=True,
    help="Delete the environment's jobs, their runs and their schedules too.",
)
@click.option(
    "--keep-jobs",
    is_flag=True,
    help="Delete only the environment record. Its jobs fall back into "
    "production and keep firing on their own schedules — say so deliberately.",
)
@click.option("--yes", is_flag=True, help="Skip the confirmation prompt.")
@click.pass_context
def env_delete(
    ctx: click.Context, name: str, with_jobs: bool, keep_jobs: bool, yes: bool
) -> None:
    """Delete an environment by name — the teardown for a merged branch.

    **An environment that holds jobs needs ``--with-jobs`` or ``--keep-jobs``.**
    Deleting the record does not delete the jobs; it reparents them, and a job
    with no environment is a production job — so ``--keep-jobs`` moves them
    into production, schedules included, where they fire alongside production's
    own jobs. An empty environment deletes with no flag.

    Warehouse assets the environment built are **not** touched: they are in the
    warehouse, not the platform, and dropping them is
    ``trilogy env delete <name> --drop-assets`` against the project itself.
    """
    client, org = _org_client(ctx)
    if with_jobs and keep_jobs:
        raise CloudError(
            "--with-jobs and --keep-jobs ask for opposite things; pass one."
        )
    env = _find_environment(client, org, name)

    if env.job_count and not (with_jobs or keep_jobs):
        raise CloudError(
            f"Environment {env.name!r} holds {env.job_count} job(s), and "
            "deleting it does not delete them — a job with no environment is a "
            "production job, so they would run in production on the schedules "
            "they have now. Pass --with-jobs to delete them with the "
            "environment, or --keep-jobs to move them into production."
        )
    if env.job_count and not yes:
        prompt = (
            f"Delete {env.job_count} job(s) in {env.name!r}, with their run "
            "history and schedules?"
            if with_jobs
            else f"Move {env.job_count} job(s) out of {env.name!r} into "
            "production, schedules included?"
        )
        click.confirm(prompt, abort=True)

    # Read before the delete: afterwards they are ordinary production jobs,
    # name-identical to the ones they were branched from.
    moved = (
        [
            job
            for job in client.get_many(f"/orgs/{org}/jobs", Job)
            if job.environment_id == env.id
        ]
        if keep_jobs and env.job_count
        else []
    )

    query = "?cascade=jobs" if with_jobs else ""
    result = client.request("DELETE", f"/orgs/{org}/environments/{env.id}{query}") or {}
    deleted = result.get("jobs_deleted", 0)
    print_success(
        f"Deleted environment {env.name!r}"
        + (
            f" and {deleted} job(s)"
            if with_jobs
            else f" ({env.job_count} job(s) left in production)"
        )
    )
    if moved:
        print_warning(
            f"{len(moved)} job(s) now run in production on their existing "
            "schedules: "
            + ", ".join(
                f"{job.name} ({job.id})" for job in sorted(moved, key=lambda j: j.name)
            )
            + ". Remove them with `trilogy cloud jobs delete <id>`."
        )


def _find_environment(client: CloudClient, org: str, name_or_id: str) -> EnvironmentExt:
    envs = client.get_many(f"/orgs/{org}/environments", EnvironmentExt)
    for env in envs:
        if env.id == name_or_id or env.name == name_or_id:
            return env
    known = ", ".join(e.name for e in envs) or "none"
    raise CloudError(f"No environment {name_or_id!r} in org {org!r}. Known: {known}.")


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
