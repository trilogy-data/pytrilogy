"""Typed models for the trilogy-cloud API.

Mirrors the serialized shape of the Rust API's response types — each model
below names its source struct. The API is the contract; when it changes, these
follow. Pydantic ignores unknown fields by default, so a server that adds a
field stays readable by an older CLI.

``SourceFingerprint`` is the exception that goes the other way: the CLI writes
it, and a server that does not yet know the field drops it. See its docstring.

Two Rust patterns matter for reading these side by side:

* ``#[serde(flatten)]`` inlines a struct's fields into its parent, so the
  ``*Ext`` wrappers are modelled here as subclasses rather than nesting.
* ``Option<T>`` is the only thing that may be ``null``; a bare ``T`` is always
  present, so it has no default here either.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any

from pydantic import BaseModel

from trilogy.scripts.source_identity import SOURCE_FINGERPRINT_VERSION, SourceOrigin

# ============================================================================
# Auth (api/src/auth)
# ============================================================================


class OrgSummary(BaseModel):
    """``org_guard.rs::OrgSummary`` — an org plus the caller's role in it."""

    id: str
    name: str
    slug: str
    role: str
    theme_primary_color: str | None = None
    theme_secondary_color: str | None = None
    created_at: datetime


class Me(BaseModel):
    """``auth/routes.rs::MeResponse`` — ``UserInfo`` flattened, plus orgs."""

    id: str
    email: str
    name: str | None = None
    picture_url: str | None = None
    provider: str
    orgs: list[OrgSummary] = []


class TokenSummary(BaseModel):
    """``auth/tokens.rs::TokenSummary`` — a token's metadata, never its value."""

    id: str
    name: str
    token_prefix: str
    created_at: datetime
    last_used_at: datetime | None = None
    expires_at: datetime | None = None


class IssuedToken(BaseModel):
    """``auth/tokens.rs::IssuedToken`` — the creation response, and the only
    place ``token`` (the full value) is ever populated."""

    id: str
    name: str
    token: str
    token_prefix: str
    created_at: datetime
    expires_at: datetime | None = None


# ============================================================================
# Jobs, runs, schedules (api/src/models/job.rs, api/src/routes)
# ============================================================================


class SourceFingerprint(BaseModel):
    """What a pushed bundle was, and where it came from.

    Written by ``jobs push`` onto the create/update payload; built by
    ``trilogy.scripts.source_identity``, which owns the rules. The one model
    here the CLI *sends* — the API ignores unknown request fields, so a server
    that does not store it yet accepts the push unchanged and simply answers
    with ``source_fingerprint`` unset. Nothing may depend on the round trip.

    ``content`` is the digest of the config text plus the exact file set sent
    (after ``--rewrite``), so two pushes agree iff they carried the same bytes
    — which is what makes "this job is running the code in my working
    directory" answerable at all. ``origin`` is the git remote when there is
    one, else an opaque local token: absolute paths never leave the machine.
    """

    version: int = SOURCE_FINGERPRINT_VERSION
    content: str
    origin: str
    origin_kind: str
    #: Directory within the repository, ``"."`` at its root. Unset for a local
    #: origin, where there is no enclosing tree to be relative to.
    path: str | None = None
    revision: str | None = None
    branch: str | None = None

    @classmethod
    def build(cls, content: str, origin: SourceOrigin) -> SourceFingerprint:
        return cls(
            content=content,
            origin=origin.location,
            origin_kind=origin.kind,
            path=origin.subpath,
            revision=origin.revision,
            branch=origin.branch,
        )


class Environment(BaseModel):
    """``models/environment.rs::Environment`` — a named parallel deployment.

    The ``name`` is what reaches the executor as ``--environment``, and what
    pytrilogy prefixes managed tables and suffixes managed files with. It is an
    unquoted SQL identifier prefix on that side, so both ends constrain it to
    ``^[A-Za-z_][A-Za-z0-9_]*$``; ``source_identity.environment_label`` is what
    derives a valid one from a branch name.

    A job whose ``environment_id`` is ``None`` is in the *default* environment,
    which builds unprefixed production addresses — so "no environment" and
    "production" are the same thing, and there is no row for it.
    """

    id: str
    org_id: str
    name: str
    description: str | None = None
    is_default: bool = False
    #: What the environment tracks (`git` / remote / branch). Advisory.
    source_kind: str | None = None
    source_location: str | None = None
    source_ref: str | None = None
    parent_environment_id: str | None = None
    created_at: datetime
    updated_at: datetime


class EnvironmentExt(Environment):
    """``routes/environments.rs::EnvironmentExt`` — plus what a prune would
    take with it, so the CLI can say how many jobs are at stake before it
    deletes anything."""

    job_count: int = 0


class Job(BaseModel):
    """``models/job.rs::Job``.

    ``config`` is the job's trilogy.toml as raw text, carried in a JSON string
    rather than a parsed table — the platform stores it verbatim and hands it
    to the worker as a file.

    The row is stable *identity*; the content fields are a copy of the newest
    ``job_versions`` row, which ``current_version_id`` names. Editing content
    (``PUT``) mints a version and moves that pointer under the same id, so run
    history and schedule bindings survive an edit — the movement of
    ``current_version_id`` across a write is how the CLI tells a real update
    from a no-op push.
    """

    id: str
    org_id: str
    name: str
    description: str | None = None
    config: Any = None
    files: Any | None = None
    schedule: str | None = None
    operation: str
    timeout_seconds: int | None = None
    memory_mb: int | None = None
    cpus: float | None = None
    secret_env: Any | None = None
    #: Workspace whose config/files/parameters this job inherits; `None` =
    #: self-contained. Identity, not content: it is moved by `PATCH`, and a
    #: content `PUT` leaves it alone.
    workspace_id: str | None = None
    #: `--param key=value` pairs, merged per key over the workspace chain's.
    parameters: Any | None = None
    #: `"shared"` (may colocate with the tenant's other shared jobs on one VM)
    #: | `"exclusive"` | `None` (inherit down the workspace chain).
    vm_class: str | None = None
    #: 0 realtime .. 4 background, and the wall-clock a queued run may wait.
    #: Content, not identity: a `PUT` replaces them, so both have to be read
    #: back off the job and resent by any update that was not told otherwise —
    #: which is the only reason they are modelled here at all.
    priority: int | None = None
    deadline_seconds: int | None = None
    #: `None` on rows that predate versioning and have never been PUT.
    current_version_id: str | None = None
    #: Unset by every server that does not record push provenance; see
    #: `SourceFingerprint`.
    source_fingerprint: SourceFingerprint | None = None
    #: Parallel deployment this job builds into; `None` = the default
    #: (production) environment. Identity, not content — a content `PUT`
    #: leaves it alone, exactly like `workspace_id`.
    environment_id: str | None = None
    #: Cross-environment identity, `{origin}#{subpath}`. This is what `sync`
    #: matches on; the name is derived from the path and free to change.
    #: `None` on jobs created before sync existed or by hand.
    source_key: str | None = None
    created_at: datetime
    updated_at: datetime


class JobVersion(BaseModel):
    """``models/job.rs::JobVersion`` — one immutable snapshot of a job's
    content, from ``GET /orgs/{slug}/jobs/{id}/versions`` (newest first).

    Content rides along: a version is small, and "what changed between v3 and
    v4" is the whole reason to ask. Rolling back is re-``PUT``ing an old
    version's content, which mints a *new* version — history is never
    rewritten.
    """

    id: str
    job_id: str
    version_number: int
    config: Any = None
    files: Any | None = None
    operation: str
    timeout_seconds: int | None = None
    memory_mb: int | None = None
    cpus: float | None = None
    secret_env: Any | None = None
    parameters: Any | None = None
    vm_class: str | None = None
    source_fingerprint: SourceFingerprint | None = None
    created_at: datetime


class JobRunEvent(BaseModel):
    """``models/job.rs::JobRunEvent`` — one entry on a run's timeline."""

    id: str
    run_id: str
    type: str
    message: str
    data: Any | None = None
    created_at: datetime


class JobStep(BaseModel):
    """``models/job.rs::JobStep`` — a per-file execution result, fanned out of
    the run's structured report by the distributor."""

    id: str
    run_id: str
    name: str
    order: int
    status: str
    started_at: datetime | None = None
    finished_at: datetime | None = None
    output: Any = None
    error: str | None = None


class JobRun(BaseModel):
    """``models/job.rs::JobRun`` — the bare run, as returned by triggering one.

    A run pins the content it executes rather than re-reading the job:
    ``job_version_id`` is stamped when the run is created and
    ``workspace_versions`` (``[{workspace_id, version_id}]``, nearest first)
    at its first dispatch. Both ``None`` means "resolve live" — a run that
    predates versioning, or one not yet dispatched.
    """

    id: str
    job_id: str
    schedule_id: str | None = None
    status: str
    started_at: datetime | None = None
    finished_at: datetime | None = None
    output: Any = None
    #: The rendered `--param` *values* this run used — never the schedule's
    #: templates. The row is the record of what actually ran.
    parameters: Any | None = None
    #: The tick's logical time for a scheduled run; `None` for a manual one.
    #: With `schedule_id` it names the run's siblings from the same firing.
    scheduled_for: datetime | None = None
    job_version_id: str | None = None
    workspace_versions: Any | None = None
    #: Declared `--partition` selectors for a slice-scoped refresh; `None` =
    #: unscoped.
    partition_targets: Any | None = None
    error: str | None = None
    stdout: str | None = None
    stderr: str | None = None
    exit_code: int | None = None
    created_at: datetime


class JobRunExt(JobRun):
    """``routes/jobs.rs::JobRunExt`` — a run joined to its job and schedule
    names. ``events`` and ``files`` are populated on the detail route only."""

    job_name: str
    schedule_name: str | None = None
    #: The pinned version's ordinal, joined from `job_versions` — the id alone
    #: means nothing to a reader. `None` = an unpinned, pre-versioning run.
    job_version_number: int | None = None
    events: list[JobRunEvent] = []
    files: list[JobStep] = []


class Schedule(BaseModel):
    """``models/job.rs::Schedule``."""

    id: str
    org_id: str
    name: str
    cron_expr: str
    is_active: bool
    next_run_at: datetime
    created_at: datetime
    updated_at: datetime


class ScheduleExt(Schedule):
    """``routes/schedules.rs::ScheduleExt`` — a schedule plus its bound jobs."""

    #: The binding, which is what reconciliation matches on. Defaulted empty
    #: because it postdates the field below: an API that does not send it
    #: leaves this list empty rather than failing to parse, and the caller
    #: falls back to names for that schedule.
    job_ids: list[str] = []
    job_names: list[str] = []


class SecretMeta(BaseModel):
    """``routes/secrets.rs::SecretMeta`` — names and timestamps; values are
    write-only and never leave the platform."""

    name: str
    created_at: datetime
    updated_at: datetime
