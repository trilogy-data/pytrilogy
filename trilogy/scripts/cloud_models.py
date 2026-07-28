"""Typed response models for the trilogy-cloud API.

Mirrors the serialized shape of the Rust API's response types — each model
below names its source struct. The API is the contract; when it changes, these
follow. Pydantic ignores unknown fields by default, so a server that adds a
field stays readable by an older CLI.

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


class Job(BaseModel):
    """``models/job.rs::Job``.

    ``config`` is the job's trilogy.toml as raw text, carried in a JSON string
    rather than a parsed table — the platform stores it verbatim and hands it
    to the worker as a file.
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
    created_at: datetime
    updated_at: datetime


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
    """``models/job.rs::JobRun`` — the bare run, as returned by triggering one."""

    id: str
    job_id: str
    schedule_id: str | None = None
    status: str
    started_at: datetime | None = None
    finished_at: datetime | None = None
    output: Any = None
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

    job_names: list[str] = []


class SecretMeta(BaseModel):
    """``routes/secrets.rs::SecretMeta`` — names and timestamps; values are
    write-only and never leave the platform."""

    name: str
    created_at: datetime
    updated_at: datetime
