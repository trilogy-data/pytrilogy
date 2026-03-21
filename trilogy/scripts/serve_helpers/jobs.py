"""Background job tracking for the serve command."""

import asyncio
import os
import uuid
from dataclasses import dataclass, field
from typing import Any


@dataclass
class Job:
    job_id: str
    status: str  # "running", "success", "error", "cancelled"
    output: str = field(default="")
    error: str = field(default="")
    return_code: int | None = None
    # Process reference held only while running; cleared on completion/cancellation.
    process: Any = field(default=None, repr=False)


_jobs: dict[str, Job] = {}

# Passed to every subprocess so rich doesn't use Windows legacy console renderer
# (which fails on cp1252 when printing Unicode like ✓) and stdout is UTF-8 clean.
_SUBPROCESS_ENV = {**os.environ, "NO_COLOR": "1", "PYTHONIOENCODING": "utf-8"}


def create_job() -> Job:
    job = Job(job_id=str(uuid.uuid4()), status="running")
    _jobs[job.job_id] = job
    return job


def get_job(job_id: str) -> Job | None:
    return _jobs.get(job_id)


def cancel_job(job_id: str) -> Job | None:
    """Terminate a running job. No-op if already finished."""
    job = _jobs.get(job_id)
    if job is None:
        return None
    if job.status == "running" and job.process is not None:
        try:
            job.process.terminate()
        except ProcessLookupError:
            pass  # finished between status check and terminate()
        job.status = "cancelled"
    return job


async def _drain_stdout(stream: asyncio.StreamReader, job: Job) -> None:
    async for line in stream:
        job.output += line.decode(errors="replace")


async def _drain_stderr(stream: asyncio.StreamReader, job: Job) -> None:
    async for line in stream:
        job.error += line.decode(errors="replace")


async def run_subprocess(job: Job, cmd: list[str], cwd: str) -> None:
    try:
        process = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            cwd=cwd,
            env=_SUBPROCESS_ENV,
        )
        job.process = process
        await asyncio.gather(
            _drain_stdout(process.stdout, job),  # type: ignore[arg-type]
            _drain_stderr(process.stderr, job),  # type: ignore[arg-type]
        )
        await process.wait()
        job.return_code = process.returncode
        if job.status != "cancelled":
            job.status = "success" if process.returncode == 0 else "error"
    except Exception as e:
        job.error += str(e)
        if job.status != "cancelled":
            job.status = "error"
    finally:
        job.process = None
