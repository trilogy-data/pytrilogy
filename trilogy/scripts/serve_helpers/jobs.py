"""Background job tracking for the serve command."""

import asyncio
import uuid
from dataclasses import dataclass, field


@dataclass
class Job:
    job_id: str
    status: str  # "running", "success", "error"
    output: str = field(default="")
    error: str = field(default="")
    return_code: int | None = None


_jobs: dict[str, Job] = {}


def create_job() -> Job:
    job = Job(job_id=str(uuid.uuid4()), status="running")
    _jobs[job.job_id] = job
    return job


def get_job(job_id: str) -> Job | None:
    return _jobs.get(job_id)


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
        )
        await asyncio.gather(
            _drain_stdout(process.stdout, job),  # type: ignore[arg-type]
            _drain_stderr(process.stderr, job),  # type: ignore[arg-type]
        )
        await process.wait()
        job.return_code = process.returncode
        job.status = "success" if process.returncode == 0 else "error"
    except Exception as e:
        job.error += str(e)
        job.status = "error"
