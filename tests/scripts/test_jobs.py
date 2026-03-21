"""Unit and integration tests for serve_helpers/jobs.py."""

import asyncio
import sys
from unittest.mock import MagicMock

from trilogy.scripts.serve_helpers.jobs import (
    Job,
    _drain_stderr,
    _drain_stdout,
    cancel_job,
    create_job,
    get_job,
    run_subprocess,
)

# ── helpers ──────────────────────────────────────────────────────────────────


def _run(coro):
    return asyncio.run(coro)


# ── create / get ──────────────────────────────────────────────────────────────


def test_create_job_registers_and_returns_running():
    job = create_job()
    assert job.status == "running"
    assert get_job(job.job_id) is job


def test_get_job_unknown_id_returns_none():
    assert get_job("no-such-id-xyz") is None


# ── cancel_job ────────────────────────────────────────────────────────────────


def test_cancel_job_unknown_returns_none():
    assert cancel_job("no-such-id-xyz") is None


def test_cancel_job_already_finished_is_noop():
    job = create_job()
    job.status = "success"
    result = cancel_job(job.job_id)
    assert result is job
    assert result.status == "success"  # unchanged


def test_cancel_job_running_with_no_process_does_not_change_status():
    job = create_job()
    job.status = "running"
    job.process = None
    result = cancel_job(job.job_id)
    assert result is job
    assert result.status == "running"  # nothing to terminate


def test_cancel_job_running_with_process_terminates_and_marks_cancelled():
    job = create_job()
    mock_proc = MagicMock()
    job.process = mock_proc
    result = cancel_job(job.job_id)
    mock_proc.terminate.assert_called_once()
    assert result.status == "cancelled"


def test_cancel_job_process_lookup_error_still_marks_cancelled():
    job = create_job()
    mock_proc = MagicMock()
    mock_proc.terminate.side_effect = ProcessLookupError
    job.process = mock_proc
    result = cancel_job(job.job_id)
    assert result.status == "cancelled"


# ── run_subprocess ────────────────────────────────────────────────────────────


def test_run_subprocess_success():
    job = create_job()
    _run(run_subprocess(job, [sys.executable, "-c", "print('hello')"], "."))
    assert job.status == "success"
    assert job.return_code == 0
    assert "hello" in job.output


def test_run_subprocess_failure_sets_error_status():
    job = create_job()
    _run(run_subprocess(job, [sys.executable, "-c", "import sys; sys.exit(1)"], "."))
    assert job.status == "error"
    assert job.return_code == 1


def test_run_subprocess_captures_stderr():
    job = create_job()
    _run(
        run_subprocess(
            job,
            [sys.executable, "-c", "import sys; sys.stderr.write('err_msg')"],
            ".",
        )
    )
    assert "err_msg" in job.error


def test_run_subprocess_bad_command_sets_error():
    job = create_job()
    _run(run_subprocess(job, ["_nonexistent_command_xyz_"], "."))
    assert job.status == "error"
    assert job.error  # some message was recorded


def test_run_subprocess_cancelled_status_preserved():
    """If job is already cancelled, completion must not flip it to success/error."""
    job = create_job()
    job.status = "cancelled"
    _run(run_subprocess(job, [sys.executable, "-c", "print('x')"], "."))
    assert job.status == "cancelled"


def test_run_subprocess_clears_process_reference_on_completion():
    job = create_job()
    _run(run_subprocess(job, [sys.executable, "-c", "pass"], "."))
    assert job.process is None


# ── drain helpers ─────────────────────────────────────────────────────────────


def test_drain_stdout_appends_lines():

    job = Job(job_id="test", status="running")

    async def _inner():
        reader = asyncio.StreamReader()
        reader.feed_data(b"line1\nline2\n")
        reader.feed_eof()
        await _drain_stdout(reader, job)

    _run(_inner())
    assert "line1" in job.output
    assert "line2" in job.output


def test_drain_stderr_appends_lines():
    job = Job(job_id="test", status="running")

    async def _inner():
        reader = asyncio.StreamReader()
        reader.feed_data(b"err1\nerr2\n")
        reader.feed_eof()
        await _drain_stderr(reader, job)

    _run(_inner())
    assert "err1" in job.error
    assert "err2" in job.error
