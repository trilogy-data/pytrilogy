from __future__ import annotations

import os
import subprocess
import sys
import time
import uuid
from collections.abc import Sequence
from pathlib import Path

from trilogy.dialect.python_source import (
    MAX_ATTEMPTS,
    RETRY_DELAYS_SECONDS,
    RETRYABLE_UV_ERROR_MARKERS,
    build_script_command,
    is_retryable_uv_error,
    retry_delay,
)

__all__ = [
    "MAX_ATTEMPTS",
    "RETRYABLE_UV_ERROR_MARKERS",
    "RETRY_DELAYS_SECONDS",
    "is_retryable_uv_error",
    "main",
    "run_with_retry",
]


def _read_error(path: Path) -> str:
    try:
        return path.read_text(encoding="utf-8", errors="replace")
    except OSError:
        return ""


def _run_uv(script: str, args: str, output_path: Path, error_path: Path) -> int:
    with output_path.open("wb") as output, error_path.open("wb") as error:
        result = subprocess.run(
            build_script_command(script, args),
            stdout=output,
            stderr=error,
            check=False,
        )
    return result.returncode


def _publish(staged: Path, output_path: Path) -> None:
    """Move a finished stream into place atomically.

    The same script and args map to the same output path, so one query that
    reads a source twice runs two of these concurrently; writing in place let
    one reader see the other's half-written file. Windows refuses to replace a
    file another reader holds open, and then that file is already a complete
    stream from the concurrent run, so it stands.
    """
    try:
        os.replace(staged, output_path)
    except PermissionError:
        staged.unlink(missing_ok=True)


def run_with_retry(script: str, args: str, output_path: Path, error_path: Path) -> int:
    staged = output_path.with_name(f"{output_path.name}.{uuid.uuid4().hex}.part")
    for attempt in range(1, MAX_ATTEMPTS + 1):
        return_code = _run_uv(script, args, staged, error_path)
        if return_code == 0:
            _publish(staged, output_path)
            print('{"name": "done"}')
            return 0

        # The error sidecar is the report; the executor reads it back when
        # DuckDB surfaces the pipe failure.
        if attempt == MAX_ATTEMPTS or not is_retryable_uv_error(
            _read_error(error_path)
        ):
            staged.unlink(missing_ok=True)
            return return_code

        time.sleep(retry_delay(attempt))

    return 1


def main(argv: Sequence[str] | None = None) -> int:
    args = list(sys.argv[1:] if argv is None else argv)
    if len(args) == 3:
        args.append("")
    if len(args) != 4:
        sys.stderr.write(
            "Usage: python -m trilogy.dialect.duckdb_uv "
            "<output.arrow> <error.log> <script.py> <args>\n"
        )
        return 2

    output_path, error_path, script, script_args = args
    return run_with_retry(
        script=script,
        args=script_args,
        output_path=Path(output_path),
        error_path=Path(error_path),
    )


if __name__ == "__main__":
    raise SystemExit(main())
