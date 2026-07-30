from __future__ import annotations

import subprocess
import sys
import time
from collections.abc import Sequence
from pathlib import Path

from trilogy.dialect.python_source import (
    MAX_ATTEMPTS,
    RETRY_DELAYS_SECONDS,
    RETRYABLE_UV_ERROR_MARKERS,
    build_uv_command,
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
            build_uv_command(script, args), stdout=output, stderr=error, check=False
        )
    return result.returncode


def run_with_retry(script: str, args: str, output_path: Path, error_path: Path) -> int:
    for attempt in range(1, MAX_ATTEMPTS + 1):
        return_code = _run_uv(script, args, output_path, error_path)
        if return_code == 0:
            print('{"name": "done"}')
            return 0

        error = _read_error(error_path)
        if attempt == MAX_ATTEMPTS or not is_retryable_uv_error(error):
            sys.stderr.write(error)
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
