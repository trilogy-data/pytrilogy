from __future__ import annotations

from pathlib import Path

TASK_TEMPLATE = """\
Trilogy project in this directory. Python datasource execution is enabled.

Answer the ONE task below by writing BOTH files:
- `{script_filename}`: a standalone Python program that writes an Arrow IPC
  stream to stdout. A PEP 723 dependency block may declare packages.
- `{filename}`: a Trilogy model/query that declares the script as a datasource
  with a `file` address pointing to `./{script_filename}` and returns the
  requested result.

Do not print logs or any non-Arrow bytes to stdout. Diagnostics may go to stderr.
Use `trilogy file write` to create either file; write `{filename}` last with
`trilogy file write {filename} --run` (body on stdin) so the write and the
validating execution happen in one call, then return control once it runs
cleanly.

Exact response column names do not matter, but position and values do.

Script task {opaque_id}:
{prompt}
"""


def build_empty_database(eval_dir: Path, filename: str) -> Path:
    import duckdb

    cache_dir = eval_dir / ".cache"
    cache_dir.mkdir(parents=True, exist_ok=True)
    path = cache_dir / filename
    if not path.exists():
        duckdb.connect(str(path)).close()
    return path
