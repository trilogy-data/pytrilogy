"""Per-run artifact writers shared by the benchmark suites.

Each suite's `run_query` drops a `zquery{label}.log` (generated SQL plus the
three size measures) and folds its timings into
`zquery_timing_{fingerprint}.log`. Both files are committed, so every writer
pins `newline="\n"` — platform-native newlines would rewrite every line of
both on Windows.
"""

from __future__ import annotations

import os
import platform
from pathlib import Path

import tomli_w
import tomllib


def fingerprint() -> str:
    machine = platform.machine()
    cpu_name = platform.processor()
    cpu_count = os.cpu_count()
    return (
        f"{machine}-{cpu_name}-{cpu_count}".lower().replace(" ", "_").replace(",", "")
    )


def load_toml_mapping(path: Path) -> dict[str, object]:
    if not path.exists():
        return {}
    raw = path.read_text(encoding="utf-8")
    if not raw.strip():
        return {}
    try:
        loaded = tomllib.loads(raw)
    except tomllib.TOMLDecodeError:
        return {}
    if isinstance(loaded, dict):
        return loaded
    return {}


def write_query_log(
    root: Path,
    label: str,
    query: str,
    gen_length: int,
    preql_size: int,
    comp_size: int,
) -> None:
    with open(root / f"zquery{label}.log", "w", encoding="utf-8", newline="\n") as f:
        f.write(
            tomli_w.dumps(
                {
                    "query_id": label,
                    "gen_length": gen_length,
                    "preql_size": preql_size,
                    "comp_size": comp_size,
                    "generated_sql": query,
                },
                multiline_strings=True,
            )
        )


def record_timing(
    root: Path,
    label: str,
    parse_time: float,
    exec_time: float,
    comp_time: float,
) -> None:
    """Fold one query's timings into this machine's timing log.

    Written to a temp file and replaced: a run interrupted mid-write would
    otherwise leave a truncated log, which `load_toml_mapping` discards whole.
    """
    timing = root / f"zquery_timing_{fingerprint()}.log"
    current = load_toml_mapping(timing)
    current[f"query_{label}"] = {
        "parse_time": parse_time,
        "exec_time": exec_time,
        "comp_time": comp_time,
    }
    final = {x: current[x] for x in sorted(current.keys())}
    temp_timing = timing.with_suffix(f"{timing.suffix}.tmp")
    temp_timing.write_text(
        tomli_w.dumps(final, multiline_strings=True),
        encoding="utf-8",
        newline="\n",
    )
    temp_timing.replace(timing)
