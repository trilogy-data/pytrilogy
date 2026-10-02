"""Per-run artifact writers shared by the benchmark suites.

Each suite's `run_query` drops a `zquery{label}.log` (generated SQL plus the
three size measures) and folds its timings into
`zquery_timing_{fingerprint}.log`. Both files are committed, so every writer
pins `newline="\n"` — platform-native newlines would rewrite every line of
both on Windows.

The committed `gen_length` and the CTE count of the committed SQL are also
the size baselines every run is checked against, so a plan that grows fails
here rather than landing as an artifact diff nobody reads.
"""

from __future__ import annotations

import os
import platform
import re
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


# Generated SQL is deterministic, so a committed gen_length is a baseline and
# not a sample. Leave room for the churn a replanned CTE name costs (names are
# words, and words differ in length); an extra CTE costs far more than that.
SIZE_REGRESSION_RATIO = 0.02
SIZE_REGRESSION_FLOOR = 64
REBASELINE_ENV = "TRILOGY_BENCHMARK_REBASELINE"


def size_budget(baseline: int) -> int:
    return baseline + max(SIZE_REGRESSION_FLOOR, int(baseline * SIZE_REGRESSION_RATIO))


_CTE_HEADER = re.compile(r"^\w+ as \(", re.MULTILINE)


def cte_count(sql: str) -> int:
    return len(_CTE_HEADER.findall(sql))


def check_query_size(root: Path, label: str, query: str, gen_length: int) -> None:
    """Fail when a query's generated SQL outgrows its committed baseline: in
    length past `size_budget`, or by any CTE. A CTE is work, and one can hide
    inside the length budget beside text that shrinks elsewhere.

    The log is NOT rewritten when it does: a run that overwrote it would leave
    the next run passing against the inflated size, which is how a 7% growth on
    two TPC-DS queries reached main unnoticed.
    """
    if os.environ.get(REBASELINE_ENV):
        return
    logged = load_toml_mapping(root / f"zquery{label}.log")
    baseline = logged.get("gen_length")
    if isinstance(baseline, int) and gen_length > size_budget(baseline):
        _fail(
            label,
            f"generated SQL grew {baseline} -> {gen_length} chars, over its "
            f"budget of {size_budget(baseline)}",
        )
    committed = logged.get("generated_sql")
    if isinstance(committed, str) and cte_count(query) > cte_count(committed):
        _fail(
            label,
            f"generated SQL grew {cte_count(committed)} -> {cte_count(query)} CTEs",
        )


def _fail(label: str, growth: str) -> None:
    raise AssertionError(
        f"query {label} {growth}; its log is left at the committed baseline. "
        f"If the growth is intended, re-run with {REBASELINE_ENV}=1 to accept "
        f"it and commit the new log."
    )


def write_query_log(
    root: Path,
    label: str,
    query: str,
    gen_length: int,
    preql_size: int,
    comp_size: int,
) -> None:
    check_query_size(root, label, query, gen_length)
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
