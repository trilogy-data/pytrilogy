from __future__ import annotations

import argparse
import csv
import json
import math
import random
import sys
from collections.abc import Iterable
from dataclasses import asdict, dataclass
from datetime import date, datetime, time, timezone
from decimal import Decimal
from pathlib import Path
from time import perf_counter
from typing import Any

from local_scripts.fuzzer.generate import generate_cases
from local_scripts.fuzzer.models import SEEDS, FuzzCase
from local_scripts.fuzzer.random_data import generate_random_seeds
from trilogy import Dialects

ROOT = Path(__file__).resolve().parent


@dataclass
class CaseResult:
    case_id: str
    seed: str
    family: str
    tags: tuple[str, ...]
    status: str
    oracle_ms: float
    compile_ms: float
    execute_ms: float
    sql_chars: int
    sql_bytes: int
    expected_rows: int
    actual_rows: int
    error_stage: str | None = None
    error: str | None = None
    error_type: str | None = None
    repro: str | None = None


@dataclass
class CaseOutcome:
    result: CaseResult
    expected: list[tuple[Any, ...]]
    actual: list[tuple[Any, ...]]
    generated_sql: str | None


def normalize_value(value: Any) -> Any:
    if isinstance(value, Decimal):
        return str(value.normalize())
    if isinstance(value, float):
        if math.isnan(value):
            return "NaN"
        if math.isinf(value):
            return "Infinity" if value > 0 else "-Infinity"
        return round(value, 10)
    if isinstance(value, (date, datetime, time)):
        return value.isoformat()
    if isinstance(value, bytes):
        return value.hex()
    return value


def normalize_rows(rows: list[tuple[Any, ...]]) -> list[tuple[Any, ...]]:
    normalized = [tuple(normalize_value(value) for value in row) for row in rows]
    return sorted(
        normalized,
        key=lambda row: json.dumps(row, default=str, sort_keys=True),
    )


def fetch_rows(result: Any) -> list[tuple[Any, ...]]:
    return [tuple(row) for row in result.fetchall()]


def run_case(case: FuzzCase) -> CaseOutcome:
    executor = Dialects.DUCK_DB.default_executor()
    expected: list[tuple[Any, ...]] = []
    actual: list[tuple[Any, ...]] = []
    generated_sql: str | None = None
    oracle_ms = 0.0
    compile_ms = 0.0
    execute_ms = 0.0
    error_stage: str | None = None
    error: str | None = None

    try:
        started = perf_counter()
        try:
            expected = fetch_rows(executor.execute_raw_sql(case.oracle_sql))
        except Exception as exc:
            error_stage = "oracle"
            error = format_exception(exc)
        oracle_ms = elapsed_ms(started)

        if error_stage is None:
            started = perf_counter()
            try:
                statements = executor.generate_sql(case.trilogy)
                if not statements:
                    raise RuntimeError("Trilogy generated no SQL")
                generated_sql = statements[-1]
            except Exception as exc:
                error_stage = "compile"
                error_type = type(exc).__name__
                error = format_exception(exc)
            compile_ms = elapsed_ms(started)

        if error_stage is None and generated_sql is not None:
            started = perf_counter()
            try:
                actual = fetch_rows(executor.execute_raw_sql(generated_sql))
            except Exception as exc:
                error_stage = "execute"
                error = format_exception(exc)
            execute_ms = elapsed_ms(started)
    finally:
        executor.close()

    expected = normalize_rows(expected)
    actual = normalize_rows(actual)
    if (
        error_stage == "compile"
        and error_type is not None
        and error_type in case.accepted_compile_errors
    ):
        status = "pass"
    elif error_stage:
        status = "harness_error" if error_stage == "oracle" else "error"
    elif expected != actual:
        status = "mismatch"
    else:
        status = "pass"

    sql_chars = len(generated_sql) if generated_sql else 0
    sql_bytes = len(generated_sql.encode("utf-8")) if generated_sql else 0
    result = CaseResult(
        case_id=case.case_id,
        seed=case.seed,
        family=case.family,
        tags=case.tags,
        status=status,
        oracle_ms=oracle_ms,
        compile_ms=compile_ms,
        execute_ms=execute_ms,
        sql_chars=sql_chars,
        sql_bytes=sql_bytes,
        expected_rows=len(expected),
        actual_rows=len(actual),
        error_stage=error_stage,
        error=error,
    )
    return CaseOutcome(result, expected, actual, generated_sql)


def write_repro(case: FuzzCase, outcome: CaseOutcome, repro_dir: Path) -> Path:
    target = repro_dir / case.case_id
    target.mkdir(parents=True, exist_ok=True)
    preql_path = target / "repro.preql"
    oracle_path = target / "oracle.sql"
    metadata_path = target / "result.json"
    readme_path = target / "README.md"

    outcome.result.repro = str(preql_path)
    preql_path.write_text(case.trilogy, encoding="utf-8")
    oracle_path.write_text(case.oracle_sql.rstrip() + "\n", encoding="utf-8")
    if outcome.generated_sql:
        (target / "generated.sql").write_text(
            outcome.generated_sql.rstrip() + "\n", encoding="utf-8"
        )
    elif (target / "generated.sql").exists():
        (target / "generated.sql").unlink()

    metadata = {
        **asdict(outcome.result),
        "description": case.description,
        "expected": outcome.expected,
        "actual": outcome.actual,
    }
    metadata_path.write_text(
        json.dumps(metadata, indent=2, default=str) + "\n", encoding="utf-8"
    )
    readme_path.write_text(
        render_repro_readme(case, outcome),
        encoding="utf-8",
    )
    return preql_path


def render_repro_readme(case: FuzzCase, outcome: CaseOutcome) -> str:
    result = outcome.result
    return f"""# Fuzzer repro: `{case.case_id}`

{case.description}

- Status: `{result.status}`
- Seed: `{case.seed}`
- Family: `{case.family}`
- Tags: `{", ".join(case.tags)}`
- Error stage: `{result.error_stage or "none"}`
- Error: `{result.error or "none"}`
- Accepted compile errors: `{", ".join(case.accepted_compile_errors) or "none"}`
- Generated SQL: {result.sql_chars} characters / {result.sql_bytes} bytes
- Timing: oracle {result.oracle_ms:.3f} ms, compile {result.compile_ms:.3f} ms, execute {result.execute_ms:.3f} ms

Run from the repository root:

```powershell
.venv/Scripts/trilogy.exe run repro.preql
```

Expected:

```text
{format_rows(outcome.expected)}
```

Actual:

```text
{format_rows(outcome.actual)}
```

`repro.preql` contains the complete query-backed dataset and Trilogy query.
`oracle.sql` is the independent DuckDB query. `generated.sql`, when present,
is the SQL emitted by Trilogy.
"""


def format_rows(rows: list[tuple[Any, ...]]) -> str:
    if not rows:
        return "[]"
    return "\n".join(repr(row) for row in rows)


def elapsed_ms(started: float) -> float:
    return (perf_counter() - started) * 1000


def format_exception(exc: Exception) -> str:
    return f"{type(exc).__name__}: {exc}"


def select_cases(cases: list[FuzzCase], args: argparse.Namespace) -> list[FuzzCase]:
    selected = [
        case
        for case in cases
        if (not args.dataset or case.seed in args.dataset)
        and (not args.family or case.family in args.family)
        and (not args.tag or all(tag in case.tags for tag in args.tag))
        and (not args.case or any(pattern in case.case_id for pattern in args.case))
    ]
    selected.sort(key=lambda case: case.case_id)
    random.Random(args.seed).shuffle(selected)
    if args.limit:
        selected = selected[: args.limit]
    return selected


def report_directory(args: argparse.Namespace) -> Path:
    if args.report_dir:
        return args.report_dir
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return ROOT / "runs" / f"{stamp}_seed{args.seed}"


def write_reports(
    outcomes: list[CaseOutcome],
    directory: Path,
    args: argparse.Namespace,
) -> None:
    directory.mkdir(parents=True, exist_ok=True)
    results = [asdict(outcome.result) for outcome in outcomes]
    summary = {
        "generation_seed": args.seed,
        "random_datasets": args.random_datasets,
        "random_only": args.random_only,
        "data_seed": args.data_seed,
        "random_rows": args.random_rows,
        "total": len(results),
        "passed": sum(result["status"] == "pass" for result in results),
        "failed": sum(result["status"] != "pass" for result in results),
        "family_metrics": family_metrics(outcomes),
        "results": results,
    }
    (directory / "summary.json").write_text(
        json.dumps(summary, indent=2, default=str) + "\n",
        encoding="utf-8",
    )
    with (directory / "metrics.csv").open("w", newline="", encoding="utf-8") as handle:
        fieldnames = list(asdict(outcomes[0].result)) if outcomes else []
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        if fieldnames:
            writer.writeheader()
            for result in results:
                result["tags"] = ",".join(result["tags"])
                writer.writerow(result)


def family_metrics(outcomes: list[CaseOutcome]) -> dict[str, dict[str, float | int]]:
    grouped: dict[str, list[CaseResult]] = {}
    for outcome in outcomes:
        grouped.setdefault(outcome.result.family, []).append(outcome.result)
    return {
        family: {
            "cases": len(results),
            "failed": sum(result.status != "pass" for result in results),
            "avg_oracle_ms": average(result.oracle_ms for result in results),
            "avg_compile_ms": average(result.compile_ms for result in results),
            "avg_execute_ms": average(result.execute_ms for result in results),
            "avg_sql_chars": average(result.sql_chars for result in results),
            "max_sql_chars": max(result.sql_chars for result in results),
        }
        for family, results in sorted(grouped.items())
    }


def average(values: Iterable[float | int]) -> float:
    collected = list(values)
    return sum(collected) / len(collected) if collected else 0.0


def print_case(outcome: CaseOutcome) -> None:
    result = outcome.result
    timing = (
        f"oracle={result.oracle_ms:.1f}ms compile={result.compile_ms:.1f}ms "
        f"execute={result.execute_ms:.1f}ms"
    )
    print(
        f"{result.status.upper():13} {result.case_id:50} "
        f"rows={result.actual_rows}/{result.expected_rows} "
        f"sql={result.sql_chars}c {timing}"
    )
    if result.error:
        print(f"  {result.error}")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run deterministic Trilogy queries against DuckDB SQL oracles."
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=0,
        help="Deterministic case ordering seed; useful with --limit.",
    )
    parser.add_argument(
        "--random-datasets",
        type=int,
        default=0,
        help="Add this many deterministic pseudo-random dataset seeds.",
    )
    parser.add_argument(
        "--random-only",
        action="store_true",
        help="Exclude the two fixed datasets from a randomized campaign.",
    )
    parser.add_argument(
        "--data-seed",
        type=int,
        default=1000,
        help="First pseudo-random dataset seed.",
    )
    parser.add_argument(
        "--random-rows",
        type=int,
        default=24,
        help="Approximate event-row count in each pseudo-random dataset.",
    )
    parser.add_argument(
        "--dataset",
        action="append",
        help="Only run this named dataset seed; repeatable.",
    )
    parser.add_argument(
        "--family",
        action="append",
        help="Only run this case family; repeatable.",
    )
    parser.add_argument(
        "--tag",
        action="append",
        help="Require this coverage tag; repeatable.",
    )
    parser.add_argument(
        "--case",
        action="append",
        help="Run case IDs containing this text; repeatable.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=0,
        help="Run at most this many cases after deterministic shuffling.",
    )
    parser.add_argument(
        "--fail-fast",
        action="store_true",
        help="Stop after the first non-passing case.",
    )
    parser.add_argument(
        "--list",
        action="store_true",
        help="List selected cases without executing them.",
    )
    parser.add_argument(
        "--report-dir",
        type=Path,
        help="Report destination; defaults to a timestamped runs directory.",
    )
    parser.add_argument(
        "--repro-dir",
        type=Path,
        default=ROOT / "repros",
        help="Directory for stable per-case failure repros.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if args.random_datasets < 0:
        print("--random-datasets cannot be negative.", file=sys.stderr)
        return 2
    if args.random_rows < 8:
        print("--random-rows must be at least 8.", file=sys.stderr)
        return 2
    if args.random_only and not args.random_datasets:
        print("--random-only requires --random-datasets.", file=sys.stderr)
        return 2
    random_seeds = generate_random_seeds(
        args.random_datasets,
        args.data_seed,
        args.random_rows,
    )
    fixed_seeds = () if args.random_only else SEEDS
    cases = select_cases(generate_cases((*fixed_seeds, *random_seeds)), args)
    if args.list:
        for case in cases:
            print(
                f"{case.case_id}\t{case.family}\t{','.join(case.tags)}\t"
                f"{case.description}"
            )
        return 0
    if not cases:
        print("No cases matched the requested filters.", file=sys.stderr)
        return 2

    outcomes = []
    for case in cases:
        outcome = run_case(case)
        if outcome.result.status != "pass":
            repro = write_repro(case, outcome, args.repro_dir)
            outcome.result.repro = str(repro)
        outcomes.append(outcome)
        print_case(outcome)
        if args.fail_fast and outcome.result.status != "pass":
            break

    reports = report_directory(args)
    write_reports(outcomes, reports, args)
    failures = sum(outcome.result.status != "pass" for outcome in outcomes)
    print(
        f"\n{len(outcomes) - failures}/{len(outcomes)} passed; "
        f"{failures} failed. Reports: {reports}"
    )
    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
