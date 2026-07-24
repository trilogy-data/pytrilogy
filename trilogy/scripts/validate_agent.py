"""Agent-backed execution for natural-language statements.

Backs two surfaces:
- standalone ``select natural '<question>'`` execution (an agent loop answers
  the question with a generated query, which is then executed for rows);
- the ``validate ... matches`` loop run by ``trilogy unit``/``integration``
  with the ``agent`` test type enabled.

The agent runs in a scratch workspace seeded with the model's text files
(``*.preql``/``trilogy.toml``/``schema.md`` — never data files; a relative
local DB path in the toml is rewritten to its absolute location), so its
writes can never dirty the model repo. Scoring executes both the agent's
candidate and the authored expected SQL through the same engine and compares
rows via :mod:`trilogy.core.validation.rows`.
"""

from __future__ import annotations

import json
import re
import shutil
import subprocess
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path

from trilogy.core.enums import QueryComparison
from trilogy.core.validation.rows import rows_equal

ANSWER_FILENAME = "answer.preql"
MOCK_DB_FILENAME = "mock_data.duckdb"
DEFAULT_TIMEOUT_SECONDS = 600

TASK_TEMPLATE = """\
Trilogy project in this directory. `trilogy.toml` configures an already-loaded
database, and the directory contains a curated Trilogy semantic model.

Answer the ONE business question below by writing a Trilogy query file to
`{filename}` in the working directory (alongside `trilogy.toml`). Validate
with `trilogy run {filename}`.

Return control once it runs cleanly to submit your result. This will be
your final action.

Exact response column names do not matter, but the position and values do.

Business question:
{question}
"""

# Files seeded into the scratch workspace. Data files are intentionally
# excluded — the workspace references the model's database in place.
_SEED_GLOBS = ("**/*.preql", "**/schema.md")

# `path = "..."` under [engine.config]; rewritten to absolute when it points at
# a local file so the copied toml still resolves from the scratch directory.
_ENGINE_PATH_RE = re.compile(r"""^(\s*path\s*=\s*)(["'])(.+?)\2""", re.MULTILINE)


@dataclass
class RepetitionResult:
    # pass | fail | error | missing | timeout | exhausted | crashed
    status: str
    detail: str = ""
    duration: float = 0.0
    tokens: int = 0
    iterations: int = 0
    candidate_rows: int | None = None
    expected_rows: int | None = None
    log_path: Path | None = None
    workspace: Path | None = None


@dataclass
class QuestionResult:
    name: str
    question: str
    target: float
    repetitions: list[RepetitionResult] = field(default_factory=list)
    tags: list[str] = field(default_factory=list)

    @property
    def passes(self) -> int:
        return sum(1 for r in self.repetitions if r.status == "pass")

    @property
    def pass_rate(self) -> float:
        if not self.repetitions:
            return 0.0
        return self.passes / len(self.repetitions)

    @property
    def passed(self) -> bool:
        return bool(self.repetitions) and self.pass_rate >= self.target

    @property
    def total_tokens(self) -> int:
        return sum(r.tokens for r in self.repetitions)

    @property
    def avg_tokens(self) -> float:
        if not self.repetitions:
            return 0.0
        return self.total_tokens / len(self.repetitions)


def check_agent_ready(model_dir: Path) -> None:
    """Raise loudly when no agent provider/model/key resolves for the model
    directory — the agent test type is opt-in and must never degrade to a
    silent skip."""
    from trilogy.scripts.agent import _build_provider
    from trilogy.scripts.common import get_runtime_config

    cfg = get_runtime_config(model_dir).agent
    _build_provider(cfg, None, None)


def _absolutize_engine_path(toml_text: str, model_dir: Path) -> str:
    from trilogy.constants import REMOTE_PREFIXES

    def _rewrite(match: re.Match[str]) -> str:
        raw = match.group(3)
        if raw.startswith(REMOTE_PREFIXES) or Path(raw).is_absolute():
            return match.group(0)
        resolved = (model_dir / raw).resolve().as_posix()
        return f'{match.group(1)}"{resolved}"'

    return _ENGINE_PATH_RE.sub(_rewrite, toml_text)


_IMPORT_PATHS_LINE_RE = re.compile(r"^import_paths\s*=\s*\[[^\]\n]*\]\s*$")


def _merged_import_paths(model_dir: Path) -> list[str]:
    """The workspace's import-resolution roots: the model directory itself,
    plus any roots the model's own toml configures (resolved absolute)."""
    import tomllib

    entries = [model_dir.resolve().as_posix()]
    toml = model_dir / "trilogy.toml"
    if toml.exists():
        raw = tomllib.loads(toml.read_text(encoding="utf-8")).get("import_paths", [])
        if isinstance(raw, str):
            raw = [raw]
        entries += [(model_dir / p).resolve().as_posix() for p in raw]
    return list(dict.fromkeys(entries))


def _import_paths_line(model_dir: Path) -> str:
    paths = ", ".join(f'"{p}"' for p in _merged_import_paths(model_dir))
    return f"import_paths = [{paths}]"


def _strip_top_level_import_paths(toml_text: str, model_dir: Path) -> str:
    """Remove a single-line top-level `import_paths = [...]` so the workspace
    toml can carry its own. A multiline array can't be safely text-stripped —
    fail loudly rather than emit a duplicate-key toml."""
    from trilogy.core.exceptions import ConfigurationException

    lines: list[str] = []
    in_section = False
    for line in toml_text.splitlines():
        stripped = line.strip()
        if stripped.startswith("[") and stripped.endswith("]"):
            in_section = True
        if not in_section and stripped.startswith("import_paths"):
            if not _IMPORT_PATHS_LINE_RE.match(stripped):
                raise ConfigurationException(
                    f"{model_dir / 'trilogy.toml'}: `import_paths` must be a "
                    "single-line array for agent validation workspaces."
                )
            continue
        lines.append(line)
    return "\n".join(lines)


def seed_workspace(model_dir: Path, workspace: Path) -> None:
    workspace.mkdir(parents=True, exist_ok=True)
    for pattern in _SEED_GLOBS:
        for source in sorted(model_dir.glob(pattern)):
            relative = source.relative_to(model_dir)
            destination = workspace / relative
            destination.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(source, destination)
    toml = model_dir / "trilogy.toml"
    if toml.exists():
        text = _absolutize_engine_path(toml.read_text(encoding="utf-8"), model_dir)
        text = _strip_top_level_import_paths(text, model_dir)
        # imports in the workspace fall back to the model directory, so the
        # agent's queries resolve `import entrypoint;` even if a seeded copy is
        # missing (and any model-configured roots keep working).
        text = f"{_import_paths_line(model_dir)}\n{text}"
        (workspace / "trilogy.toml").write_text(text, encoding="utf-8")


# File formats the unit tier can write mock data as. sql/python-script
# addresses execute code and cannot be mocked as data.
_MOCKABLE_FILE_COPY: dict[str, str] = {
    "csv": "(FORMAT CSV, HEADER)",
    "tsv": "(FORMAT CSV, HEADER, DELIMITER '\\t')",
    "parquet": "(FORMAT PARQUET)",
}


def _integration_only(datasource, reason: str) -> Exception:
    from trilogy.core.exceptions import ConfigurationException

    return ConfigurationException(
        f"Datasource '{datasource.name}' {reason}; the unit-tier mock cannot "
        "stand in for it. Run this validation under `trilogy integration "
        "--include-type agent` instead."
    )


def _mock_target(
    datasource, model_root: Path
) -> tuple[list[str] | None, tuple[Path, str] | None]:
    """Where the mock rows for a datasource go: (table identifier parts, None)
    for table addresses, (None, (model-relative file path, COPY options)) for
    file addresses under the model root. File paths absolutize against the
    env's working path at parse time, so a model-local file shows up as
    ``<model_root>/...`` — the relative part is where the mock file lands in
    the workspace (the workspace's .preql copies re-anchor it there). Quoted
    table addresses are a single opaque identifier; unquoted dotted names are
    schema-qualified."""
    from trilogy.constants import REMOTE_PREFIXES
    from trilogy.core.models.datasource import Address

    address = datasource.address
    if isinstance(address, Address):
        if address.is_file:
            copy_options = _MOCKABLE_FILE_COPY.get(address.type.value)
            if copy_options is None:
                raise _integration_only(
                    datasource, f"has a {address.type.value} address"
                )
            location = address.location
            if address.additional_locations or address.is_glob:
                raise _integration_only(
                    datasource, "reads a multi-file or glob file address"
                )
            if address.partition_columns:
                raise _integration_only(
                    datasource, "reads a hive-partitioned file address"
                )
            if location.startswith(REMOTE_PREFIXES):
                raise _integration_only(datasource, f"reads a remote file ({location})")
            try:
                relative = Path(location).resolve().relative_to(model_root.resolve())
            except ValueError:
                raise _integration_only(
                    datasource,
                    f"reads a file outside the model directory ({location})",
                ) from None
            return None, (relative, copy_options)
        if address.is_query:
            raise _integration_only(datasource, "has a query address")
        if address.quoted:
            return [address.location], None
        return [p.strip('"') for p in address.location.split(".")], None
    return [p.strip('"') for p in str(address).split(".")], None


def materialize_mock_db(environment, db_path: Path, files_root: Path) -> None:
    """Materialize deterministic, fully-populated mock rows for every
    datasource, under each datasource's ORIGINAL address so the workspace's
    model files resolve against it unchanged. Table addresses become tables in
    the DuckDB file at ``db_path``; local relative file addresses (csv/tsv/
    parquet) are written in their format under ``files_root``, which callers
    copy into each repetition workspace (relative paths resolve against the
    agent's cwd)."""
    import random

    from trilogy.dialect.config import DuckDBConfig
    from trilogy.dialect.enums import Dialects
    from trilogy.dialect.mock import MockManager

    env = environment.duplicate()
    model_root = Path(env.working_path)
    random.seed(0)
    manager = MockManager(env)
    executor = Dialects.DUCK_DB.default_executor(conf=DuckDBConfig(path=str(db_path)))
    try:
        for datasource in env.datasources.values():
            table_parts, file_target = _mock_target(datasource, model_root)
            concrete = []
            headers = []
            for key, column in datasource.concrete_columns.items():
                manager.mock_concept(column.concept)
                concrete.append(column.concept)
                headers.append(key)
            table = manager.create_mock_table(concrete, headers)
            executor.execute_raw_sql(
                "register(:name, :tbl)", {"name": "mock_tbl", "tbl": table}
            )
            if file_target is not None:
                location, options = file_target
                target = files_root / location
                target.parent.mkdir(parents=True, exist_ok=True)
                escaped = target.resolve().as_posix().replace("'", "''")
                executor.execute_raw_sql(
                    f"COPY (SELECT * FROM mock_tbl) TO '{escaped}' {options}"
                )
                continue
            assert table_parts is not None
            if len(table_parts) > 1:
                schema = ".".join(f'"{p}"' for p in table_parts[:-1])
                executor.execute_raw_sql(f"CREATE SCHEMA IF NOT EXISTS {schema}")
            quoted = ".".join(f'"{p}"' for p in table_parts)
            executor.execute_raw_sql(
                f"CREATE OR REPLACE TABLE {quoted} AS SELECT * FROM mock_tbl"
            )
        # close() discards an open transaction; commit so the file keeps the
        # mock tables for the (read-only) agent + scoring connections.
        executor.connection.commit()
    finally:
        executor.close()


def _write_unit_toml(model_dir: Path, workspace: Path, db_path: Path) -> None:
    """Workspace toml for the unit tier: the model's toml minus its [engine]
    sections, with a DuckDB engine pointed at the mock database and import
    resolution falling back to the model directory."""
    lines: list[str] = [_import_paths_line(model_dir)]
    source = model_dir / "trilogy.toml"
    if source.exists():
        text = _strip_top_level_import_paths(
            source.read_text(encoding="utf-8"), model_dir
        )
        current_section = ""
        for line in text.splitlines():
            stripped = line.strip()
            if stripped.startswith("[") and stripped.endswith("]"):
                current_section = stripped.strip("[]").strip()
            if current_section == "engine" or current_section.startswith("engine."):
                continue
            lines.append(line)
    lines += [
        "",
        "[engine]",
        'dialect = "duck_db"',
        "",
        "[engine.config]",
        f'path = "{db_path.resolve().as_posix()}"',
        "",
    ]
    (workspace / "trilogy.toml").write_text("\n".join(lines), encoding="utf-8")


@dataclass
class AgentRun:
    exit_code: int
    timed_out: bool
    duration: float
    output_tail: str


def run_agent_once(
    workspace: Path,
    task: str,
    log_path: Path,
    timeout: int,
) -> AgentRun:
    cmd = [
        sys.executable,
        "-m",
        "trilogy.scripts.trilogy",
        "agent",
        "--toolset",
        "trilogy",
        "--log-file",
        str(log_path),
        task,
    ]
    start = time.perf_counter()
    proc = subprocess.Popen(
        cmd,
        cwd=workspace,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        encoding="utf-8",
        errors="replace",
    )
    timed_out = False
    try:
        output, _ = proc.communicate(timeout=timeout)
    except subprocess.TimeoutExpired:
        timed_out = True
        proc.kill()
        output, _ = proc.communicate()
    duration = time.perf_counter() - start
    tail = "\n".join((output or "").splitlines()[-15:])
    return AgentRun(
        exit_code=proc.returncode,
        timed_out=timed_out,
        duration=duration,
        output_tail=tail,
    )


def light_metrics(log_path: Path) -> tuple[int, int]:
    """(total_tokens, iterations) from the agent's JSONL trace."""
    tokens = 0
    iterations = 0
    if not log_path.exists():
        return tokens, iterations
    for line in log_path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            event = json.loads(line)
        except json.JSONDecodeError:
            continue
        if event.get("type") == "llm_response":
            iterations += 1
            usage = event.get("usage") or {}
            tokens += usage.get("total_tokens") or 0
    return tokens, iterations


def _executor_for_workspace(workspace: Path):
    from trilogy.scripts.common import create_executor, get_runtime_config

    config = get_runtime_config(workspace)
    dialect = config.engine_dialect
    if dialect is None:
        from trilogy.core.exceptions import ConfigurationException

        raise ConfigurationException(
            f"No engine.dialect configured in {workspace / 'trilogy.toml'}; "
            "agent validation needs a runnable engine configuration."
        )
    return create_executor((), workspace, (), dialect, False, config)


def score_workspace(
    workspace: Path,
    expected_sql: str,
    comparison: QueryComparison,
) -> RepetitionResult:
    """Execute the agent's candidate and the expected SQL on the workspace's
    engine and compare rows. Returns a result with status pass/fail/error/
    missing (agent-process statuses are applied by the caller)."""
    from trilogy.core.models.environment import Environment

    candidate_file = workspace / ANSWER_FILENAME
    if not candidate_file.exists():
        return RepetitionResult(
            status="missing", detail=f"agent produced no {ANSWER_FILENAME}"
        )
    executor = _executor_for_workspace(workspace)
    try:
        try:
            executor.environment = Environment(
                working_path=workspace,
                import_paths=list(executor.environment.import_paths),
            )
            statements = executor.generate_sql(
                candidate_file.read_text(encoding="utf-8")
            )
        except Exception as exc:
            return RepetitionResult(
                status="error",
                detail=f"candidate generate_sql: {type(exc).__name__}: {exc}",
            )
        if not statements:
            return RepetitionResult(
                status="error", detail="candidate file has no executable statement"
            )
        try:
            candidate = list(executor.execute_raw_sql(statements[-1]).fetchall())
        except Exception as exc:
            return RepetitionResult(
                status="error",
                detail=f"candidate execute: {type(exc).__name__}: {exc}",
            )
        try:
            expected = list(executor.execute_raw_sql(expected_sql).fetchall())
        except Exception as exc:
            return RepetitionResult(
                status="error",
                detail=f"expected execute: {type(exc).__name__}: {exc}",
            )
    finally:
        executor.close()
    passed = rows_equal(candidate, expected, comparison)
    return RepetitionResult(
        status="pass" if passed else "fail",
        detail="" if passed else "result set differs from expected answer",
        candidate_rows=len(candidate),
        expected_rows=len(expected),
    )


def _apply_process_status(result: RepetitionResult, run: AgentRun) -> RepetitionResult:
    """Promote non-passing scores per the agent process outcome; mirrors the
    eval harness precedence: pass > timeout > exhausted > crashed."""
    from trilogy.scripts.agent import EXIT_ITERATION_EXHAUSTED

    if result.status == "pass":
        return result
    if run.timed_out:
        result.detail = f"agent timed out (was: {result.status})"
        result.status = "timeout"
    elif run.exit_code == EXIT_ITERATION_EXHAUSTED:
        result.status = "exhausted"
        result.detail = "agent exhausted its iteration budget"
    elif run.exit_code != 0:
        result.status = "crashed"
        result.detail = f"agent exited {run.exit_code}; output tail:\n{run.output_tail}"
    return result


def run_validation_question(
    *,
    name: str,
    question: str,
    expected_sql: str,
    comparison: QueryComparison,
    repetitions: int,
    target: float,
    timeout: int | None,
    tags: list[str],
    model_dir: Path,
    run_dir: Path,
    unit_mock_db: Path | None = None,
    unit_mock_files: Path | None = None,
) -> QuestionResult:
    """Run one embedded validation question: N fresh agent repetitions, each
    scored against the expected SQL. Repetitions run serially (provider
    pressure); question runs are read-only against the engine. When
    ``unit_mock_db`` is set (the unit tier) the workspace toml is rewritten to
    a DuckDB engine over that pre-materialized mock database, and any
    ``unit_mock_files`` (mock stand-ins for file-addressed datasources) are
    copied into the workspace so relative file reads resolve there."""
    result = QuestionResult(name=name, question=question, target=target, tags=tags)
    effective_timeout = timeout or DEFAULT_TIMEOUT_SECONDS
    question_dir = run_dir / name
    for index in range(repetitions):
        workspace = question_dir / f"rep{index + 1}"
        seed_workspace(model_dir, workspace)
        if unit_mock_db is not None:
            _write_unit_toml(model_dir, workspace, unit_mock_db)
        if unit_mock_files is not None and unit_mock_files.exists():
            shutil.copytree(unit_mock_files, workspace, dirs_exist_ok=True)
        log_path = question_dir / f"rep{index + 1}.jsonl"
        task = TASK_TEMPLATE.format(filename=ANSWER_FILENAME, question=question)
        run = run_agent_once(workspace, task, log_path, effective_timeout)
        rep = score_workspace(workspace, expected_sql, comparison)
        rep = _apply_process_status(rep, run)
        rep.duration = run.duration
        rep.tokens, rep.iterations = light_metrics(log_path)
        rep.log_path = log_path
        rep.workspace = workspace
        result.repetitions.append(rep)
    return result


def write_report(run_dir: Path, script: Path, results: list[QuestionResult]) -> Path:
    report = {
        "script": str(script),
        "questions": [
            {
                "name": r.name,
                "question": r.question,
                "target": r.target,
                "tags": r.tags,
                "pass_rate": r.pass_rate,
                "passed": r.passed,
                "total_tokens": r.total_tokens,
                "repetitions": [
                    {
                        "status": rep.status,
                        "detail": rep.detail,
                        "duration_seconds": round(rep.duration, 2),
                        "tokens": rep.tokens,
                        "iterations": rep.iterations,
                        "candidate_rows": rep.candidate_rows,
                        "expected_rows": rep.expected_rows,
                        "log": str(rep.log_path) if rep.log_path else None,
                        "workspace": str(rep.workspace) if rep.workspace else None,
                    }
                    for rep in r.repetitions
                ],
            }
            for r in results
        ],
    }
    run_dir.mkdir(parents=True, exist_ok=True)
    path = run_dir / "report.json"
    path.write_text(json.dumps(report, indent=2), encoding="utf-8")
    return path


def execute_natural_select(executor, question: str):
    """Standalone ``select natural`` execution: one agent repetition in a
    scratch workspace, then the candidate query runs on the CALLER's engine
    connection and its rows are returned."""
    import tempfile

    from trilogy.core.exceptions import ConfigurationException
    from trilogy.core.models.environment import Environment

    model_dir = Path(executor.environment.working_path)
    check_agent_ready(model_dir)
    scratch = Path(tempfile.mkdtemp(prefix="trilogy_natural_"))
    seed_workspace(model_dir, scratch)
    log_path = scratch / "agent.jsonl"
    task = TASK_TEMPLATE.format(filename=ANSWER_FILENAME, question=question)
    run = run_agent_once(scratch, task, log_path, DEFAULT_TIMEOUT_SECONDS)
    candidate_file = scratch / ANSWER_FILENAME
    if run.timed_out or not candidate_file.exists():
        reason = (
            "timed out"
            if run.timed_out
            else f"exited {run.exit_code} without writing {ANSWER_FILENAME}"
        )
        raise ConfigurationException(
            f"Natural select agent {reason}; workspace kept for inspection: "
            f"{scratch} (log: {log_path})"
        )
    text = candidate_file.read_text(encoding="utf-8")
    original_env = executor.environment
    try:
        # The candidate's imports resolve identically against the real model
        # directory; parsing there is read-only.
        executor.environment = Environment(
            working_path=model_dir,
            import_paths=list(original_env.import_paths),
        )
        statements = executor.generate_sql(text)
    finally:
        executor.environment = original_env
    if not statements:
        raise ConfigurationException(
            f"Natural select agent wrote no executable statement; workspace "
            f"kept for inspection: {scratch}"
        )
    output = executor.execute_raw_sql(statements[-1])
    shutil.rmtree(scratch, ignore_errors=True)
    return output
