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
from hashlib import blake2s
from pathlib import Path
from typing import Literal

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


RepetitionStatus = Literal[
    "pass", "fail", "error", "missing", "timeout", "exhausted", "crashed"
]


@dataclass
class RepetitionResult:
    status: RepetitionStatus
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


def seed_workspace(
    model_dir: Path, workspace: Path, exclude: set[Path] | None = None
) -> None:
    """Copy the model's text files + toml into ``workspace`` (integration tier /
    live natural select). ``exclude`` drops files by resolved path — used to
    keep the validations file (which holds the expected answers) out of the
    agent's reach."""
    exclude = {p.resolve() for p in exclude} if exclude else set()
    workspace.mkdir(parents=True, exist_ok=True)
    for pattern in _SEED_GLOBS:
        for source in sorted(model_dir.glob(pattern)):
            if source.resolve() in exclude:
                continue
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


def _mock_name(datasource) -> str:
    """Stable, SQL-safe table name for a datasource's mock stand-in, keyed by
    its physical address. The same key is derived from the flattened-env
    datasource (for materialization) and the per-file datasource statement (for
    repointing), so the two always agree."""
    from trilogy.core.models.datasource import Address

    address = datasource.address
    if isinstance(address, Address):
        key = f"{address.type.value}:{address.location}"
        tail = Path(address.location).stem if address.is_file else address.location
    else:
        key = str(address)
        tail = str(address)
    safe_tail = re.sub(r"\W+", "_", tail).strip("_").lower()[:24] or "ds"
    digest = blake2s(key.encode("utf-8"), digest_size=6).hexdigest()
    return f"mock_{safe_tail}_{digest}"


def _materialize_mock_tables(flattened_env, db_path: Path) -> None:
    """Materialize deterministic, fully-populated mock rows for every datasource
    into ``db_path`` as a table named by :func:`_mock_name`. A single
    MockManager over the flattened environment gives shared concepts (join keys)
    the same values across datasources, so cross-datasource joins still match."""
    import random

    from trilogy.dialect.config import DuckDBConfig
    from trilogy.dialect.enums import Dialects
    from trilogy.dialect.mock import MockManager, synthesis_order

    random.seed(0)
    manager = MockManager(flattened_env)
    executor = Dialects.DUCK_DB.default_executor(conf=DuckDBConfig(path=str(db_path)))
    seen: set[str] = set()
    try:
        for datasource in synthesis_order(
            list(flattened_env.datasources.values()), manager.canon
        ):
            name = _mock_name(datasource)
            if name in seen:  # same physical address under multiple namespaces
                continue
            seen.add(name)
            table = manager.create_mock_table(datasource)
            executor.execute_raw_sql(
                "register(:name, :tbl)", {"name": "mock_tbl", "tbl": table}
            )
            executor.execute_write_sql(
                f'CREATE OR REPLACE TABLE "{name}" AS SELECT * FROM mock_tbl'
            )
    finally:
        executor.close()


def _unit_toml(model_dir: Path, image_dir: Path, db_path: Path) -> None:
    """toml for the mock image: the model's toml minus its [engine] sections,
    with a DuckDB engine pointed at the mock database. No import_paths — the
    image is a complete, self-contained copy so working-path resolution finds
    every (repointed) file, and a fallback to the original model dir would leak
    un-repointed addresses."""
    lines: list[str] = []
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
    (image_dir / "trilogy.toml").write_text("\n".join(lines), encoding="utf-8")


def build_mock_image(
    model_dir: Path,
    image_dir: Path,
    db_path: Path,
    flattened_env,
    exclude: set[Path],
) -> None:
    """Build a self-contained, repointed copy of the model under ``image_dir``:
    every datasource repointed at a mock table (materialized into ``db_path``
    with referential integrity), the model files re-rendered so the agent (and
    the expected-answer recompile) resolve against the mock. ``exclude`` drops
    files by resolved path — the validations file must never reach the agent.

    Repointing makes the original address type irrelevant: table, file, remote,
    query, and script datasources all become mock tables, so the unit tier has
    no address-shape restrictions."""
    from trilogy.core.exceptions import ConfigurationException
    from trilogy.core.models.datasource import Datasource
    from trilogy.core.models.environment import Environment
    from trilogy.parser import parse_text
    from trilogy.parsing.render import Renderer

    exclude = {p.resolve() for p in exclude}
    image_dir.mkdir(parents=True, exist_ok=True)
    _materialize_mock_tables(flattened_env, db_path)

    import_roots = [Path(p) for p in _merged_import_paths(model_dir)]
    renderer = Renderer()
    for source in sorted(model_dir.glob("**/*.preql")):
        if source.resolve() in exclude:
            continue
        dest = image_dir / source.relative_to(model_dir)
        dest.parent.mkdir(parents=True, exist_ok=True)
        try:
            env = Environment(working_path=model_dir, import_paths=import_roots)
            _, statements = parse_text(source.read_text(encoding="utf-8"), env)
        except Exception as exc:
            # A file that doesn't parse standalone can't be repointed; copy it
            # verbatim rather than dropping the model content. Its datasources
            # (if any) keep their real address — loud on execute, not silent.
            raise ConfigurationException(
                f"Could not parse '{source}' standalone to build the unit mock "
                f"image: {type(exc).__name__}: {exc}"
            ) from exc
        for statement in statements:
            if isinstance(statement, Datasource):
                statement.repoint(_mock_name(statement))
        body = "\n".join(renderer.to_string(s) for s in statements)
        dest.write_text(body + "\n", encoding="utf-8")

    for extra in sorted(model_dir.glob("**/schema.md")):
        dest = image_dir / extra.relative_to(model_dir)
        dest.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(extra, dest)

    _unit_toml(model_dir, image_dir, db_path)


def compile_expected_against_image(
    image_dir: Path, validations_source: str
) -> list[str]:
    """Recompile the validations file's expected selects against the repointed
    mock image, returning one SQL string per ``validate ... matches`` statement
    in source order. Symmetric with how the candidate is compiled in the
    workspace, so both sides read the mock tables."""
    from trilogy.core.statements.execute import ProcessedValidateNaturalStatement

    executor = _executor_for_workspace(image_dir)
    try:
        processed = executor.parse_text(validations_source)
        return [
            executor.generator.compile_statement(p.expected)
            for p in processed
            if isinstance(p, ProcessedValidateNaturalStatement)
        ]
    finally:
        executor.close()


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


def _seed_rep_workspace(
    workspace: Path,
    *,
    image_dir: Path | None,
    model_dir: Path,
    exclude: set[Path],
) -> None:
    """Fresh per-repetition workspace: a copy of the repointed mock image (unit
    tier) or a seeded copy of the real model minus the validations file
    (integration tier)."""
    if image_dir is not None:
        shutil.copytree(image_dir, workspace, dirs_exist_ok=True)
    else:
        seed_workspace(model_dir, workspace, exclude=exclude)


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
    exclude: set[Path],
    image_dir: Path | None = None,
) -> QuestionResult:
    """Run one embedded validation question: N fresh agent repetitions, each
    scored against the expected SQL. Repetitions run serially (provider
    pressure); question runs are read-only against the engine. ``image_dir``
    (unit tier) is the pre-built repointed mock image copied into each
    workspace; without it (integration tier) the real model is seeded, minus
    ``exclude`` (the validations file)."""
    result = QuestionResult(name=name, question=question, target=target, tags=tags)
    effective_timeout = timeout or DEFAULT_TIMEOUT_SECONDS
    question_dir = run_dir / name
    for index in range(repetitions):
        workspace = question_dir / f"rep{index + 1}"
        _seed_rep_workspace(
            workspace, image_dir=image_dir, model_dir=model_dir, exclude=exclude
        )
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
