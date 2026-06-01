"""Score an agent run: parse the JSONL trace into harness metrics, and check
each generated query against the benchmark's reference query (``PRAGMA
<extension>(n)``, where ``<extension>`` is supplied by the BenchmarkSpec)."""

from __future__ import annotations

import json
from collections import Counter
from dataclasses import dataclass, field
from pathlib import Path

# Marker the agent's ``truncate_middle`` emits. We detect it in tool_result
# bodies to count how many responses came back truncated.
_TRUNCATION_MARKER = "...[truncated "


@dataclass
class ToolOutputStats:
    """Distribution of tool-result body sizes per tool name."""

    count: int = 0
    truncated: int = 0
    total_chars: int = 0
    max_chars: int = 0

    @property
    def avg_chars(self) -> float:
        return self.total_chars / self.count if self.count else 0.0

    @property
    def truncation_rate(self) -> float:
        return self.truncated / self.count if self.count else 0.0


@dataclass
class AgentMetrics:
    iterations: int = 0
    tool_calls_total: int = 0
    tool_calls_by_name: dict[str, int] = field(default_factory=dict)
    trilogy_subcommands: dict[str, int] = field(default_factory=dict)
    tool_results_total: int = 0
    tool_errors: int = 0
    prompt_tokens: int = 0
    completion_tokens: int = 0
    total_tokens: int = 0
    farewell: str = ""
    # Per-tool response-size distribution. The dominant exploration cost
    # signal is "how often did the response come back truncated, forcing the
    # agent to re-issue a similar call".
    tool_output_stats: dict[str, ToolOutputStats] = field(default_factory=dict)
    # Per-tool count of calls whose arguments matched a prior call in the same
    # query (signal: the agent is looping on the same exploration step).
    repeated_calls_by_name: dict[str, int] = field(default_factory=dict)

    @property
    def tool_success_rate(self) -> float:
        if self.tool_results_total == 0:
            return 1.0
        return 1.0 - self.tool_errors / self.tool_results_total


@dataclass
class QueryResult:
    id: int
    # pass      — candidate result set matches the TPC-DS reference
    # fail      — query ran cleanly but the result set differs
    # error     — engine threw (parse / generate_sql / execute / reference)
    # missing   — no query file produced (agent never wrote one)
    # timeout   — agent subprocess hit its wall-clock limit; overrides
    #             fail/error/missing, but not `pass`
    # exhausted — agent gave up after its --max-iterations budget (CLI
    #             exits with EXIT_ITERATION_EXHAUSTED=2). A "the model
    #             couldn't get there in N turns" signal, distinct from a
    #             real crash; overrides fail/error/missing.
    # crashed   — agent subprocess exited non-zero without timing out and
    #             without the exhaustion exit code (provider transport
    #             failure, unhandled exception, OOM, ...); overrides
    #             fail/error/missing, but not `pass` or `timeout`. Distinct
    #             from `error` so the report tells the operator "agent
    #             died" vs "scoring engine threw".
    status: str
    ref_rows: int = 0
    cand_rows: int = 0
    generated_sql_len: int = 0
    detail: str = ""


def _is_error_result(name: str, result: str) -> bool:
    """Classify a tool_result string as a failure."""
    if name == "trilogy":
        first = result.splitlines()[0].strip() if result.strip() else ""
        return first != "exit_code: 0"
    low = result.lower()
    return (
        "error:" in low
        or result.startswith("Unknown tool")
        or "refused:" in result
        or " raised " in result
    )


def _call_signature(name: str, args: dict) -> str:
    """Stable, JSON-serialisable signature for a tool call (name + arguments).
    For `trilogy`, we key on the args list specifically — that's what determines
    whether two calls are the same exploration step. Other tools key on the
    full args dict."""
    if name == "trilogy":
        return json.dumps({"name": name, "args": args.get("args")}, sort_keys=True)
    return json.dumps({"name": name, "args": args}, sort_keys=True, default=str)


def parse_agent_log(log_path: Path) -> AgentMetrics:
    """Aggregate the agent's ``--log-file`` JSONL trace into harness metrics."""
    m = AgentMetrics()
    by_name: Counter[str] = Counter()
    subcommands: Counter[str] = Counter()
    stats: dict[str, ToolOutputStats] = {}
    repeated: Counter[str] = Counter()
    seen_signatures: dict[str, str] = {}
    pending_call_name: list[tuple[str, str]] = []  # (name, signature) queue
    if not log_path.exists():
        return m

    for line in log_path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            event = json.loads(line)
        except json.JSONDecodeError:
            continue

        etype = event.get("type")
        if etype == "llm_response":
            m.iterations += 1
            usage = event.get("usage") or {}
            m.prompt_tokens += usage.get("prompt_tokens") or 0
            m.completion_tokens += usage.get("completion_tokens") or 0
            m.total_tokens += usage.get("total_tokens") or 0
        elif etype == "tool_call":
            name = str(event.get("name", "?"))
            by_name[name] += 1
            m.tool_calls_total += 1
            args = event.get("arguments") or {}
            sig = _call_signature(name, args if isinstance(args, dict) else {})
            if sig in seen_signatures.values():
                repeated[name] += 1
            seen_signatures[f"{name}#{m.tool_calls_total}"] = sig
            pending_call_name.append((name, sig))
            if name == "trilogy":
                cli_args = args.get("args") or []
                if isinstance(cli_args, list) and cli_args:
                    subcommands[str(cli_args[0])] += 1
            elif name == "return_control_to_user":
                msg = args.get("message")
                if isinstance(msg, str):
                    m.farewell = msg
        elif etype == "tool_result":
            m.tool_results_total += 1
            name = str(event.get("name", "?"))
            result = str(event.get("result") or "")
            if _is_error_result(name, result):
                m.tool_errors += 1
            bucket = stats.setdefault(name, ToolOutputStats())
            bucket.count += 1
            bucket.total_chars += len(result)
            bucket.max_chars = max(bucket.max_chars, len(result))
            if _TRUNCATION_MARKER in result:
                bucket.truncated += 1
            # Drain matching pending call (FIFO) — pairing isn't strictly
            # necessary for the aggregates we record, but keeps queue bounded.
            if pending_call_name:
                pending_call_name.pop(0)

    m.tool_calls_by_name = dict(by_name)
    m.trilogy_subcommands = dict(subcommands)
    m.tool_output_stats = stats
    m.repeated_calls_by_name = dict(repeated)
    return m


def aggregate_metrics(metrics_list: list[AgentMetrics]) -> AgentMetrics:
    """Sum per-query AgentMetrics into one — for the per-query (chunked) eval
    where each query is a fresh agent invocation. The last non-empty farewell
    wins (arbitrary but stable)."""
    agg = AgentMetrics()
    by_name: Counter[str] = Counter()
    subcmds: Counter[str] = Counter()
    repeated: Counter[str] = Counter()
    output_stats: dict[str, ToolOutputStats] = {}
    for m in metrics_list:
        agg.iterations += m.iterations
        agg.tool_calls_total += m.tool_calls_total
        agg.tool_results_total += m.tool_results_total
        agg.tool_errors += m.tool_errors
        agg.prompt_tokens += m.prompt_tokens
        agg.completion_tokens += m.completion_tokens
        agg.total_tokens += m.total_tokens
        by_name.update(m.tool_calls_by_name)
        subcmds.update(m.trilogy_subcommands)
        repeated.update(m.repeated_calls_by_name)
        for tool, s in m.tool_output_stats.items():
            bucket = output_stats.setdefault(tool, ToolOutputStats())
            bucket.count += s.count
            bucket.truncated += s.truncated
            bucket.total_chars += s.total_chars
            bucket.max_chars = max(bucket.max_chars, s.max_chars)
        if m.farewell:
            agg.farewell = m.farewell
    agg.tool_calls_by_name = dict(by_name)
    agg.trilogy_subcommands = dict(subcmds)
    agg.repeated_calls_by_name = dict(repeated)
    agg.tool_output_stats = output_stats
    return agg


def _round_cell(v: object) -> object:
    """Round floating-point cells to absorb last-ULP noise from differing
    arithmetic order (e.g. a computed percentage `a*100/b` vs `100*a/b`). The
    reference SQL and the Trilogy-generated SQL can produce values that agree to
    ~15 significant digits but differ in the final ULP, which an exact `repr`
    comparison wrongly flags as a mismatch. Rounding to 9 decimals is far finer
    than any genuine difference, so it cannot introduce false passes. Decimals
    (money/quantity) are exact and left untouched."""
    if isinstance(v, float):
        if v != v or v in (float("inf"), float("-inf")):
            return v
        return round(v, 9)
    return v


def _multiset(rows: list) -> Counter[str]:
    """Order-independent representation of a result set, for comparison.

    Both row order and column order are ignored: the prompts ask for a set of
    values, not a fixed column layout, so each row's cells are sorted before
    hashing. Only whether the right data was computed is graded."""
    return Counter(
        repr(tuple(sorted((_round_cell(c) for c in r), key=repr))) for r in rows
    )


def _find_query_file(workspace: Path, idx: int) -> Path | None:
    for name in (f"query{idx:02d}.preql", f"query{idx}.preql"):
        candidate = workspace / name
        if candidate.exists():
            return candidate
    return None


def make_scoring_engine(db_path: Path, workspace: Path, extension: str):
    """Build a Trilogy executor on the workspace's duckdb, with the benchmark's
    extension loaded. Reusable across per-query scoring calls so we don't pay
    engine setup + extension load on every query."""
    from trilogy import Dialects
    from trilogy.core.models.environment import Environment
    from trilogy.dialect.config import DuckDBConfig

    engine = Dialects.DUCK_DB.default_executor(
        environment=Environment(working_path=workspace),
        conf=DuckDBConfig(path=str(db_path)),
    )
    engine.execute_raw_sql(f"INSTALL {extension}; LOAD {extension};")
    return engine


def score_query(
    engine,
    workspace: Path,
    idx: int,
    extension: str,
    params: dict | None = None,
    custom_refs_dir: Path | None = None,
) -> QueryResult:
    """Score a single query — for live dashboard updates that don't want to
    wait for the whole run to finish before grading. ``params`` is the same
    shape as the prompt JSON's ``params`` field (``{name: {type, value}}``)
    and is injected into the executor's environment before generation."""
    return _score_one(
        engine,
        workspace,
        idx,
        extension,
        params=params,
        custom_refs_dir=custom_refs_dir,
    )


def apply_timeout(result: QueryResult, timed_out: bool) -> QueryResult:
    """Promote a non-passing result to ``status='timeout'`` when the agent
    subprocess hit its wall-clock limit. A passing query is left alone — the
    agent happened to produce a correct file before being killed."""
    if not timed_out or result.status == "pass":
        return result
    detail = result.detail or "agent timed out"
    if "timed out" not in detail.lower():
        detail = f"agent timed out (was: {result.status}) — {detail}"
    result.status = "timeout"
    result.detail = detail
    return result


def apply_exhausted(
    result: QueryResult, exit_code: int, timed_out: bool
) -> QueryResult:
    """Promote a non-passing, non-timeout result to ``status='exhausted'`` when
    the agent CLI gave up after its --max-iterations budget (signalled by
    exit code ``EXIT_ITERATION_EXHAUSTED``, currently 2). Distinct from
    ``crashed`` — the process exited cleanly, the model just didn't get
    there in N turns."""
    from trilogy.scripts.agent import EXIT_ITERATION_EXHAUSTED

    if timed_out or exit_code != EXIT_ITERATION_EXHAUSTED or result.status == "pass":
        return result
    detail = result.detail or "agent exhausted iteration budget"
    if "exhausted" not in detail.lower():
        detail = f"agent exhausted iterations (was: {result.status}) — {detail}"
    result.status = "exhausted"
    result.detail = detail
    return result


def apply_crash(result: QueryResult, exit_code: int, timed_out: bool) -> QueryResult:
    """Promote a non-passing, non-timeout, non-exhausted result to
    ``status='crashed'`` when the agent subprocess exited non-zero. Timeout
    takes precedence (it's how we kill the subprocess in the first place),
    iteration exhaustion is handled separately by :func:`apply_exhausted`,
    and ``pass`` wins — a correct query file written before the crash is
    still a correct answer."""
    from trilogy.scripts.agent import EXIT_ITERATION_EXHAUSTED

    if (
        timed_out
        or exit_code == 0
        or exit_code == EXIT_ITERATION_EXHAUSTED
        or result.status == "pass"
    ):
        return result
    detail = result.detail or "agent exited non-zero"
    if "agent crashed" not in detail.lower():
        detail = f"agent crashed (exit {exit_code}, was: {result.status}) — {detail}"
    result.status = "crashed"
    result.detail = detail
    return result


def score_queries(
    db_path: Path,
    workspace: Path,
    ids: list[int],
    extension: str,
    custom_refs_dir: Path | None = None,
) -> list[QueryResult]:
    """Run each agent-produced query against ``db_path`` and compare results to
    the benchmark's reference for that query id (``query<id>.preql`` vs
    ``PRAGMA <extension>(<id>)``, or a custom ``.sql`` file from
    ``custom_refs_dir`` when present)."""
    engine = make_scoring_engine(db_path, workspace, extension)
    return [
        _score_one(engine, workspace, idx, extension, custom_refs_dir=custom_refs_dir)
        for idx in ids
    ]


def _load_reference(
    engine, idx: int, extension: str, custom_refs_dir: Path | None
) -> list:
    """Reference rows for query ``idx``. Prefers ``<custom_refs_dir>/query<NN>.sql``
    (raw SQL against the source tables) when present — used to override the
    built-in PRAGMA template for prompts whose default filter values yield
    empty results at our scale factor. Falls back to ``PRAGMA <extension>(<idx>)``."""
    if custom_refs_dir is not None:
        for name in (f"query{idx:02d}.sql", f"query{idx}.sql"):
            sql_path = custom_refs_dir / name
            if sql_path.exists():
                sql = sql_path.read_text(encoding="utf-8")
                return list(engine.execute_raw_sql(sql).fetchall())
    return list(engine.execute_raw_sql(f"PRAGMA {extension}({idx});").fetchall())


def _score_one(
    engine,
    workspace: Path,
    idx: int,
    extension: str,
    params: dict | None = None,
    custom_refs_dir: Path | None = None,
) -> QueryResult:
    from trilogy.core.models.environment import Environment

    query_file = _find_query_file(workspace, idx)
    if query_file is None:
        return QueryResult(id=idx, status="missing", detail="no query file produced")

    text = query_file.read_text(encoding="utf-8")
    try:
        engine.environment = Environment(working_path=workspace)
        if params:
            engine.environment.set_parameters(
                **{name: spec.get("value") for name, spec in params.items()}
            )
        statements = engine.generate_sql(text)
    except Exception as exc:
        return QueryResult(
            id=idx, status="error", detail=f"generate_sql: {type(exc).__name__}: {exc}"
        )
    if not statements:
        return QueryResult(
            id=idx,
            status="error",
            detail="query file produced no executable statement (empty?)",
        )
    sql = statements[-1]

    try:
        candidate = list(engine.execute_raw_sql(sql).fetchall())
    except Exception as exc:
        return QueryResult(
            id=idx,
            status="error",
            generated_sql_len=len(sql),
            detail=f"execute: {type(exc).__name__}: {exc}",
        )

    try:
        reference = _load_reference(engine, idx, extension, custom_refs_dir)
    except Exception as exc:
        return QueryResult(
            id=idx,
            status="error",
            generated_sql_len=len(sql),
            detail=f"reference load failed: {exc}",
        )

    passed = _multiset(candidate) == _multiset(reference)
    return QueryResult(
        id=idx,
        status="pass" if passed else "fail",
        ref_rows=len(reference),
        cand_rows=len(candidate),
        generated_sql_len=len(sql),
        detail="" if passed else "result set differs from reference",
    )
