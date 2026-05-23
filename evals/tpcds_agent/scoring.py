"""Score an agent run: parse the JSONL trace into harness metrics, and check
each generated query against the TPC-DS reference (``PRAGMA tpcds(n)``)."""

from __future__ import annotations

import json
from collections import Counter
from dataclasses import dataclass, field
from pathlib import Path


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

    @property
    def tool_success_rate(self) -> float:
        if self.tool_results_total == 0:
            return 1.0
        return 1.0 - self.tool_errors / self.tool_results_total


@dataclass
class QueryResult:
    id: int
    status: str  # pass | fail | error | missing
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


def parse_agent_log(log_path: Path) -> AgentMetrics:
    """Aggregate the agent's ``--log-file`` JSONL trace into harness metrics."""
    m = AgentMetrics()
    by_name: Counter[str] = Counter()
    subcommands: Counter[str] = Counter()
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
            if _is_error_result(name, str(event.get("result") or "")):
                m.tool_errors += 1

    m.tool_calls_by_name = dict(by_name)
    m.trilogy_subcommands = dict(subcommands)
    return m


def aggregate_metrics(metrics_list: list[AgentMetrics]) -> AgentMetrics:
    """Sum per-query AgentMetrics into one — for the per-query (chunked) eval
    where each query is a fresh agent invocation. The last non-empty farewell
    wins (arbitrary but stable)."""
    agg = AgentMetrics()
    by_name: Counter[str] = Counter()
    subcmds: Counter[str] = Counter()
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
        if m.farewell:
            agg.farewell = m.farewell
    agg.tool_calls_by_name = dict(by_name)
    agg.trilogy_subcommands = dict(subcmds)
    return agg


def _multiset(rows: list) -> Counter[str]:
    """Order-independent representation of a result set, for comparison.

    Both row order and column order are ignored: the prompts ask for a set of
    values, not a fixed column layout, so each row's cells are sorted before
    hashing. Only whether the right data was computed is graded."""
    return Counter(repr(tuple(sorted(r, key=repr))) for r in rows)


def _find_query_file(workspace: Path, idx: int) -> Path | None:
    for name in (f"query{idx:02d}.preql", f"query{idx}.preql"):
        candidate = workspace / name
        if candidate.exists():
            return candidate
    return None


def score_queries(db_path: Path, workspace: Path, ids: list[int]) -> list[QueryResult]:
    """Run each agent-produced query against ``db_path`` and compare results to
    the TPC-DS reference for that query id (``query<id>.preql`` vs
    ``PRAGMA tpcds(<id>)``)."""
    from trilogy import Dialects
    from trilogy.core.models.environment import Environment
    from trilogy.dialect.config import DuckDBConfig

    engine = Dialects.DUCK_DB.default_executor(
        environment=Environment(working_path=workspace),
        conf=DuckDBConfig(path=str(db_path)),
    )
    engine.execute_raw_sql("INSTALL tpcds; LOAD tpcds;")

    return [_score_one(engine, workspace, idx) for idx in ids]


def _score_one(engine, workspace: Path, idx: int) -> QueryResult:
    from trilogy.core.models.environment import Environment

    query_file = _find_query_file(workspace, idx)
    if query_file is None:
        return QueryResult(id=idx, status="missing", detail="no query file produced")

    text = query_file.read_text(encoding="utf-8")
    try:
        engine.environment = Environment(working_path=workspace)
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
        reference = list(engine.execute_raw_sql(f"PRAGMA tpcds({idx});").fetchall())
    except Exception as exc:
        return QueryResult(
            id=idx,
            status="error",
            generated_sql_len=len(sql),
            detail=f"reference PRAGMA failed: {exc}",
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
