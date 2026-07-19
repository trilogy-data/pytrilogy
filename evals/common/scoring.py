"""Score an agent run: parse the JSONL trace into harness metrics, and check
each generated query against the benchmark's reference query (``PRAGMA
<extension>(n)``, where ``<extension>`` is supplied by the BenchmarkSpec)."""

from __future__ import annotations

import json
import math
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from decimal import Decimal
from pathlib import Path
from typing import cast

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
    # Submit-reviewer activity. `reviewer_kickbacks` (NOT_DONE verdicts) should
    # stay low — each one is a forced re-loop; a high count signals the reviewer
    # is over-firing (false positives) and burning agent budget.
    reviewer_verdicts: int = 0
    reviewer_kickbacks: int = 0
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
        elif etype == "reviewer_verdict":
            m.reviewer_verdicts += 1
            if not event.get("is_done"):
                m.reviewer_kickbacks += 1
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
        agg.reviewer_verdicts += m.reviewer_verdicts
        agg.reviewer_kickbacks += m.reviewer_kickbacks
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


def metrics_to_dict(m: AgentMetrics) -> dict:
    """Serialize per-query AgentMetrics for storage in report.json, so a later
    spliced run can re-aggregate the full benchmark without re-parsing logs."""
    return {
        "iterations": m.iterations,
        "tool_calls_total": m.tool_calls_total,
        "tool_calls_by_name": dict(m.tool_calls_by_name),
        "trilogy_subcommands": dict(m.trilogy_subcommands),
        "tool_results_total": m.tool_results_total,
        "tool_errors": m.tool_errors,
        "prompt_tokens": m.prompt_tokens,
        "completion_tokens": m.completion_tokens,
        "total_tokens": m.total_tokens,
        "reviewer_verdicts": m.reviewer_verdicts,
        "reviewer_kickbacks": m.reviewer_kickbacks,
        "farewell": m.farewell,
        "repeated_calls_by_name": dict(m.repeated_calls_by_name),
        "tool_output_stats": {
            tool: {
                "count": s.count,
                "truncated": s.truncated,
                "total_chars": s.total_chars,
                "max_chars": s.max_chars,
            }
            for tool, s in m.tool_output_stats.items()
        },
    }


def metrics_from_dict(d: dict) -> AgentMetrics:
    """Inverse of :func:`metrics_to_dict`."""
    return AgentMetrics(
        iterations=d.get("iterations", 0),
        tool_calls_total=d.get("tool_calls_total", 0),
        tool_calls_by_name=dict(d.get("tool_calls_by_name", {})),
        trilogy_subcommands=dict(d.get("trilogy_subcommands", {})),
        tool_results_total=d.get("tool_results_total", 0),
        tool_errors=d.get("tool_errors", 0),
        prompt_tokens=d.get("prompt_tokens", 0),
        completion_tokens=d.get("completion_tokens", 0),
        total_tokens=d.get("total_tokens", 0),
        reviewer_verdicts=d.get("reviewer_verdicts", 0),
        reviewer_kickbacks=d.get("reviewer_kickbacks", 0),
        farewell=d.get("farewell", ""),
        repeated_calls_by_name=dict(d.get("repeated_calls_by_name", {})),
        tool_output_stats={
            tool: ToolOutputStats(
                count=s.get("count", 0),
                truncated=s.get("truncated", 0),
                total_chars=s.get("total_chars", 0),
                max_chars=s.get("max_chars", 0),
            )
            for tool, s in d.get("tool_output_stats", {}).items()
        },
    )


# Significant figures used to canonicalize non-integer numeric cells before
# comparison. A single-precision (`float32`) accumulation — e.g. a `0::float`
# placeholder that coerces a money column to REAL — carries only ~7 significant
# digits, so exact `DECIMAL(7,2)` reference sums and the Trilogy float sum can
# diverge in the 7th+ significant digit (q05: grand total 112458735.49 vs
# 112458734.70). Rounding both sides to 6 significant figures absorbs that drift
# while staying far stricter than any genuine TPC-DS/H result difference (which
# is proportionally much larger than 1e-6). Integer counts/ids are kept EXACT
# (see below) so this tolerance never merges two distinct row counts.
COMPARISON_SIG_FIGS = 6


def _sig_round(x: float, sig: int) -> float:
    """Round ``x`` to ``sig`` significant figures (relative precision)."""
    if x == 0.0:
        return 0.0
    return round(x, -int(math.floor(math.log10(abs(x)))) + (sig - 1))


def _round_cell(v: object) -> object:
    """Canonicalize numeric cells so values that are numerically equal compare
    equal regardless of Python type. The reference SQL emits ``Decimal`` for
    money/quantity columns while Trilogy-generated SQL often emits ``float``; an
    exact ``repr`` comparison wrongly flagged equal values as mismatches (e.g.
    ``19640463.31`` vs ``Decimal('19640463.31')``), silently failing correct
    answers. We coerce ``int``/``float``/``Decimal`` to a float; exact-integer
    values (row counts, ids) are kept precise, while fractional values are
    rounded to ``COMPARISON_SIG_FIGS`` significant figures. That absorbs
    float32/last-ULP arithmetic-order noise (`a*100/b` vs `100*a/b`) proportional
    to magnitude — which a fixed decimal-place round cannot, since the drift on a
    large sum is an absolute value, not a sub-decimal one. Booleans, non-finite
    values, out-of-range ints, and non-numeric cells are left untouched."""
    if isinstance(v, bool):
        return v
    if isinstance(v, Decimal):
        if not v.is_finite():
            return v
        v = float(v)
    if isinstance(v, int):
        if abs(v) >= 2**53:
            return v
        v = float(v)
    if isinstance(v, float):
        if v != v or v in (float("inf"), float("-inf")):
            return v
        if v == int(v) and abs(v) < 2**53:
            return float(int(v))  # exact integer value (count/id): keep precise
        return _sig_round(v, COMPARISON_SIG_FIGS)
    return v


def _multiset(rows: list) -> Counter[str]:
    """Order-independent representation of a result set, for comparison.

    Both row order and column order are ignored: the prompts ask for a set of
    values, not a fixed column layout, so each row's cells are sorted before
    hashing. Only whether the right data was computed is graded."""
    return Counter(
        repr(tuple(sorted((_round_cell(c) for c in r), key=repr))) for r in rows
    )


COMPARISON_REL_TOL = 10 ** (1 - COMPARISON_SIG_FIGS)
COMPARISON_ABS_TOL = 1e-9


def _comparison_cell(value: object) -> tuple[str, object]:
    """Split non-numerics from numerics and retain exact-integer provenance."""
    if isinstance(value, bool):
        return ("exact", repr(value))
    if isinstance(value, Decimal):
        if not value.is_finite():
            return ("exact", repr(value))
        if value == value.to_integral_value():
            return ("numeric", (float(value), True))
        return ("numeric", (float(value), False))
    if isinstance(value, int):
        return ("numeric", (float(value), True))
    if isinstance(value, float):
        if not math.isfinite(value):
            return ("exact", repr(value))
        if value.is_integer() and abs(value) < 2**53:
            return ("numeric", (value, True))
        return ("numeric", (value, False))
    return ("exact", repr(value))


def _comparison_row(
    row: list | tuple,
) -> tuple[tuple[tuple[str, object], ...], tuple[tuple[float, bool], ...]]:
    cells = [_comparison_cell(value) for value in row]
    exact = tuple(sorted((cell for cell in cells if cell[0] == "exact"), key=repr))
    numeric = tuple(
        sorted(
            (
                cast(tuple[float, bool], cell[1])
                for cell in cells
                if cell[0] == "numeric"
            ),
            key=lambda x: x[0],
        )
    )
    return exact, numeric


def _numeric_rows_close(
    left: tuple[tuple[float, bool], ...], right: tuple[tuple[float, bool], ...]
) -> bool:
    if len(left) != len(right):
        return False
    for (a, a_is_integer), (b, b_is_integer) in zip(left, right):
        if a_is_integer and b_is_integer:
            if a != b:
                return False
        elif not math.isclose(
            a, b, rel_tol=COMPARISON_REL_TOL, abs_tol=COMPARISON_ABS_TOL
        ):
            return False
    return True


def _bucket_matches(
    candidate: list[tuple[tuple[float, bool], ...]],
    reference: list[tuple[tuple[float, bool], ...]],
) -> bool:
    """Maximum-match one exact-value bucket under tolerant numeric equality."""
    if len(candidate) != len(reference):
        return False
    matched_candidate: dict[int, int] = {}

    def assign(reference_idx: int, seen: set[int]) -> bool:
        for candidate_idx, candidate_row in enumerate(candidate):
            if candidate_idx in seen or not _numeric_rows_close(
                candidate_row, reference[reference_idx]
            ):
                continue
            seen.add(candidate_idx)
            previous = matched_candidate.get(candidate_idx)
            if previous is None or assign(previous, seen):
                matched_candidate[candidate_idx] = reference_idx
                return True
        return False

    return all(assign(idx, set()) for idx in range(len(reference)))


def _results_equal(candidate: list, reference: list) -> bool:
    """Compare unordered rows/columns with exact integers and tolerant fractions.

    Independent significant-figure rounding is not suitable for equality: two
    nearly identical values can land on opposite sides of a rounding boundary.
    Bucket rows by their exact cells, then maximum-match fractional cells with
    ``isclose`` so multiset cardinality is still enforced.
    """
    if len(candidate) != len(reference):
        return False
    candidate_buckets: dict[
        tuple[tuple[str, object], ...], list[tuple[tuple[float, bool], ...]]
    ] = defaultdict(list)
    reference_buckets: dict[
        tuple[tuple[str, object], ...], list[tuple[tuple[float, bool], ...]]
    ] = defaultdict(list)
    for row in candidate:
        exact, numeric = _comparison_row(row)
        candidate_buckets[exact].append(numeric)
    for row in reference:
        exact, numeric = _comparison_row(row)
        reference_buckets[exact].append(numeric)
    if candidate_buckets.keys() != reference_buckets.keys():
        return False
    return all(
        _bucket_matches(rows, reference_buckets[exact])
        for exact, rows in candidate_buckets.items()
    )


def _find_query_file(workspace: Path, idx: int) -> Path | None:
    # `.preql` (Trilogy categories) is checked before `.sql` (no-Trilogy SQL
    # baselines); a given run only produces one of them, so order is moot in
    # practice and just makes a stray file deterministic.
    for name in (
        f"query{idx:02d}.preql",
        f"query{idx}.preql",
        f"query{idx:02d}.sql",
        f"query{idx}.sql",
    ):
        candidate = workspace / name
        if candidate.exists():
            return candidate
    return None


def _last_sql_statement(text: str) -> str:
    """Last non-empty ``;``-delimited statement. Agent SQL answers are a single
    statement; this mirrors ``generate_sql``'s ``statements[-1]`` for the rare
    multi-statement file. Naive split (no string-literal awareness) is fine for
    benchmark answers, which don't embed ``;`` in literals."""
    parts = [s.strip() for s in text.split(";")]
    nonempty = [s for s in parts if s]
    return nonempty[-1] if nonempty else ""


def make_scoring_engine(db_path: Path, workspace: Path, extension: str):
    """Build a Trilogy executor on the workspace's duckdb, with the benchmark's
    extension loaded. Reusable across per-query scoring calls so we don't pay
    engine setup + extension load on every query.

    Opened read-only: scoring only runs SELECTs, and a read-only DuckDB handle
    is shareable, so concurrent scorers (and ad-hoc probes against a live run's
    workspace db) coexist instead of fighting over the single-writer file lock."""
    from trilogy import Dialects
    from trilogy.core.models.environment import Environment
    from trilogy.dialect.config import DuckDBConfig

    engine = Dialects.DUCK_DB.default_executor(
        environment=Environment(working_path=workspace),
        conf=DuckDBConfig(path=str(db_path), read_only=True),
    )
    if extension:
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


def _score_subprocess_target(
    db_path: str,
    workspace: str,
    idx: int,
    extension: str,
    params: dict | None,
    custom_refs_dir: str | None,
    conn,
) -> None:
    """Run in a spawned child: build a fresh engine and score one query. Sends
    ``("ok", QueryResult)`` or ``("err", message)`` back over the pipe. Isolating
    scoring in a killable process is what lets the parent bound a hung
    `generate_sql` loop or a runaway DuckDB query with a hard timeout."""
    try:
        engine = make_scoring_engine(Path(db_path), Path(workspace), extension)
        result = _score_one(
            engine,
            Path(workspace),
            idx,
            extension,
            params=params,
            custom_refs_dir=Path(custom_refs_dir) if custom_refs_dir else None,
        )
        conn.send(("ok", result))
    except Exception as exc:  # pragma: no cover - serialised back to parent
        conn.send(("err", f"{type(exc).__name__}: {exc}"))
    finally:
        conn.close()


def score_query_timed(
    db_path: Path,
    workspace: Path,
    idx: int,
    extension: str,
    timeout: float,
    params: dict | None = None,
    custom_refs_dir: Path | None = None,
) -> QueryResult:
    """Score one query in a child process bounded by ``timeout`` seconds. A
    hang in `generate_sql` (planner loop) or DuckDB execution can no longer
    block the worker (and, under the shared scoring lock, the whole run) — on
    timeout the child is killed and the query is graded ``error``."""
    import multiprocessing as mp

    ctx = mp.get_context("spawn")
    parent_conn, child_conn = ctx.Pipe(duplex=False)
    proc = ctx.Process(
        target=_score_subprocess_target,
        args=(
            str(db_path),
            str(workspace),
            idx,
            extension,
            params,
            str(custom_refs_dir) if custom_refs_dir else None,
            child_conn,
        ),
        daemon=True,
    )
    proc.start()
    child_conn.close()  # only the child writes
    try:
        if parent_conn.poll(timeout):
            kind, payload = parent_conn.recv()
            proc.join(5)
            if kind == "ok":
                return payload
            return QueryResult(id=idx, status="error", detail=f"score: {payload}")
        # Timed out — kill the child so its DB connection / lock is released.
        proc.terminate()
        proc.join(5)
        if proc.is_alive():
            proc.kill()
            proc.join(5)
        return QueryResult(
            id=idx,
            status="error",
            detail=f"scoring timed out after {timeout:g}s (likely planner loop "
            "or runaway query)",
        )
    finally:
        parent_conn.close()


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
    if query_file.suffix == ".sql":
        # No-Trilogy baseline: the candidate IS SQL — execute it directly,
        # skipping the Trilogy transpile step.
        sql = _last_sql_statement(text)
        if not sql:
            return QueryResult(id=idx, status="error", detail="query file is empty")
    else:
        try:
            engine.environment = Environment(working_path=workspace)
            if params:
                engine.environment.set_parameters(
                    **{name: spec.get("value") for name, spec in params.items()}
                )
            statements = engine.generate_sql(text)
        except Exception as exc:
            return QueryResult(
                id=idx,
                status="error",
                detail=f"generate_sql: {type(exc).__name__}: {exc}",
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

    passed = _results_equal(candidate, reference)
    return QueryResult(
        id=idx,
        status="pass" if passed else "fail",
        ref_rows=len(reference),
        cand_rows=len(candidate),
        generated_sql_len=len(sql),
        detail="" if passed else "result set differs from reference",
    )
