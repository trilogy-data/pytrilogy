"""Per-benchmark eval entry point. Each benchmark's ``run_eval.py`` builds a
``BenchmarkSpec`` and calls :func:`run`."""

from __future__ import annotations

import argparse
import io
import json
import os
import shutil
import sys
import threading
import time
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path

from . import agent_runner, analyze_run, db, prompts, scoring
from .categories import CATEGORIES, FUNNEL_ORDER, get_category
from .report import agent_metric_fields, build_report, load_env, render_markdown
from .spec import BenchmarkSpec

REPO_ROOT = Path(__file__).resolve().parents[2]

PROVIDER_ENV = {
    "openrouter": "OPENROUTER_API_KEY",
    "anthropic": "ANTHROPIC_API_KEY",
    "openai": "OPENAI_API_KEY",
    "google": "GOOGLE_API_KEY",
    "deepseek": "DEEPSEEK_API_KEY",
}

# OpenRouter multiplexes a model across providers; we only block AtlasCloud
# because it hard-400s our tool requests. Providers are otherwise fungible —
# letting OpenRouter pick gives the broadest pool and best resilience to a
# single upstream rate-limiting.
OPENROUTER_ROUTING = {
    "ignore": ["AtlasCloud"],
    "allow_fallbacks": True,
}

# Default to direct DeepSeek — bypasses OpenRouter's routing layer (and the
# keep-alive comments it injects that mask upstream hangs from httpx's read
# timeout). Pass --provider openrouter to fall back to the multiplexed route.
#
# Model is `deepseek-chat` (non-thinking variant) rather than `deepseek-v4-flash`
# because the latter runs in thinking mode by default, which rejects ANY explicit
# `tool_choice` (required, named, or otherwise) with a 400. Our agent loop uses
# `tool_choice: required` to force tool calls — incompatible with thinking mode.
# `deepseek-chat` deprecates 2026-07-24; revisit when DeepSeek exposes a way to
# put `deepseek-v4-flash` into non-thinking mode, or relax the agent's reliance
# on `tool_choice` so it works against thinking-mode APIs.
DEFAULT_PROVIDER = "deepseek"
DEFAULT_MODEL = "deepseek-chat"

# Hard ceiling for scoring ONE query (generate_sql + execute + reference), run
# in a child process so it can be killed. Legit scoring is seconds; this only
# trips on a planner loop or a runaway generated query.
SCORE_TIMEOUT = 180.0


def _force_utf8_stdio() -> None:
    """Survive redirection on Windows, where stdout defaults to cp1252 and
    chokes on non-ASCII in the feed and report."""
    for stream in (sys.stdout, sys.stderr):
        if not isinstance(stream, io.TextIOWrapper):
            continue
        try:
            stream.reconfigure(encoding="utf-8")
        except ValueError:
            pass


def _build_argparser(spec: BenchmarkSpec) -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=f"On-demand {spec.name} agent eval",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--model", default=DEFAULT_MODEL, help="LLM model id")
    parser.add_argument("--provider", default=DEFAULT_PROVIDER, help="LLM provider")
    parser.add_argument(
        "--scale-factor",
        type=float,
        default=spec.default_scale_factor,
        help=f"{spec.name} scale factor",
    )
    parser.add_argument(
        "--num-queries",
        type=int,
        default=spec.default_num_queries,
        help="number of queries to attempt",
    )
    parser.add_argument(
        "--max-iterations",
        type=int,
        default=75,
        help="agent tool-loop budget PER QUERY (each query is a fresh agent)",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=900,
        help="agent subprocess timeout PER QUERY (seconds)",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=None,
        help="run output dir (default: results/<ts>)",
    )
    parser.add_argument(
        "--env-file",
        type=Path,
        default=REPO_ROOT / ".env.secrets",
        help="env file providing the provider API key",
    )
    parser.add_argument(
        "--monitor",
        choices=["feed", "raw", "both", "quiet"],
        default="feed",
        help="live monitoring: feed=parsed progress, raw=stdout passthrough, "
        "both=raw+heartbeat, quiet=phase markers only",
    )
    parser.add_argument(
        "--concurrency",
        type=int,
        default=1,
        help="parallel agents; >1 forces monitor=quiet and uses per-worker "
        "workspace copies so DuckDB's file lock doesn't serialise workers",
    )
    parser.add_argument(
        "--enriched-model-dir",
        type=Path,
        default=None,
        help="skip `trilogy ingest --all`; seed raw/ from this directory of "
        "hand-curated *.preql files instead",
    )
    parser.add_argument(
        "--query-ids",
        type=str,
        default=None,
        help="comma-separated query ids to run (e.g. 13 or 5,13,18). "
        "Overrides --num-queries; useful for re-running specific prompts.",
    )
    parser.add_argument(
        "--splice-from",
        type=str,
        default="auto",
        help="when --query-ids is set, splice unrun queries from a prior run "
        "so the report headline reflects the full benchmark. Pass a run dir, "
        "'auto' (default) to pick the latest other run, or 'none' to disable.",
    )
    parser.add_argument(
        "--category",
        choices=sorted(CATEGORIES),
        default=None,
        help="which eval category this run is: sql_bare (db only), sql_schema "
        "(db+schema.md), ingest (auto Trilogy model), enriched (curated model). "
        "Defaults to 'enriched' if --enriched-model-dir is set, else 'ingest'.",
    )
    parser.add_argument(
        "--categories",
        type=str,
        default=None,
        help="comma-separated categories to run in parallel, then render the "
        "cross-category funnel + matrix (e.g. sql_bare,sql_schema,ingest,enriched). "
        "Each leg writes its own results subdir (<ts>_<category>).",
    )
    parser.add_argument(
        "--both-modes",
        action="store_true",
        help="alias for --categories ingest,enriched (the legacy base-vs-enriched "
        "comparison).",
    )
    parser.add_argument(
        "--force-tool-choice",
        action="store_true",
        help="force tool_choice=required every turn (no plain-text reasoning). "
        "Default is tool_choice: auto, which lets the model deliberate before "
        "acting; pass this to A/B the old forced-tool behavior.",
    )
    parser.add_argument(
        "--enable-todo",
        action="store_true",
        help="re-enable the todo tool for the Trilogy agent. The eval defaults "
        "todo OFF (fewer tools/less context for short single-query tasks; no "
        "effect on the SQL toolset, which has no todo). Pass this to A/B it.",
    )
    parser.add_argument(
        "--scope-diagnostics",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="expose derived-value scope diagnostics to Trilogy agents; pass "
        "--no-scope-diagnostics for a clean baseline",
    )
    return parser


def _find_latest_prior_run(spec: BenchmarkSpec, exclude: Path) -> Path | None:
    """Most recent results/<ts> dir other than ``exclude`` that has report.json."""
    if not spec.results_dir.exists():
        return None
    candidates = sorted(
        (
            p
            for p in spec.results_dir.iterdir()
            if p.is_dir() and p.resolve() != exclude.resolve()
        ),
        key=lambda p: p.name,
        reverse=True,
    )
    for c in candidates:
        if (c / "report.json").exists():
            return c
    return None


def _spliced_query_metrics(
    prior_report: dict, prior_dir: Path, spliced_ids: list[int]
) -> list[dict]:
    """Per-query metric dicts for the spliced ids. Prefer the prior report's
    stored ``per_query_metrics`` (present since the lossless-splice change, and
    already full-benchmark for a prior that itself spliced); fall back to
    re-parsing that run's ``agent_log`` for reports that predate the field."""
    want = set(spliced_ids)
    stored = {
        m["id"]: m for m in prior_report.get("per_query_metrics", []) if m["id"] in want
    }
    out: list[dict] = []
    for qid in spliced_ids:
        if qid in stored:
            out.append(stored[qid])
            continue
        log = prior_dir / f"agent_log.q{qid:02d}.jsonl"
        parsed = (
            scoring.parse_agent_log(log) if log.exists() else scoring.AgentMetrics()
        )
        out.append({"id": qid, **scoring.metrics_to_dict(parsed)})
    return out


def _splice_prior_results(
    report: dict, prior_report: dict, fresh_ids: set[int], prior_dir: Path
) -> dict:
    """Merge missing-id entries from ``prior_report`` into ``report`` so the
    headline reflects the full benchmark. Fresh entries are tagged source=this;
    spliced entries are tagged source=<prior_dir.name>. Summary AND the aggregate
    ``agent`` harness metrics (tokens/iterations/tool stats that feed the charts)
    are recomputed over the full merged set — not left reflecting only the freshly
    rerun subset."""
    src_tag = prior_dir.name

    fresh_queries = list(report.get("queries", []))
    for q in fresh_queries:
        q.setdefault("source", "this_run")
    spliced_queries = [
        {**q, "source": src_tag}
        for q in prior_report.get("queries", [])
        if q["id"] not in fresh_ids
    ]
    merged_queries = sorted(fresh_queries + spliced_queries, key=lambda q: q["id"])

    fresh_per_query = list(report.get("per_query", []))
    for r in fresh_per_query:
        r.setdefault("source", "this_run")
    spliced_per_query = [
        {**r, "source": src_tag}
        for r in prior_report.get("per_query", [])
        if r["id"] not in fresh_ids
    ]
    merged_per_query = sorted(
        fresh_per_query + spliced_per_query, key=lambda r: r["id"]
    )

    statuses: Counter[str] = Counter(q["status"] for q in merged_queries)
    pass_count = statuses.get("pass", 0)
    total = len(merged_queries)

    spliced_ids = sorted(q["id"] for q in spliced_queries)
    fresh_metrics = list(report.get("per_query_metrics", []))
    spliced_metrics = _spliced_query_metrics(prior_report, prior_dir, spliced_ids)
    merged_metrics = sorted(fresh_metrics + spliced_metrics, key=lambda m: m["id"])
    agg = scoring.aggregate_metrics(
        [scoring.metrics_from_dict(m) for m in merged_metrics]
    )
    merged_duration = sum(r.get("duration_seconds", 0) for r in merged_per_query)

    report = dict(report)
    report["queries"] = merged_queries
    report["per_query"] = merged_per_query
    report["per_query_metrics"] = merged_metrics
    report["agent"] = {
        **report["agent"],
        **agent_metric_fields(agg),
        "duration_seconds": round(merged_duration, 1),
        "avg_query_seconds": (round(merged_duration / total, 1) if total else 0.0),
    }
    report["summary"] = {
        "pass_count": pass_count,
        "pass_rate": round(pass_count / total, 3) if total else 0.0,
        "status_breakdown": dict(statuses),
    }
    report["meta"] = {**report["meta"], "num_queries": total}
    report["spliced_from"] = {
        "run_dir": str(prior_dir),
        "fresh_ids": sorted(fresh_ids),
        "spliced_ids": spliced_ids,
    }
    return report


_BOTH_MODES_DROP_FLAGS = {"--both-modes"}
_BOTH_MODES_DROP_FLAGS_WITH_VALUE = {
    "--categories",
    "--category",
    "--output-dir",
    "--monitor",
}


def _argv_has_flag(argv: list[str], flag: str) -> bool:
    """True iff ``flag`` is set on the command line — either space-separated
    (``--foo VALUE``) or equals form (``--foo=VALUE``). Used to distinguish
    'user actually said this' from 'argparse fell back to its default'."""
    eq = flag + "="
    return any(a == flag or a.startswith(eq) for a in argv)


def _filter_both_modes_argv(argv: list[str]) -> list[str]:
    """Strip `--both-modes` and any of the per-leg-overridden flags (with their
    values) from the original CLI args so we can re-invoke the eval as a
    subprocess for each leg without duplicating those flags."""
    result: list[str] = []
    skip_next = False
    for a in argv:
        if skip_next:
            skip_next = False
            continue
        if a in _BOTH_MODES_DROP_FLAGS:
            continue
        if a in _BOTH_MODES_DROP_FLAGS_WITH_VALUE:
            skip_next = True
            continue
        if any(a.startswith(f + "=") for f in _BOTH_MODES_DROP_FLAGS_WITH_VALUE):
            continue
        result.append(a)
    return result


def _pump_with_prefix(stream, label: str) -> None:
    prefix = f"[{label}] "
    for line in stream:
        sys.stdout.write(prefix + line)
        sys.stdout.flush()
    stream.close()


def _run_categories(
    spec: BenchmarkSpec, raw_argv: list[str], category_keys: list[str]
) -> int:
    """Spawn one parallel leg per category (each a subprocess of this
    ``run_eval.py`` with its own ``--category``, ``--output-dir`` and forced
    ``--monitor quiet``). After all finish, render the cross-category funnel +
    per-query matrix from their reports."""
    import subprocess

    for key in category_keys:
        get_category(key)  # validate early

    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")
    script = Path(sys.argv[0]).resolve()
    common_argv = _filter_both_modes_argv(raw_argv)

    # Split the single-leg default concurrency across the parallel legs so they
    # don't together exceed it (rate-limit pressure). Respect an explicit
    # --concurrency verbatim for every leg.
    per_leg_concurrency: list[str] = []
    if not _argv_has_flag(raw_argv, "--concurrency"):
        default_concurrency = _build_argparser(spec).get_default("concurrency")
        split = max(1, default_concurrency // len(category_keys))
        if split != default_concurrency:
            print(
                f"[categories] splitting concurrency: {default_concurrency} → "
                f"{split} per leg across {len(category_keys)} legs "
                "(pass --concurrency explicitly to override)."
            )
        per_leg_concurrency = ["--concurrency", str(split)]

    outs: dict[str, Path] = {
        key: spec.results_dir / f"{timestamp}_{key}" for key in category_keys
    }
    print("[categories] spawning parallel runs:")
    for key, out in outs.items():
        print(f"   {key:<10} → {out}")

    procs: list[tuple[str, subprocess.Popen]] = []
    pumps: list[threading.Thread] = []
    for key in category_keys:
        leg_argv = [
            *common_argv,
            *per_leg_concurrency,
            "--category",
            key,
            "--output-dir",
            str(outs[key]),
            "--monitor",
            "quiet",
        ]
        proc = subprocess.Popen(
            [sys.executable, str(script), *leg_argv],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            encoding="utf-8",
            errors="replace",
            bufsize=1,
        )
        procs.append((key, proc))
        t = threading.Thread(
            target=_pump_with_prefix, args=(proc.stdout, key), daemon=True
        )
        t.start()
        pumps.append(t)

    exit_codes = [(key, proc.wait()) for key, proc in procs]
    for t in pumps:
        t.join(timeout=5)
    for key, rc in exit_codes:
        print(f"[categories] {key} leg exit={rc}")

    reports_by_key: dict[str, dict] = {}
    for key in category_keys:
        if (outs[key] / "report.json").exists():
            report, _ = analyze_run.load_run(outs[key])
            reports_by_key[key] = report
        else:
            print(f"[categories] {key} leg produced no report.json", file=sys.stderr)

    if len(reports_by_key) >= 2:
        ordered = {k: reports_by_key[k] for k in FUNNEL_ORDER if k in reports_by_key}
        png = analyze_run.render_funnel(ordered, spec.charts_dir / "funnel_v2.png")
        md = analyze_run.write_funnel_report(ordered, spec.charts_dir / "funnel.md")
        print(f"[categories] wrote {png} and {md}")
    else:
        print(
            "[categories] skipping funnel render: need >=2 reports, "
            f"got {sorted(reports_by_key)}",
            file=sys.stderr,
        )

    return max((rc for _, rc in exit_codes), default=0)


def run(spec: BenchmarkSpec) -> int:
    args = _build_argparser(spec).parse_args()
    _force_utf8_stdio()

    if args.both_modes or args.categories:
        if args.categories:
            keys = [k.strip() for k in args.categories.split(",") if k.strip()]
        else:
            keys = ["ingest", "enriched"]
        return _run_categories(spec, sys.argv[1:], keys)

    # Resolve this run's category: explicit --category wins; else enriched when
    # an enriched dir was given (back-compat), else ingest.
    category_key = args.category or (
        "enriched" if args.enriched_model_dir else "ingest"
    )
    category = get_category(category_key)

    load_env(args.env_file)
    if args.provider == "openrouter":
        os.environ.setdefault("OPENROUTER_PROVIDER", json.dumps(OPENROUTER_ROUTING))
        # DeepSeek (and some other OpenRouter upstreams) escape `<`/`>` in
        # tool-call arguments, producing `&lt;-` instead of `<-`. Decode at the
        # provider level so the agent doesn't have to learn the workaround.
        os.environ.setdefault("OPENROUTER_SANITIZE_HTML_ESCAPES", "true")
    api_env = PROVIDER_ENV.get(args.provider)
    if api_env is None:
        print(
            f"ERROR: unknown provider {args.provider!r} — known providers: "
            f"{sorted(PROVIDER_ENV)}. Add it to PROVIDER_ENV in "
            "evals/common/main.py and to api_key_env_map in agent_runner.py.",
            file=sys.stderr,
        )
        return 2
    if not os.environ.get(api_env):
        print(
            f"ERROR: {api_env} is not set (looked in env and {args.env_file}).",
            file=sys.stderr,
        )
        return 2

    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")
    # Absolute: agent subprocesses run with cwd set to their per-worker workspace
    # copy, so a relative run dir would resolve to a path that doesn't exist there.
    run_dir = (args.output_dir or (spec.results_dir / timestamp)).resolve()
    workspace = run_dir / "workspace"
    workspace.mkdir(parents=True, exist_ok=True)
    print(f"[1/5] Building {spec.name} DuckDB (sf={args.scale_factor:g}) ...")
    cached = db.build_database(spec, args.scale_factor)
    workspace_db = db.copy_database(cached, workspace / spec.db_filename)

    # Doc files are per-category: DABstep hands the markdown docs to the SQL
    # baselines (and auto-ingest), but the enriched leg gets only the curated
    # model — that leg measures whether the semantic layer carries the domain
    # knowledge on its own.
    leg_docs = spec.doc_files if category.key in spec.doc_categories else ()
    agent_runner.write_trilogy_toml(
        workspace,
        spec,
        args.provider,
        args.model,
        args.max_iterations,
        force_tool_choice=args.force_tool_choice,
        # Per-query generation runs against a pre-populated raw/ model — the
        # agent should explore the model, not list raw DB tables. (No effect on
        # the SQL legs, which don't use the trilogy tool.)
        allow_database_introspection=False,
        # Schema discovery goes through `explore`; file read opens up only when
        # this leg ships doc files the agent must consult (DABstep's manual).
        allow_file_read=bool(leg_docs),
        disable_todo=not args.enable_todo,
    )
    for doc in leg_docs:
        shutil.copy2(doc, workspace / doc.name)
    if args.query_ids:
        wanted = {int(x.strip()) for x in args.query_ids.split(",") if x.strip()}
        active = [p for p in prompts.active_prompts(spec) if p["id"] in wanted]
        missing = wanted - {p["id"] for p in active}
        if missing:
            print(
                f"ERROR: --query-ids referenced unknown id(s): {sorted(missing)}",
                file=sys.stderr,
            )
            return 2
    else:
        active = prompts.active_prompts(spec)[: args.num_queries]
    query_ids = [p["id"] for p in active]

    print(f"[2/5] Category '{category.key}': setting up workspace ...")
    ingest = category.setup(
        workspace, spec, db_path=workspace_db, enriched_dir=args.enriched_model_dir
    )
    (run_dir / "ingest_output.txt").write_text(
        f"exit={ingest['exit_code']}  duration={ingest['duration']:.1f}s\n"
        f"--- stdout ---\n{ingest['stdout']}\n--- stderr ---\n{ingest['stderr']}\n",
        encoding="utf-8",
    )
    if ingest["exit_code"] != 0:
        print(
            f"  setup failed (exit {ingest['exit_code']}); see ingest_output.txt",
            file=sys.stderr,
        )
        # Trilogy per-query tasks tell the agent raw/ is already populated;
        # without it every query starts from a broken premise. Abort.
        return 2

    concurrency = max(1, args.concurrency)
    monitor_mode = "quiet" if concurrency > 1 else args.monitor
    print(
        f"[3/5] Running agent per query ({len(active)} queries, fresh context each,"
        f" concurrency={concurrency}) — {args.provider}/{args.model} ..."
    )
    # Always materialise per-worker workspaces — even at concurrency=1 — so the
    # agent's DuckDB file lock doesn't collide with the long-lived scoring
    # engine, which holds the workspace db open in read-write mode for the run.
    worker_dirs = [
        agent_runner.prepare_worker_workspace(workspace, i, spec.db_filename)
        for i in range(concurrency)
    ]

    per_query_runs: list[dict] = [None] * len(active)  # type: ignore[list-item]
    per_query_metrics: list[scoring.AgentMetrics] = [
        scoring.AgentMetrics() for _ in active
    ]
    per_query_scores: list[scoring.QueryResult | None] = [None] * len(active)
    worker_pool: list[Path] = list(worker_dirs)
    pool_lock = threading.Lock()
    # True wall clock for the agent phase — distinct from the sum-of-per-query
    # `duration` below. At concurrency=1 they match; at concurrency>1 the wall
    # clock is shorter because queries run in parallel. We report both: wall
    # for "how long did this take", sum/N for "how long does an average query
    # take" (which we want independent of parallelism).
    agent_phase_t0 = time.perf_counter()

    def acquire_worker() -> Path:
        with pool_lock:
            return worker_pool.pop()

    def release_worker(worker: Path) -> None:
        with pool_lock:
            worker_pool.append(worker)

    # All categories share charts/; the per-category suffix keeps each leg's
    # in-flight and final dashboard from clobbering the others.
    suffix = f"_{category.key}"

    # --- live dashboard machinery ---------------------------------------
    # Build the scoring engine ONCE and reuse per query — avoids paying
    # engine setup + extension load on every grade.
    # Scoring runs in a per-query CHILD PROCESS (scoring.score_query_timed) so a
    # hung generate_sql / runaway DuckDB query can be killed by a hard timeout
    # instead of deadlocking the worker — and, under the shared lock, the whole
    # run (a single stuck score previously froze every worker indefinitely). We
    # therefore do NOT hold a persistent engine open: a live connection would
    # lock the duckdb file against the scoring subprocess. Per-query failures
    # surface as that query's `error` result.
    scoring_available = workspace_db.exists()
    # Reference SQL directory: per-spec folder where each ``query<NN>.sql`` is
    # the executable SQL the scorer compares the agent's output against, in
    # place of ``PRAGMA <extension>(<NN>)``. Most files reproduce the PRAGMA
    # output verbatim; a few override it with filter values that actually
    # produce non-empty results at our scale factor (e.g. q32/q41/q44 at
    # SF=0.1). Falls back to PRAGMA when the dir is unset or the file is
    # missing for a given id.
    references_dir: Path | None = spec.references_dir
    if references_dir is not None and not references_dir.exists():
        references_dir = None
    scoring_lock = threading.Lock()
    render_lock = threading.Lock()
    last_render = [0.0]
    RENDER_MIN_INTERVAL = 10.0  # seconds — debounce so matplotlib isn't hot

    def maybe_render_dashboard(force: bool = False) -> None:
        if not scoring_available:
            return
        with render_lock:
            now = time.perf_counter()
            if not force and now - last_render[0] < RENDER_MIN_INTERVAL:
                return
            last_render[0] = now
            graded = [s for s in per_query_scores if s is not None]
            done_runs = [r for r in per_query_runs if r is not None]
            agent_run_partial = {
                "exit_code": 0,
                "timed_out": any(r.get("timed_out") for r in done_runs),
                "duration": sum(r["duration"] for r in done_runs),
                "wall_duration": time.perf_counter() - agent_phase_t0,
                "output": "",
            }
            metrics_partial = scoring.aggregate_metrics(per_query_metrics)
            try:
                partial_report = build_report(
                    spec, args, timestamp, agent_run_partial, metrics_partial, graded
                )
                # In-progress queries land in build_report as a smaller total
                # than meta.num_queries — patch the headline so the dashboard
                # title reflects the live denominator.
                partial_report["meta"]["num_queries"] = len(active)
                _, events = analyze_run.load_run_events(run_dir)
                analyze_run.render(
                    partial_report,
                    events,
                    spec.charts_dir / f"dashboard{suffix}_v2.png",
                )
            except Exception as exc:
                print(
                    f"  live dashboard render skipped: {type(exc).__name__}: {exc}",
                    file=sys.stderr,
                )

    def run_one(index: int, entry: dict) -> None:
        qid = entry["id"]
        worker = acquire_worker()
        try:
            log_path = run_dir / f"agent_log.q{qid:02d}.jsonl"
            task = category.build_task(spec, entry, include_docs=bool(leg_docs))
            (run_dir / f"task.q{qid:02d}.txt").write_text(task, encoding="utf-8")
            print(f"  [q{qid:02d}] starting (worker {worker.name})", flush=True)
            result = agent_runner.run_agent(
                worker,
                log_path,
                args.provider,
                args.model,
                task,
                args.timeout,
                monitor_mode,
                toolset=category.harness,
                scope_diagnostics=args.scope_diagnostics,
            )
            result["id"] = qid
            # Persist the subprocess stdout/stderr on a non-zero exit — it holds
            # the crash traceback, which the structured jsonl log never captures.
            if result.get("exit_code", 0) != 0 and result.get("output"):
                (run_dir / f"crash.q{qid:02d}.txt").write_text(
                    result["output"], encoding="utf-8"
                )
            prompts.stage_candidate_for_scoring(
                worker,
                workspace,
                spec,
                qid,
                category.candidate_ext,
                remove_source=True,
            )
            per_query_runs[index] = result
            per_query_metrics[index] = scoring.parse_agent_log(log_path)
            if scoring_available:
                try:
                    with scoring_lock:
                        score = scoring.score_query_timed(
                            workspace_db,
                            workspace,
                            qid,
                            spec.duckdb_extension,
                            SCORE_TIMEOUT,
                            params=entry.get("params"),
                            custom_refs_dir=references_dir,
                        )
                except Exception as exc:
                    score = scoring.QueryResult(
                        id=qid,
                        status="error",
                        detail=f"inline-score: {type(exc).__name__}: {exc}",
                    )
                timed_out = result.get("timed_out", False)
                exit_code = result.get("exit_code", 0)
                score = scoring.apply_timeout(score, timed_out)
                score = scoring.apply_exhausted(score, exit_code, timed_out)
                per_query_scores[index] = scoring.apply_crash(
                    score, exit_code, timed_out
                )
            scored = per_query_scores[index]
            status = scored.status if scored else "?"
            print(
                f"  [q{qid:02d}] done in {result['duration']:.0f}s"
                f" (exit {result['exit_code']}, score={status})",
                flush=True,
            )
            maybe_render_dashboard()
        finally:
            release_worker(worker)

    if concurrency == 1:
        for i, entry in enumerate(active):
            run_one(i, entry)
    else:
        with ThreadPoolExecutor(max_workers=concurrency) as ex:
            futures = [ex.submit(run_one, i, e) for i, e in enumerate(active)]
            for fut in as_completed(futures):
                fut.result()

    agent_phase_wall = time.perf_counter() - agent_phase_t0
    metrics = scoring.aggregate_metrics(per_query_metrics)
    agent_run = {
        "exit_code": 0 if all(r["exit_code"] == 0 for r in per_query_runs) else 1,
        "timed_out": any(r.get("timed_out") for r in per_query_runs),
        # `duration` is the sum across per-query agent subprocesses — useful
        # for "avg per query" but NOT the user-facing elapsed time when
        # concurrency>1. `wall_duration` is the true elapsed wall clock of
        # the agent phase, which collapses parallel work.
        "duration": sum(r["duration"] for r in per_query_runs),
        "wall_duration": agent_phase_wall,
        "output": "\n".join(
            f"=== q{r['id']:02d} (exit {r['exit_code']}, {r['duration']:.0f}s) ===\n"
            f"{r['output']}"
            for r in per_query_runs
        ),
    }
    (run_dir / "agent_output.txt").write_text(agent_run["output"], encoding="utf-8")

    print(f"[4/5] Finalising scores for {len(query_ids)} queries ...")
    if scoring_available:
        for i, entry in enumerate(active):
            if per_query_scores[i] is None:
                try:
                    with scoring_lock:
                        score = scoring.score_query_timed(
                            workspace_db,
                            workspace,
                            entry["id"],
                            spec.duckdb_extension,
                            SCORE_TIMEOUT,
                            params=entry.get("params"),
                            custom_refs_dir=references_dir,
                        )
                except Exception as exc:
                    score = scoring.QueryResult(
                        id=entry["id"],
                        status="error",
                        detail=f"backfill-score: {type(exc).__name__}: {exc}",
                    )
                run = per_query_runs[i]
                timed_out = run.get("timed_out", False) if run else False
                exit_code = run.get("exit_code", 0) if run else 0
                score = scoring.apply_timeout(score, timed_out)
                score = scoring.apply_exhausted(score, exit_code, timed_out)
                per_query_scores[i] = scoring.apply_crash(score, exit_code, timed_out)
        query_results = [
            (
                s
                if s is not None
                else scoring.QueryResult(
                    id=entry["id"], status="error", detail="never scored"
                )
            )
            for s, entry in zip(per_query_scores, active)
        ]
    else:
        try:
            query_results = scoring.score_queries(
                workspace_db,
                workspace,
                query_ids,
                spec.duckdb_extension,
                custom_refs_dir=references_dir,
            )
        except Exception as exc:
            print(f"  scoring aborted: {type(exc).__name__}: {exc}", file=sys.stderr)
            query_results = [
                scoring.QueryResult(id=i, status="error", detail="scoring aborted")
                for i in query_ids
            ]
        query_results = [
            scoring.apply_crash(
                scoring.apply_exhausted(
                    scoring.apply_timeout(
                        qr,
                        (
                            per_query_runs[i].get("timed_out", False)
                            if per_query_runs[i]
                            else False
                        ),
                    ),
                    (per_query_runs[i].get("exit_code", 0) if per_query_runs[i] else 0),
                    (
                        per_query_runs[i].get("timed_out", False)
                        if per_query_runs[i]
                        else False
                    ),
                ),
                (per_query_runs[i].get("exit_code", 0) if per_query_runs[i] else 0),
                (
                    per_query_runs[i].get("timed_out", False)
                    if per_query_runs[i]
                    else False
                ),
            )
            for i, qr in enumerate(query_results)
        ]

    report = build_report(spec, args, timestamp, agent_run, metrics, query_results)
    report["meta"]["category"] = category.key
    report["meta"]["category_label"] = category.label
    report["per_query"] = [
        {
            "id": r["id"],
            "exit_code": r["exit_code"],
            "timed_out": r.get("timed_out", False),
            "duration_seconds": round(r["duration"], 1),
        }
        for r in per_query_runs
    ]
    # Per-query harness metrics keyed by id, so a later --query-ids rerun can
    # splice + re-aggregate the full-benchmark `agent` block losslessly.
    report["per_query_metrics"] = [
        {"id": r["id"], **scoring.metrics_to_dict(m)}
        for r, m in zip(per_query_runs, per_query_metrics)
    ]
    report["ingest"] = {
        "exit_code": ingest["exit_code"],
        "duration_seconds": round(ingest["duration"], 1),
    }

    if args.query_ids and args.splice_from != "none":
        if args.splice_from == "auto":
            prior_dir = _find_latest_prior_run(spec, run_dir)
        else:
            candidate = Path(args.splice_from)
            prior_dir = candidate if (candidate / "report.json").exists() else None
            if prior_dir is None:
                print(
                    f"  splice: --splice-from {args.splice_from!r} has no report.json",
                    file=sys.stderr,
                )
        if prior_dir is not None:
            try:
                prior_report = json.loads(
                    (prior_dir / "report.json").read_text(encoding="utf-8")
                )
                fresh_ids = {p["id"] for p in active}
                report = _splice_prior_results(
                    report, prior_report, fresh_ids, prior_dir
                )
                print(
                    f"  spliced {len(report['spliced_from']['spliced_ids'])} "
                    f"prior results from {prior_dir.name}"
                )
            except Exception as exc:
                print(f"  splice skipped: {type(exc).__name__}: {exc}", file=sys.stderr)
    (run_dir / "report.json").write_text(json.dumps(report, indent=2), encoding="utf-8")
    markdown = render_markdown(spec, report)
    (run_dir / "report.md").write_text(markdown, encoding="utf-8")

    try:
        _, events = analyze_run.load_run_spliced(run_dir)
        dashboard = analyze_run.render(
            report,
            events,
            spec.charts_dir / f"dashboard{suffix}_v2.png",
        )
        failures = analyze_run.collect_failures(events)
        failures_md = analyze_run.write_failures_report(
            run_dir,
            report,
            failures,
            spec.charts_dir / f"trilogy_failures{suffix}.md",
        )
        print(
            f"  wrote {dashboard}  ({len(failures)} trilogy failures -> {failures_md})"
        )
    except Exception as exc:
        print(f"  analyze_run skipped: {type(exc).__name__}: {exc}", file=sys.stderr)

    print(f"[5/5] Done. Artifacts in {run_dir}\n")
    print(markdown)
    return 0
