"""Per-benchmark eval entry point. Each benchmark's ``run_eval.py`` builds a
``BenchmarkSpec`` and calls :func:`run`."""

from __future__ import annotations

import argparse
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
from .report import build_report, load_env, render_markdown
from .spec import BenchmarkSpec

REPO_ROOT = Path(__file__).resolve().parents[2]

PROVIDER_ENV = {
    "openrouter": "OPENROUTER_API_KEY",
    "anthropic": "ANTHROPIC_API_KEY",
    "openai": "OPENAI_API_KEY",
    "google": "GOOGLE_API_KEY",
}

# OpenRouter multiplexes a model across providers; we only block AtlasCloud
# because it hard-400s our tool requests. Providers are otherwise fungible —
# letting OpenRouter pick gives the broadest pool and best resilience to a
# single upstream rate-limiting.
OPENROUTER_ROUTING = {
    "ignore": ["AtlasCloud"],
    "allow_fallbacks": True,
}

DEFAULT_MODEL = "deepseek/deepseek-v4-flash"


def _force_utf8_stdio() -> None:
    """Survive redirection on Windows, where stdout defaults to cp1252 and
    chokes on non-ASCII in the feed and report."""
    for stream in (sys.stdout, sys.stderr):
        try:
            stream.reconfigure(encoding="utf-8")
        except (AttributeError, ValueError):
            pass


def _build_argparser(spec: BenchmarkSpec) -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=f"On-demand {spec.name} agent eval",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--model", default=DEFAULT_MODEL, help="LLM model id")
    parser.add_argument("--provider", default="openrouter", help="LLM provider")
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
        default=50,
        help="agent tool-loop budget PER QUERY (each query is a fresh agent)",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=600,
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
        "--both-modes",
        action="store_true",
        help="run BOTH the base (ingest-only) and enriched legs in parallel, "
        "then render dashboard_compare.png. Each leg writes its own results "
        "subdir (<ts>_base, <ts>_enriched). Requires either --enriched-model-dir "
        "or a `default_enriched_dir` configured on the BenchmarkSpec.",
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


def _splice_prior_results(
    report: dict, prior_report: dict, fresh_ids: set[int], prior_dir: Path
) -> dict:
    """Merge missing-id entries from ``prior_report`` into ``report`` so the
    headline reflects the full benchmark. Fresh entries are tagged source=this;
    spliced entries are tagged source=<prior_dir.name>. summary is recomputed."""
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

    report = dict(report)
    report["queries"] = merged_queries
    report["per_query"] = merged_per_query
    report["summary"] = {
        "pass_count": pass_count,
        "pass_rate": round(pass_count / total, 3) if total else 0.0,
        "status_breakdown": dict(statuses),
    }
    report["meta"] = {**report["meta"], "num_queries": total}
    report["spliced_from"] = {
        "run_dir": str(prior_dir),
        "fresh_ids": sorted(fresh_ids),
        "spliced_ids": sorted(q["id"] for q in spliced_queries),
    }
    return report


_BOTH_MODES_DROP_FLAGS = {"--both-modes"}
_BOTH_MODES_DROP_FLAGS_WITH_VALUE = {
    "--enriched-model-dir",
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


def _extract_enriched_dir_from_argv(argv: list[str]) -> str | None:
    """Pick up an explicit ``--enriched-model-dir`` value the user passed on
    the same CLI invocation that requested ``--both-modes``."""
    for i, a in enumerate(argv):
        if a == "--enriched-model-dir" and i + 1 < len(argv):
            return argv[i + 1]
        if a.startswith("--enriched-model-dir="):
            return a.split("=", 1)[1]
    return None


def _pump_with_prefix(stream, label: str) -> None:
    prefix = f"[{label}] "
    for line in stream:
        sys.stdout.write(prefix + line)
        sys.stdout.flush()
    stream.close()


def _run_both_modes(spec: BenchmarkSpec, raw_argv: list[str]) -> int:
    """Spawn the base (ingest-only) and enriched legs as parallel subprocesses
    of the same ``run_eval.py``. Each leg gets its own ``--output-dir`` and
    forced ``--monitor quiet`` so the streamed output stays legible. After both
    finish, render ``dashboard_compare.png`` from their respective reports."""
    import subprocess

    user_enriched = _extract_enriched_dir_from_argv(raw_argv)
    enriched_path = user_enriched or (
        str(spec.default_enriched_dir) if spec.default_enriched_dir else None
    )
    if enriched_path is None:
        print(
            "ERROR: --both-modes requires --enriched-model-dir on the command "
            "line, or `default_enriched_dir` configured on the BenchmarkSpec.",
            file=sys.stderr,
        )
        return 2

    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")
    base_out = spec.results_dir / f"{timestamp}_base"
    enriched_out = spec.results_dir / f"{timestamp}_enriched"

    script = Path(sys.argv[0]).resolve()
    common_argv = _filter_both_modes_argv(raw_argv)

    # When the user didn't explicitly set --concurrency, halve the default for
    # each leg so the parallel legs together don't exceed the single-leg
    # default (limits OpenRouter rate-limit pressure). If the user passed
    # --concurrency explicitly, respect it verbatim for both legs.
    per_leg_concurrency: list[str] = []
    if not _argv_has_flag(raw_argv, "--concurrency"):
        default_concurrency = _build_argparser(spec).get_default("concurrency")
        halved = max(1, default_concurrency // 2)
        if halved != default_concurrency:
            print(
                f"[both-modes] auto-halving concurrency: "
                f"{default_concurrency} → {halved} per leg "
                "(pass --concurrency explicitly to override)."
            )
        per_leg_concurrency = ["--concurrency", str(halved)]

    base_argv = [
        *common_argv,
        *per_leg_concurrency,
        "--output-dir",
        str(base_out),
        "--monitor",
        "quiet",
    ]
    enriched_argv = [
        *common_argv,
        *per_leg_concurrency,
        "--output-dir",
        str(enriched_out),
        "--monitor",
        "quiet",
        "--enriched-model-dir",
        enriched_path,
    ]

    print(
        f"[both-modes] spawning two parallel runs:\n"
        f"   base     → {base_out}\n"
        f"   enriched → {enriched_out}  (model dir: {enriched_path})"
    )

    procs: list[tuple[str, subprocess.Popen]] = []
    pumps: list[threading.Thread] = []
    for label, leg_argv in (("base", base_argv), ("enriched", enriched_argv)):
        proc = subprocess.Popen(
            [sys.executable, str(script), *leg_argv],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            encoding="utf-8",
            errors="replace",
            bufsize=1,
        )
        procs.append((label, proc))
        t = threading.Thread(
            target=_pump_with_prefix, args=(proc.stdout, label), daemon=True
        )
        t.start()
        pumps.append(t)

    exit_codes = [(label, proc.wait()) for label, proc in procs]
    for t in pumps:
        t.join(timeout=5)

    for label, rc in exit_codes:
        print(f"[both-modes] {label} leg exit={rc}")

    base_report = base_out / "report.json"
    enriched_report = enriched_out / "report.json"
    if base_report.exists() and enriched_report.exists():
        report_a, events_a = analyze_run.load_run(base_out)
        report_b, events_b = analyze_run.load_run(enriched_out)
        out = analyze_run.render_comparison(
            report_a,
            events_a,
            report_b,
            events_b,
            spec.charts_dir / "dashboard_compare.png",
            label_a="base",
            label_b="enriched",
        )
        print(f"[both-modes] wrote {out}")
    else:
        print(
            "[both-modes] skipping comparison render: "
            f"base report present={base_report.exists()}, "
            f"enriched report present={enriched_report.exists()}",
            file=sys.stderr,
        )

    return max((rc for _, rc in exit_codes), default=0)


def run(spec: BenchmarkSpec) -> int:
    args = _build_argparser(spec).parse_args()
    _force_utf8_stdio()

    if args.both_modes:
        return _run_both_modes(spec, sys.argv[1:])

    load_env(args.env_file)
    if args.provider == "openrouter":
        os.environ.setdefault("OPENROUTER_PROVIDER", json.dumps(OPENROUTER_ROUTING))
        # DeepSeek (and some other OpenRouter upstreams) escape `<`/`>` in
        # tool-call arguments, producing `&lt;-` instead of `<-`. Decode at the
        # provider level so the agent doesn't have to learn the workaround.
        os.environ.setdefault("OPENROUTER_SANITIZE_HTML_ESCAPES", "true")
    api_env = PROVIDER_ENV.get(args.provider, "OPENROUTER_API_KEY")
    if not os.environ.get(api_env):
        print(
            f"ERROR: {api_env} is not set (looked in env and {args.env_file}).",
            file=sys.stderr,
        )
        return 2

    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")
    run_dir = args.output_dir or (spec.results_dir / timestamp)
    workspace = run_dir / "workspace"
    workspace.mkdir(parents=True, exist_ok=True)
    print(f"[1/5] Building {spec.name} DuckDB (sf={args.scale_factor:g}) ...")
    cached = db.build_database(spec, args.scale_factor)
    workspace_db = db.copy_database(cached, workspace / spec.db_filename)

    agent_runner.write_trilogy_toml(
        workspace, spec, args.provider, args.model, args.max_iterations
    )
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

    if args.enriched_model_dir:
        print(
            f"[2/5] Seeding raw/ from enriched model dir {args.enriched_model_dir} ..."
        )
        ingest = agent_runner.install_enriched_model(
            workspace, args.enriched_model_dir, spec.enriched_skip_prefixes
        )
    else:
        print("[2/5] Pre-ingesting model with `trilogy ingest --all` ...")
        ingest = agent_runner.run_pre_ingest(workspace)
    (run_dir / "ingest_output.txt").write_text(
        f"exit={ingest['exit_code']}  duration={ingest['duration']:.1f}s\n"
        f"--- stdout ---\n{ingest['stdout']}\n--- stderr ---\n{ingest['stderr']}\n",
        encoding="utf-8",
    )
    if ingest["exit_code"] != 0:
        print(
            f"  ingest failed (exit {ingest['exit_code']}); see ingest_output.txt",
            file=sys.stderr,
        )
        # Per-query tasks tell the agent raw/ is already populated; without it
        # every query starts from a broken premise. Abort instead of grading 0/N.
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

    def acquire_worker() -> Path:
        with pool_lock:
            return worker_pool.pop()

    def release_worker(worker: Path) -> None:
        with pool_lock:
            worker_pool.append(worker)

    # Enriched runs share charts/ with base runs; suffix keeps the in-flight
    # and final dashboards from clobbering the base baseline.
    suffix = "_enriched" if args.enriched_model_dir else ""

    # --- live dashboard machinery ---------------------------------------
    # Build the scoring engine ONCE and reuse per query — avoids paying
    # engine setup + extension load on every grade.
    try:
        scoring_engine = scoring.make_scoring_engine(
            workspace_db, workspace, spec.duckdb_extension
        )
    except Exception as exc:
        scoring_engine = None
        print(
            f"  scoring engine init failed; live dashboard disabled: "
            f"{type(exc).__name__}: {exc}",
            file=sys.stderr,
        )
    scoring_lock = threading.Lock()
    render_lock = threading.Lock()
    last_render = [0.0]
    RENDER_MIN_INTERVAL = 10.0  # seconds — debounce so matplotlib isn't hot

    def maybe_render_dashboard(force: bool = False) -> None:
        if scoring_engine is None:
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
                    spec.charts_dir / f"dashboard{suffix}.png",
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
            task = prompts.build_single_query_task(spec, entry)
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
            )
            result["id"] = qid
            if worker != workspace:
                produced = worker / f"query{qid:02d}.preql"
                if produced.exists():
                    shutil.copy2(produced, workspace / f"query{qid:02d}.preql")
                    produced.unlink()
            per_query_runs[index] = result
            per_query_metrics[index] = scoring.parse_agent_log(log_path)
            if scoring_engine is not None:
                try:
                    with scoring_lock:
                        per_query_scores[index] = scoring.score_query(
                            scoring_engine,
                            workspace,
                            qid,
                            spec.duckdb_extension,
                            params=entry.get("params"),
                        )
                except Exception as exc:
                    per_query_scores[index] = scoring.QueryResult(
                        id=qid,
                        status="error",
                        detail=f"inline-score: {type(exc).__name__}: {exc}",
                    )
                per_query_scores[index] = scoring.apply_timeout(
                    per_query_scores[index], result.get("timed_out", False)
                )
            status = per_query_scores[index].status if per_query_scores[index] else "?"
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

    metrics = scoring.aggregate_metrics(per_query_metrics)
    agent_run = {
        "exit_code": 0 if all(r["exit_code"] == 0 for r in per_query_runs) else 1,
        "timed_out": any(r.get("timed_out") for r in per_query_runs),
        "duration": sum(r["duration"] for r in per_query_runs),
        "output": "\n".join(
            f"=== q{r['id']:02d} (exit {r['exit_code']}, {r['duration']:.0f}s) ===\n"
            f"{r['output']}"
            for r in per_query_runs
        ),
    }
    (run_dir / "agent_output.txt").write_text(agent_run["output"], encoding="utf-8")

    print(f"[4/5] Finalising scores for {len(query_ids)} queries ...")
    if scoring_engine is not None:
        for i, entry in enumerate(active):
            if per_query_scores[i] is None:
                try:
                    with scoring_lock:
                        per_query_scores[i] = scoring.score_query(
                            scoring_engine,
                            workspace,
                            entry["id"],
                            spec.duckdb_extension,
                            params=entry.get("params"),
                        )
                except Exception as exc:
                    per_query_scores[i] = scoring.QueryResult(
                        id=entry["id"],
                        status="error",
                        detail=f"backfill-score: {type(exc).__name__}: {exc}",
                    )
                per_query_scores[i] = scoring.apply_timeout(
                    per_query_scores[i],
                    (
                        per_query_runs[i].get("timed_out", False)
                        if per_query_runs[i]
                        else False
                    ),
                )
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
                workspace_db, workspace, query_ids, spec.duckdb_extension
            )
        except Exception as exc:
            print(f"  scoring aborted: {type(exc).__name__}: {exc}", file=sys.stderr)
            query_results = [
                scoring.QueryResult(id=i, status="error", detail="scoring aborted")
                for i in query_ids
            ]
        query_results = [
            scoring.apply_timeout(
                qr,
                (
                    per_query_runs[i].get("timed_out", False)
                    if per_query_runs[i]
                    else False
                ),
            )
            for i, qr in enumerate(query_results)
        ]

    report = build_report(spec, args, timestamp, agent_run, metrics, query_results)
    report["per_query"] = [
        {
            "id": r["id"],
            "exit_code": r["exit_code"],
            "timed_out": r.get("timed_out", False),
            "duration_seconds": round(r["duration"], 1),
        }
        for r in per_query_runs
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
        _, events = analyze_run.load_run(run_dir)
        dashboard = analyze_run.render(
            report,
            events,
            spec.charts_dir / f"dashboard{suffix}.png",
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
