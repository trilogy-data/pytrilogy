"""Build (and optionally serve) the trajectory viewer.

    python evals/trajectory_viewer.py [results_dir] [--eval KEY] [--serve PORT]

With no arguments, views the run dir with the most recently modified agent log
across every suite. ``--eval tpcds`` scopes the default to one suite; an
explicit ``results_dir`` wins and its suite is inferred from its location.

Always writes ``<results_dir>/viewer.html`` — a self-contained static page
(the CSS/JS under ``static/`` is inlined at build time). With ``--serve`` it
also starts a local server that adds live polling, an eval/run picker across
all suites, an all-evals summary page, and Replay/Archive actions.
"""

from __future__ import annotations

import argparse
import io
import json
import sys
from pathlib import Path

from .collect import collect
from .server import serve
from .suites import (
    Suite,
    discover_suites,
    find_suite_for,
    latest_run_dir,
    list_run_dirs,
)

_STATIC = Path(__file__).resolve().parent / "static"


def _force_utf8_stdio() -> None:
    """Replay progress can carry engine text; Windows consoles default to cp1252
    and would raise mid-replay on the first non-ASCII byte."""
    for stream in (sys.stdout, sys.stderr):
        if isinstance(stream, io.TextIOWrapper):
            try:
                stream.reconfigure(encoding="utf-8", errors="replace")
            except ValueError:
                pass


def render_html(title: str, data_json: str) -> str:
    html = (_STATIC / "viewer.html").read_text(encoding="utf-8")
    html = html.replace("__TITLE__", title)
    html = html.replace("/*__CSS__*/", (_STATIC / "viewer.css").read_text("utf-8"))
    html = html.replace("/*__JS__*/", (_STATIC / "viewer.js").read_text("utf-8"))
    # Data last: log text could contain any of the placeholders above.
    return html.replace("__DATA__", data_json)


def build_html(results_dir: Path, suite: Suite) -> Path:
    runs = collect(results_dir, suite.spec)
    if not runs:
        raise SystemExit(f"no agent_log.*.jsonl found in {results_dir}")
    data = json.dumps(runs).replace("</", "<\\/")
    out = results_dir / "viewer.html"
    out.write_text(render_html(results_dir.name, data), encoding="utf-8")
    print(f"wrote {out}  ({len(runs)} runs, {suite.label})")
    return out


def _resolve_target(
    suites: dict[str, Suite], results_dir: Path | None, suite_key: str | None
) -> tuple[Suite, Path]:
    if results_dir is not None:
        suite = suites.get(suite_key or "") or find_suite_for(suites, results_dir)
        if suite is None:
            raise SystemExit(
                f"{results_dir} is not inside a known eval suite; pass --eval"
            )
        return suite, results_dir
    if suite_key:
        suite = suites[suite_key]
        names = list_run_dirs(suite)
        if not names:
            raise SystemExit(f"no runs under {suite.results_dir}")
        return suite, suite.results_dir / names[0]
    found = latest_run_dir(suites)
    if found is None:
        raise SystemExit("no agent_log.*.jsonl found under any suite's results dir")
    return found


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument(
        "results_dir",
        type=Path,
        nargs="?",
        help="run dir to view (default: the most recently updated one, any suite)",
    )
    p.add_argument(
        "--eval",
        dest="suite",
        help="eval suite short name (e.g. tpcds, tpch); default: inferred",
    )
    p.add_argument("--serve", type=int, default=None, help="serve the dir on this port")
    p.add_argument(
        "--log-requests",
        action="store_true",
        help="log every HTTP request (off by default: the page polls forever)",
    )
    args = p.parse_args()
    _force_utf8_stdio()
    suites = discover_suites()
    if not suites:
        raise SystemExit("no eval suites found (evals/*/spec.py exporting SPEC)")
    if args.suite and args.suite not in suites:
        raise SystemExit(
            f"unknown eval {args.suite!r}; available: {', '.join(sorted(suites))}"
        )
    suite, results_dir = _resolve_target(suites, args.results_dir, args.suite)
    build_html(results_dir, suite)
    if args.serve is not None:
        serve(suites, suite, results_dir, args.serve, args.log_requests)
    return 0
