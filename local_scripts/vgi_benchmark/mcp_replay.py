"""Replay the queries an agent sent `trilogy serve --mcp` under several engine configs.

The input is an `--mcp-log` JSONL file. Each (config, session) runs in a fresh
subprocess, so worker pools and connections from one transport cannot flatter
the other; the uv cache is shared, as it is for a real server. Within a
session every pass runs the whole query list: pass 1 meets each script cold,
later passes warm.

    python local_scripts/vgi_benchmark/mcp_replay.py <model_dir> <mcp_log.jsonl> \\
        --config shell=<model_dir>/trilogy.toml --config vgi=<model_dir>/trilogy.vgi.toml \\
        --sessions 3 --passes 2 --out replay.jsonl
"""

from __future__ import annotations

import argparse
import contextlib
import json
import statistics
import subprocess
import sys
import tempfile
from collections import defaultdict
from pathlib import Path


def load_queries(log: Path) -> list[str]:
    records = [
        json.loads(line) for line in log.read_text(encoding="utf-8").splitlines()
    ]
    return [r["query"] for r in records if "error" not in r]


def run_session(
    model_dir: Path, config: Path, queries: list[str], passes: int
) -> list[dict]:
    from trilogy import Dialects
    from trilogy.scripts.common import get_runtime_config
    from trilogy.scripts.serve_helpers.mcp_server import ModelSession, QueryLog

    with tempfile.TemporaryDirectory() as tmp:
        log_path = Path(tmp) / "session.jsonl"
        session = ModelSession(
            model_dir,
            Dialects.DUCK_DB,
            get_runtime_config(model_dir, config),
            log=QueryLog(log_path),
        )
        for _ in range(passes):
            for query in queries:
                # A failed query is recorded in the log with its error.
                with contextlib.suppress(Exception):
                    session.run_query(query)
        if session._executor is not None:
            session._executor.close()
        records = [
            json.loads(x) for x in log_path.read_text(encoding="utf-8").splitlines()
        ]
    for i, record in enumerate(records):
        record["pass"] = i // len(queries) + 1
        record["query_index"] = i % len(queries)
    return records


def child(args: argparse.Namespace) -> None:
    queries = json.loads(Path(args.queries).read_text(encoding="utf-8"))
    records = run_session(
        Path(args.model_dir), Path(args.child_config), queries, args.passes
    )
    for record in records:
        record.update(config=args.child_name, session_index=args.session_index)
        print(json.dumps(record))


def summarize(records: list[dict]) -> None:
    by_key: dict[tuple[str, int], list[dict]] = defaultdict(list)
    for r in records:
        by_key[(r["config"], r["pass"])].append(r)
    print(
        f"{'config':8} {'pass':>4} {'queries':>7} {'errors':>6} {'median ms':>10} {'p90 ms':>8} {'session sum s':>14}"
    )
    for (config, pass_), rs in sorted(by_key.items()):
        ok = sorted(r["total_ms"] for r in rs if "error" not in r)
        sessions: dict[int, float] = defaultdict(float)
        for r in rs:
            sessions[r["session_index"]] += r["total_ms"]
        p90 = ok[max(0, int(len(ok) * 0.9) - 1)] if ok else 0
        print(
            f"{config:8} {pass_:>4} {len(rs):>7} {len(rs) - len(ok):>6} "
            f"{statistics.median(ok) if ok else 0:>10.0f} {p90:>8.0f} "
            f"{statistics.median(sessions.values()) / 1000:>14.1f}"
        )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("model_dir")
    parser.add_argument("log", nargs="?")
    parser.add_argument(
        "--config", action="append", default=[], help="name=path/to/trilogy.toml"
    )
    parser.add_argument("--sessions", type=int, default=3)
    parser.add_argument("--passes", type=int, default=2)
    parser.add_argument("--out", default="replay.jsonl")
    parser.add_argument("--queries")
    parser.add_argument("--child-name")
    parser.add_argument("--child-config")
    parser.add_argument("--session-index", type=int, default=0)
    args = parser.parse_args()
    if args.child_name:
        child(args)
        return

    queries = load_queries(Path(args.log))
    out = Path(args.out)
    out.unlink(missing_ok=True)
    with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False) as f:
        json.dump(queries, f)
    records: list[dict] = []
    for session_index in range(args.sessions):
        # Interleaved so drift in machine load hits every config alike.
        for spec in args.config:
            name, config = spec.split("=", 1)
            result = subprocess.run(
                [
                    sys.executable,
                    __file__,
                    args.model_dir,
                    "--queries",
                    f.name,
                    "--child-name",
                    name,
                    "--child-config",
                    config,
                    "--session-index",
                    str(session_index),
                    "--passes",
                    str(args.passes),
                ],
                capture_output=True,
                text=True,
                check=True,
            )
            lines = [
                json.loads(x) for x in result.stdout.splitlines() if x.startswith("{")
            ]
            records.extend(lines)
            with out.open("a", encoding="utf-8") as handle:
                for line in lines:
                    handle.write(json.dumps(line) + "\n")
            print(
                f"session {session_index} {name}: {len(lines)} queries", file=sys.stderr
            )
    Path(f.name).unlink(missing_ok=True)
    summarize(records)


if __name__ == "__main__":
    main()
