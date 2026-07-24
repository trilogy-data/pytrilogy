"""Subprocess plumbing for the ``trilogy agent`` per-query runs.

Shared across all benchmarks. Knows nothing about TPC-DS/TPC-H specifics —
benchmark-specific paths/filenames come in through ``BenchmarkSpec``.
"""

from __future__ import annotations

import argparse
import os
import re
import shutil
import subprocess
import sys
import threading
import time
from pathlib import Path

from . import monitor
from .spec import BenchmarkSpec


def add_scope_flags(parser: argparse.ArgumentParser) -> None:
    """Register the shared agent scope-surfacing flags on an eval argparser.

    Defaults mirror the CLI: the full scope block is off, distilled warnings on.
    """
    parser.add_argument(
        "--scope-diagnostics",
        action=argparse.BooleanOptionalAction,
        default=False,
        help="expose the full derived-value scope block (agg_window_rows_used) "
        "to the agent; off by default",
    )
    parser.add_argument(
        "--scope-warnings",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="expose distilled scope warnings to the agent; pass "
        "--no-scope-warnings for a clean baseline",
    )


def add_force_tool_choice_flag(parser: argparse.ArgumentParser) -> None:
    """Register the shared --force-tool-choice A/B flag on an eval argparser."""
    parser.add_argument(
        "--force-tool-choice",
        action="store_true",
        help="force tool_choice=required every turn (no plain-text reasoning). "
        "Default is tool_choice: auto, which lets the model deliberate before "
        "acting; pass this to A/B the old forced-tool behavior.",
    )


HEARTBEAT_INTERVAL = 30.0
POLL_INTERVAL = 0.3


def _pump_output(stream, sink: list[str], echo: bool) -> None:
    """Drain the subprocess output stream into ``sink``, optionally echoing it
    to the console as it arrives."""
    for line in stream:
        sink.append(line)
        if echo:
            sys.stdout.write(line)
            sys.stdout.flush()
    stream.close()


def run_agent(
    workspace: Path,
    log_path: Path,
    provider: str,
    model: str,
    task: str,
    timeout: int,
    monitor_mode: str,
    toolset: str = "trilogy",
    scope_diagnostics: bool = False,
    scope_warnings: bool = True,
) -> dict:
    cmd = [
        sys.executable,
        "-m",
        "trilogy.scripts.trilogy",
        "agent",
        "--provider",
        provider,
        "--model",
        model,
        "--toolset",
        toolset,
        "--log-file",
        str(log_path),
        task,
    ]
    start = time.perf_counter()
    proc = subprocess.Popen(
        cmd,
        cwd=workspace,
        env={
            **os.environ,
            "TRILOGY_AGENT_SCOPE_DIAGNOSTICS": "1" if scope_diagnostics else "0",
            "TRILOGY_AGENT_SCOPE_WARNINGS": "1" if scope_warnings else "0",
        },
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        encoding="utf-8",
        errors="replace",
        bufsize=1,
    )
    out_lines: list[str] = []
    pump = threading.Thread(
        target=_pump_output,
        args=(proc.stdout, out_lines, monitor_mode in ("raw", "both")),
        daemon=True,
    )
    pump.start()

    track = monitor_mode in ("feed", "both")
    state = monitor.ProgressState(start=start)
    processed = 0
    last_beat = start
    timed_out = False

    while proc.poll() is None:
        if track:
            processed = monitor.drain_feed(
                log_path, processed, state, emit=monitor_mode == "feed"
            )
        if monitor_mode == "both" and time.perf_counter() - last_beat >= (
            HEARTBEAT_INTERVAL
        ):
            print(monitor.heartbeat(state), flush=True)
            last_beat = time.perf_counter()
        if time.perf_counter() - start > timeout:
            timed_out = True
            proc.kill()
            break
        time.sleep(POLL_INTERVAL)

    proc.wait()
    pump.join(timeout=5)
    if track:
        monitor.drain_feed(log_path, processed, state, emit=monitor_mode == "feed")
        print(monitor.heartbeat(state), flush=True)

    output = "".join(out_lines)
    if timed_out:
        output += f"\n[eval] agent timed out after {timeout}s\n"
    return {
        "exit_code": -1 if timed_out else proc.returncode,
        "timed_out": timed_out,
        "duration": time.perf_counter() - start,
        "output": output,
    }


def prepare_worker_workspace(src: Path, worker_idx: int, db_filename: str) -> Path:
    """Materialise a self-contained per-worker copy of the eval workspace.

    DuckDB takes an exclusive file lock on open; parallel agents pointing at the
    same database would serialise. Each worker gets its own copy of the duckdb,
    its own ``raw/``, and its own ``trilogy.toml``."""
    worker_dir = src / f"_worker_{worker_idx}"
    worker_dir.mkdir(exist_ok=True)
    shutil.copy2(src / db_filename, worker_dir / db_filename)
    shutil.copy2(src / "trilogy.toml", worker_dir / "trilogy.toml")
    # raw/ (Trilogy categories) is copied only when present — SQL baselines
    # have none. Top-level *.md covers schema.md (sql_schema) plus any
    # spec.doc_files installed in the parent workspace (DABstep's manual.md).
    if (src / "raw").exists():
        worker_raw = worker_dir / "raw"
        if worker_raw.exists():
            shutil.rmtree(worker_raw)
        shutil.copytree(src / "raw", worker_raw)
    for md in src.glob("*.md"):
        shutil.copy2(md, worker_dir / md.name)
    return worker_dir


_ADDRESS_DB_PREFIX = re.compile(r"^(address\s+)([A-Za-z_]\w*)\.", re.MULTILINE)


def install_enriched_model(
    workspace: Path, src_dir: Path, skip_prefixes: tuple[str, ...]
) -> dict:
    """Skip ingest and seed ``workspace/raw/`` from a hand-curated model directory.

    Used by the enriched-eval variant to measure how much a richer semantic layer
    (concept descriptions, derived metrics, named import aliases) lifts the
    agent above what bare ``trilogy ingest --all`` produces. Files whose name
    starts with any of ``skip_prefixes`` are excluded — typically
    ``query``/``adhoc`` (reference answers) and ``cache`` (build helpers).

    The source models were authored against an in-memory DuckDB (``address
    memory.<table>``), but the eval workspace's DuckDB file attaches as a
    different database name — so we strip the leading database qualifier on
    each ``address`` line as we copy. Tables resolve via DuckDB's default
    catalog search path, which is what we want for an arbitrary db file."""
    raw = workspace / "raw"
    if raw.exists():
        shutil.rmtree(raw)
    raw.mkdir(parents=True)
    start = time.perf_counter()
    copied: list[str] = []
    rewrites = 0
    for path in sorted(src_dir.glob("*.preql")):
        name = path.name
        if any(name.startswith(p) for p in skip_prefixes):
            continue
        text = path.read_text(encoding="utf-8")
        new_text, n = _ADDRESS_DB_PREFIX.subn(r"\1", text)
        rewrites += n
        (raw / name).write_text(new_text, encoding="utf-8")
        copied.append(name)
    return {
        "exit_code": 0,
        "duration": time.perf_counter() - start,
        "stdout": (
            f"copied {len(copied)} files from {src_dir} "
            f"(stripped db-qualifier on {rewrites} address lines): {copied}\n"
        ),
        "stderr": "",
    }


def run_pre_ingest(workspace: Path, timeout: int = 600) -> dict:
    """Run ``trilogy ingest --all`` once before any agent invocations.

    Per-query mode hands each agent a populated ``raw/``, so the agent doesn't
    burn iterations rebuilding the model (and we eliminate ingest variability
    as a confound)."""
    cmd = [
        sys.executable,
        "-m",
        "trilogy.scripts.trilogy",
        "ingest",
        "--all",
    ]
    start = time.perf_counter()
    proc = subprocess.run(
        cmd,
        cwd=workspace,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        timeout=timeout,
        check=False,
    )
    return {
        "exit_code": proc.returncode,
        "duration": time.perf_counter() - start,
        "stdout": proc.stdout,
        "stderr": proc.stderr,
    }


def write_trilogy_toml(
    workspace: Path,
    spec: BenchmarkSpec,
    provider: str,
    model: str,
    max_iterations: int,
    force_tool_choice: bool = False,
    allow_database_introspection: bool = True,
    disable_todo: bool = False,
    allow_file_read: bool = True,
) -> None:
    """Configure the agent subprocess: DuckDB pointing at the benchmark file,
    provider/model, and the per-query iteration budget. ``quiet = true`` drops
    the show_message tool — long unattended runs blow up otherwise."""
    # No silent fallback — an unknown provider here would inherit a wrong env
    # var and 401 against the actual API. Fail loud so the misconfiguration
    # surfaces at workspace-setup time, not 50 turns into a hung eval.
    api_key_env_map = {
        "openrouter": "OPENROUTER_API_KEY",
        "anthropic": "ANTHROPIC_API_KEY",
        "openai": "OPENAI_API_KEY",
        "google": "GOOGLE_API_KEY",
        "deepseek": "DEEPSEEK_API_KEY",
    }
    if provider not in api_key_env_map:
        raise ValueError(
            f"Unknown provider {provider!r} in eval harness — known: "
            f"{sorted(api_key_env_map)}. Add the api_key_env mapping in "
            "evals/common/agent_runner.py:write_trilogy_toml."
        )
    api_key_env = api_key_env_map[provider]
    workspace.joinpath("trilogy.toml").write_text(
        f"""\
[engine]
dialect = "duck_db"

[engine.config]
db_location = "{spec.db_filename}"

[agent]
provider = "{provider}"
model = "{model}"
api_key_env = "{api_key_env}"
max_iterations = {max_iterations}
# agent-info is ~26KB and carries the Trilogy language reference; the default
# 8KB limit middle-truncates the syntax rules away, so give it real headroom.
tool_output_limit = 32768
# Narration messages compound quadratically through history replays in long
# unattended runs; the eval drops show_message entirely.
quiet = true
# Drop the todo tool (and its prompt mention) — A/B knob for short single-query
# tasks where the scratch list tends to invite over-planning.
disable_todo = {str(disable_todo).lower()}
# When false, the model may reason in plain text before calling a tool
# (tool_choice: auto) instead of being forced to act every turn.
force_tool_choice = {str(force_tool_choice).lower()}
# When false, the trilogy tool refuses `database list/describe` and the prompt
# omits them — raw-table introspection is for ingest, not query generation.
allow_database_introspection = {str(allow_database_introspection).lower()}
# When false, `trilogy file read` is refused (gentle deny pointing at explore);
# `file list` still works. Schema discovery should go through `explore`.
allow_file_read = {str(allow_file_read).lower()}
# Disabled for now: A/B-ing whether the post-submit reviewer gate helps or just
# adds false kickbacks (e.g. q14 mis-quoted "can't see them due to the limit").
disable_reviewer = true
""",
        encoding="utf-8",
    )
