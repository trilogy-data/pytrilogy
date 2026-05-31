"""Subprocess plumbing for the ``trilogy agent`` per-query runs.

Shared across all benchmarks. Knows nothing about TPC-DS/TPC-H specifics —
benchmark-specific paths/filenames come in through ``BenchmarkSpec``.
"""

from __future__ import annotations

import re
import shutil
import subprocess
import sys
import threading
import time
from pathlib import Path

from . import monitor
from .spec import BenchmarkSpec

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
    worker_raw = worker_dir / "raw"
    if worker_raw.exists():
        shutil.rmtree(worker_raw)
    shutil.copytree(src / "raw", worker_raw)
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
# When false, the model may reason in plain text before calling a tool
# (tool_choice: auto) instead of being forced to act every turn.
force_tool_choice = {str(force_tool_choice).lower()}
""",
        encoding="utf-8",
    )
