"""Eval-suite discovery.

Any ``evals/<dir>/spec.py`` that exports a ``SPEC`` BenchmarkSpec is a viewable
suite — adding a new benchmark needs no viewer change. Suites are keyed by
``spec.short_name`` ('tpcds', 'tpch', ...).
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from common.siblings import load_sibling
from common.spec import BenchmarkSpec

EVALS_ROOT = Path(__file__).resolve().parents[1]


@dataclass(frozen=True)
class Suite:
    key: str
    spec: BenchmarkSpec

    @property
    def label(self) -> str:
        return self.spec.name

    @property
    def results_dir(self) -> Path:
        return self.spec.results_dir


def _load_spec(spec_file: Path) -> BenchmarkSpec | None:
    try:
        module = load_sibling(spec_file, "spec")
    except Exception as exc:
        # Silence here used to drop a whole benchmark from the picker with no
        # trace of why.
        print(f"viewer: skipping {spec_file} ({type(exc).__name__}: {exc})")
        return None
    found = module.__dict__.get("SPEC")
    return found if isinstance(found, BenchmarkSpec) else None


def discover_suites() -> dict[str, Suite]:
    suites: dict[str, Suite] = {}
    for spec_file in sorted(EVALS_ROOT.glob("*/spec.py")):
        if spec_file.parent.name == "common":
            continue
        spec = _load_spec(spec_file)
        if spec is not None:
            suites[spec.short_name] = Suite(key=spec.short_name, spec=spec)
    return suites


def list_run_dirs(suite: Suite) -> list[str]:
    """Run-dir names under the suite's results dir, newest first."""
    root = suite.results_dir
    if not root.is_dir():
        return []
    dirs = [
        c for c in root.iterdir() if c.is_dir() and any(c.glob("agent_log.*.jsonl"))
    ]
    dirs.sort(key=lambda p: p.stat().st_mtime, reverse=True)
    return [p.name for p in dirs]


def find_suite_for(suites: dict[str, Suite], results_dir: Path) -> Suite | None:
    """The suite whose eval dir contains ``results_dir``."""
    resolved = results_dir.resolve()
    for suite in suites.values():
        if suite.spec.eval_dir in resolved.parents:
            return suite
    return None


def latest_run_dir(suites: dict[str, Suite]) -> tuple[Suite, Path] | None:
    """(suite, run dir) of the most recently modified agent log across all suites."""
    best: tuple[float, Suite, Path] | None = None
    for suite in suites.values():
        for log in suite.results_dir.glob("*/agent_log.*.jsonl"):
            mtime = log.stat().st_mtime
            if best is None or mtime > best[0]:
                best = (mtime, suite, log.parent)
    return (best[1], best[2]) if best else None
