#!/usr/bin/env python
"""TPC-DS post-run analysis shim. See ``evals/common/analyze_run.py``."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from common.analyze_run import run_main  # noqa: E402
from spec import SPEC  # noqa: E402

if __name__ == "__main__":
    raise SystemExit(run_main(SPEC.results_dir, SPEC.charts_dir))
