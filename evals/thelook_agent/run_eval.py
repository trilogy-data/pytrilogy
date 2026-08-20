#!/usr/bin/env python
"""Run the thelook eval and attach its partial-bridge recovery metrics."""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from common.main import run
from error_recovery import enrich_changed_reports, report_mtimes
from spec import SPEC


def main() -> int:
    before = report_mtimes(SPEC.results_dir)
    exit_code = run(SPEC)
    enrich_changed_reports(SPEC.results_dir, before)
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
