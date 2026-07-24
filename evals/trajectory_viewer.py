#!/usr/bin/env python
"""Agent trajectory viewer across all eval suites (TPC-DS, TPC-H, ...).

    python trajectory_viewer.py [results_dir] [--eval KEY] [--serve PORT]

Thin entry point — the implementation lives in ``evals/viewer/``.
"""

import sys
from pathlib import Path

# Make the sibling `common` and `viewer` packages importable when run as a script.
sys.path.insert(0, str(Path(__file__).resolve().parent))

from viewer.cli import main

if __name__ == "__main__":
    raise SystemExit(main())
