#!/usr/bin/env python
"""TPC-DS per-query agent eval. See ``evals/common/main.py`` for everything."""

import sys
from pathlib import Path

# Make the sibling `common` package importable when invoked as a script.
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from common.main import run  # noqa: E402
from spec import SPEC  # noqa: E402

if __name__ == "__main__":
    raise SystemExit(run(SPEC))
