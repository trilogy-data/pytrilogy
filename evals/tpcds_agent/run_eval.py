#!/usr/bin/env python
"""TPC-DS per-query agent eval. See ``evals/common/main.py`` for everything."""

import sys
from pathlib import Path

# Make the sibling `common` package importable when invoked as a script.
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from common.main import run
from spec import SPEC

if __name__ == "__main__":
    raise SystemExit(run(SPEC))
