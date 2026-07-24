#!/usr/bin/env python
"""DABstep per-query agent eval. See ``evals/common/main.py`` for everything."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from common.main import run
from spec import SPEC

if __name__ == "__main__":
    raise SystemExit(run(SPEC))
