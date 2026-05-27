#!/usr/bin/env python
"""TPC-DS ingest-mode agent eval. See ``evals/common/ingest_main.py``."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from common.ingest_main import run  # noqa: E402
from spec import SPEC  # noqa: E402

if __name__ == "__main__":
    raise SystemExit(run(SPEC))
