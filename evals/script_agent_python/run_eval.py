#!/usr/bin/env python

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from common.main import run
from spec import SPEC

if __name__ == "__main__":
    raise SystemExit(run(SPEC))
