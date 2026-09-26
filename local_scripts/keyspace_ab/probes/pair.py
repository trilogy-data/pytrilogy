"""Generic rows-oracle pair: derived vs materialized for each argv query."""

import importlib.util
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[3]

sys.path.insert(0, str(REPO))

spec = importlib.util.spec_from_file_location(
    "tdk",
    str(REPO / "tests" / "engine" / "test_derived_key_domain.py"),
)
tdk = importlib.util.module_from_spec(spec)
spec.loader.exec_module(tdk)

derived = tdk._executor(tdk._DERIVED)
materialized = tdk._executor(tdk._MATERIALIZED)


def run(executor, query):
    try:
        return tdk._rows(executor, query)
    except Exception as e:
        return f"ERROR {type(e).__name__}: {str(e)[:160]}"


for q in sys.argv[1:]:
    d, m = run(derived, q), run(materialized, q)
    print(("SAME" if d == m else "DIFF"), q)
    print("    derived:     ", d)
    if d != m:
        print("    materialized:", m)
