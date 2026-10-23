"""Generate the TPC-DS / TPC-H parquet fixtures with the installed duckdb.

dsdgen's output differs between duckdb majors (2.0 draws different
store_returns rows), and several modeling tests pin results to the 1.x data.
CI cells testing a newer duckdb run this under 1.x first; the conftest
helpers then find the parquet in place and skip regeneration.

    python .scripts/generate_tpc_fixtures.py
"""

import sys
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
ROOT = REPO / "tests" / "modeling"

if __name__ == "__main__":
    sys.path.insert(0, str(REPO))
    from tests.modeling.tpc_ds_duckdb import conftest as tpcds
    from tests.modeling.tpc_h import conftest as tpch

    for subdir, sf in (("memory", 1), ("memory_sf01", 0.1), ("memory_sf001", 0.01)):
        tpcds._ensure_dataset(ROOT / "tpc_ds_duckdb" / subdir, sf)
    for subdir, sf in (("memory", 0.1), ("memory_sf001", 0.01)):
        tpch._ensure_dataset(ROOT / "tpc_h" / subdir, sf)
