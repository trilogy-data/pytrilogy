"""Print the `[v4] built grp:` bucket lines (and any failure lines) for argv
queries on UNSOLD_MODEL (--guest: GUEST_ALLDESC_MODEL, --two: two-prop
guest), then the rows or the error."""

import importlib.util
import logging
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[3]

sys.path.insert(0, str(REPO))
from trilogy import Dialects, Environment

spec = importlib.util.spec_from_file_location(
    "t",
    str(REPO / "tests" / "engine" / "test_duckdb_rowset_null_group_rejoin.py"),
)
t = importlib.util.module_from_spec(spec)
spec.loader.exec_module(t)

model = t.UNSOLD_MODEL
if "--guest" in sys.argv:
    model = t.GUEST_ALLDESC_MODEL
if "--two" in sys.argv:
    model = t.TWO_PROP_GUEST_ALLDESC_MODEL

KEEP = ("built grp", "fail", "skip", "could not", "FINAL", "contributor")


class _Filter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        msg = record.getMessage()
        return "--all" in sys.argv or any(k in msg for k in KEEP)


handler = logging.StreamHandler(sys.stdout)
handler.addFilter(_Filter())
handler.setFormatter(logging.Formatter("%(message)s"))
log = logging.getLogger("trilogy")
log.addHandler(handler)
log.setLevel(logging.INFO)

env = Environment()
env.parse(model)
ex = Dialects.DUCK_DB.default_executor(environment=env)
for q in [x for x in sys.argv[1:] if not x.startswith("--")]:
    print(f"\n=== {q}")
    try:
        print("  rows:", ex.execute_query(q).fetchall())
    except Exception as e:
        print("  ERROR", type(e).__name__, str(e)[:300])
