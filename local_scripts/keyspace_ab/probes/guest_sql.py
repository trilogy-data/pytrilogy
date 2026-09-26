"""Rows + SQL for argv queries on the GUEST / GUEST_ALLDESC models (`--alldesc`)."""

import importlib.util
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[3]

import trilogy
from trilogy import Dialects, Environment

print("trilogy from", trilogy.__file__)
spec = importlib.util.spec_from_file_location(
    "t",
    str(REPO / "tests" / "engine" / "test_duckdb_rowset_null_group_rejoin.py"),
)
t = importlib.util.module_from_spec(spec)
spec.loader.exec_module(t)

model = t.UNSOLD_MODEL.replace("i_sk: ~item_sk", "i_sk: ~?item_sk").replace(
    "union all select 4, 30, 3'''",
    "union all select 4, 30, 3 union all select 5, null, 7'''",
)
if "--alldesc" in sys.argv:
    model = model.replace("cast(null as varchar)", "'delta'")
env = Environment()
env.parse(model)
ex = Dialects.DUCK_DB.default_executor(environment=env)
for q in [a for a in sys.argv[1:] if not a.startswith("--")]:
    try:
        print(ex.execute_query(q).fetchall(), "<=", q)
    except Exception as e:
        print("ERROR", type(e).__name__, str(e)[:200], "<=", q)
    if "--sql" in sys.argv:
        print(ex.generate_sql(q)[-1])
