import sys
from pathlib import Path

from trilogy import Dialects
from trilogy.constants import CONFIG
from trilogy.core.models.environment import Environment

wp = Path("tests/modeling/tpc_ds_duckdb")
text = (wp / "query68.preql").read_text()


def gen(v4: bool):
    CONFIG.use_v4_discovery = v4
    engine = Dialects.DUCK_DB.default_executor(environment=Environment(working_path=wp))
    return engine.generate_sql(text)[-1]


def run(v4: bool):
    CONFIG.use_v4_discovery = v4
    engine = Dialects.DUCK_DB.default_executor(environment=Environment(working_path=wp))
    engine.execute_raw_sql(f"IMPORT DATABASE '{wp / 'memory'}';")
    engine.execute_raw_sql("SET enable_progress_bar=false;")
    rows = engine.execute_raw_sql("PRAGMA tpcds(68);").fetchall()
    comp = engine.execute_text(text)[-1].fetchall()
    return [tuple(r) for r in rows], [tuple(r) for r in comp]


mode = sys.argv[1] if len(sys.argv) > 1 else "both"
if mode == "rows":
    ref, comp = run(True)
    print("REF count", len(ref), "V4 count", len(comp))
    for i, (a, b) in enumerate(zip(ref, comp)):
        if a != b:
            print(f"  DIFF row {i}: ref={a}  v4={b}")
            if i > 15:
                break
elif mode in ("v3", "v4"):
    print(gen(mode == "v4"))
else:
    print("===== V3 =====")
    print(gen(False))
    print("\n===== V4 =====")
    print(gen(True))
