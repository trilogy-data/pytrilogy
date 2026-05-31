from trilogy import Environment, Dialects
from pathlib import Path

src = Path('evals/tpcds_agent/_repro_rollup.preql').read_text()
env = Environment()
_env, stmts = env.parse(src)
for name in ['total','rnk','g1','g2']:
    c = env.concepts[name]
    print(f"== {name} ==")
    print("  grain:", c.grain)
    print("  keys:", getattr(c,'keys',None))
    print("  lineage:", type(c.lineage).__name__, c.lineage)
