from trilogy import Environment
from pathlib import Path
src = Path('evals/tpcds_agent/_repro_rollup.preql').read_text()
env = Environment()
env.parse(src)
build_env = env.materialize_for_select()
for n in ['g1','g2','total','rnk']:
    c = build_env.concepts['local.'+n]
    print(n, "| purpose:", c.purpose, "| derivation:", c.derivation, "| keys:", getattr(c,'keys',None), "| grain:", c.grain)
