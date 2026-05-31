from trilogy import Environment
from trilogy.core.models.build import BuildGrain
from pathlib import Path

src = Path('evals/tpcds_agent/_repro_rollup.preql').read_text()
env = Environment()
benv, stmts = env.parse(src)
# build environment
from trilogy.core.models.environment import Environment as Env
build_env = env.materialize_for_select()
g1 = build_env.concepts['local.g1']
g2 = build_env.concepts['local.g2']
total = build_env.concepts['local.total']
rnk = build_env.concepts['local.rnk']
print("grain([g1,total,rnk]):", BuildGrain.from_concepts([g1,total,rnk], environment=build_env))
print("grain([g1,total]):", BuildGrain.from_concepts([g1,total], environment=build_env))
print("grain([total]):", BuildGrain.from_concepts([total], environment=build_env))
print("grain([g1,g2,total,rnk]):", BuildGrain.from_concepts([g1,g2,total,rnk], environment=build_env))
print("total.grain:", total.grain, "keys:", total.keys)
print("rnk.grain:", rnk.grain, "keys:", rnk.keys)
