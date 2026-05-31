from trilogy import Environment, Executor, Dialects
from pathlib import Path

src = Path('evals/tpcds_agent/_repro_rollup.preql').read_text()
env = Environment()
exec = Dialects.DUCK_DB.default_executor(environment=env)
sqls = exec.generate_sql(src)
for s in sqls:
    print(s)
