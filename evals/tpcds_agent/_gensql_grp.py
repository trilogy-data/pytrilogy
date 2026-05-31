from trilogy import Environment, Dialects
from pathlib import Path
src = Path('evals/tpcds_agent/_repro_grouping.preql').read_text()
env = Environment()
exec = Dialects.DUCK_DB.default_executor(environment=env)
print(exec.generate_sql(src)[-1])
