from trilogy import Environment, Dialects
from pathlib import Path

src = Path('evals/tpcds_agent/_repro_rollup.preql').read_text()
env = Environment()
exec = Dialects.DUCK_DB.default_executor(environment=env)
# get the processed query / build to inspect node structure
from trilogy.parsing.parse_engine import parse_text
_, statements = env.parse(src)
print("parsed ok")
