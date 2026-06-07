from pathlib import Path
from trilogy import Dialects
from trilogy.core.models.environment import Environment
env = Environment(working_path=Path("tests/modeling/tpc_ds_duckdb"))
eng = Dialects.DUCK_DB.default_executor(environment=env)
print(eng.generate_sql(Path("repro.preql").read_text())[-1])
