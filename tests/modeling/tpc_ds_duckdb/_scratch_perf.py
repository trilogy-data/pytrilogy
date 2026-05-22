import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parents[3]))

from trilogy import Dialects  # noqa: E402
from trilogy.core.models.environment import Environment  # noqa: E402

working_path = Path(__file__).parent
engine = Dialects.DUCK_DB.default_executor(
    environment=Environment(working_path=working_path)
)
engine.execute_raw_sql(f"IMPORT DATABASE '{working_path / 'memory'}';")


def measure(preql_file):
    engine.environment = Environment(working_path=working_path)
    text = (working_path / preql_file).read_text()
    sql = engine.generate_sql(text)[-1]
    rows = 0
    best = 1e9
    for _ in range(6):
        start = time.perf_counter()
        rows = len(list(engine.execute_raw_sql(sql).fetchall()))
        best = min(best, time.perf_counter() - start)
    return best, rows, len(sql)


for idx in ["04", "11", "14", "32", "58", "83", "92"]:
    orig = measure(f"query{idx}.preql")
    tw = measure(f"query{idx}-then-where.preql")
    print(
        f"q{idx}: orig {orig[0]*1000:7.1f}ms ({orig[1]} rows, {orig[2]}b) | "
        f"then-where {tw[0]*1000:7.1f}ms ({tw[1]} rows, {tw[2]}b)"
    )
