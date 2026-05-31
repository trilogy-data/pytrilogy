import sys; sys.path.insert(0, 'evals')
from common import scoring
from pathlib import Path
ws = Path('evals/tpcds_agent/results/20260531-035151_enriched/workspace')
eng = scoring.make_scoring_engine(ws/'tpcds.duckdb', ws, 'tpcds')
gen = (ws/'query70.preql').read_text()
sql = eng.generate_sql(gen)[-1]
rows = list(eng.execute_raw_sql(sql).fetchall())
print("row count:", len(rows))
null_total = sum(1 for r in rows if r[0] is None)
print("rows with NULL total:", null_total)
for r in rows[:15]:
    print(r)
