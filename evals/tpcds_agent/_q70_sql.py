import sys; sys.path.insert(0, 'evals')
from common import scoring
from pathlib import Path
ws = Path('evals/tpcds_agent/results/20260531-035151_enriched/workspace')
eng = scoring.make_scoring_engine(ws/'tpcds.duckdb', ws, 'tpcds')
sql = eng.generate_sql((ws/'query70.preql').read_text())[-1]
import re
# count rollup CTEs and joins
print("ROLLUP occurrences:", sql.upper().count("ROLLUP"))
print("INNER JOIN / FULL JOIN occurrences:", sql.upper().count(" JOIN "))
