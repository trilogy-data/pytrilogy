import sys
from pathlib import Path
sys.path.insert(0, 'evals')
from common import scoring

ws = Path('evals/tpcds_agent/results/20260709-020529_enriched/workspace')
eng = scoring.make_scoring_engine(ws / 'tpcds.duckdb', ws, 'tpcds')

works = """
import raw.all_sales as s;
import raw.date as d;

rowset all_months <- where d.year = 2001
  select d.month_of_year as month, 1 as join_key;

rowset wh_groups <- where s.channel in ('WEB','CATALOG') and s.date.year = 2001 and s.warehouse.id is not null
  select s.warehouse.id as wh_id, s.warehouse.square_feet as w_sqft, 1 as join_key;

select wh_groups.w_sqft, all_months.month
inner join wh_groups.join_key = all_months.join_key;
"""

fails = """
import raw.all_sales as s;
import raw.date as d;

rowset all_months <- where d.year = 2001
  select d.month_of_year as month, 1 as join_key;

rowset wh_groups <- where s.channel in ('WEB','CATALOG') and s.date.year = 2001 and s.warehouse.id is not null
  select s.warehouse.id as wh_id, s.warehouse.square_feet as w_sqft, 1 as join_key;

select wh_groups.w_sqft * 2 as r, all_months.month
inner join wh_groups.join_key = all_months.join_key;
"""

print("=== WORKS case ===")
try:
    eng.generate_sql(works)
    print("OK - generated sql")
except Exception as e:
    print("ERROR:", type(e).__name__, str(e)[:300])

print("=== FAILS case ===")
try:
    eng.generate_sql(fails)
    print("OK - generated sql")
except Exception as e:
    print("ERROR:", type(e).__name__, str(e)[:300])
