import sys
from pathlib import Path
sys.path.insert(0, 'evals')
from common import scoring

ws = Path('evals/tpcds_agent/results/20260709-020529_enriched/workspace')
eng = scoring.make_scoring_engine(ws / 'tpcds.duckdb', ws, 'tpcds')

base = """
import raw.all_sales as s;
import raw.date as d;

rowset all_months <- where d.year = 2001
  select d.month_of_year as month, 1 as join_key;

rowset wh_groups <- where s.channel in ('WEB','CATALOG') and s.date.year = 2001 and s.warehouse.id is not null
  select s.warehouse.id as wh_id, s.warehouse.square_feet as w_sqft, 1 as join_key;

rowset sales_agg <- where s.date.year = 2001
  select s.warehouse.id as wh_id, sum(s.sales_price) as monthly_sales, 1 as join_key;
"""

cases = {
    "raw cols 3rs": "select wh_groups.w_sqft, all_months.month, sales_agg.monthly_sales JOIN wh_groups.join_key = all_months.join_key JOIN sales_agg.join_key = all_months.join_key;",
    "derived measure/dim cross-rowset": "select sales_agg.monthly_sales / wh_groups.w_sqft as r, all_months.month JOIN wh_groups.join_key = all_months.join_key JOIN sales_agg.join_key = all_months.join_key;",
    "derived measure + dim": "select sales_agg.monthly_sales + all_months.month as r JOIN wh_groups.join_key = all_months.join_key JOIN sales_agg.join_key = all_months.join_key;",
    "w_sqft*2 alongside": "select wh_groups.w_sqft * 2 as r, all_months.month, sales_agg.monthly_sales JOIN wh_groups.join_key = all_months.join_key JOIN sales_agg.join_key = all_months.join_key;",
}

for jt in ["full join", "left join", "subset join"]:
    print(f"\n########## JOIN TYPE: {jt} ##########")
    for name, tail in cases.items():
        body = base + "\n" + tail.replace("JOIN", jt)
        try:
            eng.generate_sql(body)
            print(f"  OK    {name}")
        except Exception as e:
            print(f"  FAIL  {name}: {type(e).__name__}: {str(e)[:160]}")
