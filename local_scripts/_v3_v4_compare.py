"""Compare v3 vs v4 SQL+results for an arbitrary preql (local test harness)."""
import sys, re
from pathlib import Path
sys.path.insert(0, str(Path("local_scripts").resolve()))
from trilogy import Dialects
from trilogy.core.models.environment import Environment
import discovery_v4 as dv, discovery_v4_compare as cmp

WORKING = dv.TPCDS_DIR

def runsql(sql):
    for k,v in {":period_start":"DATE '2000-08-23'",":period_end":"DATE '2000-09-22'"}.items():
        sql=sql.replace(k,v)
    sql=re.sub(r":\w+","'x'",sql)
    return cmp.get_duckdb_connection().execute(sql).fetchall()

def gen_v3(preql):
    env = Environment(working_path=WORKING)
    eng = Dialects.DUCK_DB.default_executor(environment=env)
    return eng.generate_sql(preql)[-1]

def gen_v4(preql):
    env = Environment(working_path=WORKING)
    _, queries = env.parse(preql)
    select = dv._find_select(queries)
    hist = dv.V4History(base_environment=env)
    bstmt, benv, conds = dv._materialize_for_query(env, select, hist)
    from trilogy.core.processing.concept_strategies_v4 import search_concepts
    from trilogy.core.env_processor import generate_graph
    info = search_concepts(mandatory_list=list(bstmt.output_components), history=hist,
                           environment=benv, depth=0, g=generate_graph(benv),
                           conditions=[conds] if conds else [])
    return dv.compile_sql(info, benv, bstmt)

CATALOG_ARM = """
import catalog_sales as cs;
import catalog_returns as cr;
const period_start <- '2000-08-23'::date;
const period_end <- '2000-09-22'::date;
rowset cr_grouped <- where cr.date.date between period_start and period_end
select coalesce(cr.call_center.id, -1) as cr_cc_key, sum(cr.return_amount) as cr_returns_per_cc;
rowset cr_totals <- select count(cr_grouped.cr_cc_key) as cr_n_groups, sum(cr_grouped.cr_returns_per_cc) as cr_total_returns;
where cs.date.date between period_start and period_end
select 'catalog channel' as u_channel_c, cs.call_center.id as u_id_c,
  sum(cs.ext_sales_price)*cr_totals.cr_n_groups::numeric(15,2) as u_sales_c,
  cr_totals.cr_total_returns::numeric(15,2) as u_returns_c,
  sum(cs.net_profit)*cr_totals.cr_n_groups - cr_totals.cr_total_returns::numeric(15,2) as u_profit_c
order by u_id_c asc nulls first;
"""
for name, gen in [("v3", gen_v3), ("v4", gen_v4)]:
    try:
        sql = gen(CATALOG_ARM)
        rows = runsql(sql)
        print(f"=== {name}: {len(rows)} rows, 1=1={sql.count('on 1=1')}")
        for r in rows[:6]: print("   ", r)
    except Exception as e:
        import traceback; print(f"=== {name} ERR:", str(e).splitlines()[-1][:160])

print("\n##### V3 SQL #####")
print(gen_v3(CATALOG_ARM))
