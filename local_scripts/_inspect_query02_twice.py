import sys
sys.path.insert(0, "local_scripts")
from discovery_v4 import compile_sql, run_tpcds_query

for i in range(3):
    info, benv, _, stmt = run_tpcds_query("02")
    sql = compile_sql(info, benv, stmt)
    print(f"=== run {i} ===")
    print(f"len: {len(sql)}  has channel filter: {'WEB' in sql}")
    print(f"has year filter: {'2001' in sql}")
    print(f"CTE names: {[line.strip() for line in sql.split(chr(10)) if ' as (' in line][:5]}")
