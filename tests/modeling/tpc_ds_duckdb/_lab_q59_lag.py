"""q59 spike: lag with normalized_week as partition key.

For year1 rows (month_seq 1212-1223): normalized_week = week_seq
For year2 rows (month_seq 1224-1235): normalized_week = week_seq - 52
So matched year1/year2 weeks share normalized_week and can be aligned via
lead-1 over (partition by store_id, normalized_week order by in_year2).

This produces CORRECT math (verified via row-by-row diff against reference SQL —
ratios match; the only test failure was an extra `year1_flag` output column
required to prevent trilogy from pushing the filter past the window function).

Two generator issues are visible in the generated SQL:
1. Each of the 7 lead() columns gets its own CTE LEFT JOIN'd back to the wss-
   equivalent (concerned/young/sparkling/abhorrent/sweltering/late/macho chain).
   The 7 leads all share the same OVER clause and could fuse into a single
   projection — see `concerned` CTE which already has all 7 lead expressions.
   What follows is concept-by-concept threading.
2. WHERE/HAVING filters get pushed into source CTEs even when a window function
   sits between them; removing year1_flag from SELECT causes WHERE in_year1=1
   to be inferred and inserted into the source `vacuous`, which empties out the
   year2 rows the lead needs to read.
"""

from pathlib import Path

from trilogy import Dialects
from trilogy.core.models.environment import Environment

WORKING = Path(__file__).parent


def gen(preql: str) -> str:
    env = Environment(working_path=WORKING)
    engine = Dialects.DUCK_DB.default_executor(environment=env)
    return engine.generate_sql(preql)[-1]


VARIANT = """
import store_sales as ss;

def day_sales(d) -> sum(ss.sales_price ? ss.date.day_name = d) by ss.store.sk, ss.date.week_seq;

with wss as
where ss.date.month_seq between 1212 and 1235 and ss.store.sk is not null and ss.date.week_seq is not null
SELECT
    ss.store.sk as store_id,
    ss.store.name as store_name,
    ss.store.id as store_text_id,
    ss.date.week_seq as week_seq,
    @day_sales('Sunday')    as sun_sales,
    @day_sales('Monday')    as mon_sales,
    @day_sales('Tuesday')   as tue_sales,
    @day_sales('Wednesday') as wed_sales,
    @day_sales('Thursday')  as thu_sales,
    @day_sales('Friday')    as fri_sales,
    @day_sales('Saturday')  as sat_sales,
    max(case when ss.date.month_seq >= 1212 and ss.date.month_seq <= 1223 then 1 else 0 end) as in_year1,
    max(case when ss.date.month_seq >= 1224 and ss.date.month_seq <= 1235 then 1 else 0 end) as in_year2,
;

# Normalized week-within-year so year1's W lines up with year2's W+52
auto normalized_week <- wss.week_seq - (case when wss.in_year2 = 1 then 52 else 0 end);

SELECT
    wss.store_name as s_store_name1,
    wss.store_text_id as s_store_id1,
    wss.week_seq as d_week_seq1,
    wss.in_year1 as year1_flag,
    wss.sun_sales / lead(wss.sun_sales, 1) over (partition by wss.store_id, normalized_week order by wss.in_year2 asc) as sun_sales_ratio,
    wss.mon_sales / lead(wss.mon_sales, 1) over (partition by wss.store_id, normalized_week order by wss.in_year2 asc) as mon_sales_ratio,
    wss.tue_sales / lead(wss.tue_sales, 1) over (partition by wss.store_id, normalized_week order by wss.in_year2 asc) as tue_sales_ratio,
    wss.wed_sales / lead(wss.wed_sales, 1) over (partition by wss.store_id, normalized_week order by wss.in_year2 asc) as wed_sales_ratio,
    wss.thu_sales / lead(wss.thu_sales, 1) over (partition by wss.store_id, normalized_week order by wss.in_year2 asc) as thu_sales_ratio,
    wss.fri_sales / lead(wss.fri_sales, 1) over (partition by wss.store_id, normalized_week order by wss.in_year2 asc) as fri_sales_ratio,
    wss.sat_sales / lead(wss.sat_sales, 1) over (partition by wss.store_id, normalized_week order by wss.in_year2 asc) as sat_sales_ratio,
HAVING year1_flag = 1 AND sun_sales_ratio is not null
ORDER BY
    s_store_name1 asc nulls first,
    s_store_id1 asc nulls first,
    d_week_seq1 asc nulls first
LIMIT 100;
"""

if __name__ == "__main__":
    try:
        sql = gen(VARIANT)
        print(sql)
        print()
        print(f"--- LENGTH: {len(sql)} chars ---")
    except Exception as e:
        import traceback

        traceback.print_exc()
        print(f"FAILED: {type(e).__name__}: {e}")
