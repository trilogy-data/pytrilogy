# Query 59

**Status:** `match`

| Stage | Result |
| --- | --- |
| v4 SQL generation | OK |
| v4 execution | OK (100 rows) |
| reference execution | OK (100 rows) |
| results identical | YES |

## Result comparison

v4 rows: 100 (100 distinct)
ref rows: 100 (100 distinct)

## SQL size + execution time

| Source | Chars | Lines | Exec (min of 4) |
| --- | --- | --- | --- |
| v4 | 6704 | 140 | 93.57 ms |
| reference | 6704 | 140 | 97.88 ms |
| v4 / ref | 1.00x | 1.00x | 0.96x |

## Preql

```
import physical_sales as ss;

def day_sales(d) -> sum(ss.sales_price ? ss.date.day_name = d) by ss.store.id, ss.date.week_seq;

auto sun_sales <- @day_sales('Sunday');
auto mon_sales <- @day_sales('Monday');
auto tue_sales <- @day_sales('Tuesday');
auto wed_sales <- @day_sales('Wednesday');
auto thu_sales <- @day_sales('Thursday');
auto fri_sales <- @day_sales('Friday');
auto sat_sales <- @day_sales('Saturday');

auto year_flag <- max(case when ss.date.month_seq between 1212 and  1223 then 1 
when ss.date.month_seq between 1224 and 1235 then 2 else 0 end)
    by ss.store.id, ss.date.week_seq;


# Normalized week-within-year so year1's W lines up with year2's W+52
auto normalized_week <- ss.date.week_seq - (case when year_flag = 2 then 52 else 0 end);

auto sun_ratio <- sun_sales / lead(sun_sales, 1) over (partition by ss.store.id, normalized_week order by year_flag asc);
auto mon_ratio <- mon_sales / lead(mon_sales, 1) over (partition by ss.store.id, normalized_week order by year_flag asc);
auto tue_ratio <- tue_sales / lead(tue_sales, 1) over (partition by ss.store.id, normalized_week order by year_flag asc);
auto wed_ratio <- wed_sales / lead(wed_sales, 1) over (partition by ss.store.id, normalized_week order by year_flag asc);
auto thu_ratio <- thu_sales / lead(thu_sales, 1) over (partition by ss.store.id, normalized_week order by year_flag asc);
auto fri_ratio <- fri_sales / lead(fri_sales, 1) over (partition by ss.store.id, normalized_week order by year_flag asc);
auto sat_ratio <- sat_sales / lead(sat_sales, 1) over (partition by ss.store.id, normalized_week order by year_flag asc);

where
    ss.date.month_seq between 1212 and 1235
    and ss.store.id is not null
    and ss.date.week_seq is not null
select
    --ss.store.id,
    --normalized_week,
    ss.store.name as s_store_name1,
    ss.store.text_id as s_store_id1,
    ss.date.week_seq as d_week_seq1,
    --year_flag,
    sun_ratio as sun_sales_ratio,
    mon_ratio as mon_sales_ratio,
    tue_ratio as tue_sales_ratio,
    wed_ratio as wed_sales_ratio,
    thu_ratio as thu_sales_ratio,
    fri_ratio as fri_sales_ratio,
    sat_ratio as sat_sales_ratio,
having
    year_flag = 1 and sun_ratio is not null

order by
    s_store_name1 asc nulls first,
    s_store_id1 asc nulls first,
    d_week_seq1 asc nulls first
limit 100
;
```

## v4 generated SQL

```sql
WITH 
wakeful as (
SELECT
    "ss_date_date"."D_DAY_NAME" as "ss_date_day_name",
    "ss_date_date"."D_MONTH_SEQ" as "ss_date_month_seq",
    "ss_date_date"."D_WEEK_SEQ" as "ss_date_week_seq",
    "ss_store_sales"."SS_SALES_PRICE" as "ss_sales_price",
    "ss_store_sales"."SS_STORE_SK" as "ss_store_id"
FROM
    "memory"."store_sales" as "ss_store_sales"
    INNER JOIN "memory"."date_dim" as "ss_date_date" on "ss_store_sales"."SS_SOLD_DATE_SK" = "ss_date_date"."D_DATE_SK"
WHERE
    "ss_date_date"."D_MONTH_SEQ" BETWEEN 1212 AND 1235 and "ss_store_sales"."SS_STORE_SK" is not null
),
vacuous as (
SELECT
    "ss_store_store"."S_STORE_ID" as "s_store_id1",
    "ss_store_store"."S_STORE_NAME" as "s_store_name1",
    "ss_store_store"."S_STORE_SK" as "ss_store_id"
FROM
    "memory"."store" as "ss_store_store"),
questionable as (
SELECT
    "wakeful"."ss_date_week_seq" as "ss_date_week_seq",
    "wakeful"."ss_store_id" as "ss_store_id",
    max(CASE
	WHEN "wakeful"."ss_date_month_seq" BETWEEN 1212 AND 1223 THEN 1
	WHEN "wakeful"."ss_date_month_seq" BETWEEN 1224 AND 1235 THEN 2
	ELSE 0
	END) as "year_flag"
FROM
    "wakeful"
GROUP BY
    1,
    2),
cheerful as (
SELECT
    "wakeful"."ss_date_week_seq" as "ss_date_week_seq",
    "wakeful"."ss_store_id" as "ss_store_id",
    sum(CASE WHEN "wakeful"."ss_date_day_name" = 'Friday' THEN "wakeful"."ss_sales_price" ELSE NULL END) as "fri_sales",
    sum(CASE WHEN "wakeful"."ss_date_day_name" = 'Monday' THEN "wakeful"."ss_sales_price" ELSE NULL END) as "mon_sales",
    sum(CASE WHEN "wakeful"."ss_date_day_name" = 'Saturday' THEN "wakeful"."ss_sales_price" ELSE NULL END) as "sat_sales",
    sum(CASE WHEN "wakeful"."ss_date_day_name" = 'Sunday' THEN "wakeful"."ss_sales_price" ELSE NULL END) as "sun_sales",
    sum(CASE WHEN "wakeful"."ss_date_day_name" = 'Thursday' THEN "wakeful"."ss_sales_price" ELSE NULL END) as "thu_sales",
    sum(CASE WHEN "wakeful"."ss_date_day_name" = 'Tuesday' THEN "wakeful"."ss_sales_price" ELSE NULL END) as "tue_sales",
    sum(CASE WHEN "wakeful"."ss_date_day_name" = 'Wednesday' THEN "wakeful"."ss_sales_price" ELSE NULL END) as "wed_sales"
FROM
    "wakeful"
GROUP BY
    1,
    2),
abundant as (
SELECT
    "questionable"."ss_date_week_seq" - (CASE
	WHEN "questionable"."year_flag" = 2 THEN 52
	ELSE 0
	END) as "normalized_week",
    "questionable"."ss_date_week_seq" as "ss_date_week_seq",
    "questionable"."ss_store_id" as "ss_store_id",
    "questionable"."year_flag" as "year_flag"
FROM
    "questionable"),
uneven as (
SELECT
    "abundant"."year_flag" as "year_flag",
    "cheerful"."fri_sales" as "fri_sales",
    "cheerful"."mon_sales" as "mon_sales",
    "cheerful"."sat_sales" as "sat_sales",
    "cheerful"."ss_date_week_seq" as "ss_date_week_seq",
    "cheerful"."ss_store_id" as "ss_store_id",
    "cheerful"."sun_sales" as "sun_sales",
    "cheerful"."thu_sales" as "thu_sales",
    "cheerful"."tue_sales" as "tue_sales",
    "cheerful"."wed_sales" as "wed_sales",
    lead("cheerful"."fri_sales", 1) over (partition by "cheerful"."ss_store_id","abundant"."normalized_week" order by "abundant"."year_flag" asc ) as "_virt_window_lead_2781518767952423",
    lead("cheerful"."mon_sales", 1) over (partition by "cheerful"."ss_store_id","abundant"."normalized_week" order by "abundant"."year_flag" asc ) as "_virt_window_lead_2528494396952318",
    lead("cheerful"."sat_sales", 1) over (partition by "cheerful"."ss_store_id","abundant"."normalized_week" order by "abundant"."year_flag" asc ) as "_virt_window_lead_1424385664204750",
    lead("cheerful"."sun_sales", 1) over (partition by "cheerful"."ss_store_id","abundant"."normalized_week" order by "abundant"."year_flag" asc ) as "_virt_window_lead_6227340597452426",
    lead("cheerful"."thu_sales", 1) over (partition by "cheerful"."ss_store_id","abundant"."normalized_week" order by "abundant"."year_flag" asc ) as "_virt_window_lead_644735902284924",
    lead("cheerful"."tue_sales", 1) over (partition by "cheerful"."ss_store_id","abundant"."normalized_week" order by "abundant"."year_flag" asc ) as "_virt_window_lead_1092584691553364",
    lead("cheerful"."wed_sales", 1) over (partition by "cheerful"."ss_store_id","abundant"."normalized_week" order by "abundant"."year_flag" asc ) as "_virt_window_lead_8976818883664558"
FROM
    "abundant"
    INNER JOIN "cheerful" on "abundant"."ss_date_week_seq" = "cheerful"."ss_date_week_seq" AND "abundant"."ss_store_id" = "cheerful"."ss_store_id"),
juicy as (
SELECT
    "uneven"."fri_sales" / "uneven"."_virt_window_lead_2781518767952423" as "fri_sales_ratio",
    "uneven"."mon_sales" / "uneven"."_virt_window_lead_2528494396952318" as "mon_sales_ratio",
    "uneven"."sat_sales" / "uneven"."_virt_window_lead_1424385664204750" as "sat_sales_ratio",
    "uneven"."ss_date_week_seq" as "d_week_seq1",
    "uneven"."ss_store_id" as "ss_store_id",
    "uneven"."sun_sales" / "uneven"."_virt_window_lead_6227340597452426" as "sun_sales_ratio",
    "uneven"."thu_sales" / "uneven"."_virt_window_lead_644735902284924" as "thu_sales_ratio",
    "uneven"."tue_sales" / "uneven"."_virt_window_lead_1092584691553364" as "tue_sales_ratio",
    "uneven"."wed_sales" / "uneven"."_virt_window_lead_8976818883664558" as "wed_sales_ratio",
    "uneven"."year_flag" as "year_flag"
FROM
    "uneven"
WHERE
    "uneven"."year_flag" = 1
),
young as (
SELECT
    "juicy"."d_week_seq1" as "d_week_seq1",
    "juicy"."fri_sales_ratio" as "fri_sales_ratio",
    "juicy"."mon_sales_ratio" as "mon_sales_ratio",
    "juicy"."sat_sales_ratio" as "sat_sales_ratio",
    "juicy"."sun_sales_ratio" as "sun_sales_ratio",
    "juicy"."thu_sales_ratio" as "thu_sales_ratio",
    "juicy"."tue_sales_ratio" as "tue_sales_ratio",
    "juicy"."wed_sales_ratio" as "wed_sales_ratio",
    "vacuous"."s_store_id1" as "s_store_id1",
    "vacuous"."s_store_name1" as "s_store_name1"
FROM
    "juicy"
    INNER JOIN "vacuous" on "juicy"."ss_store_id" = "vacuous"."ss_store_id"
WHERE
    "juicy"."year_flag" = 1
)
SELECT
    "young"."s_store_name1" as "s_store_name1",
    "young"."s_store_id1" as "s_store_id1",
    "young"."d_week_seq1" as "d_week_seq1",
    "young"."sun_sales_ratio" as "sun_sales_ratio",
    "young"."mon_sales_ratio" as "mon_sales_ratio",
    "young"."tue_sales_ratio" as "tue_sales_ratio",
    "young"."wed_sales_ratio" as "wed_sales_ratio",
    "young"."thu_sales_ratio" as "thu_sales_ratio",
    "young"."fri_sales_ratio" as "fri_sales_ratio",
    "young"."sat_sales_ratio" as "sat_sales_ratio"
FROM
    "young"
WHERE
    "young"."sun_sales_ratio" is not null

ORDER BY 
    "young"."s_store_name1" asc nulls first,
    "young"."s_store_id1" asc nulls first,
    "young"."d_week_seq1" asc nulls first
LIMIT (100)
```

## Reference SQL (zquery log)

```sql
WITH 
wakeful as (
SELECT
    "ss_date_date"."D_DAY_NAME" as "ss_date_day_name",
    "ss_date_date"."D_MONTH_SEQ" as "ss_date_month_seq",
    "ss_date_date"."D_WEEK_SEQ" as "ss_date_week_seq",
    "ss_store_sales"."SS_SALES_PRICE" as "ss_sales_price",
    "ss_store_sales"."SS_STORE_SK" as "ss_store_id"
FROM
    "memory"."store_sales" as "ss_store_sales"
    INNER JOIN "memory"."date_dim" as "ss_date_date" on "ss_store_sales"."SS_SOLD_DATE_SK" = "ss_date_date"."D_DATE_SK"
WHERE
    "ss_date_date"."D_MONTH_SEQ" BETWEEN 1212 AND 1235 and "ss_store_sales"."SS_STORE_SK" is not null
),
vacuous as (
SELECT
    "ss_store_store"."S_STORE_ID" as "s_store_id1",
    "ss_store_store"."S_STORE_NAME" as "s_store_name1",
    "ss_store_store"."S_STORE_SK" as "ss_store_id"
FROM
    "memory"."store" as "ss_store_store"),
questionable as (
SELECT
    "wakeful"."ss_date_week_seq" as "ss_date_week_seq",
    "wakeful"."ss_store_id" as "ss_store_id",
    max(CASE
	WHEN "wakeful"."ss_date_month_seq" BETWEEN 1212 AND 1223 THEN 1
	WHEN "wakeful"."ss_date_month_seq" BETWEEN 1224 AND 1235 THEN 2
	ELSE 0
	END) as "year_flag"
FROM
    "wakeful"
GROUP BY
    1,
    2),
cheerful as (
SELECT
    "wakeful"."ss_date_week_seq" as "ss_date_week_seq",
    "wakeful"."ss_store_id" as "ss_store_id",
    sum(CASE WHEN "wakeful"."ss_date_day_name" = 'Friday' THEN "wakeful"."ss_sales_price" ELSE NULL END) as "fri_sales",
    sum(CASE WHEN "wakeful"."ss_date_day_name" = 'Monday' THEN "wakeful"."ss_sales_price" ELSE NULL END) as "mon_sales",
    sum(CASE WHEN "wakeful"."ss_date_day_name" = 'Saturday' THEN "wakeful"."ss_sales_price" ELSE NULL END) as "sat_sales",
    sum(CASE WHEN "wakeful"."ss_date_day_name" = 'Sunday' THEN "wakeful"."ss_sales_price" ELSE NULL END) as "sun_sales",
    sum(CASE WHEN "wakeful"."ss_date_day_name" = 'Thursday' THEN "wakeful"."ss_sales_price" ELSE NULL END) as "thu_sales",
    sum(CASE WHEN "wakeful"."ss_date_day_name" = 'Tuesday' THEN "wakeful"."ss_sales_price" ELSE NULL END) as "tue_sales",
    sum(CASE WHEN "wakeful"."ss_date_day_name" = 'Wednesday' THEN "wakeful"."ss_sales_price" ELSE NULL END) as "wed_sales"
FROM
    "wakeful"
GROUP BY
    1,
    2),
abundant as (
SELECT
    "questionable"."ss_date_week_seq" - (CASE
	WHEN "questionable"."year_flag" = 2 THEN 52
	ELSE 0
	END) as "normalized_week",
    "questionable"."ss_date_week_seq" as "ss_date_week_seq",
    "questionable"."ss_store_id" as "ss_store_id",
    "questionable"."year_flag" as "year_flag"
FROM
    "questionable"),
uneven as (
SELECT
    "abundant"."year_flag" as "year_flag",
    "cheerful"."fri_sales" as "fri_sales",
    "cheerful"."mon_sales" as "mon_sales",
    "cheerful"."sat_sales" as "sat_sales",
    "cheerful"."ss_date_week_seq" as "ss_date_week_seq",
    "cheerful"."ss_store_id" as "ss_store_id",
    "cheerful"."sun_sales" as "sun_sales",
    "cheerful"."thu_sales" as "thu_sales",
    "cheerful"."tue_sales" as "tue_sales",
    "cheerful"."wed_sales" as "wed_sales",
    lead("cheerful"."fri_sales", 1) over (partition by "cheerful"."ss_store_id","abundant"."normalized_week" order by "abundant"."year_flag" asc ) as "_virt_window_lead_2781518767952423",
    lead("cheerful"."mon_sales", 1) over (partition by "cheerful"."ss_store_id","abundant"."normalized_week" order by "abundant"."year_flag" asc ) as "_virt_window_lead_2528494396952318",
    lead("cheerful"."sat_sales", 1) over (partition by "cheerful"."ss_store_id","abundant"."normalized_week" order by "abundant"."year_flag" asc ) as "_virt_window_lead_1424385664204750",
    lead("cheerful"."sun_sales", 1) over (partition by "cheerful"."ss_store_id","abundant"."normalized_week" order by "abundant"."year_flag" asc ) as "_virt_window_lead_6227340597452426",
    lead("cheerful"."thu_sales", 1) over (partition by "cheerful"."ss_store_id","abundant"."normalized_week" order by "abundant"."year_flag" asc ) as "_virt_window_lead_644735902284924",
    lead("cheerful"."tue_sales", 1) over (partition by "cheerful"."ss_store_id","abundant"."normalized_week" order by "abundant"."year_flag" asc ) as "_virt_window_lead_1092584691553364",
    lead("cheerful"."wed_sales", 1) over (partition by "cheerful"."ss_store_id","abundant"."normalized_week" order by "abundant"."year_flag" asc ) as "_virt_window_lead_8976818883664558"
FROM
    "abundant"
    INNER JOIN "cheerful" on "abundant"."ss_date_week_seq" = "cheerful"."ss_date_week_seq" AND "abundant"."ss_store_id" = "cheerful"."ss_store_id"),
juicy as (
SELECT
    "uneven"."fri_sales" / "uneven"."_virt_window_lead_2781518767952423" as "fri_sales_ratio",
    "uneven"."mon_sales" / "uneven"."_virt_window_lead_2528494396952318" as "mon_sales_ratio",
    "uneven"."sat_sales" / "uneven"."_virt_window_lead_1424385664204750" as "sat_sales_ratio",
    "uneven"."ss_date_week_seq" as "d_week_seq1",
    "uneven"."ss_store_id" as "ss_store_id",
    "uneven"."sun_sales" / "uneven"."_virt_window_lead_6227340597452426" as "sun_sales_ratio",
    "uneven"."thu_sales" / "uneven"."_virt_window_lead_644735902284924" as "thu_sales_ratio",
    "uneven"."tue_sales" / "uneven"."_virt_window_lead_1092584691553364" as "tue_sales_ratio",
    "uneven"."wed_sales" / "uneven"."_virt_window_lead_8976818883664558" as "wed_sales_ratio",
    "uneven"."year_flag" as "year_flag"
FROM
    "uneven"
WHERE
    "uneven"."year_flag" = 1
),
young as (
SELECT
    "juicy"."d_week_seq1" as "d_week_seq1",
    "juicy"."fri_sales_ratio" as "fri_sales_ratio",
    "juicy"."mon_sales_ratio" as "mon_sales_ratio",
    "juicy"."sat_sales_ratio" as "sat_sales_ratio",
    "juicy"."sun_sales_ratio" as "sun_sales_ratio",
    "juicy"."thu_sales_ratio" as "thu_sales_ratio",
    "juicy"."tue_sales_ratio" as "tue_sales_ratio",
    "juicy"."wed_sales_ratio" as "wed_sales_ratio",
    "vacuous"."s_store_id1" as "s_store_id1",
    "vacuous"."s_store_name1" as "s_store_name1"
FROM
    "juicy"
    INNER JOIN "vacuous" on "juicy"."ss_store_id" = "vacuous"."ss_store_id"
WHERE
    "juicy"."year_flag" = 1
)
SELECT
    "young"."s_store_name1" as "s_store_name1",
    "young"."s_store_id1" as "s_store_id1",
    "young"."d_week_seq1" as "d_week_seq1",
    "young"."sun_sales_ratio" as "sun_sales_ratio",
    "young"."mon_sales_ratio" as "mon_sales_ratio",
    "young"."tue_sales_ratio" as "tue_sales_ratio",
    "young"."wed_sales_ratio" as "wed_sales_ratio",
    "young"."thu_sales_ratio" as "thu_sales_ratio",
    "young"."fri_sales_ratio" as "fri_sales_ratio",
    "young"."sat_sales_ratio" as "sat_sales_ratio"
FROM
    "young"
WHERE
    "young"."sun_sales_ratio" is not null

ORDER BY 
    "young"."s_store_name1" asc nulls first,
    "young"."s_store_id1" asc nulls first,
    "young"."d_week_seq1" asc nulls first
LIMIT (100)
```
