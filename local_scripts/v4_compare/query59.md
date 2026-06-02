# Query 59

**Status:** `match`

| Stage | Result |
| --- | --- |
| v4 SQL generation | OK |
| v4 execution | OK (21 rows) |
| reference execution | OK (21 rows) |
| results identical | YES |

## Result comparison

v4 rows: 21 (21 distinct)
ref rows: 21 (21 distinct)

## SQL size + execution time

| Source | Chars | Lines | Exec (min of 4) |
| --- | --- | --- | --- |
| v4 | 6315 | 131 | 52.79 ms |
| reference | 6372 | 130 | 67.80 ms |
| v4 / ref | 0.99x | 1.01x | 0.78x |

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
    "ss_date_date"."D_WEEK_SEQ" as "ss_date_week_seq",
    "ss_store_sales"."SS_STORE_SK" as "ss_store_id",
    max(CASE
	WHEN "ss_date_date"."D_MONTH_SEQ" BETWEEN 1212 AND 1223 THEN 1
	WHEN "ss_date_date"."D_MONTH_SEQ" BETWEEN 1224 AND 1235 THEN 2
	ELSE 0
	END) as "year_flag",
    sum(CASE WHEN "ss_date_date"."D_DAY_NAME" = 'Friday' THEN "ss_store_sales"."SS_SALES_PRICE" ELSE NULL END) as "fri_sales",
    sum(CASE WHEN "ss_date_date"."D_DAY_NAME" = 'Monday' THEN "ss_store_sales"."SS_SALES_PRICE" ELSE NULL END) as "mon_sales",
    sum(CASE WHEN "ss_date_date"."D_DAY_NAME" = 'Saturday' THEN "ss_store_sales"."SS_SALES_PRICE" ELSE NULL END) as "sat_sales",
    sum(CASE WHEN "ss_date_date"."D_DAY_NAME" = 'Sunday' THEN "ss_store_sales"."SS_SALES_PRICE" ELSE NULL END) as "sun_sales",
    sum(CASE WHEN "ss_date_date"."D_DAY_NAME" = 'Thursday' THEN "ss_store_sales"."SS_SALES_PRICE" ELSE NULL END) as "thu_sales",
    sum(CASE WHEN "ss_date_date"."D_DAY_NAME" = 'Tuesday' THEN "ss_store_sales"."SS_SALES_PRICE" ELSE NULL END) as "tue_sales",
    sum(CASE WHEN "ss_date_date"."D_DAY_NAME" = 'Wednesday' THEN "ss_store_sales"."SS_SALES_PRICE" ELSE NULL END) as "wed_sales"
FROM
    "memory"."store_sales" as "ss_store_sales"
    INNER JOIN "memory"."date_dim" as "ss_date_date" on "ss_store_sales"."SS_SOLD_DATE_SK" = "ss_date_date"."D_DATE_SK"
WHERE
    "ss_date_date"."D_MONTH_SEQ" BETWEEN 1212 AND 1235 and "ss_store_sales"."SS_STORE_SK" is not null

GROUP BY
    1,
    2),
uneven as (
SELECT
    "ss_store_store"."S_STORE_ID" as "s_store_id1",
    "ss_store_store"."S_STORE_NAME" as "s_store_name1",
    "ss_store_store"."S_STORE_SK" as "ss_store_id"
FROM
    "memory"."store" as "ss_store_store"),
cooperative as (
SELECT
    "wakeful"."fri_sales" as "fri_sales",
    "wakeful"."mon_sales" as "mon_sales",
    "wakeful"."sat_sales" as "sat_sales",
    "wakeful"."ss_date_week_seq" as "ss_date_week_seq",
    "wakeful"."ss_store_id" as "ss_store_id",
    "wakeful"."sun_sales" as "sun_sales",
    "wakeful"."thu_sales" as "thu_sales",
    "wakeful"."tue_sales" as "tue_sales",
    "wakeful"."wed_sales" as "wed_sales",
    "wakeful"."year_flag" as "year_flag",
    lead("wakeful"."fri_sales", 1) over (partition by "wakeful"."ss_store_id","wakeful"."ss_date_week_seq" - (CASE
	WHEN "wakeful"."year_flag" = 2 THEN 52
	ELSE 0
	END) order by "wakeful"."year_flag" asc ) as "_virt_window_lead_2781518767952423",
    lead("wakeful"."mon_sales", 1) over (partition by "wakeful"."ss_store_id","wakeful"."ss_date_week_seq" - (CASE
	WHEN "wakeful"."year_flag" = 2 THEN 52
	ELSE 0
	END) order by "wakeful"."year_flag" asc ) as "_virt_window_lead_2528494396952318",
    lead("wakeful"."sat_sales", 1) over (partition by "wakeful"."ss_store_id","wakeful"."ss_date_week_seq" - (CASE
	WHEN "wakeful"."year_flag" = 2 THEN 52
	ELSE 0
	END) order by "wakeful"."year_flag" asc ) as "_virt_window_lead_1424385664204750",
    lead("wakeful"."sun_sales", 1) over (partition by "wakeful"."ss_store_id","wakeful"."ss_date_week_seq" - (CASE
	WHEN "wakeful"."year_flag" = 2 THEN 52
	ELSE 0
	END) order by "wakeful"."year_flag" asc ) as "_virt_window_lead_6227340597452426",
    lead("wakeful"."thu_sales", 1) over (partition by "wakeful"."ss_store_id","wakeful"."ss_date_week_seq" - (CASE
	WHEN "wakeful"."year_flag" = 2 THEN 52
	ELSE 0
	END) order by "wakeful"."year_flag" asc ) as "_virt_window_lead_644735902284924",
    lead("wakeful"."tue_sales", 1) over (partition by "wakeful"."ss_store_id","wakeful"."ss_date_week_seq" - (CASE
	WHEN "wakeful"."year_flag" = 2 THEN 52
	ELSE 0
	END) order by "wakeful"."year_flag" asc ) as "_virt_window_lead_1092584691553364",
    lead("wakeful"."wed_sales", 1) over (partition by "wakeful"."ss_store_id","wakeful"."ss_date_week_seq" - (CASE
	WHEN "wakeful"."year_flag" = 2 THEN 52
	ELSE 0
	END) order by "wakeful"."year_flag" asc ) as "_virt_window_lead_8976818883664558"
FROM
    "wakeful"),
abundant as (
SELECT
    "cooperative"."fri_sales" / "cooperative"."_virt_window_lead_2781518767952423" as "fri_sales_ratio",
    "cooperative"."mon_sales" / "cooperative"."_virt_window_lead_2528494396952318" as "mon_sales_ratio",
    "cooperative"."sat_sales" / "cooperative"."_virt_window_lead_1424385664204750" as "sat_sales_ratio",
    "cooperative"."ss_date_week_seq" as "d_week_seq1",
    "cooperative"."ss_store_id" as "ss_store_id",
    "cooperative"."sun_sales" / "cooperative"."_virt_window_lead_6227340597452426" as "sun_sales_ratio",
    "cooperative"."thu_sales" / "cooperative"."_virt_window_lead_644735902284924" as "thu_sales_ratio",
    "cooperative"."tue_sales" / "cooperative"."_virt_window_lead_1092584691553364" as "tue_sales_ratio",
    "cooperative"."wed_sales" / "cooperative"."_virt_window_lead_8976818883664558" as "wed_sales_ratio",
    "cooperative"."year_flag" as "year_flag"
FROM
    "cooperative"
WHERE
    "cooperative"."year_flag" = 1
),
juicy as (
SELECT
    "abundant"."d_week_seq1" as "d_week_seq1",
    "abundant"."fri_sales_ratio" as "fri_sales_ratio",
    "abundant"."mon_sales_ratio" as "mon_sales_ratio",
    "abundant"."sat_sales_ratio" as "sat_sales_ratio",
    "abundant"."sun_sales_ratio" as "sun_sales_ratio",
    "abundant"."thu_sales_ratio" as "thu_sales_ratio",
    "abundant"."tue_sales_ratio" as "tue_sales_ratio",
    "abundant"."wed_sales_ratio" as "wed_sales_ratio",
    "uneven"."s_store_id1" as "s_store_id1",
    "uneven"."s_store_name1" as "s_store_name1"
FROM
    "abundant"
    INNER JOIN "uneven" on "abundant"."ss_store_id" = "uneven"."ss_store_id"
WHERE
    "abundant"."year_flag" = 1
)
SELECT
    "juicy"."s_store_name1" as "s_store_name1",
    "juicy"."s_store_id1" as "s_store_id1",
    "juicy"."d_week_seq1" as "d_week_seq1",
    "juicy"."sun_sales_ratio" as "sun_sales_ratio",
    "juicy"."mon_sales_ratio" as "mon_sales_ratio",
    "juicy"."tue_sales_ratio" as "tue_sales_ratio",
    "juicy"."wed_sales_ratio" as "wed_sales_ratio",
    "juicy"."thu_sales_ratio" as "thu_sales_ratio",
    "juicy"."fri_sales_ratio" as "fri_sales_ratio",
    "juicy"."sat_sales_ratio" as "sat_sales_ratio"
FROM
    "juicy"
WHERE
    "juicy"."sun_sales_ratio" is not null

ORDER BY 
    "juicy"."s_store_name1" asc nulls first,
    "juicy"."s_store_id1" asc nulls first,
    "juicy"."d_week_seq1" asc nulls first
LIMIT (100)
```

## Reference SQL (zquery log)

```sql
WITH 
cooperative as (
SELECT
    "ss_store_sales"."SS_SOLD_DATE_SK" as "ss_date_id",
    "ss_store_sales"."SS_STORE_SK" as "ss_store_id"
FROM
    "memory"."store_sales" as "ss_store_sales"
WHERE
    "ss_store_sales"."SS_STORE_SK" is not null
),
wakeful as (
SELECT
    "ss_date_date"."D_WEEK_SEQ" as "ss_date_week_seq",
    "ss_store_sales"."SS_STORE_SK" as "ss_store_id",
    sum(CASE WHEN "ss_date_date"."D_DAY_NAME" = 'Friday' THEN "ss_store_sales"."SS_SALES_PRICE" ELSE NULL END) as "fri_sales",
    sum(CASE WHEN "ss_date_date"."D_DAY_NAME" = 'Monday' THEN "ss_store_sales"."SS_SALES_PRICE" ELSE NULL END) as "mon_sales",
    sum(CASE WHEN "ss_date_date"."D_DAY_NAME" = 'Saturday' THEN "ss_store_sales"."SS_SALES_PRICE" ELSE NULL END) as "sat_sales",
    sum(CASE WHEN "ss_date_date"."D_DAY_NAME" = 'Sunday' THEN "ss_store_sales"."SS_SALES_PRICE" ELSE NULL END) as "sun_sales",
    sum(CASE WHEN "ss_date_date"."D_DAY_NAME" = 'Thursday' THEN "ss_store_sales"."SS_SALES_PRICE" ELSE NULL END) as "thu_sales",
    sum(CASE WHEN "ss_date_date"."D_DAY_NAME" = 'Tuesday' THEN "ss_store_sales"."SS_SALES_PRICE" ELSE NULL END) as "tue_sales",
    sum(CASE WHEN "ss_date_date"."D_DAY_NAME" = 'Wednesday' THEN "ss_store_sales"."SS_SALES_PRICE" ELSE NULL END) as "wed_sales"
FROM
    "memory"."store_sales" as "ss_store_sales"
    INNER JOIN "memory"."date_dim" as "ss_date_date" on "ss_store_sales"."SS_SOLD_DATE_SK" = "ss_date_date"."D_DATE_SK"
WHERE
    "ss_date_date"."D_MONTH_SEQ" BETWEEN 1212 AND 1235 and "ss_store_sales"."SS_STORE_SK" is not null

GROUP BY
    1,
    2),
questionable as (
SELECT
    "cooperative"."ss_store_id" as "ss_store_id",
    "ss_date_date"."D_WEEK_SEQ" as "ss_date_week_seq",
    max(CASE
	WHEN "ss_date_date"."D_MONTH_SEQ" BETWEEN 1212 AND 1223 THEN 1
	WHEN "ss_date_date"."D_MONTH_SEQ" BETWEEN 1224 AND 1235 THEN 2
	ELSE 0
	END) as "year_flag"
FROM
    "cooperative"
    INNER JOIN "memory"."date_dim" as "ss_date_date" on "cooperative"."ss_date_id" = "ss_date_date"."D_DATE_SK"
WHERE
    "ss_date_date"."D_MONTH_SEQ" BETWEEN 1212 AND 1235

GROUP BY
    1,
    2),
uneven as (
SELECT
    "questionable"."ss_date_week_seq" as "ss_date_week_seq",
    "questionable"."ss_store_id" as "ss_store_id",
    "questionable"."year_flag" as "year_flag",
    "wakeful"."fri_sales" as "fri_sales",
    "wakeful"."mon_sales" as "mon_sales",
    "wakeful"."sat_sales" as "sat_sales",
    "wakeful"."sun_sales" as "sun_sales",
    "wakeful"."thu_sales" as "thu_sales",
    "wakeful"."tue_sales" as "tue_sales",
    "wakeful"."wed_sales" as "wed_sales",
    lead("wakeful"."fri_sales", 1) over (partition by "questionable"."ss_store_id","questionable"."ss_date_week_seq" - (CASE
	WHEN "questionable"."year_flag" = 2 THEN 52
	ELSE 0
	END) order by "questionable"."year_flag" asc ) as "_virt_window_lead_2781518767952423",
    lead("wakeful"."mon_sales", 1) over (partition by "questionable"."ss_store_id","questionable"."ss_date_week_seq" - (CASE
	WHEN "questionable"."year_flag" = 2 THEN 52
	ELSE 0
	END) order by "questionable"."year_flag" asc ) as "_virt_window_lead_2528494396952318",
    lead("wakeful"."sat_sales", 1) over (partition by "questionable"."ss_store_id","questionable"."ss_date_week_seq" - (CASE
	WHEN "questionable"."year_flag" = 2 THEN 52
	ELSE 0
	END) order by "questionable"."year_flag" asc ) as "_virt_window_lead_1424385664204750",
    lead("wakeful"."sun_sales", 1) over (partition by "questionable"."ss_store_id","questionable"."ss_date_week_seq" - (CASE
	WHEN "questionable"."year_flag" = 2 THEN 52
	ELSE 0
	END) order by "questionable"."year_flag" asc ) as "_virt_window_lead_6227340597452426",
    lead("wakeful"."thu_sales", 1) over (partition by "questionable"."ss_store_id","questionable"."ss_date_week_seq" - (CASE
	WHEN "questionable"."year_flag" = 2 THEN 52
	ELSE 0
	END) order by "questionable"."year_flag" asc ) as "_virt_window_lead_644735902284924",
    lead("wakeful"."tue_sales", 1) over (partition by "questionable"."ss_store_id","questionable"."ss_date_week_seq" - (CASE
	WHEN "questionable"."year_flag" = 2 THEN 52
	ELSE 0
	END) order by "questionable"."year_flag" asc ) as "_virt_window_lead_1092584691553364",
    lead("wakeful"."wed_sales", 1) over (partition by "questionable"."ss_store_id","questionable"."ss_date_week_seq" - (CASE
	WHEN "questionable"."year_flag" = 2 THEN 52
	ELSE 0
	END) order by "questionable"."year_flag" asc ) as "_virt_window_lead_8976818883664558"
FROM
    "questionable"
    INNER JOIN "wakeful" on "questionable"."ss_date_week_seq" = "wakeful"."ss_date_week_seq" AND "questionable"."ss_store_id" = "wakeful"."ss_store_id"),
vacuous as (
SELECT
    "ss_store_store"."S_STORE_ID" as "s_store_id1",
    "ss_store_store"."S_STORE_NAME" as "s_store_name1",
    "uneven"."fri_sales" / "uneven"."_virt_window_lead_2781518767952423" as "fri_sales_ratio",
    "uneven"."mon_sales" / "uneven"."_virt_window_lead_2528494396952318" as "mon_sales_ratio",
    "uneven"."sat_sales" / "uneven"."_virt_window_lead_1424385664204750" as "sat_sales_ratio",
    "uneven"."ss_date_week_seq" as "d_week_seq1",
    "uneven"."sun_sales" / "uneven"."_virt_window_lead_6227340597452426" as "sun_sales_ratio",
    "uneven"."thu_sales" / "uneven"."_virt_window_lead_644735902284924" as "thu_sales_ratio",
    "uneven"."tue_sales" / "uneven"."_virt_window_lead_1092584691553364" as "tue_sales_ratio",
    "uneven"."wed_sales" / "uneven"."_virt_window_lead_8976818883664558" as "wed_sales_ratio"
FROM
    "uneven"
    INNER JOIN "memory"."store" as "ss_store_store" on "uneven"."ss_store_id" = "ss_store_store"."S_STORE_SK"
WHERE
    "uneven"."year_flag" = 1
)
SELECT
    "vacuous"."s_store_name1" as "s_store_name1",
    "vacuous"."s_store_id1" as "s_store_id1",
    "vacuous"."d_week_seq1" as "d_week_seq1",
    "vacuous"."sun_sales_ratio" as "sun_sales_ratio",
    "vacuous"."mon_sales_ratio" as "mon_sales_ratio",
    "vacuous"."tue_sales_ratio" as "tue_sales_ratio",
    "vacuous"."wed_sales_ratio" as "wed_sales_ratio",
    "vacuous"."thu_sales_ratio" as "thu_sales_ratio",
    "vacuous"."fri_sales_ratio" as "fri_sales_ratio",
    "vacuous"."sat_sales_ratio" as "sat_sales_ratio"
FROM
    "vacuous"
WHERE
    "vacuous"."sun_sales_ratio" is not null

ORDER BY 
    "vacuous"."s_store_name1" asc nulls first,
    "vacuous"."s_store_id1" asc nulls first,
    "vacuous"."d_week_seq1" asc nulls first
LIMIT (100)
```
