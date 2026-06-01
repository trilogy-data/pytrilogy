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
| v4 | 8128 | 158 | 123.56 ms |
| reference | 7234 | 143 | 150.47 ms |
| v4 / ref | 1.12x | 1.10x | 0.82x |

## Preql

```
import physical_sales as ss;

def day_sales(d) -> sum(ss.sales_price ? ss.date.day_name = d) by ss.store.id, ss.date.week_seq;

rowset wss <- where
    ss.date.month_seq between 1212 and 1235
    and ss.store.id is not null
    and ss.date.week_seq is not null
select
    ss.store.id as store_id,
    ss.store.name as store_name,
    ss.store.text_id as store_text_id,
    ss.date.week_seq as week_seq,
    @day_sales('Sunday') as sun_sales,
    @day_sales('Monday') as mon_sales,
    @day_sales('Tuesday') as tue_sales,
    @day_sales('Wednesday') as wed_sales,
    @day_sales('Thursday') as thu_sales,
    @day_sales('Friday') as fri_sales,
    @day_sales('Saturday') as sat_sales,
    max(
            case
                when ss.date.month_seq >= 1212 and ss.date.month_seq <= 1223 then 1
                else 0
            end
        ) as in_year1,
    max(
            case
                when ss.date.month_seq >= 1224 and ss.date.month_seq <= 1235 then 1
                else 0
            end
        ) as in_year2,
;

# Normalized week-within-year so year1's W lines up with year2's W+52
auto normalized_week <- wss.week_seq - (case
    when wss.in_year2 = 1 then 52
    else 0
end);

select
    wss.store_name as s_store_name1,
    wss.store_text_id as s_store_id1,
    wss.week_seq as d_week_seq1,
    --wss.in_year1 as year1_flag,
    wss.sun_sales / lead(wss.sun_sales, 1)
            over (partition by wss.store_id, normalized_week order by wss.in_year2 asc) as sun_sales_ratio,
    wss.mon_sales / lead(wss.mon_sales, 1)
            over (partition by wss.store_id, normalized_week order by wss.in_year2 asc) as mon_sales_ratio,
    wss.tue_sales / lead(wss.tue_sales, 1)
            over (partition by wss.store_id, normalized_week order by wss.in_year2 asc) as tue_sales_ratio,
    wss.wed_sales / lead(wss.wed_sales, 1)
            over (partition by wss.store_id, normalized_week order by wss.in_year2 asc) as wed_sales_ratio,
    wss.thu_sales / lead(wss.thu_sales, 1)
            over (partition by wss.store_id, normalized_week order by wss.in_year2 asc) as thu_sales_ratio,
    wss.fri_sales / lead(wss.fri_sales, 1)
            over (partition by wss.store_id, normalized_week order by wss.in_year2 asc) as fri_sales_ratio,
    wss.sat_sales / lead(wss.sat_sales, 1)
            over (partition by wss.store_id, normalized_week order by wss.in_year2 asc) as sat_sales_ratio,
having
    year1_flag = 1 and sun_sales_ratio is not null

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
    "ss_store_sales"."SS_SOLD_DATE_SK" as "ss_date_id",
    "ss_store_sales"."SS_STORE_SK" as "ss_store_id"
FROM
    "memory"."store_sales" as "ss_store_sales"
WHERE
    "ss_store_sales"."SS_STORE_SK" is not null
),
abundant as (
SELECT
    "ss_date_date"."D_WEEK_SEQ" as "ss_date_week_seq",
    "ss_store_sales"."SS_STORE_SK" as "ss_store_id",
    sum(CASE WHEN "ss_date_date"."D_DAY_NAME" = 'Friday' THEN "ss_store_sales"."SS_SALES_PRICE" ELSE NULL END) as "_wss_fri_sales",
    sum(CASE WHEN "ss_date_date"."D_DAY_NAME" = 'Monday' THEN "ss_store_sales"."SS_SALES_PRICE" ELSE NULL END) as "_wss_mon_sales",
    sum(CASE WHEN "ss_date_date"."D_DAY_NAME" = 'Saturday' THEN "ss_store_sales"."SS_SALES_PRICE" ELSE NULL END) as "_wss_sat_sales",
    sum(CASE WHEN "ss_date_date"."D_DAY_NAME" = 'Sunday' THEN "ss_store_sales"."SS_SALES_PRICE" ELSE NULL END) as "_wss_sun_sales",
    sum(CASE WHEN "ss_date_date"."D_DAY_NAME" = 'Thursday' THEN "ss_store_sales"."SS_SALES_PRICE" ELSE NULL END) as "_wss_thu_sales",
    sum(CASE WHEN "ss_date_date"."D_DAY_NAME" = 'Tuesday' THEN "ss_store_sales"."SS_SALES_PRICE" ELSE NULL END) as "_wss_tue_sales",
    sum(CASE WHEN "ss_date_date"."D_DAY_NAME" = 'Wednesday' THEN "ss_store_sales"."SS_SALES_PRICE" ELSE NULL END) as "_wss_wed_sales"
FROM
    "memory"."store_sales" as "ss_store_sales"
    INNER JOIN "memory"."date_dim" as "ss_date_date" on "ss_store_sales"."SS_SOLD_DATE_SK" = "ss_date_date"."D_DATE_SK"
WHERE
    "ss_date_date"."D_MONTH_SEQ" BETWEEN 1212 AND 1235 and "ss_store_sales"."SS_STORE_SK" is not null

GROUP BY
    1,
    2),
cheerful as (
SELECT
    "ss_date_date"."D_WEEK_SEQ" as "ss_date_week_seq",
    "ss_store_store"."S_STORE_ID" as "ss_store_text_id",
    "wakeful"."ss_store_id" as "ss_store_id",
    max(CASE
	WHEN "ss_date_date"."D_MONTH_SEQ" >= 1212 and "ss_date_date"."D_MONTH_SEQ" <= 1223 THEN 1
	ELSE 0
	END) as "_wss_in_year1",
    max(CASE
	WHEN "ss_date_date"."D_MONTH_SEQ" >= 1224 and "ss_date_date"."D_MONTH_SEQ" <= 1235 THEN 1
	ELSE 0
	END) as "_wss_in_year2"
FROM
    "wakeful"
    INNER JOIN "memory"."date_dim" as "ss_date_date" on "wakeful"."ss_date_id" = "ss_date_date"."D_DATE_SK"
    LEFT OUTER JOIN "memory"."store" as "ss_store_store" on "wakeful"."ss_store_id" = "ss_store_store"."S_STORE_SK"
WHERE
    "ss_date_date"."D_MONTH_SEQ" BETWEEN 1212 AND 1235

GROUP BY
    1,
    2,
    3),
cooperative as (
SELECT
    "cheerful"."_wss_in_year1" as "_wss_in_year1",
    "cheerful"."_wss_in_year2" as "_wss_in_year2",
    "cheerful"."ss_date_week_seq" as "ss_date_week_seq",
    "cheerful"."ss_store_id" as "ss_store_id",
    "cheerful"."ss_store_text_id" as "ss_store_text_id",
    "ss_store_store"."S_STORE_NAME" as "ss_store_name"
FROM
    "cheerful"
    INNER JOIN "memory"."store" as "ss_store_store" on "cheerful"."ss_store_id" = "ss_store_store"."S_STORE_SK"),
juicy as (
SELECT
    "abundant"."_wss_fri_sales" as "wss_fri_sales",
    "abundant"."_wss_mon_sales" as "wss_mon_sales",
    "abundant"."_wss_sat_sales" as "wss_sat_sales",
    "abundant"."_wss_sun_sales" as "wss_sun_sales",
    "abundant"."_wss_thu_sales" as "wss_thu_sales",
    "abundant"."_wss_tue_sales" as "wss_tue_sales",
    "abundant"."_wss_wed_sales" as "wss_wed_sales",
    "cooperative"."_wss_in_year1" as "year1_flag",
    "cooperative"."_wss_in_year2" as "wss_in_year2",
    "cooperative"."ss_date_week_seq" - (CASE
	WHEN "cooperative"."_wss_in_year2" = 1 THEN 52
	ELSE 0
	END) as "normalized_week",
    "cooperative"."ss_date_week_seq" as "wss_week_seq",
    "cooperative"."ss_store_id" as "wss_store_id",
    "cooperative"."ss_store_name" as "s_store_name1",
    "cooperative"."ss_store_text_id" as "s_store_id1"
FROM
    "cooperative"
    LEFT OUTER JOIN "abundant" on "cooperative"."ss_date_week_seq" = "abundant"."ss_date_week_seq" AND "cooperative"."ss_store_id" = "abundant"."ss_store_id"),
concerned as (
SELECT
    "juicy"."s_store_id1" as "s_store_id1",
    "juicy"."wss_fri_sales" as "wss_fri_sales",
    "juicy"."wss_mon_sales" as "wss_mon_sales",
    "juicy"."wss_sat_sales" as "wss_sat_sales",
    "juicy"."wss_store_id" as "wss_store_id",
    "juicy"."wss_sun_sales" as "wss_sun_sales",
    "juicy"."wss_thu_sales" as "wss_thu_sales",
    "juicy"."wss_tue_sales" as "wss_tue_sales",
    "juicy"."wss_wed_sales" as "wss_wed_sales",
    "juicy"."wss_week_seq" as "d_week_seq1",
    lead("juicy"."wss_fri_sales", 1) over (partition by "juicy"."wss_store_id","juicy"."normalized_week" order by "juicy"."wss_in_year2" asc ) as "_virt_window_lead_6145286498170393",
    lead("juicy"."wss_mon_sales", 1) over (partition by "juicy"."wss_store_id","juicy"."normalized_week" order by "juicy"."wss_in_year2" asc ) as "_virt_window_lead_4810407976310175",
    lead("juicy"."wss_sat_sales", 1) over (partition by "juicy"."wss_store_id","juicy"."normalized_week" order by "juicy"."wss_in_year2" asc ) as "_virt_window_lead_5231294923035285",
    lead("juicy"."wss_sun_sales", 1) over (partition by "juicy"."wss_store_id","juicy"."normalized_week" order by "juicy"."wss_in_year2" asc ) as "_virt_window_lead_5413357402804969",
    lead("juicy"."wss_thu_sales", 1) over (partition by "juicy"."wss_store_id","juicy"."normalized_week" order by "juicy"."wss_in_year2" asc ) as "_virt_window_lead_4156631954188575",
    lead("juicy"."wss_tue_sales", 1) over (partition by "juicy"."wss_store_id","juicy"."normalized_week" order by "juicy"."wss_in_year2" asc ) as "_virt_window_lead_6115154150547261",
    lead("juicy"."wss_wed_sales", 1) over (partition by "juicy"."wss_store_id","juicy"."normalized_week" order by "juicy"."wss_in_year2" asc ) as "_virt_window_lead_6511286120906247"
FROM
    "juicy"),
young as (
SELECT
    "concerned"."d_week_seq1" as "d_week_seq1",
    "concerned"."s_store_id1" as "s_store_id1",
    "concerned"."wss_fri_sales" / "concerned"."_virt_window_lead_6145286498170393" as "fri_sales_ratio",
    "concerned"."wss_mon_sales" / "concerned"."_virt_window_lead_4810407976310175" as "mon_sales_ratio",
    "concerned"."wss_sat_sales" / "concerned"."_virt_window_lead_5231294923035285" as "sat_sales_ratio",
    "concerned"."wss_store_id" as "wss_store_id",
    "concerned"."wss_sun_sales" / "concerned"."_virt_window_lead_5413357402804969" as "sun_sales_ratio",
    "concerned"."wss_thu_sales" / "concerned"."_virt_window_lead_4156631954188575" as "thu_sales_ratio",
    "concerned"."wss_tue_sales" / "concerned"."_virt_window_lead_6115154150547261" as "tue_sales_ratio",
    "concerned"."wss_wed_sales" / "concerned"."_virt_window_lead_6511286120906247" as "wed_sales_ratio"
FROM
    "concerned"),
sparkling as (
SELECT
    "juicy"."s_store_id1" as "s_store_id1",
    "juicy"."s_store_name1" as "s_store_name1",
    "young"."d_week_seq1" as "d_week_seq1",
    "young"."fri_sales_ratio" as "fri_sales_ratio",
    "young"."mon_sales_ratio" as "mon_sales_ratio",
    "young"."sat_sales_ratio" as "sat_sales_ratio",
    "young"."sun_sales_ratio" as "sun_sales_ratio",
    "young"."thu_sales_ratio" as "thu_sales_ratio",
    "young"."tue_sales_ratio" as "tue_sales_ratio",
    "young"."wed_sales_ratio" as "wed_sales_ratio"
FROM
    "juicy"
    INNER JOIN "young" on "juicy"."s_store_id1" = "young"."s_store_id1" AND "juicy"."wss_store_id" = "young"."wss_store_id" AND "juicy"."wss_week_seq" = "young"."d_week_seq1"
WHERE
    "juicy"."year1_flag" = 1 and "young"."sun_sales_ratio" is not null
)
SELECT
    "sparkling"."s_store_name1" as "s_store_name1",
    "sparkling"."s_store_id1" as "s_store_id1",
    "sparkling"."d_week_seq1" as "d_week_seq1",
    "sparkling"."sun_sales_ratio" as "sun_sales_ratio",
    "sparkling"."mon_sales_ratio" as "mon_sales_ratio",
    "sparkling"."tue_sales_ratio" as "tue_sales_ratio",
    "sparkling"."wed_sales_ratio" as "wed_sales_ratio",
    "sparkling"."thu_sales_ratio" as "thu_sales_ratio",
    "sparkling"."fri_sales_ratio" as "fri_sales_ratio",
    "sparkling"."sat_sales_ratio" as "sat_sales_ratio"
FROM
    "sparkling"
ORDER BY 
    "sparkling"."s_store_name1" asc nulls first,
    "sparkling"."s_store_id1" asc nulls first,
    "sparkling"."d_week_seq1" asc nulls first
LIMIT (100)
```

## Reference SQL (zquery log)

```sql
WITH 
wakeful as (
SELECT
    "ss_store_sales"."SS_SOLD_DATE_SK" as "ss_date_id",
    "ss_store_sales"."SS_STORE_SK" as "ss_store_id"
FROM
    "memory"."store_sales" as "ss_store_sales"
WHERE
    "ss_store_sales"."SS_STORE_SK" is not null
),
abundant as (
SELECT
    "ss_date_date"."D_WEEK_SEQ" as "ss_date_week_seq",
    "ss_store_sales"."SS_STORE_SK" as "ss_store_id",
    sum(CASE WHEN "ss_date_date"."D_DAY_NAME" = 'Friday' THEN "ss_store_sales"."SS_SALES_PRICE" ELSE NULL END) as "_wss_fri_sales",
    sum(CASE WHEN "ss_date_date"."D_DAY_NAME" = 'Monday' THEN "ss_store_sales"."SS_SALES_PRICE" ELSE NULL END) as "_wss_mon_sales",
    sum(CASE WHEN "ss_date_date"."D_DAY_NAME" = 'Saturday' THEN "ss_store_sales"."SS_SALES_PRICE" ELSE NULL END) as "_wss_sat_sales",
    sum(CASE WHEN "ss_date_date"."D_DAY_NAME" = 'Sunday' THEN "ss_store_sales"."SS_SALES_PRICE" ELSE NULL END) as "_wss_sun_sales",
    sum(CASE WHEN "ss_date_date"."D_DAY_NAME" = 'Thursday' THEN "ss_store_sales"."SS_SALES_PRICE" ELSE NULL END) as "_wss_thu_sales",
    sum(CASE WHEN "ss_date_date"."D_DAY_NAME" = 'Tuesday' THEN "ss_store_sales"."SS_SALES_PRICE" ELSE NULL END) as "_wss_tue_sales",
    sum(CASE WHEN "ss_date_date"."D_DAY_NAME" = 'Wednesday' THEN "ss_store_sales"."SS_SALES_PRICE" ELSE NULL END) as "_wss_wed_sales"
FROM
    "memory"."store_sales" as "ss_store_sales"
    INNER JOIN "memory"."date_dim" as "ss_date_date" on "ss_store_sales"."SS_SOLD_DATE_SK" = "ss_date_date"."D_DATE_SK"
WHERE
    "ss_date_date"."D_MONTH_SEQ" BETWEEN 1212 AND 1235 and "ss_store_sales"."SS_STORE_SK" is not null

GROUP BY
    1,
    2),
cheerful as (
SELECT
    "ss_date_date"."D_WEEK_SEQ" as "ss_date_week_seq",
    "ss_store_store"."S_STORE_ID" as "ss_store_text_id",
    "wakeful"."ss_store_id" as "ss_store_id",
    max(CASE
	WHEN "ss_date_date"."D_MONTH_SEQ" >= 1212 and "ss_date_date"."D_MONTH_SEQ" <= 1223 THEN 1
	ELSE 0
	END) as "_wss_in_year1",
    max(CASE
	WHEN "ss_date_date"."D_MONTH_SEQ" >= 1224 and "ss_date_date"."D_MONTH_SEQ" <= 1235 THEN 1
	ELSE 0
	END) as "_wss_in_year2"
FROM
    "wakeful"
    INNER JOIN "memory"."date_dim" as "ss_date_date" on "wakeful"."ss_date_id" = "ss_date_date"."D_DATE_SK"
    LEFT OUTER JOIN "memory"."store" as "ss_store_store" on "wakeful"."ss_store_id" = "ss_store_store"."S_STORE_SK"
WHERE
    "ss_date_date"."D_MONTH_SEQ" BETWEEN 1212 AND 1235

GROUP BY
    1,
    2,
    3),
cooperative as (
SELECT
    "cheerful"."_wss_in_year1" as "_wss_in_year1",
    "cheerful"."_wss_in_year2" as "_wss_in_year2",
    "cheerful"."ss_date_week_seq" as "ss_date_week_seq",
    "cheerful"."ss_store_id" as "ss_store_id",
    "cheerful"."ss_store_text_id" as "ss_store_text_id",
    "ss_store_store"."S_STORE_NAME" as "ss_store_name"
FROM
    "cheerful"
    INNER JOIN "memory"."store" as "ss_store_store" on "cheerful"."ss_store_id" = "ss_store_store"."S_STORE_SK"),
juicy as (
SELECT
    "abundant"."_wss_fri_sales" as "wss_fri_sales",
    "abundant"."_wss_mon_sales" as "wss_mon_sales",
    "abundant"."_wss_sat_sales" as "wss_sat_sales",
    "abundant"."_wss_sun_sales" as "wss_sun_sales",
    "abundant"."_wss_thu_sales" as "wss_thu_sales",
    "abundant"."_wss_tue_sales" as "wss_tue_sales",
    "abundant"."_wss_wed_sales" as "wss_wed_sales",
    "cooperative"."_wss_in_year1" as "wss_in_year1",
    "cooperative"."_wss_in_year2" as "wss_in_year2",
    "cooperative"."ss_date_week_seq" - (CASE
	WHEN "cooperative"."_wss_in_year2" = 1 THEN 52
	ELSE 0
	END) as "normalized_week",
    "cooperative"."ss_date_week_seq" as "wss_week_seq",
    "cooperative"."ss_store_id" as "wss_store_id",
    "cooperative"."ss_store_name" as "wss_store_name",
    "cooperative"."ss_store_text_id" as "wss_store_text_id"
FROM
    "cooperative"
    LEFT OUTER JOIN "abundant" on "cooperative"."ss_date_week_seq" = "abundant"."ss_date_week_seq" AND "cooperative"."ss_store_id" = "abundant"."ss_store_id"),
vacuous as (
SELECT
    "juicy"."normalized_week" as "normalized_week",
    "juicy"."wss_fri_sales" as "wss_fri_sales",
    "juicy"."wss_in_year2" as "wss_in_year2",
    "juicy"."wss_mon_sales" as "wss_mon_sales",
    "juicy"."wss_sat_sales" as "wss_sat_sales",
    "juicy"."wss_store_id" as "wss_store_id",
    "juicy"."wss_sun_sales" as "wss_sun_sales",
    "juicy"."wss_thu_sales" as "wss_thu_sales",
    "juicy"."wss_tue_sales" as "wss_tue_sales",
    "juicy"."wss_wed_sales" as "wss_wed_sales",
    "juicy"."wss_week_seq" as "wss_week_seq",
    lead("juicy"."wss_fri_sales", 1) over (partition by "juicy"."wss_store_id","juicy"."normalized_week" order by "juicy"."wss_in_year2" asc ) as "_virt_window_lead_6145286498170393",
    lead("juicy"."wss_mon_sales", 1) over (partition by "juicy"."wss_store_id","juicy"."normalized_week" order by "juicy"."wss_in_year2" asc ) as "_virt_window_lead_4810407976310175",
    lead("juicy"."wss_sat_sales", 1) over (partition by "juicy"."wss_store_id","juicy"."normalized_week" order by "juicy"."wss_in_year2" asc ) as "_virt_window_lead_5231294923035285",
    lead("juicy"."wss_sun_sales", 1) over (partition by "juicy"."wss_store_id","juicy"."normalized_week" order by "juicy"."wss_in_year2" asc ) as "_virt_window_lead_5413357402804969",
    lead("juicy"."wss_thu_sales", 1) over (partition by "juicy"."wss_store_id","juicy"."normalized_week" order by "juicy"."wss_in_year2" asc ) as "_virt_window_lead_4156631954188575",
    lead("juicy"."wss_tue_sales", 1) over (partition by "juicy"."wss_store_id","juicy"."normalized_week" order by "juicy"."wss_in_year2" asc ) as "_virt_window_lead_6115154150547261",
    lead("juicy"."wss_wed_sales", 1) over (partition by "juicy"."wss_store_id","juicy"."normalized_week" order by "juicy"."wss_in_year2" asc ) as "_virt_window_lead_6511286120906247"
FROM
    "juicy")
SELECT
    "juicy"."wss_store_name" as "s_store_name1",
    "juicy"."wss_store_text_id" as "s_store_id1",
    "juicy"."wss_week_seq" as "d_week_seq1",
    "vacuous"."wss_sun_sales" / "vacuous"."_virt_window_lead_5413357402804969" as "sun_sales_ratio",
    "vacuous"."wss_mon_sales" / "vacuous"."_virt_window_lead_4810407976310175" as "mon_sales_ratio",
    "vacuous"."wss_tue_sales" / "vacuous"."_virt_window_lead_6115154150547261" as "tue_sales_ratio",
    "vacuous"."wss_wed_sales" / "vacuous"."_virt_window_lead_6511286120906247" as "wed_sales_ratio",
    "vacuous"."wss_thu_sales" / "vacuous"."_virt_window_lead_4156631954188575" as "thu_sales_ratio",
    "vacuous"."wss_fri_sales" / "vacuous"."_virt_window_lead_6145286498170393" as "fri_sales_ratio",
    "vacuous"."wss_sat_sales" / "vacuous"."_virt_window_lead_5231294923035285" as "sat_sales_ratio"
FROM
    "juicy"
    LEFT OUTER JOIN "vacuous" on "juicy"."normalized_week" = "vacuous"."normalized_week" AND "juicy"."wss_in_year2" = "vacuous"."wss_in_year2" AND "juicy"."wss_store_id" = "vacuous"."wss_store_id" AND "juicy"."wss_week_seq" = "vacuous"."wss_week_seq"
WHERE
    "juicy"."wss_in_year1" = 1 and "vacuous"."wss_sun_sales" / "vacuous"."_virt_window_lead_5413357402804969" is not null

GROUP BY
    1,
    2,
    3,
    4,
    5,
    6,
    7,
    8,
    9,
    10,
    "juicy"."wss_in_year1"
ORDER BY 
    "s_store_name1" asc nulls first,
    "s_store_id1" asc nulls first,
    "d_week_seq1" asc nulls first
LIMIT (100)
```
