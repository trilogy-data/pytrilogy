# Query 90

**Status:** `mismatch`

| Stage | Result |
| --- | --- |
| v4 SQL generation | OK |
| v4 execution | OK (1 rows) |
| reference execution | OK (1 rows) |
| results identical | NO |

## Result comparison

v4 rows: 1 (1 distinct)
ref rows: 1 (1 distinct)
only in v4 (showing up to 5 of 1):
  1x  (0.5621890547263682, 113, 201)
only in ref (showing up to 5 of 1):
  1x  (0.5621890547263682,)

## SQL size

| Source | Chars | Lines |
| --- | --- | --- |
| v4 | 2744 | 65 |
| reference | 1110 | 16 |
| v4 / ref | 2.47x | 4.06x |

## Preql

```
import web_sales as ws;

auto amc <- sum(ws.row_counter ? ws.time.hour between 8 and 9);
auto pmc <- sum(ws.row_counter ? ws.time.hour between 19 and 20);

where
    ws.ship_household_demographic.dependent_count = 6
    and ws.web_page.char_count between 5000 and 5200
    and ws.time.hour in (8, 9, 19, 20)
select
    case
            when pmc = 0 then null
            else amc::numeric(15,4) / pmc::numeric(15,4)
        end as am_pm_ratio,
order by
    am_pm_ratio asc nulls first
limit 100
;
```

## v4 generated SQL

```sql
WITH 
cheerful as (
SELECT
    "ws_web_sales"."WS_SHIP_HDEMO_SK" as "ws_ship_household_demographic_id",
    "ws_web_sales"."WS_SOLD_TIME_SK" as "ws_time_id",
    "ws_web_sales"."WS_WEB_PAGE_SK" as "ws_web_page_id",
    1 as "ws_row_counter"
FROM
    "memory"."web_sales" as "ws_web_sales"),
wakeful as (
SELECT
    "ws_web_page_web_page"."WP_CHAR_COUNT" as "ws_web_page_char_count",
    "ws_web_page_web_page"."WP_WEB_PAGE_SK" as "ws_web_page_id"
FROM
    "memory"."web_page" as "ws_web_page_web_page"),
highfalutin as (
SELECT
    "ws_time_time"."T_HOUR" as "ws_time_hour",
    "ws_time_time"."T_TIME_SK" as "ws_time_id"
FROM
    "memory"."time_dim" as "ws_time_time"),
quizzical as (
SELECT
    "ws_ship_household_demographic_household_demographics"."HD_DEMO_SK" as "ws_ship_household_demographic_id",
    "ws_ship_household_demographic_household_demographics"."HD_DEP_COUNT" as "ws_ship_household_demographic_dependent_count"
FROM
    "memory"."household_demographics" as "ws_ship_household_demographic_household_demographics"),
thoughtful as (
SELECT
    "cheerful"."ws_row_counter" as "ws_row_counter",
    "highfalutin"."ws_time_hour" as "ws_time_hour",
    "quizzical"."ws_ship_household_demographic_dependent_count" as "ws_ship_household_demographic_dependent_count",
    "wakeful"."ws_web_page_char_count" as "ws_web_page_char_count"
FROM
    "cheerful"
    LEFT OUTER JOIN "highfalutin" on "cheerful"."ws_time_id" = "highfalutin"."ws_time_id"
    LEFT OUTER JOIN "wakeful" on "cheerful"."ws_web_page_id" = "wakeful"."ws_web_page_id"
    LEFT OUTER JOIN "quizzical" on "cheerful"."ws_ship_household_demographic_id" = "quizzical"."ws_ship_household_demographic_id"
WHERE
    "quizzical"."ws_ship_household_demographic_dependent_count" = 6 and "wakeful"."ws_web_page_char_count" BETWEEN 5000 AND 5200 and "highfalutin"."ws_time_hour" in (8,9,19,20)
),
cooperative as (
SELECT
    CASE WHEN "thoughtful"."ws_time_hour" BETWEEN 19 AND 20 THEN "thoughtful"."ws_row_counter" ELSE NULL END as "_virt_filter_row_counter_9358734061005169",
    CASE WHEN "thoughtful"."ws_time_hour" BETWEEN 8 AND 9 THEN "thoughtful"."ws_row_counter" ELSE NULL END as "_virt_filter_row_counter_1151917014322069"
FROM
    "thoughtful"),
questionable as (
SELECT
    sum("cooperative"."_virt_filter_row_counter_1151917014322069") as "amc",
    sum("cooperative"."_virt_filter_row_counter_9358734061005169") as "pmc"
FROM
    "cooperative")
SELECT
    CASE
	WHEN "questionable"."pmc" = 0 THEN null
	ELSE cast("questionable"."amc" as numeric(15,4)) / cast("questionable"."pmc" as numeric(15,4))
	END as "am_pm_ratio",
    "questionable"."amc" as "amc",
    "questionable"."pmc" as "pmc"
FROM
    "questionable"
ORDER BY 
    "am_pm_ratio" asc nulls first
LIMIT (100)
```

## Reference SQL (zquery log)

```sql
SELECT
    CASE
	WHEN sum(CASE WHEN "ws_time_time"."T_HOUR" BETWEEN 19 AND 20 THEN 1 ELSE NULL END) = 0 THEN null
	ELSE cast(sum(CASE WHEN "ws_time_time"."T_HOUR" BETWEEN 8 AND 9 THEN 1 ELSE NULL END) as numeric(15,4)) / cast(sum(CASE WHEN "ws_time_time"."T_HOUR" BETWEEN 19 AND 20 THEN 1 ELSE NULL END) as numeric(15,4))
	END as "am_pm_ratio"
FROM
    "memory"."web_sales" as "ws_web_sales"
    INNER JOIN "memory"."time_dim" as "ws_time_time" on "ws_web_sales"."WS_SOLD_TIME_SK" = "ws_time_time"."T_TIME_SK"
    INNER JOIN "memory"."web_page" as "ws_web_page_web_page" on "ws_web_sales"."WS_WEB_PAGE_SK" = "ws_web_page_web_page"."WP_WEB_PAGE_SK"
    INNER JOIN "memory"."household_demographics" as "ws_ship_household_demographic_household_demographics" on "ws_web_sales"."WS_SHIP_HDEMO_SK" = "ws_ship_household_demographic_household_demographics"."HD_DEMO_SK"
WHERE
    "ws_ship_household_demographic_household_demographics"."HD_DEP_COUNT" = 6 and "ws_web_page_web_page"."WP_CHAR_COUNT" BETWEEN 5000 AND 5200 and "ws_time_time"."T_HOUR" in (8,9,19,20)

ORDER BY 
    "am_pm_ratio" asc nulls first
LIMIT (100)
```
