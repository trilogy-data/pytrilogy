# Query 90

**Status:** `match`

| Stage | Result |
| --- | --- |
| v4 SQL generation | OK |
| v4 execution | OK (1 rows) |
| reference execution | OK (1 rows) |
| results identical | YES |

## Result comparison

v4 rows: 1 (1 distinct)
ref rows: 1 (1 distinct)

## SQL size + execution time

| Source | Chars | Lines | Exec (min of 4) |
| --- | --- | --- | --- |
| v4 | 1168 | 23 | 7.16 ms |
| reference | 1110 | 16 | 7.10 ms |
| v4 / ref | 1.05x | 1.44x | 1.01x |

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
thoughtful as (
SELECT
    sum(CASE WHEN "ws_time_time"."T_HOUR" BETWEEN 19 AND 20 THEN 1 ELSE NULL END) as "pmc",
    sum(CASE WHEN "ws_time_time"."T_HOUR" BETWEEN 8 AND 9 THEN 1 ELSE NULL END) as "amc"
FROM
    "memory"."web_sales" as "ws_web_sales"
    INNER JOIN "memory"."time_dim" as "ws_time_time" on "ws_web_sales"."WS_SOLD_TIME_SK" = "ws_time_time"."T_TIME_SK"
    INNER JOIN "memory"."web_page" as "ws_web_page_web_page" on "ws_web_sales"."WS_WEB_PAGE_SK" = "ws_web_page_web_page"."WP_WEB_PAGE_SK"
    INNER JOIN "memory"."household_demographics" as "ws_ship_household_demographic_household_demographics" on "ws_web_sales"."WS_SHIP_HDEMO_SK" = "ws_ship_household_demographic_household_demographics"."HD_DEMO_SK"
WHERE
    "ws_ship_household_demographic_household_demographics"."HD_DEP_COUNT" = 6 and "ws_web_page_web_page"."WP_CHAR_COUNT" BETWEEN 5000 AND 5200 and "ws_time_time"."T_HOUR" in (8,9,19,20)
)
SELECT
    CASE
	WHEN "thoughtful"."pmc" = 0 THEN null
	ELSE cast("thoughtful"."amc" as numeric(15,4)) / cast("thoughtful"."pmc" as numeric(15,4))
	END as "am_pm_ratio"
FROM
    "thoughtful"
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
