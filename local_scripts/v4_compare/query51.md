# Query 51

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
| v4 | 3033 | 77 | 229.09 ms |
| reference | 3934 | 107 | 357.49 ms |
| v4 / ref | 0.77x | 0.72x | 0.64x |

## Preql

```
import all_sales as sales;

# Per-channel daily ext_sales_price aggregated at (item, date) grain.
# Uses inline `?` filter so only that channel's rows contribute to that channel's
# daily total. The (item, date) grain naturally spans every (item, date) tuple
# where EITHER web or store had a sale, giving us the FULL JOIN union without
# explicit merge.
auto web_daily <- sum(sales.sales_price ? sales.sales_channel = 'WEB') by sales.item.id, sales.date.date;
auto store_daily <- sum(sales.sales_price ? sales.sales_channel = 'STORE') by sales.item.id, sales.date.date;

# Per-channel row-presence flag. The reference's per-channel CTE has a row
# whenever a sale exists on this (item, date), even if sales_price is NULL â€”
# can't use `daily IS NOT NULL` since SUM of NULL prices yields NULL.
auto web_has_row <- max(
    case
        when sales.sales_channel = 'WEB' then 1
        else 0
    end
)
    by sales.item.id, sales.date.date;
auto store_has_row <- max(
    case
        when sales.sales_channel = 'STORE' then 1
        else 0
    end
)
    by sales.item.id, sales.date.date;

# Cumulative sum per channel. SQL SUM() ignores NULLs, so on (item, date) rows
# where this channel had no sale, the cumulative is the same as the prior day's
# cumulative (forward-fill behavior matches the reference's outer max(cume_sales)).
auto web_cume <- sum(web_daily) over (partition by sales.item.id order by sales.date.date asc);
auto store_cume <- sum(store_daily) over (partition by sales.item.id order by sales.date.date asc);

# Reference's `web_sales` column is NULL on (item, date) rows where web had no
# sale row at all â€” distinct from `web_cumulative` (which forward-fills).
auto web_sales_visible <- case
    when web_has_row = 1 then web_cume
    else null
end;
auto store_sales_visible <- case
    when store_has_row = 1 then store_cume
    else null
end;

where
    sales.date.month_seq between 1200 and 1211
    and sales.item.id is not null
    and sales.sales_channel in ('WEB', 'STORE')
select
    sales.item.id as item_sk,
    sales.date.date as d_date,
    web_sales_visible as web_sales,
    store_sales_visible as store_sales,
    web_cume as web_cumulative,
    store_cume as store_cumulative,
having
    web_cumulative > store_cumulative

order by
    item_sk asc nulls first,
    d_date asc nulls first
limit 100
;
```

## v4 generated SQL

```sql
WITH 
cheerful as (
SELECT
    "sales_store_sales_unified"."SS_SOLD_DATE_SK" as "sales_date_id",
    "sales_store_sales_unified"."SS_ITEM_SK" as "sales_item_id",
     'STORE'  as "sales_sales_channel",
    "sales_store_sales_unified"."SS_SALES_PRICE" as "sales_sales_price"
FROM
    "memory"."store_sales" as "sales_store_sales_unified"
    INNER JOIN "memory"."date_dim" as "sales_date_date" on "sales_store_sales_unified"."SS_SOLD_DATE_SK" = "sales_date_date"."D_DATE_SK"
WHERE
    "sales_store_sales_unified"."SS_ITEM_SK" is not null and "sales_date_date"."D_MONTH_SEQ" BETWEEN 1200 AND 1211

UNION ALL
SELECT
    "sales_web_sales_unified"."WS_SOLD_DATE_SK" as "sales_date_id",
    "sales_web_sales_unified"."WS_ITEM_SK" as "sales_item_id",
     'WEB'  as "sales_sales_channel",
    "sales_web_sales_unified"."WS_SALES_PRICE" as "sales_sales_price"
FROM
    "memory"."web_sales" as "sales_web_sales_unified"
    INNER JOIN "memory"."date_dim" as "sales_date_date" on "sales_web_sales_unified"."WS_SOLD_DATE_SK" = "sales_date_date"."D_DATE_SK"
WHERE
    "sales_web_sales_unified"."WS_ITEM_SK" is not null and "sales_date_date"."D_MONTH_SEQ" BETWEEN 1200 AND 1211
),
cooperative as (
SELECT
    "cheerful"."sales_item_id" as "sales_item_id",
    cast("sales_date_date"."D_DATE" as date) as "sales_date_date",
    max(CASE
	WHEN "cheerful"."sales_sales_channel" = 'STORE' THEN 1
	ELSE 0
	END) as "store_has_row",
    max(CASE
	WHEN "cheerful"."sales_sales_channel" = 'WEB' THEN 1
	ELSE 0
	END) as "web_has_row",
    sum(CASE WHEN "cheerful"."sales_sales_channel" = 'STORE' THEN "cheerful"."sales_sales_price" ELSE NULL END) as "store_daily",
    sum(CASE WHEN "cheerful"."sales_sales_channel" = 'WEB' THEN "cheerful"."sales_sales_price" ELSE NULL END) as "web_daily"
FROM
    "cheerful"
    LEFT OUTER JOIN "memory"."date_dim" as "sales_date_date" on "cheerful"."sales_date_id" = "sales_date_date"."D_DATE_SK"
GROUP BY
    1,
    2),
uneven as (
SELECT
    "cooperative"."sales_date_date" as "sales_date_date",
    "cooperative"."sales_item_id" as "sales_item_id",
    "cooperative"."store_has_row" as "store_has_row",
    "cooperative"."web_has_row" as "web_has_row",
    sum("cooperative"."store_daily") over (partition by "cooperative"."sales_item_id" order by "cooperative"."sales_date_date" asc ) as "store_cume",
    sum("cooperative"."web_daily") over (partition by "cooperative"."sales_item_id" order by "cooperative"."sales_date_date" asc ) as "web_cume"
FROM
    "cooperative")
SELECT
    "uneven"."sales_item_id" as "item_sk",
    "uneven"."sales_date_date" as "d_date",
    CASE
	WHEN "uneven"."web_has_row" = 1 THEN "uneven"."web_cume"
	ELSE null
	END as "web_sales",
    CASE
	WHEN "uneven"."store_has_row" = 1 THEN "uneven"."store_cume"
	ELSE null
	END as "store_sales",
    "uneven"."web_cume" as "web_cumulative",
    "uneven"."store_cume" as "store_cumulative"
FROM
    "uneven"
WHERE
    "uneven"."web_cume" > "uneven"."store_cume"

ORDER BY 
    "item_sk" asc nulls first,
    "d_date" asc nulls first
LIMIT (100)
```

## Reference SQL (zquery log)

```sql
WITH 
cheerful as (
SELECT
    "sales_store_sales_unified"."SS_SOLD_DATE_SK" as "sales_date_id",
    "sales_store_sales_unified"."SS_ITEM_SK" as "sales_item_id",
     'STORE'  as "sales_sales_channel",
    "sales_store_sales_unified"."SS_SALES_PRICE" as "sales_sales_price"
FROM
    "memory"."store_sales" as "sales_store_sales_unified"
    INNER JOIN "memory"."date_dim" as "sales_date_date" on "sales_store_sales_unified"."SS_SOLD_DATE_SK" = "sales_date_date"."D_DATE_SK"
WHERE
    "sales_store_sales_unified"."SS_ITEM_SK" is not null and "sales_date_date"."D_MONTH_SEQ" BETWEEN 1200 AND 1211

UNION ALL
SELECT
    "sales_web_sales_unified"."WS_SOLD_DATE_SK" as "sales_date_id",
    "sales_web_sales_unified"."WS_ITEM_SK" as "sales_item_id",
     'WEB'  as "sales_sales_channel",
    "sales_web_sales_unified"."WS_SALES_PRICE" as "sales_sales_price"
FROM
    "memory"."web_sales" as "sales_web_sales_unified"
    INNER JOIN "memory"."date_dim" as "sales_date_date" on "sales_web_sales_unified"."WS_SOLD_DATE_SK" = "sales_date_date"."D_DATE_SK"
WHERE
    "sales_web_sales_unified"."WS_ITEM_SK" is not null and "sales_date_date"."D_MONTH_SEQ" BETWEEN 1200 AND 1211
),
yummy as (
SELECT
    "cheerful"."sales_item_id" as "sales_item_id",
    "cheerful"."sales_sales_channel" as "sales_sales_channel",
    cast("sales_date_date"."D_DATE" as date) as "sales_date_date"
FROM
    "cheerful"
    LEFT OUTER JOIN "memory"."date_dim" as "sales_date_date" on "cheerful"."sales_date_id" = "sales_date_date"."D_DATE_SK"
GROUP BY
    1,
    2,
    3),
cooperative as (
SELECT
    "cheerful"."sales_item_id" as "sales_item_id",
    cast("sales_date_date"."D_DATE" as date) as "sales_date_date",
    sum(CASE WHEN "cheerful"."sales_sales_channel" = 'STORE' THEN "cheerful"."sales_sales_price" ELSE NULL END) as "store_daily",
    sum(CASE WHEN "cheerful"."sales_sales_channel" = 'WEB' THEN "cheerful"."sales_sales_price" ELSE NULL END) as "web_daily"
FROM
    "cheerful"
    LEFT OUTER JOIN "memory"."date_dim" as "sales_date_date" on "cheerful"."sales_date_id" = "sales_date_date"."D_DATE_SK"
GROUP BY
    1,
    2),
juicy as (
SELECT
    "yummy"."sales_date_date" as "sales_date_date",
    "yummy"."sales_item_id" as "sales_item_id",
    max(CASE
	WHEN "yummy"."sales_sales_channel" = 'STORE' THEN 1
	ELSE 0
	END) as "store_has_row",
    max(CASE
	WHEN "yummy"."sales_sales_channel" = 'WEB' THEN 1
	ELSE 0
	END) as "web_has_row"
FROM
    "yummy"
GROUP BY
    1,
    2),
uneven as (
SELECT
    "cooperative"."sales_date_date" as "sales_date_date",
    "cooperative"."sales_item_id" as "sales_item_id",
    sum("cooperative"."store_daily") over (partition by "cooperative"."sales_item_id" order by "cooperative"."sales_date_date" asc ) as "store_cume",
    sum("cooperative"."web_daily") over (partition by "cooperative"."sales_item_id" order by "cooperative"."sales_date_date" asc ) as "web_cume"
FROM
    "cooperative"),
vacuous as (
SELECT
    "juicy"."sales_date_date" as "d_date",
    "juicy"."sales_item_id" as "item_sk",
    "uneven"."store_cume" as "store_cumulative",
    "uneven"."web_cume" as "web_cumulative",
    CASE
	WHEN "juicy"."store_has_row" = 1 THEN "uneven"."store_cume"
	ELSE null
	END as "store_sales",
    CASE
	WHEN "juicy"."web_has_row" = 1 THEN "uneven"."web_cume"
	ELSE null
	END as "web_sales"
FROM
    "juicy"
    LEFT OUTER JOIN "uneven" on "juicy"."sales_date_date" = "uneven"."sales_date_date" AND "juicy"."sales_item_id" = "uneven"."sales_item_id")
SELECT
    "vacuous"."item_sk" as "item_sk",
    "vacuous"."d_date" as "d_date",
    "vacuous"."web_sales" as "web_sales",
    "vacuous"."store_sales" as "store_sales",
    "vacuous"."web_cumulative" as "web_cumulative",
    "vacuous"."store_cumulative" as "store_cumulative"
FROM
    "vacuous"
WHERE
    "vacuous"."web_cumulative" > "vacuous"."store_cumulative"

ORDER BY 
    "vacuous"."item_sk" asc nulls first,
    "vacuous"."d_date" asc nulls first
LIMIT (100)
```
