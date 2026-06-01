# Query 43

**Status:** `match`

| Stage | Result |
| --- | --- |
| v4 SQL generation | OK |
| v4 execution | OK (6 rows) |
| reference execution | OK (6 rows) |
| results identical | YES |

## Result comparison

v4 rows: 6 (6 distinct)
ref rows: 6 (6 distinct)

## SQL size + execution time

| Source | Chars | Lines | Exec (min of 4) |
| --- | --- | --- | --- |
| v4 | 2006 | 31 | 32.10 ms |
| reference | 2006 | 31 | 29.08 ms |
| v4 / ref | 1.00x | 1.00x | 1.10x |

## Preql

```
import physical_sales as physical_sales;

where
    physical_sales.store.gmt_offset = -5 and physical_sales.date.year = 2000
select
    physical_sales.store.name,
    physical_sales.store.text_id,
    sum(physical_sales.sales_price ? physical_sales.date.day_name = 'Sunday') as sun_sales,
    sum(physical_sales.sales_price ? physical_sales.date.day_name = 'Monday') as mon_sales,
    sum(physical_sales.sales_price ? physical_sales.date.day_name = 'Tuesday') as tue_sales,
    sum(physical_sales.sales_price ? physical_sales.date.day_name = 'Wednesday') as wed_sales,
    sum(physical_sales.sales_price ? physical_sales.date.day_name = 'Thursday') as thu_sales,
    sum(physical_sales.sales_price ? physical_sales.date.day_name = 'Friday') as fri_sales,
    sum(physical_sales.sales_price ? physical_sales.date.day_name = 'Saturday') as sat_sales,
order by
    physical_sales.store.name asc,
    physical_sales.store.text_id asc,
    sun_sales asc,
    mon_sales asc,
    tue_sales asc,
    wed_sales asc,
    thu_sales asc,
    fri_sales asc,
    sat_sales asc
limit 100
;
```

## v4 generated SQL

```sql
SELECT
    sum(CASE WHEN "physical_sales_date_date"."D_DAY_NAME" = 'Friday' THEN "physical_sales_store_sales"."SS_SALES_PRICE" ELSE NULL END) as "fri_sales",
    sum(CASE WHEN "physical_sales_date_date"."D_DAY_NAME" = 'Monday' THEN "physical_sales_store_sales"."SS_SALES_PRICE" ELSE NULL END) as "mon_sales",
    sum(CASE WHEN "physical_sales_date_date"."D_DAY_NAME" = 'Saturday' THEN "physical_sales_store_sales"."SS_SALES_PRICE" ELSE NULL END) as "sat_sales",
    sum(CASE WHEN "physical_sales_date_date"."D_DAY_NAME" = 'Sunday' THEN "physical_sales_store_sales"."SS_SALES_PRICE" ELSE NULL END) as "sun_sales",
    sum(CASE WHEN "physical_sales_date_date"."D_DAY_NAME" = 'Thursday' THEN "physical_sales_store_sales"."SS_SALES_PRICE" ELSE NULL END) as "thu_sales",
    sum(CASE WHEN "physical_sales_date_date"."D_DAY_NAME" = 'Tuesday' THEN "physical_sales_store_sales"."SS_SALES_PRICE" ELSE NULL END) as "tue_sales",
    sum(CASE WHEN "physical_sales_date_date"."D_DAY_NAME" = 'Wednesday' THEN "physical_sales_store_sales"."SS_SALES_PRICE" ELSE NULL END) as "wed_sales",
    "physical_sales_store_store"."S_STORE_NAME" as "physical_sales_store_name",
    "physical_sales_store_store"."S_STORE_ID" as "physical_sales_store_text_id"
FROM
    "memory"."store_sales" as "physical_sales_store_sales"
    INNER JOIN "memory"."date_dim" as "physical_sales_date_date" on "physical_sales_store_sales"."SS_SOLD_DATE_SK" = "physical_sales_date_date"."D_DATE_SK"
    INNER JOIN "memory"."store" as "physical_sales_store_store" on "physical_sales_store_sales"."SS_STORE_SK" = "physical_sales_store_store"."S_STORE_SK"
WHERE
    "physical_sales_store_store"."S_GMT_OFFSET" = -5 and "physical_sales_date_date"."D_YEAR" = 2000

GROUP BY
    8,
    9
ORDER BY 
    "physical_sales_store_store"."S_STORE_NAME" asc,
    "physical_sales_store_store"."S_STORE_ID" asc,
    "sun_sales" asc,
    "mon_sales" asc,
    "tue_sales" asc,
    "wed_sales" asc,
    "thu_sales" asc,
    "fri_sales" asc,
    "sat_sales" asc
LIMIT (100)
```

## Reference SQL (zquery log)

```sql
SELECT
    "physical_sales_store_store"."S_STORE_NAME" as "physical_sales_store_name",
    "physical_sales_store_store"."S_STORE_ID" as "physical_sales_store_text_id",
    sum(CASE WHEN "physical_sales_date_date"."D_DAY_NAME" = 'Sunday' THEN "physical_sales_store_sales"."SS_SALES_PRICE" ELSE NULL END) as "sun_sales",
    sum(CASE WHEN "physical_sales_date_date"."D_DAY_NAME" = 'Monday' THEN "physical_sales_store_sales"."SS_SALES_PRICE" ELSE NULL END) as "mon_sales",
    sum(CASE WHEN "physical_sales_date_date"."D_DAY_NAME" = 'Tuesday' THEN "physical_sales_store_sales"."SS_SALES_PRICE" ELSE NULL END) as "tue_sales",
    sum(CASE WHEN "physical_sales_date_date"."D_DAY_NAME" = 'Wednesday' THEN "physical_sales_store_sales"."SS_SALES_PRICE" ELSE NULL END) as "wed_sales",
    sum(CASE WHEN "physical_sales_date_date"."D_DAY_NAME" = 'Thursday' THEN "physical_sales_store_sales"."SS_SALES_PRICE" ELSE NULL END) as "thu_sales",
    sum(CASE WHEN "physical_sales_date_date"."D_DAY_NAME" = 'Friday' THEN "physical_sales_store_sales"."SS_SALES_PRICE" ELSE NULL END) as "fri_sales",
    sum(CASE WHEN "physical_sales_date_date"."D_DAY_NAME" = 'Saturday' THEN "physical_sales_store_sales"."SS_SALES_PRICE" ELSE NULL END) as "sat_sales"
FROM
    "memory"."store_sales" as "physical_sales_store_sales"
    INNER JOIN "memory"."date_dim" as "physical_sales_date_date" on "physical_sales_store_sales"."SS_SOLD_DATE_SK" = "physical_sales_date_date"."D_DATE_SK"
    INNER JOIN "memory"."store" as "physical_sales_store_store" on "physical_sales_store_sales"."SS_STORE_SK" = "physical_sales_store_store"."S_STORE_SK"
WHERE
    "physical_sales_store_store"."S_GMT_OFFSET" = -5 and "physical_sales_date_date"."D_YEAR" = 2000

GROUP BY
    1,
    2
ORDER BY 
    "physical_sales_store_store"."S_STORE_NAME" asc,
    "physical_sales_store_store"."S_STORE_ID" asc,
    "sun_sales" asc,
    "mon_sales" asc,
    "tue_sales" asc,
    "wed_sales" asc,
    "thu_sales" asc,
    "fri_sales" asc,
    "sat_sales" asc
LIMIT (100)
```
