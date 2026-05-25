# Query 38

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

## SQL size

| Source | Chars | Lines |
| --- | --- | --- |
| v4 | 3515 | 100 |
| reference | 2436 | 66 |
| v4 / ref | 1.44x | 1.52x |

## Preql

```
import unified_sales as sales;

# Per-channel presence flag for each (customer.last_name, customer.first_name, date.date) tuple,
# evaluated within month_seq 1200..1211. Tuples with rows in ALL three channels are intersect.
auto store_in_window <- sum(
    case
        when sales.sales_channel = 'STORE'
        and sales.date.month_seq between 1200 and 1200 + 11
        and sales.customer.id is not null then 1
        else 0
    end
)
    by sales.customer.last_name, sales.customer.first_name, sales.date.date;
auto catalog_in_window <- sum(
    case
        when sales.sales_channel = 'CATALOG'
        and sales.date.month_seq between 1200 and 1200 + 11
        and sales.customer.id is not null then 1
        else 0
    end
)
    by sales.customer.last_name, sales.customer.first_name, sales.date.date;
auto web_in_window <- sum(
    case
        when sales.sales_channel = 'WEB'
        and sales.date.month_seq between 1200 and 1200 + 11
        and sales.customer.id is not null then 1
        else 0
    end
)
    by sales.customer.last_name, sales.customer.first_name, sales.date.date;

where
    sales.date.month_seq between 1200 and 1200 + 11
select
    sum(
            case
                when store_in_window > 0 and catalog_in_window > 0 and web_in_window > 0 then 1
                else 0
            end
        ) as cnt,
limit 100
;
```

## v4 generated SQL

```sql
WITH 
questionable as (
SELECT
    "sales_date_date"."D_DATE_SK" as "sales_date_id",
    "sales_date_date"."D_MONTH_SEQ" as "sales_date_month_seq",
    cast("sales_date_date"."D_DATE" as date) as "sales_date_date"
FROM
    "memory"."date_dim" as "sales_date_date"),
cooperative as (
SELECT
    "sales_customer_customers"."C_CUSTOMER_SK" as "sales_customer_id",
    "sales_customer_customers"."C_FIRST_NAME" as "sales_customer_first_name",
    "sales_customer_customers"."C_LAST_NAME" as "sales_customer_last_name"
FROM
    "memory"."customer" as "sales_customer_customers"),
cheerful as (
SELECT
    "sales_catalog_sales_unified"."CS_BILL_CUSTOMER_SK" as "sales_customer_id",
    "sales_catalog_sales_unified"."CS_SOLD_DATE_SK" as "sales_date_id",
     'CATALOG'  as "sales_sales_channel"
FROM
    "memory"."catalog_sales" as "sales_catalog_sales_unified"
UNION ALL
SELECT
    "sales_store_sales_unified"."SS_CUSTOMER_SK" as "sales_customer_id",
    "sales_store_sales_unified"."SS_SOLD_DATE_SK" as "sales_date_id",
     'STORE'  as "sales_sales_channel"
FROM
    "memory"."store_sales" as "sales_store_sales_unified"
UNION ALL
SELECT
    "sales_web_sales_unified"."WS_BILL_CUSTOMER_SK" as "sales_customer_id",
    "sales_web_sales_unified"."WS_SOLD_DATE_SK" as "sales_date_id",
     'WEB'  as "sales_sales_channel"
FROM
    "memory"."web_sales" as "sales_web_sales_unified"),
thoughtful as (
SELECT
    "cheerful"."sales_customer_id" as "sales_customer_id",
    "cheerful"."sales_date_id" as "sales_date_id",
    "cheerful"."sales_sales_channel" as "sales_sales_channel"
FROM
    "cheerful"
GROUP BY
    1,
    2,
    3),
abundant as (
SELECT
    "cooperative"."sales_customer_first_name" as "sales_customer_first_name",
    "cooperative"."sales_customer_last_name" as "sales_customer_last_name",
    "questionable"."sales_date_date" as "sales_date_date",
    "questionable"."sales_date_month_seq" as "sales_date_month_seq",
    "thoughtful"."sales_customer_id" as "sales_customer_id",
    "thoughtful"."sales_sales_channel" as "sales_sales_channel"
FROM
    "thoughtful"
    LEFT OUTER JOIN "questionable" on "thoughtful"."sales_date_id" = "questionable"."sales_date_id"
    LEFT OUTER JOIN "cooperative" on "thoughtful"."sales_customer_id" = "cooperative"."sales_customer_id"
WHERE
    "questionable"."sales_date_month_seq" BETWEEN 1200 AND 1200 + 11

GROUP BY
    1,
    2,
    3,
    4,
    5,
    6),
uneven as (
SELECT
    "abundant"."sales_customer_first_name" as "sales_customer_first_name",
    "abundant"."sales_customer_last_name" as "sales_customer_last_name",
    "abundant"."sales_date_date" as "sales_date_date",
    sum(CASE
	WHEN "abundant"."sales_sales_channel" = 'CATALOG' and "abundant"."sales_date_month_seq" BETWEEN 1200 AND 1200 + 11 and "abundant"."sales_customer_id" is not null THEN 1
	ELSE 0
	END) as "catalog_in_window",
    sum(CASE
	WHEN "abundant"."sales_sales_channel" = 'STORE' and "abundant"."sales_date_month_seq" BETWEEN 1200 AND 1200 + 11 and "abundant"."sales_customer_id" is not null THEN 1
	ELSE 0
	END) as "store_in_window",
    sum(CASE
	WHEN "abundant"."sales_sales_channel" = 'WEB' and "abundant"."sales_date_month_seq" BETWEEN 1200 AND 1200 + 11 and "abundant"."sales_customer_id" is not null THEN 1
	ELSE 0
	END) as "web_in_window"
FROM
    "abundant"
GROUP BY
    1,
    2,
    3)
SELECT
    sum(CASE
	WHEN "uneven"."store_in_window" > 0 and "uneven"."catalog_in_window" > 0 and "uneven"."web_in_window" > 0 THEN 1
	ELSE 0
	END) as "cnt"
FROM
    "uneven"
LIMIT (100)
```

## Reference SQL (zquery log)

```sql
WITH 
cheerful as (
SELECT
    "sales_catalog_sales_unified"."CS_BILL_CUSTOMER_SK" as "sales_customer_id",
    "sales_catalog_sales_unified"."CS_SOLD_DATE_SK" as "sales_date_id",
     'CATALOG'  as "sales_sales_channel"
FROM
    "memory"."catalog_sales" as "sales_catalog_sales_unified"
UNION ALL
SELECT
    "sales_store_sales_unified"."SS_CUSTOMER_SK" as "sales_customer_id",
    "sales_store_sales_unified"."SS_SOLD_DATE_SK" as "sales_date_id",
     'STORE'  as "sales_sales_channel"
FROM
    "memory"."store_sales" as "sales_store_sales_unified"
UNION ALL
SELECT
    "sales_web_sales_unified"."WS_BILL_CUSTOMER_SK" as "sales_customer_id",
    "sales_web_sales_unified"."WS_SOLD_DATE_SK" as "sales_date_id",
     'WEB'  as "sales_sales_channel"
FROM
    "memory"."web_sales" as "sales_web_sales_unified"),
thoughtful as (
SELECT
    "cheerful"."sales_customer_id" as "sales_customer_id",
    "cheerful"."sales_date_id" as "sales_date_id",
    "cheerful"."sales_sales_channel" as "sales_sales_channel"
FROM
    "cheerful"
GROUP BY
    1,
    2,
    3),
abundant as (
SELECT
    sum(CASE
	WHEN "thoughtful"."sales_sales_channel" = 'CATALOG' and "sales_date_date"."D_MONTH_SEQ" BETWEEN 1200 AND 1200 + 11 and "thoughtful"."sales_customer_id" is not null THEN 1
	ELSE 0
	END) as "catalog_in_window",
    sum(CASE
	WHEN "thoughtful"."sales_sales_channel" = 'STORE' and "sales_date_date"."D_MONTH_SEQ" BETWEEN 1200 AND 1200 + 11 and "thoughtful"."sales_customer_id" is not null THEN 1
	ELSE 0
	END) as "store_in_window",
    sum(CASE
	WHEN "thoughtful"."sales_sales_channel" = 'WEB' and "sales_date_date"."D_MONTH_SEQ" BETWEEN 1200 AND 1200 + 11 and "thoughtful"."sales_customer_id" is not null THEN 1
	ELSE 0
	END) as "web_in_window"
FROM
    "thoughtful"
    INNER JOIN "memory"."date_dim" as "sales_date_date" on "thoughtful"."sales_date_id" = "sales_date_date"."D_DATE_SK"
    LEFT OUTER JOIN "memory"."customer" as "sales_customer_customers" on "thoughtful"."sales_customer_id" = "sales_customer_customers"."C_CUSTOMER_SK"
WHERE
    "sales_date_date"."D_MONTH_SEQ" BETWEEN 1200 AND 1200 + 11

GROUP BY
    "sales_customer_customers"."C_FIRST_NAME",
    "sales_customer_customers"."C_LAST_NAME",
    cast("sales_date_date"."D_DATE" as date))
SELECT
    sum(CASE
	WHEN "abundant"."store_in_window" > 0 and "abundant"."catalog_in_window" > 0 and "abundant"."web_in_window" > 0 THEN 1
	ELSE 0
	END) as "cnt"
FROM
    "abundant"
LIMIT (100)
```
