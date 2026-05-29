# Query 87

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
| v4 | 2424 | 65 | 95.05 ms |
| reference | 2424 | 65 | 97.02 ms |
| v4 / ref | 1.00x | 1.00x | 0.98x |

## Preql

```
import unified_sales as sales;

# For each (customer.last_name, customer.first_name, date.date) tuple,
# count rows present in each channel within month_seq 1200..1211.
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
                when store_in_window > 0 and catalog_in_window = 0 and web_in_window = 0 then 1
                else 0
            end
        ) as cnt,
;
```

## v4 generated SQL

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
	WHEN "abundant"."store_in_window" > 0 and "abundant"."catalog_in_window" = 0 and "abundant"."web_in_window" = 0 THEN 1
	ELSE 0
	END) as "cnt"
FROM
    "abundant"
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
	WHEN "abundant"."store_in_window" > 0 and "abundant"."catalog_in_window" = 0 and "abundant"."web_in_window" = 0 THEN 1
	ELSE 0
	END) as "cnt"
FROM
    "abundant"
```
