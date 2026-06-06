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
| v4 | 2844 | 63 | 46.98 ms |
| reference | 2548 | 65 | 36.91 ms |
| v4 / ref | 1.12x | 0.97x | 1.27x |

## Preql

```
import all_sales as sales;

# For each (customer.last_name, customer.first_name, date.date) tuple,
# count rows present in each channel within month_seq 1200..1211.
auto store_in_window <- sum(
    case
        when sales.sales_channel = 'STORE'
        and sales.date.month_seq between 1200 and 1200 + 11
        and sales.billing_customer.id is not null then 1
        else 0
    end
)
    by sales.billing_customer.last_name, sales.billing_customer.first_name, sales.date.date;
auto catalog_in_window <- sum(
    case
        when sales.sales_channel = 'CATALOG'
        and sales.date.month_seq between 1200 and 1200 + 11
        and sales.billing_customer.id is not null then 1
        else 0
    end
)
    by sales.billing_customer.last_name, sales.billing_customer.first_name, sales.date.date;
auto web_in_window <- sum(
    case
        when sales.sales_channel = 'WEB'
        and sales.date.month_seq between 1200 and 1200 + 11
        and sales.billing_customer.id is not null then 1
        else 0
    end
)
    by sales.billing_customer.last_name, sales.billing_customer.first_name, sales.date.date;

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
thoughtful as (
SELECT
    "sales_catalog_sales_unified"."CS_BILL_CUSTOMER_SK" as "sales_billing_customer_id",
    "sales_catalog_sales_unified"."CS_SOLD_DATE_SK" as "sales_date_id",
     'CATALOG'  as "sales_sales_channel"
FROM
    "memory"."catalog_sales" as "sales_catalog_sales_unified"
    INNER JOIN "memory"."date_dim" as "sales_date_date" on "sales_catalog_sales_unified"."CS_SOLD_DATE_SK" = "sales_date_date"."D_DATE_SK"
WHERE
    "sales_date_date"."D_MONTH_SEQ" BETWEEN 1200 AND 1200 + 11

UNION ALL
SELECT
    "sales_store_sales_unified"."SS_CUSTOMER_SK" as "sales_billing_customer_id",
    "sales_store_sales_unified"."SS_SOLD_DATE_SK" as "sales_date_id",
     'STORE'  as "sales_sales_channel"
FROM
    "memory"."store_sales" as "sales_store_sales_unified"
    INNER JOIN "memory"."date_dim" as "sales_date_date" on "sales_store_sales_unified"."SS_SOLD_DATE_SK" = "sales_date_date"."D_DATE_SK"
WHERE
    "sales_date_date"."D_MONTH_SEQ" BETWEEN 1200 AND 1200 + 11

UNION ALL
SELECT
    "sales_web_sales_unified"."WS_BILL_CUSTOMER_SK" as "sales_billing_customer_id",
    "sales_web_sales_unified"."WS_SOLD_DATE_SK" as "sales_date_id",
     'WEB'  as "sales_sales_channel"
FROM
    "memory"."web_sales" as "sales_web_sales_unified"
    INNER JOIN "memory"."date_dim" as "sales_date_date" on "sales_web_sales_unified"."WS_SOLD_DATE_SK" = "sales_date_date"."D_DATE_SK"
WHERE
    "sales_date_date"."D_MONTH_SEQ" BETWEEN 1200 AND 1200 + 11
),
questionable as (
SELECT
    sum(CASE
	WHEN "thoughtful"."sales_sales_channel" = 'CATALOG' and "sales_date_date"."D_MONTH_SEQ" BETWEEN 1200 AND 1200 + 11 and "thoughtful"."sales_billing_customer_id" is not null THEN 1
	ELSE 0
	END) as "catalog_in_window",
    sum(CASE
	WHEN "thoughtful"."sales_sales_channel" = 'STORE' and "sales_date_date"."D_MONTH_SEQ" BETWEEN 1200 AND 1200 + 11 and "thoughtful"."sales_billing_customer_id" is not null THEN 1
	ELSE 0
	END) as "store_in_window",
    sum(CASE
	WHEN "thoughtful"."sales_sales_channel" = 'WEB' and "sales_date_date"."D_MONTH_SEQ" BETWEEN 1200 AND 1200 + 11 and "thoughtful"."sales_billing_customer_id" is not null THEN 1
	ELSE 0
	END) as "web_in_window"
FROM
    "thoughtful"
    LEFT OUTER JOIN "memory"."date_dim" as "sales_date_date" on "thoughtful"."sales_date_id" = "sales_date_date"."D_DATE_SK"
    LEFT OUTER JOIN "memory"."customer" as "sales_billing_customer_customers" on "thoughtful"."sales_billing_customer_id" = "sales_billing_customer_customers"."C_CUSTOMER_SK"
GROUP BY
    "sales_billing_customer_customers"."C_FIRST_NAME",
    "sales_billing_customer_customers"."C_LAST_NAME",
    cast("sales_date_date"."D_DATE" as date))
SELECT
    sum(CASE
	WHEN "questionable"."store_in_window" > 0 and "questionable"."catalog_in_window" = 0 and "questionable"."web_in_window" = 0 THEN 1
	ELSE 0
	END) as "cnt"
FROM
    "questionable"
```

## Reference SQL (zquery log)

```sql
WITH 
thoughtful as (
SELECT
    "sales_catalog_sales_unified"."CS_BILL_CUSTOMER_SK" as "sales_billing_customer_id",
    "sales_catalog_sales_unified"."CS_SOLD_DATE_SK" as "sales_date_id",
     'CATALOG'  as "sales_sales_channel"
FROM
    "memory"."catalog_sales" as "sales_catalog_sales_unified"
UNION ALL
SELECT
    "sales_store_sales_unified"."SS_CUSTOMER_SK" as "sales_billing_customer_id",
    "sales_store_sales_unified"."SS_SOLD_DATE_SK" as "sales_date_id",
     'STORE'  as "sales_sales_channel"
FROM
    "memory"."store_sales" as "sales_store_sales_unified"
UNION ALL
SELECT
    "sales_web_sales_unified"."WS_BILL_CUSTOMER_SK" as "sales_billing_customer_id",
    "sales_web_sales_unified"."WS_SOLD_DATE_SK" as "sales_date_id",
     'WEB'  as "sales_sales_channel"
FROM
    "memory"."web_sales" as "sales_web_sales_unified"),
cooperative as (
SELECT
    "thoughtful"."sales_billing_customer_id" as "sales_billing_customer_id",
    "thoughtful"."sales_date_id" as "sales_date_id",
    "thoughtful"."sales_sales_channel" as "sales_sales_channel"
FROM
    "thoughtful"
GROUP BY
    1,
    2,
    3),
abundant as (
SELECT
    sum(CASE
	WHEN "cooperative"."sales_sales_channel" = 'CATALOG' and "sales_date_date"."D_MONTH_SEQ" BETWEEN 1200 AND 1200 + 11 and "cooperative"."sales_billing_customer_id" is not null THEN 1
	ELSE 0
	END) as "catalog_in_window",
    sum(CASE
	WHEN "cooperative"."sales_sales_channel" = 'STORE' and "sales_date_date"."D_MONTH_SEQ" BETWEEN 1200 AND 1200 + 11 and "cooperative"."sales_billing_customer_id" is not null THEN 1
	ELSE 0
	END) as "store_in_window",
    sum(CASE
	WHEN "cooperative"."sales_sales_channel" = 'WEB' and "sales_date_date"."D_MONTH_SEQ" BETWEEN 1200 AND 1200 + 11 and "cooperative"."sales_billing_customer_id" is not null THEN 1
	ELSE 0
	END) as "web_in_window"
FROM
    "cooperative"
    INNER JOIN "memory"."date_dim" as "sales_date_date" on "cooperative"."sales_date_id" = "sales_date_date"."D_DATE_SK"
    LEFT OUTER JOIN "memory"."customer" as "sales_billing_customer_customers" on "cooperative"."sales_billing_customer_id" = "sales_billing_customer_customers"."C_CUSTOMER_SK"
WHERE
    "sales_date_date"."D_MONTH_SEQ" BETWEEN 1200 AND 1200 + 11

GROUP BY
    "sales_billing_customer_customers"."C_FIRST_NAME",
    "sales_billing_customer_customers"."C_LAST_NAME",
    cast("sales_date_date"."D_DATE" as date))
SELECT
    sum(CASE
	WHEN "abundant"."store_in_window" > 0 and "abundant"."catalog_in_window" = 0 and "abundant"."web_in_window" = 0 THEN 1
	ELSE 0
	END) as "cnt"
FROM
    "abundant"
```
