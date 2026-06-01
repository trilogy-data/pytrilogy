# Query 74

**Status:** `mismatch`

| Stage | Result |
| --- | --- |
| v4 SQL generation | OK |
| v4 execution | OK (93 rows) |
| reference execution | OK (92 rows) |
| results identical | NO |

## Result comparison

v4 rows: 93 (93 distinct)
ref rows: 92 (92 distinct)
only in v4 (showing up to 5 of 1):
  1x  (None, None, None)

## SQL size + execution time

| Source | Chars | Lines | Exec (min of 4) |
| --- | --- | --- | --- |
| v4 | 3245 | 71 | 71.65 ms |
| reference | 3347 | 71 | 93.59 ms |
| v4 / ref | 0.97x | 1.00x | 0.77x |

## Preql

```
import unified_sales as sales;

const first_year <- 2001;
const second_year <- 2002;

def channel_year_total(channel, year) -> sum(sales.net_paid ? sales.sales_channel = channel and sales.date.year = year) by sales.customer.id;

auto store_first_year <- sum(sales.net_paid ? sales.sales_channel = 'STORE' and sales.date.year = first_year)
    by sales.customer.id;
auto web_first_year <- sum(sales.net_paid ? sales.sales_channel = 'WEB' and sales.date.year = first_year)
    by sales.customer.id;
auto store_second_year <- sum(sales.net_paid ? sales.sales_channel = 'STORE' and sales.date.year = second_year)
    by sales.customer.id;
auto web_second_year <- sum(sales.net_paid ? sales.sales_channel = 'WEB' and sales.date.year = second_year)
    by sales.customer.id;

where
    sales.sales_channel in ('STORE', 'WEB')
    and sales.customer.id is not null
    and store_first_year > 0
    and web_first_year > 0
    and (case
            when web_first_year > 0 then web_second_year / web_first_year
            else null
        end) > (case
            when store_first_year > 0 then store_second_year / store_first_year
            else null
        end)
select
    sales.customer.text_id as customer_id,
    sales.customer.first_name as customer_first_name,
    sales.customer.last_name as customer_last_name,
order by
    customer_id asc nulls first
limit 100
;
```

## v4 generated SQL

```sql
WITH 
cheerful as (
SELECT
    "sales_catalog_sales_unified"."CS_BILL_CUSTOMER_SK" as "sales_customer_id",
    "sales_catalog_sales_unified"."CS_SOLD_DATE_SK" as "sales_date_id",
    "sales_catalog_sales_unified"."CS_NET_PAID" as "sales_net_paid",
     'CATALOG'  as "sales_sales_channel"
FROM
    "memory"."catalog_sales" as "sales_catalog_sales_unified"
UNION ALL
SELECT
    "sales_store_sales_unified"."SS_CUSTOMER_SK" as "sales_customer_id",
    "sales_store_sales_unified"."SS_SOLD_DATE_SK" as "sales_date_id",
    "sales_store_sales_unified"."SS_NET_PAID" as "sales_net_paid",
     'STORE'  as "sales_sales_channel"
FROM
    "memory"."store_sales" as "sales_store_sales_unified"
UNION ALL
SELECT
    "sales_web_sales_unified"."WS_BILL_CUSTOMER_SK" as "sales_customer_id",
    "sales_web_sales_unified"."WS_SOLD_DATE_SK" as "sales_date_id",
    "sales_web_sales_unified"."WS_NET_PAID" as "sales_net_paid",
     'WEB'  as "sales_sales_channel"
FROM
    "memory"."web_sales" as "sales_web_sales_unified"),
cooperative as (
SELECT
    "cheerful"."sales_customer_id" as "sales_customer_id",
    sum(CASE WHEN "cheerful"."sales_sales_channel" = 'STORE' and "sales_date_date"."D_YEAR" = 2001 THEN "cheerful"."sales_net_paid" ELSE NULL END) as "store_first_year",
    sum(CASE WHEN "cheerful"."sales_sales_channel" = 'STORE' and "sales_date_date"."D_YEAR" = 2002 THEN "cheerful"."sales_net_paid" ELSE NULL END) as "store_second_year",
    sum(CASE WHEN "cheerful"."sales_sales_channel" = 'WEB' and "sales_date_date"."D_YEAR" = 2001 THEN "cheerful"."sales_net_paid" ELSE NULL END) as "web_first_year",
    sum(CASE WHEN "cheerful"."sales_sales_channel" = 'WEB' and "sales_date_date"."D_YEAR" = 2002 THEN "cheerful"."sales_net_paid" ELSE NULL END) as "web_second_year"
FROM
    "cheerful"
    LEFT OUTER JOIN "memory"."date_dim" as "sales_date_date" on "cheerful"."sales_date_id" = "sales_date_date"."D_DATE_SK"
GROUP BY
    1),
yummy as (
SELECT
    "cooperative"."sales_customer_id" as "sales_customer_id",
    "cooperative"."store_first_year" as "store_first_year",
    "cooperative"."store_second_year" as "store_second_year",
    "cooperative"."web_first_year" as "web_first_year",
    "cooperative"."web_second_year" as "web_second_year",
    "sales_customer_customers"."C_CUSTOMER_ID" as "sales_customer_text_id",
    "sales_customer_customers"."C_FIRST_NAME" as "sales_customer_first_name",
    "sales_customer_customers"."C_LAST_NAME" as "sales_customer_last_name"
FROM
    "cooperative"
    LEFT OUTER JOIN "memory"."customer" as "sales_customer_customers" on "cooperative"."sales_customer_id" = "sales_customer_customers"."C_CUSTOMER_SK"
WHERE
    "cooperative"."store_first_year" > 0
)
SELECT
    "yummy"."sales_customer_first_name" as "customer_first_name",
    "yummy"."sales_customer_text_id" as "customer_id",
    "yummy"."sales_customer_last_name" as "customer_last_name"
FROM
    "yummy"
WHERE
    "yummy"."web_first_year" > 0 and ( CASE
	WHEN "yummy"."web_first_year" > 0 THEN "yummy"."web_second_year" / "yummy"."web_first_year"
	ELSE null
	END ) > ( CASE
	WHEN "yummy"."store_first_year" > 0 THEN "yummy"."store_second_year" / "yummy"."store_first_year"
	ELSE null
	END )

ORDER BY 
    "customer_id" asc nulls first
LIMIT (100)
```

## Reference SQL (zquery log)

```sql
WITH 
cheerful as (
SELECT
    "sales_store_sales_unified"."SS_CUSTOMER_SK" as "sales_customer_id",
    "sales_store_sales_unified"."SS_SOLD_DATE_SK" as "sales_date_id",
    "sales_store_sales_unified"."SS_NET_PAID" as "sales_net_paid",
     'STORE'  as "sales_sales_channel"
FROM
    "memory"."store_sales" as "sales_store_sales_unified"
WHERE
    "sales_store_sales_unified"."SS_CUSTOMER_SK" is not null

UNION ALL
SELECT
    "sales_web_sales_unified"."WS_BILL_CUSTOMER_SK" as "sales_customer_id",
    "sales_web_sales_unified"."WS_SOLD_DATE_SK" as "sales_date_id",
    "sales_web_sales_unified"."WS_NET_PAID" as "sales_net_paid",
     'WEB'  as "sales_sales_channel"
FROM
    "memory"."web_sales" as "sales_web_sales_unified"
WHERE
    "sales_web_sales_unified"."WS_BILL_CUSTOMER_SK" is not null
),
thoughtful as (
SELECT
    "cheerful"."sales_customer_id" as "sales_customer_id"
FROM
    "cheerful"
GROUP BY
    1,
    "cheerful"."sales_sales_channel"),
uneven as (
SELECT
    "cheerful"."sales_customer_id" as "sales_customer_id"
FROM
    "cheerful"
    LEFT OUTER JOIN "memory"."date_dim" as "sales_date_date" on "cheerful"."sales_date_id" = "sales_date_date"."D_DATE_SK"
GROUP BY
    1
HAVING
    sum(CASE WHEN "cheerful"."sales_sales_channel" = 'STORE' and "sales_date_date"."D_YEAR" = 2001 THEN "cheerful"."sales_net_paid" ELSE NULL END) > 0 and sum(CASE WHEN "cheerful"."sales_sales_channel" = 'WEB' and "sales_date_date"."D_YEAR" = 2001 THEN "cheerful"."sales_net_paid" ELSE NULL END) > 0 and ( CASE
	WHEN sum(CASE WHEN "cheerful"."sales_sales_channel" = 'WEB' and "sales_date_date"."D_YEAR" = 2001 THEN "cheerful"."sales_net_paid" ELSE NULL END) > 0 THEN sum(CASE WHEN "cheerful"."sales_sales_channel" = 'WEB' and "sales_date_date"."D_YEAR" = 2002 THEN "cheerful"."sales_net_paid" ELSE NULL END) / sum(CASE WHEN "cheerful"."sales_sales_channel" = 'WEB' and "sales_date_date"."D_YEAR" = 2001 THEN "cheerful"."sales_net_paid" ELSE NULL END)
	ELSE null
	END ) > ( CASE
	WHEN sum(CASE WHEN "cheerful"."sales_sales_channel" = 'STORE' and "sales_date_date"."D_YEAR" = 2001 THEN "cheerful"."sales_net_paid" ELSE NULL END) > 0 THEN sum(CASE WHEN "cheerful"."sales_sales_channel" = 'STORE' and "sales_date_date"."D_YEAR" = 2002 THEN "cheerful"."sales_net_paid" ELSE NULL END) / sum(CASE WHEN "cheerful"."sales_sales_channel" = 'STORE' and "sales_date_date"."D_YEAR" = 2001 THEN "cheerful"."sales_net_paid" ELSE NULL END)
	ELSE null
	END )
),
questionable as (
SELECT
    "sales_customer_customers"."C_CUSTOMER_ID" as "sales_customer_text_id",
    "sales_customer_customers"."C_FIRST_NAME" as "sales_customer_first_name",
    "sales_customer_customers"."C_LAST_NAME" as "sales_customer_last_name",
    "thoughtful"."sales_customer_id" as "sales_customer_id"
FROM
    "thoughtful"
    LEFT OUTER JOIN "memory"."customer" as "sales_customer_customers" on "thoughtful"."sales_customer_id" = "sales_customer_customers"."C_CUSTOMER_SK")
SELECT
    "questionable"."sales_customer_text_id" as "customer_id",
    "questionable"."sales_customer_first_name" as "customer_first_name",
    "questionable"."sales_customer_last_name" as "customer_last_name"
FROM
    "questionable"
    INNER JOIN "uneven" on "questionable"."sales_customer_id" = "uneven"."sales_customer_id"
GROUP BY
    1,
    2,
    3
ORDER BY 
    "customer_id" asc nulls first
LIMIT (100)
```
