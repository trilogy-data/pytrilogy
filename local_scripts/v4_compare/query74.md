# Query 74

**Status:** `match`

| Stage | Result |
| --- | --- |
| v4 SQL generation | OK |
| v4 execution | OK (92 rows) |
| reference execution | OK (92 rows) |
| results identical | YES |

## Result comparison

v4 rows: 92 (92 distinct)
ref rows: 92 (92 distinct)

## SQL size + execution time

| Source | Chars | Lines | Exec (min of 4) |
| --- | --- | --- | --- |
| v4 | 3668 | 80 | 99.39 ms |
| reference | 3573 | 71 | 118.82 ms |
| v4 / ref | 1.03x | 1.13x | 0.84x |

## Preql

```
import all_sales as sales;

const first_year <- 2001;
const second_year <- 2002;

def channel_year_total(channel, year) -> sum(sales.net_paid ? sales.sales_channel = channel and sales.date.year = year) by sales.billing_customer.id;

auto store_first_year <- sum(sales.net_paid ? sales.sales_channel = 'STORE' and sales.date.year = first_year)
    by sales.billing_customer.id;
auto web_first_year <- sum(sales.net_paid ? sales.sales_channel = 'WEB' and sales.date.year = first_year)
    by sales.billing_customer.id;
auto store_second_year <- sum(sales.net_paid ? sales.sales_channel = 'STORE' and sales.date.year = second_year)
    by sales.billing_customer.id;
auto web_second_year <- sum(sales.net_paid ? sales.sales_channel = 'WEB' and sales.date.year = second_year)
    by sales.billing_customer.id;

where
    sales.sales_channel in ('STORE', 'WEB')
    and sales.billing_customer.id is not null
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
    sales.billing_customer.text_id as customer_id,
    sales.billing_customer.first_name as customer_first_name,
    sales.billing_customer.last_name as customer_last_name,
order by
    customer_id asc nulls first
limit 100
;
```

## v4 generated SQL

```sql
WITH 
thoughtful as (
SELECT
    "sales_catalog_sales_unified"."CS_BILL_CUSTOMER_SK" as "sales_billing_customer_id",
    "sales_catalog_sales_unified"."CS_SOLD_DATE_SK" as "sales_date_id",
    "sales_catalog_sales_unified"."CS_NET_PAID" as "sales_net_paid",
     'CATALOG'  as "sales_sales_channel"
FROM
    "memory"."catalog_sales" as "sales_catalog_sales_unified"
WHERE
    "sales_catalog_sales_unified"."CS_BILL_CUSTOMER_SK" is not null

UNION ALL
SELECT
    "sales_store_sales_unified"."SS_CUSTOMER_SK" as "sales_billing_customer_id",
    "sales_store_sales_unified"."SS_SOLD_DATE_SK" as "sales_date_id",
    "sales_store_sales_unified"."SS_NET_PAID" as "sales_net_paid",
     'STORE'  as "sales_sales_channel"
FROM
    "memory"."store_sales" as "sales_store_sales_unified"
WHERE
    "sales_store_sales_unified"."SS_CUSTOMER_SK" is not null

UNION ALL
SELECT
    "sales_web_sales_unified"."WS_BILL_CUSTOMER_SK" as "sales_billing_customer_id",
    "sales_web_sales_unified"."WS_SOLD_DATE_SK" as "sales_date_id",
    "sales_web_sales_unified"."WS_NET_PAID" as "sales_net_paid",
     'WEB'  as "sales_sales_channel"
FROM
    "memory"."web_sales" as "sales_web_sales_unified"
WHERE
    "sales_web_sales_unified"."WS_BILL_CUSTOMER_SK" is not null
),
questionable as (
SELECT
    "thoughtful"."sales_billing_customer_id" as "sales_billing_customer_id",
    sum(CASE WHEN "thoughtful"."sales_sales_channel" = 'STORE' and "sales_date_date"."D_YEAR" = 2001 THEN "thoughtful"."sales_net_paid" ELSE NULL END) as "store_first_year",
    sum(CASE WHEN "thoughtful"."sales_sales_channel" = 'STORE' and "sales_date_date"."D_YEAR" = 2002 THEN "thoughtful"."sales_net_paid" ELSE NULL END) as "store_second_year",
    sum(CASE WHEN "thoughtful"."sales_sales_channel" = 'WEB' and "sales_date_date"."D_YEAR" = 2001 THEN "thoughtful"."sales_net_paid" ELSE NULL END) as "web_first_year",
    sum(CASE WHEN "thoughtful"."sales_sales_channel" = 'WEB' and "sales_date_date"."D_YEAR" = 2002 THEN "thoughtful"."sales_net_paid" ELSE NULL END) as "web_second_year"
FROM
    "thoughtful"
    LEFT OUTER JOIN "memory"."date_dim" as "sales_date_date" on "thoughtful"."sales_date_id" = "sales_date_date"."D_DATE_SK"
GROUP BY
    1
HAVING
    "store_first_year" > 0
),
yummy as (
SELECT
    "questionable"."store_first_year" as "store_first_year",
    "questionable"."store_second_year" as "store_second_year",
    "questionable"."web_first_year" as "web_first_year",
    "questionable"."web_second_year" as "web_second_year",
    "sales_billing_customer_customers"."C_CUSTOMER_ID" as "sales_billing_customer_text_id",
    "sales_billing_customer_customers"."C_CUSTOMER_SK" as "sales_billing_customer_id",
    "sales_billing_customer_customers"."C_FIRST_NAME" as "sales_billing_customer_first_name",
    "sales_billing_customer_customers"."C_LAST_NAME" as "sales_billing_customer_last_name"
FROM
    "questionable"
    INNER JOIN "memory"."customer" as "sales_billing_customer_customers" on "questionable"."sales_billing_customer_id" = "sales_billing_customer_customers"."C_CUSTOMER_SK")
SELECT
    "yummy"."sales_billing_customer_text_id" as "customer_id",
    "yummy"."sales_billing_customer_first_name" as "customer_first_name",
    "yummy"."sales_billing_customer_last_name" as "customer_last_name"
FROM
    "yummy"
WHERE
    "yummy"."store_first_year" > 0 and "yummy"."web_first_year" > 0 and ( CASE
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
thoughtful as (
SELECT
    "sales_store_sales_unified"."SS_CUSTOMER_SK" as "sales_billing_customer_id",
    "sales_store_sales_unified"."SS_SOLD_DATE_SK" as "sales_date_id",
    "sales_store_sales_unified"."SS_NET_PAID" as "sales_net_paid",
     'STORE'  as "sales_sales_channel"
FROM
    "memory"."store_sales" as "sales_store_sales_unified"
WHERE
    "sales_store_sales_unified"."SS_CUSTOMER_SK" is not null

UNION ALL
SELECT
    "sales_web_sales_unified"."WS_BILL_CUSTOMER_SK" as "sales_billing_customer_id",
    "sales_web_sales_unified"."WS_SOLD_DATE_SK" as "sales_date_id",
    "sales_web_sales_unified"."WS_NET_PAID" as "sales_net_paid",
     'WEB'  as "sales_sales_channel"
FROM
    "memory"."web_sales" as "sales_web_sales_unified"
WHERE
    "sales_web_sales_unified"."WS_BILL_CUSTOMER_SK" is not null
),
cooperative as (
SELECT
    "thoughtful"."sales_billing_customer_id" as "sales_billing_customer_id"
FROM
    "thoughtful"
GROUP BY
    1,
    "thoughtful"."sales_sales_channel"),
uneven as (
SELECT
    "thoughtful"."sales_billing_customer_id" as "sales_billing_customer_id"
FROM
    "thoughtful"
    LEFT OUTER JOIN "memory"."date_dim" as "sales_date_date" on "thoughtful"."sales_date_id" = "sales_date_date"."D_DATE_SK"
GROUP BY
    1
HAVING
    sum(CASE WHEN "thoughtful"."sales_sales_channel" = 'STORE' and "sales_date_date"."D_YEAR" = 2001 THEN "thoughtful"."sales_net_paid" ELSE NULL END) > 0 and sum(CASE WHEN "thoughtful"."sales_sales_channel" = 'WEB' and "sales_date_date"."D_YEAR" = 2001 THEN "thoughtful"."sales_net_paid" ELSE NULL END) > 0 and ( CASE
	WHEN sum(CASE WHEN "thoughtful"."sales_sales_channel" = 'WEB' and "sales_date_date"."D_YEAR" = 2001 THEN "thoughtful"."sales_net_paid" ELSE NULL END) > 0 THEN sum(CASE WHEN "thoughtful"."sales_sales_channel" = 'WEB' and "sales_date_date"."D_YEAR" = 2002 THEN "thoughtful"."sales_net_paid" ELSE NULL END) / sum(CASE WHEN "thoughtful"."sales_sales_channel" = 'WEB' and "sales_date_date"."D_YEAR" = 2001 THEN "thoughtful"."sales_net_paid" ELSE NULL END)
	ELSE null
	END ) > ( CASE
	WHEN sum(CASE WHEN "thoughtful"."sales_sales_channel" = 'STORE' and "sales_date_date"."D_YEAR" = 2001 THEN "thoughtful"."sales_net_paid" ELSE NULL END) > 0 THEN sum(CASE WHEN "thoughtful"."sales_sales_channel" = 'STORE' and "sales_date_date"."D_YEAR" = 2002 THEN "thoughtful"."sales_net_paid" ELSE NULL END) / sum(CASE WHEN "thoughtful"."sales_sales_channel" = 'STORE' and "sales_date_date"."D_YEAR" = 2001 THEN "thoughtful"."sales_net_paid" ELSE NULL END)
	ELSE null
	END )
),
questionable as (
SELECT
    "cooperative"."sales_billing_customer_id" as "sales_billing_customer_id",
    "sales_billing_customer_customers"."C_CUSTOMER_ID" as "sales_billing_customer_text_id",
    "sales_billing_customer_customers"."C_FIRST_NAME" as "sales_billing_customer_first_name",
    "sales_billing_customer_customers"."C_LAST_NAME" as "sales_billing_customer_last_name"
FROM
    "cooperative"
    LEFT OUTER JOIN "memory"."customer" as "sales_billing_customer_customers" on "cooperative"."sales_billing_customer_id" = "sales_billing_customer_customers"."C_CUSTOMER_SK")
SELECT
    "questionable"."sales_billing_customer_text_id" as "customer_id",
    "questionable"."sales_billing_customer_first_name" as "customer_first_name",
    "questionable"."sales_billing_customer_last_name" as "customer_last_name"
FROM
    "questionable"
    INNER JOIN "uneven" on "questionable"."sales_billing_customer_id" = "uneven"."sales_billing_customer_id"
GROUP BY
    1,
    2,
    3
ORDER BY 
    "customer_id" asc nulls first
LIMIT (100)
```
