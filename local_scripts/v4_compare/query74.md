# Query 74

**Status:** `mismatch`

| Stage | Result |
| --- | --- |
| v4 SQL generation | OK |
| v4 execution | OK (100 rows) |
| reference execution | OK (92 rows) |
| results identical | NO |

## Result comparison

v4 rows: 100 (100 distinct)
ref rows: 92 (92 distinct)
only in v4 (showing up to 5 of 100):
  1x  ('Sandra', 'AAAAAAAAAAABAAAA', 'Williams', 'Sandra', 4096, 'Williams', 'AAAAAAAAAAABAAAA', 1999, Decimal('17.28'), 'WEB')
  1x  ('Sandra', 'AAAAAAAAAAABAAAA', 'Williams', 'Sandra', 4096, 'Williams', 'AAAAAAAAAAABAAAA', 1999, Decimal('1066.56'), 'WEB')
  1x  ('Sandra', 'AAAAAAAAAAABAAAA', 'Williams', 'Sandra', 4096, 'Williams', 'AAAAAAAAAAABAAAA', 1999, Decimal('6.67'), 'STORE')
  1x  ('Sandra', 'AAAAAAAAAAABAAAA', 'Williams', 'Sandra', 4096, 'Williams', 'AAAAAAAAAAABAAAA', 1999, Decimal('290.29'), 'WEB')
  1x  ('Sandra', 'AAAAAAAAAAABAAAA', 'Williams', 'Sandra', 4096, 'Williams', 'AAAAAAAAAAABAAAA', 1999, Decimal('3.03'), 'WEB')
only in ref (showing up to 5 of 92):
  1x  ('Tricia', 'AAAAAAAAAEDMAAAA', 'Medina')
  1x  ('Howard', 'AAAAAAAAAFGBBAAA', 'Major')
  1x  ('Kenneth', 'AAAAAAAAAMGDAAAA', 'Harlan')
  1x  ('Jerry', 'AAAAAAAAAOPFBAAA', 'Fields')
  1x  ('James', 'AAAAAAAABIJBAAAA', 'White')

## SQL size

| Source | Chars | Lines |
| --- | --- | --- |
| v4 | 3491 | 92 |
| reference | 3347 | 71 |
| v4 / ref | 1.04x | 1.30x |

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
questionable as (
SELECT
    "sales_date_date"."D_DATE_SK" as "sales_date_id",
    "sales_date_date"."D_YEAR" as "sales_date_year"
FROM
    "memory"."date_dim" as "sales_date_date"),
cooperative as (
SELECT
    "sales_customer_customers"."C_CUSTOMER_ID" as "sales_customer_text_id",
    "sales_customer_customers"."C_CUSTOMER_SK" as "sales_customer_id",
    "sales_customer_customers"."C_FIRST_NAME" as "sales_customer_first_name",
    "sales_customer_customers"."C_LAST_NAME" as "sales_customer_last_name"
FROM
    "memory"."customer" as "sales_customer_customers"),
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
thoughtful as (
SELECT
    "cheerful"."sales_customer_id" as "sales_customer_id",
    "cheerful"."sales_date_id" as "sales_date_id",
    "cheerful"."sales_net_paid" as "sales_net_paid",
    "cheerful"."sales_sales_channel" as "sales_sales_channel"
FROM
    "cheerful"
GROUP BY
    1,
    2,
    3,
    4),
abundant as (
SELECT
    "cooperative"."sales_customer_first_name" as "sales_customer_first_name",
    "cooperative"."sales_customer_last_name" as "sales_customer_last_name",
    "cooperative"."sales_customer_text_id" as "sales_customer_text_id",
    "questionable"."sales_date_year" as "sales_date_year",
    "thoughtful"."sales_customer_id" as "sales_customer_id",
    "thoughtful"."sales_net_paid" as "sales_net_paid",
    "thoughtful"."sales_sales_channel" as "sales_sales_channel"
FROM
    "thoughtful"
    LEFT OUTER JOIN "questionable" on "thoughtful"."sales_date_id" = "questionable"."sales_date_id"
    LEFT OUTER JOIN "cooperative" on "thoughtful"."sales_customer_id" = "cooperative"."sales_customer_id"
WHERE
    "thoughtful"."sales_sales_channel" in ('STORE','WEB') and "thoughtful"."sales_customer_id" is not null

GROUP BY
    1,
    2,
    3,
    4,
    5,
    6,
    7)
SELECT
    "abundant"."sales_customer_text_id" as "customer_id",
    "abundant"."sales_customer_first_name" as "customer_first_name",
    "abundant"."sales_customer_last_name" as "customer_last_name",
    "abundant"."sales_net_paid" as "sales_net_paid",
    "abundant"."sales_customer_first_name" as "sales_customer_first_name",
    "abundant"."sales_customer_text_id" as "sales_customer_text_id",
    "abundant"."sales_date_year" as "sales_date_year",
    "abundant"."sales_customer_last_name" as "sales_customer_last_name",
    "abundant"."sales_sales_channel" as "sales_sales_channel",
    "abundant"."sales_customer_id" as "sales_customer_id"
FROM
    "abundant"
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
    "uneven"
    INNER JOIN "questionable" on "uneven"."sales_customer_id" = "questionable"."sales_customer_id"
GROUP BY
    1,
    2,
    3
ORDER BY 
    "customer_id" asc nulls first
LIMIT (100)
```
