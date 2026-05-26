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
  1x  ('Sandra', 'AAAAAAAAAAABAAAA', 'Williams', 'Sandra', 4096, 'Williams', 'AAAAAAAAAAABAAAA', 1999, 17326, Decimal('340.78'), 65997, 'STORE')
  1x  ('Sandra', 'AAAAAAAAAAABAAAA', 'Williams', 'Sandra', 4096, 'Williams', 'AAAAAAAAAAABAAAA', 1999, 17426, Decimal('3.03'), 11200, 'WEB')
  1x  ('Sandra', 'AAAAAAAAAAABAAAA', 'Williams', 'Sandra', 4096, 'Williams', 'AAAAAAAAAAABAAAA', 1999, 4570, Decimal('1572.85'), 65997, 'STORE')
  1x  ('Sandra', 'AAAAAAAAAAABAAAA', 'Williams', 'Sandra', 4096, 'Williams', 'AAAAAAAAAAABAAAA', 2001, 13429, Decimal('1306.52'), 182451, 'STORE')
  1x  ('Sandra', 'AAAAAAAAAAABAAAA', 'Williams', 'Sandra', 4096, 'Williams', 'AAAAAAAAAAABAAAA', 2001, 13877, Decimal('2525.04'), 182451, 'STORE')
only in ref (showing up to 5 of 92):
  1x  ('Tricia', 'AAAAAAAAAEDMAAAA', 'Medina')
  1x  ('Howard', 'AAAAAAAAAFGBBAAA', 'Major')
  1x  ('Kenneth', 'AAAAAAAAAMGDAAAA', 'Harlan')
  1x  ('Jerry', 'AAAAAAAAAOPFBAAA', 'Fields')
  1x  ('James', 'AAAAAAAABIJBAAAA', 'White')

## SQL size + execution time

| Source | Chars | Lines | Exec (min of 4) |
| --- | --- | --- | --- |
| v4 | 2574 | 52 | 49.45 ms |
| reference | 3347 | 71 | 114.47 ms |
| v4 / ref | 0.77x | 0.73x | 0.43x |

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
    "sales_store_sales_unified"."SS_CUSTOMER_SK" as "sales_customer_id",
    "sales_store_sales_unified"."SS_SOLD_DATE_SK" as "sales_date_id",
    "sales_store_sales_unified"."SS_ITEM_SK" as "sales_item_id",
    "sales_store_sales_unified"."SS_NET_PAID" as "sales_net_paid",
    "sales_store_sales_unified"."SS_TICKET_NUMBER" as "sales_order_id",
     'STORE'  as "sales_sales_channel"
FROM
    "memory"."store_sales" as "sales_store_sales_unified"
    INNER JOIN "memory"."customer" as "sales_customer_customers" on "sales_store_sales_unified"."SS_CUSTOMER_SK" = "sales_customer_customers"."C_CUSTOMER_SK"
WHERE
    "sales_store_sales_unified"."SS_CUSTOMER_SK" is not null

UNION ALL
SELECT
    "sales_web_sales_unified"."WS_BILL_CUSTOMER_SK" as "sales_customer_id",
    "sales_web_sales_unified"."WS_SOLD_DATE_SK" as "sales_date_id",
    "sales_web_sales_unified"."WS_ITEM_SK" as "sales_item_id",
    "sales_web_sales_unified"."WS_NET_PAID" as "sales_net_paid",
    "sales_web_sales_unified"."WS_ORDER_NUMBER" as "sales_order_id",
     'WEB'  as "sales_sales_channel"
FROM
    "memory"."web_sales" as "sales_web_sales_unified"
    INNER JOIN "memory"."customer" as "sales_customer_customers" on "sales_web_sales_unified"."WS_BILL_CUSTOMER_SK" = "sales_customer_customers"."C_CUSTOMER_SK"
WHERE
    "sales_web_sales_unified"."WS_BILL_CUSTOMER_SK" is not null
)
SELECT
    "sales_customer_customers"."C_CUSTOMER_ID" as "customer_id",
    "sales_customer_customers"."C_FIRST_NAME" as "customer_first_name",
    "sales_customer_customers"."C_LAST_NAME" as "customer_last_name",
    "sales_customer_customers"."C_CUSTOMER_ID" as "sales_customer_text_id",
    "sales_date_date"."D_YEAR" as "sales_date_year",
    "sales_customer_customers"."C_LAST_NAME" as "sales_customer_last_name",
    "cheerful"."sales_sales_channel" as "sales_sales_channel",
    "sales_customer_customers"."C_FIRST_NAME" as "sales_customer_first_name",
    "cheerful"."sales_net_paid" as "sales_net_paid",
    "cheerful"."sales_item_id" as "sales_item_id",
    "cheerful"."sales_customer_id" as "sales_customer_id",
    "cheerful"."sales_order_id" as "sales_order_id"
FROM
    "cheerful"
    LEFT OUTER JOIN "memory"."date_dim" as "sales_date_date" on "cheerful"."sales_date_id" = "sales_date_date"."D_DATE_SK"
    LEFT OUTER JOIN "memory"."customer" as "sales_customer_customers" on "cheerful"."sales_customer_id" = "sales_customer_customers"."C_CUSTOMER_SK"
WHERE
    "cheerful"."sales_sales_channel" in ('STORE','WEB')

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
