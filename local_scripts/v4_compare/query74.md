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
| v4 | 6352 | 133 | 621.44 ms |
| reference | 6352 | 133 | 601.99 ms |
| v4 / ref | 1.00x | 1.00x | 1.03x |

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
    "sales_catalog_sales_unified"."CS_ITEM_SK" as "sales_item_id",
    "sales_catalog_sales_unified"."CS_NET_PAID" as "sales_net_paid",
    "sales_catalog_sales_unified"."CS_ORDER_NUMBER" as "sales_order_id",
     'CATALOG'  as "sales_sales_channel"
FROM
    "memory"."catalog_sales" as "sales_catalog_sales_unified"
WHERE
    "sales_catalog_sales_unified"."CS_BILL_CUSTOMER_SK" is not null

UNION ALL
SELECT
    "sales_store_sales_unified"."SS_CUSTOMER_SK" as "sales_billing_customer_id",
    "sales_store_sales_unified"."SS_SOLD_DATE_SK" as "sales_date_id",
    "sales_store_sales_unified"."SS_ITEM_SK" as "sales_item_id",
    "sales_store_sales_unified"."SS_NET_PAID" as "sales_net_paid",
    "sales_store_sales_unified"."SS_TICKET_NUMBER" as "sales_order_id",
     'STORE'  as "sales_sales_channel"
FROM
    "memory"."store_sales" as "sales_store_sales_unified"
WHERE
    "sales_store_sales_unified"."SS_CUSTOMER_SK" is not null

UNION ALL
SELECT
    "sales_web_sales_unified"."WS_BILL_CUSTOMER_SK" as "sales_billing_customer_id",
    "sales_web_sales_unified"."WS_SOLD_DATE_SK" as "sales_date_id",
    "sales_web_sales_unified"."WS_ITEM_SK" as "sales_item_id",
    "sales_web_sales_unified"."WS_NET_PAID" as "sales_net_paid",
    "sales_web_sales_unified"."WS_ORDER_NUMBER" as "sales_order_id",
     'WEB'  as "sales_sales_channel"
FROM
    "memory"."web_sales" as "sales_web_sales_unified"
WHERE
    "sales_web_sales_unified"."WS_BILL_CUSTOMER_SK" is not null
),
questionable as (
SELECT
    "sales_date_date"."D_YEAR" as "sales_date_year",
    "thoughtful"."sales_billing_customer_id" as "sales_billing_customer_id",
    "thoughtful"."sales_item_id" as "sales_item_id",
    "thoughtful"."sales_net_paid" as "sales_net_paid",
    "thoughtful"."sales_order_id" as "sales_order_id",
    "thoughtful"."sales_sales_channel" as "sales_sales_channel"
FROM
    "thoughtful"
    LEFT OUTER JOIN "memory"."date_dim" as "sales_date_date" on "thoughtful"."sales_date_id" = "sales_date_date"."D_DATE_SK"
WHERE
    "thoughtful"."sales_billing_customer_id" is not null
),
abundant as (
SELECT
    "questionable"."sales_billing_customer_id" as "sales_billing_customer_id",
    "questionable"."sales_item_id" as "sales_item_id",
    "questionable"."sales_order_id" as "sales_order_id",
    "questionable"."sales_sales_channel" as "sales_sales_channel",
    CASE WHEN "questionable"."sales_sales_channel" = 'STORE' and "questionable"."sales_date_year" = 2001 THEN "questionable"."sales_net_paid" ELSE NULL END as "_virt_filter_net_paid_8931261601532283",
    CASE WHEN "questionable"."sales_sales_channel" = 'STORE' and "questionable"."sales_date_year" = 2002 THEN "questionable"."sales_net_paid" ELSE NULL END as "_virt_filter_net_paid_7283285738060558",
    CASE WHEN "questionable"."sales_sales_channel" = 'WEB' and "questionable"."sales_date_year" = 2001 THEN "questionable"."sales_net_paid" ELSE NULL END as "_virt_filter_net_paid_1417284312679536",
    CASE WHEN "questionable"."sales_sales_channel" = 'WEB' and "questionable"."sales_date_year" = 2002 THEN "questionable"."sales_net_paid" ELSE NULL END as "_virt_filter_net_paid_2204116219608499"
FROM
    "questionable"
WHERE
    "questionable"."sales_sales_channel" in ('STORE','WEB')
),
uneven as (
SELECT
    "abundant"."_virt_filter_net_paid_1417284312679536" as "_virt_filter_net_paid_1417284312679536",
    "abundant"."_virt_filter_net_paid_2204116219608499" as "_virt_filter_net_paid_2204116219608499",
    "abundant"."_virt_filter_net_paid_7283285738060558" as "_virt_filter_net_paid_7283285738060558",
    "abundant"."_virt_filter_net_paid_8931261601532283" as "_virt_filter_net_paid_8931261601532283",
    "abundant"."sales_billing_customer_id" as "sales_billing_customer_id"
FROM
    "abundant"
    FULL JOIN "questionable" on "abundant"."sales_item_id" = "questionable"."sales_item_id" AND "abundant"."sales_order_id" = "questionable"."sales_order_id" AND "abundant"."sales_sales_channel" = "questionable"."sales_sales_channel"
GROUP BY
    1,
    2,
    3,
    4,
    5,
    coalesce("abundant"."sales_item_id","questionable"."sales_item_id"),
    coalesce("abundant"."sales_order_id","questionable"."sales_order_id"),
    coalesce("abundant"."sales_sales_channel","questionable"."sales_sales_channel")),
juicy as (
SELECT
    "uneven"."sales_billing_customer_id" as "sales_billing_customer_id",
    sum("uneven"."_virt_filter_net_paid_1417284312679536") as "web_first_year",
    sum("uneven"."_virt_filter_net_paid_2204116219608499") as "web_second_year",
    sum("uneven"."_virt_filter_net_paid_7283285738060558") as "store_second_year",
    sum("uneven"."_virt_filter_net_paid_8931261601532283") as "store_first_year"
FROM
    "uneven"
GROUP BY
    1
HAVING
    "store_first_year" > 0
),
vacuous as (
SELECT
    "juicy"."store_first_year" as "store_first_year",
    "juicy"."store_second_year" as "store_second_year",
    "juicy"."web_first_year" as "web_first_year",
    "juicy"."web_second_year" as "web_second_year",
    "sales_billing_customer_customers"."C_CUSTOMER_ID" as "sales_billing_customer_text_id",
    "sales_billing_customer_customers"."C_CUSTOMER_SK" as "sales_billing_customer_id",
    "sales_billing_customer_customers"."C_FIRST_NAME" as "sales_billing_customer_first_name",
    "sales_billing_customer_customers"."C_LAST_NAME" as "sales_billing_customer_last_name"
FROM
    "juicy"
    INNER JOIN "memory"."customer" as "sales_billing_customer_customers" on "juicy"."sales_billing_customer_id" = "sales_billing_customer_customers"."C_CUSTOMER_SK")
SELECT
    "vacuous"."sales_billing_customer_text_id" as "customer_id",
    "vacuous"."sales_billing_customer_first_name" as "customer_first_name",
    "vacuous"."sales_billing_customer_last_name" as "customer_last_name"
FROM
    "vacuous"
WHERE
    "vacuous"."store_first_year" > 0 and "vacuous"."web_first_year" > 0 and ( CASE
	WHEN "vacuous"."web_first_year" > 0 THEN "vacuous"."web_second_year" / "vacuous"."web_first_year"
	ELSE null
	END ) > ( CASE
	WHEN "vacuous"."store_first_year" > 0 THEN "vacuous"."store_second_year" / "vacuous"."store_first_year"
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
    "sales_catalog_sales_unified"."CS_BILL_CUSTOMER_SK" as "sales_billing_customer_id",
    "sales_catalog_sales_unified"."CS_SOLD_DATE_SK" as "sales_date_id",
    "sales_catalog_sales_unified"."CS_ITEM_SK" as "sales_item_id",
    "sales_catalog_sales_unified"."CS_NET_PAID" as "sales_net_paid",
    "sales_catalog_sales_unified"."CS_ORDER_NUMBER" as "sales_order_id",
     'CATALOG'  as "sales_sales_channel"
FROM
    "memory"."catalog_sales" as "sales_catalog_sales_unified"
WHERE
    "sales_catalog_sales_unified"."CS_BILL_CUSTOMER_SK" is not null

UNION ALL
SELECT
    "sales_store_sales_unified"."SS_CUSTOMER_SK" as "sales_billing_customer_id",
    "sales_store_sales_unified"."SS_SOLD_DATE_SK" as "sales_date_id",
    "sales_store_sales_unified"."SS_ITEM_SK" as "sales_item_id",
    "sales_store_sales_unified"."SS_NET_PAID" as "sales_net_paid",
    "sales_store_sales_unified"."SS_TICKET_NUMBER" as "sales_order_id",
     'STORE'  as "sales_sales_channel"
FROM
    "memory"."store_sales" as "sales_store_sales_unified"
WHERE
    "sales_store_sales_unified"."SS_CUSTOMER_SK" is not null

UNION ALL
SELECT
    "sales_web_sales_unified"."WS_BILL_CUSTOMER_SK" as "sales_billing_customer_id",
    "sales_web_sales_unified"."WS_SOLD_DATE_SK" as "sales_date_id",
    "sales_web_sales_unified"."WS_ITEM_SK" as "sales_item_id",
    "sales_web_sales_unified"."WS_NET_PAID" as "sales_net_paid",
    "sales_web_sales_unified"."WS_ORDER_NUMBER" as "sales_order_id",
     'WEB'  as "sales_sales_channel"
FROM
    "memory"."web_sales" as "sales_web_sales_unified"
WHERE
    "sales_web_sales_unified"."WS_BILL_CUSTOMER_SK" is not null
),
questionable as (
SELECT
    "sales_date_date"."D_YEAR" as "sales_date_year",
    "thoughtful"."sales_billing_customer_id" as "sales_billing_customer_id",
    "thoughtful"."sales_item_id" as "sales_item_id",
    "thoughtful"."sales_net_paid" as "sales_net_paid",
    "thoughtful"."sales_order_id" as "sales_order_id",
    "thoughtful"."sales_sales_channel" as "sales_sales_channel"
FROM
    "thoughtful"
    LEFT OUTER JOIN "memory"."date_dim" as "sales_date_date" on "thoughtful"."sales_date_id" = "sales_date_date"."D_DATE_SK"
WHERE
    "thoughtful"."sales_billing_customer_id" is not null
),
abundant as (
SELECT
    "questionable"."sales_billing_customer_id" as "sales_billing_customer_id",
    "questionable"."sales_item_id" as "sales_item_id",
    "questionable"."sales_order_id" as "sales_order_id",
    "questionable"."sales_sales_channel" as "sales_sales_channel",
    CASE WHEN "questionable"."sales_sales_channel" = 'STORE' and "questionable"."sales_date_year" = 2001 THEN "questionable"."sales_net_paid" ELSE NULL END as "_virt_filter_net_paid_8931261601532283",
    CASE WHEN "questionable"."sales_sales_channel" = 'STORE' and "questionable"."sales_date_year" = 2002 THEN "questionable"."sales_net_paid" ELSE NULL END as "_virt_filter_net_paid_7283285738060558",
    CASE WHEN "questionable"."sales_sales_channel" = 'WEB' and "questionable"."sales_date_year" = 2001 THEN "questionable"."sales_net_paid" ELSE NULL END as "_virt_filter_net_paid_1417284312679536",
    CASE WHEN "questionable"."sales_sales_channel" = 'WEB' and "questionable"."sales_date_year" = 2002 THEN "questionable"."sales_net_paid" ELSE NULL END as "_virt_filter_net_paid_2204116219608499"
FROM
    "questionable"
WHERE
    "questionable"."sales_sales_channel" in ('STORE','WEB')
),
uneven as (
SELECT
    "abundant"."_virt_filter_net_paid_1417284312679536" as "_virt_filter_net_paid_1417284312679536",
    "abundant"."_virt_filter_net_paid_2204116219608499" as "_virt_filter_net_paid_2204116219608499",
    "abundant"."_virt_filter_net_paid_7283285738060558" as "_virt_filter_net_paid_7283285738060558",
    "abundant"."_virt_filter_net_paid_8931261601532283" as "_virt_filter_net_paid_8931261601532283",
    "abundant"."sales_billing_customer_id" as "sales_billing_customer_id"
FROM
    "abundant"
    FULL JOIN "questionable" on "abundant"."sales_item_id" = "questionable"."sales_item_id" AND "abundant"."sales_order_id" = "questionable"."sales_order_id" AND "abundant"."sales_sales_channel" = "questionable"."sales_sales_channel"
GROUP BY
    1,
    2,
    3,
    4,
    5,
    coalesce("abundant"."sales_item_id","questionable"."sales_item_id"),
    coalesce("abundant"."sales_order_id","questionable"."sales_order_id"),
    coalesce("abundant"."sales_sales_channel","questionable"."sales_sales_channel")),
juicy as (
SELECT
    "uneven"."sales_billing_customer_id" as "sales_billing_customer_id",
    sum("uneven"."_virt_filter_net_paid_1417284312679536") as "web_first_year",
    sum("uneven"."_virt_filter_net_paid_2204116219608499") as "web_second_year",
    sum("uneven"."_virt_filter_net_paid_7283285738060558") as "store_second_year",
    sum("uneven"."_virt_filter_net_paid_8931261601532283") as "store_first_year"
FROM
    "uneven"
GROUP BY
    1
HAVING
    "store_first_year" > 0
),
vacuous as (
SELECT
    "juicy"."store_first_year" as "store_first_year",
    "juicy"."store_second_year" as "store_second_year",
    "juicy"."web_first_year" as "web_first_year",
    "juicy"."web_second_year" as "web_second_year",
    "sales_billing_customer_customers"."C_CUSTOMER_ID" as "sales_billing_customer_text_id",
    "sales_billing_customer_customers"."C_CUSTOMER_SK" as "sales_billing_customer_id",
    "sales_billing_customer_customers"."C_FIRST_NAME" as "sales_billing_customer_first_name",
    "sales_billing_customer_customers"."C_LAST_NAME" as "sales_billing_customer_last_name"
FROM
    "juicy"
    INNER JOIN "memory"."customer" as "sales_billing_customer_customers" on "juicy"."sales_billing_customer_id" = "sales_billing_customer_customers"."C_CUSTOMER_SK")
SELECT
    "vacuous"."sales_billing_customer_text_id" as "customer_id",
    "vacuous"."sales_billing_customer_first_name" as "customer_first_name",
    "vacuous"."sales_billing_customer_last_name" as "customer_last_name"
FROM
    "vacuous"
WHERE
    "vacuous"."store_first_year" > 0 and "vacuous"."web_first_year" > 0 and ( CASE
	WHEN "vacuous"."web_first_year" > 0 THEN "vacuous"."web_second_year" / "vacuous"."web_first_year"
	ELSE null
	END ) > ( CASE
	WHEN "vacuous"."store_first_year" > 0 THEN "vacuous"."store_second_year" / "vacuous"."store_first_year"
	ELSE null
	END )

ORDER BY 
    "customer_id" asc nulls first
LIMIT (100)
```
