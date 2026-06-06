# Query 11

**Status:** `match`

| Stage | Result |
| --- | --- |
| v4 SQL generation | OK |
| v4 execution | OK (0 rows) |
| reference execution | OK (0 rows) |
| results identical | YES |

## Result comparison

v4 rows: 0 (0 distinct)
ref rows: 0 (0 distinct)

## SQL size + execution time

| Source | Chars | Lines | Exec (min of 4) |
| --- | --- | --- | --- |
| v4 | 4191 | 59 | 6.49 ms |
| reference | 5425 | 97 | 10.56 ms |
| v4 / ref | 0.77x | 0.61x | 0.61x |

## Preql

```
import all_sales as sales;

const first_year <- 2001;
const second_year <- 2002;

def channel_year_total(channel, year) -> sum(
    sales.ext_list_price - sales.ext_discount_amount ? sales.sales_channel = channel and sales.date.year = year
)
    by sales.billing_customer.id;

auto store_first_year <- sum(
    sales.ext_list_price - sales.ext_discount_amount ? sales.sales_channel = 'STORE' and sales.date.year = first_year
)
    by sales.billing_customer.id;
auto web_first_year <- sum(
    sales.ext_list_price - sales.ext_discount_amount ? sales.sales_channel = 'WEB' and sales.date.year = first_year
)
    by sales.billing_customer.id;
auto store_second_year <- sum(
    sales.ext_list_price - sales.ext_discount_amount ? sales.sales_channel = 'STORE' and sales.date.year = second_year
)
    by sales.billing_customer.id;
auto web_second_year <- sum(
    sales.ext_list_price - sales.ext_discount_amount ? sales.sales_channel = 'WEB' and sales.date.year = second_year
)
    by sales.billing_customer.id;

where
    sales.sales_channel in ('STORE', 'WEB')
    and sales.date.year in (first_year, second_year)
    and sales.billing_customer.id is not null
    and store_first_year > 0
    and web_first_year > 0
    and (case
            when web_first_year > 0 then (web_second_year * 1.0) / web_first_year
            else 0.0
        end) > (case
            when store_first_year > 0 then (store_second_year * 1.0) / store_first_year
            else 0.0
        end)
select
    sales.billing_customer.text_id,
    sales.billing_customer.first_name,
    sales.billing_customer.last_name,
    sales.billing_customer.preferred_cust_flag,
order by
    sales.billing_customer.text_id asc nulls first,
    sales.billing_customer.first_name asc nulls first,
    sales.billing_customer.last_name asc nulls first,
    sales.billing_customer.preferred_cust_flag asc nulls first
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
    "sales_catalog_sales_unified"."CS_EXT_DISCOUNT_AMT" as "sales_ext_discount_amount",
    "sales_catalog_sales_unified"."CS_EXT_LIST_PRICE" as "sales_ext_list_price",
     'CATALOG'  as "sales_sales_channel"
FROM
    "memory"."catalog_sales" as "sales_catalog_sales_unified"
UNION ALL
SELECT
    "sales_store_sales_unified"."SS_CUSTOMER_SK" as "sales_billing_customer_id",
    "sales_store_sales_unified"."SS_SOLD_DATE_SK" as "sales_date_id",
    "sales_store_sales_unified"."SS_EXT_DISCOUNT_AMT" as "sales_ext_discount_amount",
    "sales_store_sales_unified"."SS_EXT_LIST_PRICE" as "sales_ext_list_price",
     'STORE'  as "sales_sales_channel"
FROM
    "memory"."store_sales" as "sales_store_sales_unified"
UNION ALL
SELECT
    "sales_web_sales_unified"."WS_BILL_CUSTOMER_SK" as "sales_billing_customer_id",
    "sales_web_sales_unified"."WS_SOLD_DATE_SK" as "sales_date_id",
    "sales_web_sales_unified"."WS_EXT_DISCOUNT_AMT" as "sales_ext_discount_amount",
    "sales_web_sales_unified"."WS_EXT_LIST_PRICE" as "sales_ext_list_price",
     'WEB'  as "sales_sales_channel"
FROM
    "memory"."web_sales" as "sales_web_sales_unified"),
questionable as (
SELECT
    "thoughtful"."sales_billing_customer_id" as "sales_billing_customer_id"
FROM
    "thoughtful"
    LEFT OUTER JOIN "memory"."date_dim" as "sales_date_date" on "thoughtful"."sales_date_id" = "sales_date_date"."D_DATE_SK"
GROUP BY
    1
HAVING
    sum("thoughtful"."sales_ext_list_price" - CASE WHEN "thoughtful"."sales_sales_channel" = 'STORE' and "sales_date_date"."D_YEAR" = 2001 THEN "thoughtful"."sales_ext_discount_amount" ELSE NULL END) > 0 and sum("thoughtful"."sales_ext_list_price" - CASE WHEN "thoughtful"."sales_sales_channel" = 'WEB' and "sales_date_date"."D_YEAR" = 2001 THEN "thoughtful"."sales_ext_discount_amount" ELSE NULL END) > 0 and ( CASE
	WHEN sum("thoughtful"."sales_ext_list_price" - CASE WHEN "thoughtful"."sales_sales_channel" = 'WEB' and "sales_date_date"."D_YEAR" = 2001 THEN "thoughtful"."sales_ext_discount_amount" ELSE NULL END) > 0 THEN (sum("thoughtful"."sales_ext_list_price" - CASE WHEN "thoughtful"."sales_sales_channel" = 'WEB' and "sales_date_date"."D_YEAR" = 2002 THEN "thoughtful"."sales_ext_discount_amount" ELSE NULL END) * 1.0) / sum("thoughtful"."sales_ext_list_price" - CASE WHEN "thoughtful"."sales_sales_channel" = 'WEB' and "sales_date_date"."D_YEAR" = 2001 THEN "thoughtful"."sales_ext_discount_amount" ELSE NULL END)
	ELSE 0.0
	END ) > ( CASE
	WHEN sum("thoughtful"."sales_ext_list_price" - CASE WHEN "thoughtful"."sales_sales_channel" = 'STORE' and "sales_date_date"."D_YEAR" = 2001 THEN "thoughtful"."sales_ext_discount_amount" ELSE NULL END) > 0 THEN (sum("thoughtful"."sales_ext_list_price" - CASE WHEN "thoughtful"."sales_sales_channel" = 'STORE' and "sales_date_date"."D_YEAR" = 2002 THEN "thoughtful"."sales_ext_discount_amount" ELSE NULL END) * 1.0) / sum("thoughtful"."sales_ext_list_price" - CASE WHEN "thoughtful"."sales_sales_channel" = 'STORE' and "sales_date_date"."D_YEAR" = 2001 THEN "thoughtful"."sales_ext_discount_amount" ELSE NULL END)
	ELSE 0.0
	END )
)
SELECT
    "sales_billing_customer_customers"."C_CUSTOMER_ID" as "sales_billing_customer_text_id",
    "sales_billing_customer_customers"."C_FIRST_NAME" as "sales_billing_customer_first_name",
    "sales_billing_customer_customers"."C_LAST_NAME" as "sales_billing_customer_last_name",
    "sales_billing_customer_customers"."C_PREFERRED_CUST_FLAG" as "sales_billing_customer_preferred_cust_flag"
FROM
    "questionable"
    LEFT OUTER JOIN "memory"."customer" as "sales_billing_customer_customers" on "questionable"."sales_billing_customer_id" = "sales_billing_customer_customers"."C_CUSTOMER_SK"
ORDER BY 
    "sales_billing_customer_customers"."C_CUSTOMER_ID" asc nulls first,
    "sales_billing_customer_customers"."C_FIRST_NAME" asc nulls first,
    "sales_billing_customer_customers"."C_LAST_NAME" asc nulls first,
    "sales_billing_customer_customers"."C_PREFERRED_CUST_FLAG" asc nulls first
LIMIT (100)
```

## Reference SQL (zquery log)

```sql
WITH 
thoughtful as (
SELECT
    "sales_store_sales_unified"."SS_CUSTOMER_SK" as "sales_billing_customer_id",
    "sales_store_sales_unified"."SS_SOLD_DATE_SK" as "sales_date_id",
    "sales_store_sales_unified"."SS_EXT_DISCOUNT_AMT" as "sales_ext_discount_amount",
    "sales_store_sales_unified"."SS_EXT_LIST_PRICE" as "sales_ext_list_price",
     'STORE'  as "sales_sales_channel",
    "sales_date_date"."D_YEAR" as "sales_date_year"
FROM
    "memory"."store_sales" as "sales_store_sales_unified"
    INNER JOIN "memory"."date_dim" as "sales_date_date" on "sales_store_sales_unified"."SS_SOLD_DATE_SK" = "sales_date_date"."D_DATE_SK"
WHERE
    "sales_store_sales_unified"."SS_CUSTOMER_SK" is not null and "sales_date_date"."D_YEAR" in (2001,2002)

UNION ALL
SELECT
    "sales_web_sales_unified"."WS_BILL_CUSTOMER_SK" as "sales_billing_customer_id",
    "sales_web_sales_unified"."WS_SOLD_DATE_SK" as "sales_date_id",
    "sales_web_sales_unified"."WS_EXT_DISCOUNT_AMT" as "sales_ext_discount_amount",
    "sales_web_sales_unified"."WS_EXT_LIST_PRICE" as "sales_ext_list_price",
     'WEB'  as "sales_sales_channel",
    "sales_date_date"."D_YEAR" as "sales_date_year"
FROM
    "memory"."web_sales" as "sales_web_sales_unified"
    INNER JOIN "memory"."date_dim" as "sales_date_date" on "sales_web_sales_unified"."WS_SOLD_DATE_SK" = "sales_date_date"."D_DATE_SK"
WHERE
    "sales_web_sales_unified"."WS_BILL_CUSTOMER_SK" is not null and "sales_date_date"."D_YEAR" in (2001,2002)
),
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
uneven as (
SELECT
    "thoughtful"."sales_billing_customer_id" as "sales_billing_customer_id"
FROM
    "thoughtful"
GROUP BY
    1
HAVING
    sum("thoughtful"."sales_ext_list_price" - CASE WHEN "thoughtful"."sales_sales_channel" = 'STORE' and "thoughtful"."sales_date_year" = 2001 THEN "thoughtful"."sales_ext_discount_amount" ELSE NULL END) > 0 and sum("thoughtful"."sales_ext_list_price" - CASE WHEN "thoughtful"."sales_sales_channel" = 'WEB' and "thoughtful"."sales_date_year" = 2001 THEN "thoughtful"."sales_ext_discount_amount" ELSE NULL END) > 0 and ( CASE
	WHEN sum("thoughtful"."sales_ext_list_price" - CASE WHEN "thoughtful"."sales_sales_channel" = 'WEB' and "thoughtful"."sales_date_year" = 2001 THEN "thoughtful"."sales_ext_discount_amount" ELSE NULL END) > 0 THEN (sum("thoughtful"."sales_ext_list_price" - CASE WHEN "thoughtful"."sales_sales_channel" = 'WEB' and "thoughtful"."sales_date_year" = 2002 THEN "thoughtful"."sales_ext_discount_amount" ELSE NULL END) * 1.0) / sum("thoughtful"."sales_ext_list_price" - CASE WHEN "thoughtful"."sales_sales_channel" = 'WEB' and "thoughtful"."sales_date_year" = 2001 THEN "thoughtful"."sales_ext_discount_amount" ELSE NULL END)
	ELSE 0.0
	END ) > ( CASE
	WHEN sum("thoughtful"."sales_ext_list_price" - CASE WHEN "thoughtful"."sales_sales_channel" = 'STORE' and "thoughtful"."sales_date_year" = 2001 THEN "thoughtful"."sales_ext_discount_amount" ELSE NULL END) > 0 THEN (sum("thoughtful"."sales_ext_list_price" - CASE WHEN "thoughtful"."sales_sales_channel" = 'STORE' and "thoughtful"."sales_date_year" = 2002 THEN "thoughtful"."sales_ext_discount_amount" ELSE NULL END) * 1.0) / sum("thoughtful"."sales_ext_list_price" - CASE WHEN "thoughtful"."sales_sales_channel" = 'STORE' and "thoughtful"."sales_date_year" = 2001 THEN "thoughtful"."sales_ext_discount_amount" ELSE NULL END)
	ELSE 0.0
	END )
),
abundant as (
SELECT
    "cooperative"."sales_billing_customer_id" as "sales_billing_customer_id",
    "sales_billing_customer_customers"."C_CUSTOMER_ID" as "sales_billing_customer_text_id",
    "sales_billing_customer_customers"."C_FIRST_NAME" as "sales_billing_customer_first_name",
    "sales_billing_customer_customers"."C_LAST_NAME" as "sales_billing_customer_last_name",
    "sales_billing_customer_customers"."C_PREFERRED_CUST_FLAG" as "sales_billing_customer_preferred_cust_flag"
FROM
    "cooperative"
    INNER JOIN "memory"."date_dim" as "sales_date_date" on "cooperative"."sales_date_id" = "sales_date_date"."D_DATE_SK"
    LEFT OUTER JOIN "memory"."customer" as "sales_billing_customer_customers" on "cooperative"."sales_billing_customer_id" = "sales_billing_customer_customers"."C_CUSTOMER_SK"
WHERE
    "sales_date_date"."D_YEAR" in (2001,2002)

GROUP BY
    1,
    2,
    3,
    4,
    5,
    "cooperative"."sales_sales_channel",
    "sales_date_date"."D_YEAR")
SELECT
    "abundant"."sales_billing_customer_text_id" as "sales_billing_customer_text_id",
    "abundant"."sales_billing_customer_first_name" as "sales_billing_customer_first_name",
    "abundant"."sales_billing_customer_last_name" as "sales_billing_customer_last_name",
    "abundant"."sales_billing_customer_preferred_cust_flag" as "sales_billing_customer_preferred_cust_flag"
FROM
    "abundant"
    INNER JOIN "uneven" on "abundant"."sales_billing_customer_id" = "uneven"."sales_billing_customer_id"
GROUP BY
    1,
    2,
    3,
    4
ORDER BY 
    "abundant"."sales_billing_customer_text_id" asc nulls first,
    "abundant"."sales_billing_customer_first_name" asc nulls first,
    "abundant"."sales_billing_customer_last_name" asc nulls first,
    "abundant"."sales_billing_customer_preferred_cust_flag" asc nulls first
LIMIT (100)
```
