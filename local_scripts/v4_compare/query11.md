# Query 11

**Status:** `match`

| Stage | Result |
| --- | --- |
| v4 SQL generation | OK |
| v4 execution | OK (90 rows) |
| reference execution | OK (90 rows) |
| results identical | YES |

## Result comparison

v4 rows: 90 (90 distinct)
ref rows: 90 (90 distinct)

## SQL size + execution time

| Source | Chars | Lines | Exec (min of 4) |
| --- | --- | --- | --- |
| v4 | 3579 | 63 | 80.70 ms |
| reference | 5077 | 97 | 139.74 ms |
| v4 / ref | 0.70x | 0.65x | 0.58x |

## Preql

```
import unified_sales as sales;

const first_year <- 2001;
const second_year <- 2002;

def channel_year_total(channel, year) -> sum(
    sales.ext_list_price - sales.ext_discount_amount ? sales.sales_channel = channel and sales.date.year = year
)
    by sales.customer.id;

auto store_first_year <- sum(
    sales.ext_list_price - sales.ext_discount_amount ? sales.sales_channel = 'STORE' and sales.date.year = first_year
)
    by sales.customer.id;
auto web_first_year <- sum(
    sales.ext_list_price - sales.ext_discount_amount ? sales.sales_channel = 'WEB' and sales.date.year = first_year
)
    by sales.customer.id;
auto store_second_year <- sum(
    sales.ext_list_price - sales.ext_discount_amount ? sales.sales_channel = 'STORE' and sales.date.year = second_year
)
    by sales.customer.id;
auto web_second_year <- sum(
    sales.ext_list_price - sales.ext_discount_amount ? sales.sales_channel = 'WEB' and sales.date.year = second_year
)
    by sales.customer.id;

where
    sales.sales_channel in ('STORE', 'WEB')
    and sales.date.year in (first_year, second_year)
    and sales.customer.id is not null
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
    sales.customer.text_id,
    sales.customer.first_name,
    sales.customer.last_name,
    sales.customer.preferred_cust_flag,
order by
    sales.customer.text_id asc nulls first,
    sales.customer.first_name asc nulls first,
    sales.customer.last_name asc nulls first,
    sales.customer.preferred_cust_flag asc nulls first
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
    "sales_catalog_sales_unified"."CS_EXT_DISCOUNT_AMT" as "sales_ext_discount_amount",
    "sales_catalog_sales_unified"."CS_EXT_LIST_PRICE" as "sales_ext_list_price",
     'CATALOG'  as "sales_sales_channel"
FROM
    "memory"."catalog_sales" as "sales_catalog_sales_unified"
UNION ALL
SELECT
    "sales_store_sales_unified"."SS_CUSTOMER_SK" as "sales_customer_id",
    "sales_store_sales_unified"."SS_SOLD_DATE_SK" as "sales_date_id",
    "sales_store_sales_unified"."SS_EXT_DISCOUNT_AMT" as "sales_ext_discount_amount",
    "sales_store_sales_unified"."SS_EXT_LIST_PRICE" as "sales_ext_list_price",
     'STORE'  as "sales_sales_channel"
FROM
    "memory"."store_sales" as "sales_store_sales_unified"
UNION ALL
SELECT
    "sales_web_sales_unified"."WS_BILL_CUSTOMER_SK" as "sales_customer_id",
    "sales_web_sales_unified"."WS_SOLD_DATE_SK" as "sales_date_id",
    "sales_web_sales_unified"."WS_EXT_DISCOUNT_AMT" as "sales_ext_discount_amount",
    "sales_web_sales_unified"."WS_EXT_LIST_PRICE" as "sales_ext_list_price",
     'WEB'  as "sales_sales_channel"
FROM
    "memory"."web_sales" as "sales_web_sales_unified"),
cooperative as (
SELECT
    "cheerful"."sales_customer_id" as "sales_customer_id",
    sum("cheerful"."sales_ext_list_price" - CASE WHEN "cheerful"."sales_sales_channel" = 'STORE' and "sales_date_date"."D_YEAR" = 2001 THEN "cheerful"."sales_ext_discount_amount" ELSE NULL END) as "store_first_year",
    sum("cheerful"."sales_ext_list_price" - CASE WHEN "cheerful"."sales_sales_channel" = 'STORE' and "sales_date_date"."D_YEAR" = 2002 THEN "cheerful"."sales_ext_discount_amount" ELSE NULL END) as "store_second_year",
    sum("cheerful"."sales_ext_list_price" - CASE WHEN "cheerful"."sales_sales_channel" = 'WEB' and "sales_date_date"."D_YEAR" = 2001 THEN "cheerful"."sales_ext_discount_amount" ELSE NULL END) as "web_first_year",
    sum("cheerful"."sales_ext_list_price" - CASE WHEN "cheerful"."sales_sales_channel" = 'WEB' and "sales_date_date"."D_YEAR" = 2002 THEN "cheerful"."sales_ext_discount_amount" ELSE NULL END) as "web_second_year"
FROM
    "cheerful"
    LEFT OUTER JOIN "memory"."date_dim" as "sales_date_date" on "cheerful"."sales_date_id" = "sales_date_date"."D_DATE_SK"
GROUP BY
    1)
SELECT
    "sales_customer_customers"."C_FIRST_NAME" as "sales_customer_first_name",
    "sales_customer_customers"."C_LAST_NAME" as "sales_customer_last_name",
    "sales_customer_customers"."C_PREFERRED_CUST_FLAG" as "sales_customer_preferred_cust_flag",
    "sales_customer_customers"."C_CUSTOMER_ID" as "sales_customer_text_id"
FROM
    "cooperative"
    LEFT OUTER JOIN "memory"."customer" as "sales_customer_customers" on "cooperative"."sales_customer_id" = "sales_customer_customers"."C_CUSTOMER_SK"
WHERE
    "cooperative"."store_first_year" > 0 and "cooperative"."web_first_year" > 0 and ( CASE
	WHEN "cooperative"."web_first_year" > 0 THEN ("cooperative"."web_second_year" * 1.0) / "cooperative"."web_first_year"
	ELSE 0.0
	END ) > ( CASE
	WHEN "cooperative"."store_first_year" > 0 THEN ("cooperative"."store_second_year" * 1.0) / "cooperative"."store_first_year"
	ELSE 0.0
	END )

ORDER BY 
    "sales_customer_customers"."C_CUSTOMER_ID" asc nulls first,
    "sales_customer_customers"."C_FIRST_NAME" asc nulls first,
    "sales_customer_customers"."C_LAST_NAME" asc nulls first,
    "sales_customer_customers"."C_PREFERRED_CUST_FLAG" asc nulls first
LIMIT (100)
```

## Reference SQL (zquery log)

```sql
WITH 
cheerful as (
SELECT
    "sales_store_sales_unified"."SS_CUSTOMER_SK" as "sales_customer_id",
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
    "sales_web_sales_unified"."WS_BILL_CUSTOMER_SK" as "sales_customer_id",
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
uneven as (
SELECT
    "cheerful"."sales_customer_id" as "sales_customer_id"
FROM
    "cheerful"
GROUP BY
    1
HAVING
    sum("cheerful"."sales_ext_list_price" - CASE WHEN "cheerful"."sales_sales_channel" = 'STORE' and "cheerful"."sales_date_year" = 2001 THEN "cheerful"."sales_ext_discount_amount" ELSE NULL END) > 0 and sum("cheerful"."sales_ext_list_price" - CASE WHEN "cheerful"."sales_sales_channel" = 'WEB' and "cheerful"."sales_date_year" = 2001 THEN "cheerful"."sales_ext_discount_amount" ELSE NULL END) > 0 and ( CASE
	WHEN sum("cheerful"."sales_ext_list_price" - CASE WHEN "cheerful"."sales_sales_channel" = 'WEB' and "cheerful"."sales_date_year" = 2001 THEN "cheerful"."sales_ext_discount_amount" ELSE NULL END) > 0 THEN (sum("cheerful"."sales_ext_list_price" - CASE WHEN "cheerful"."sales_sales_channel" = 'WEB' and "cheerful"."sales_date_year" = 2002 THEN "cheerful"."sales_ext_discount_amount" ELSE NULL END) * 1.0) / sum("cheerful"."sales_ext_list_price" - CASE WHEN "cheerful"."sales_sales_channel" = 'WEB' and "cheerful"."sales_date_year" = 2001 THEN "cheerful"."sales_ext_discount_amount" ELSE NULL END)
	ELSE 0.0
	END ) > ( CASE
	WHEN sum("cheerful"."sales_ext_list_price" - CASE WHEN "cheerful"."sales_sales_channel" = 'STORE' and "cheerful"."sales_date_year" = 2001 THEN "cheerful"."sales_ext_discount_amount" ELSE NULL END) > 0 THEN (sum("cheerful"."sales_ext_list_price" - CASE WHEN "cheerful"."sales_sales_channel" = 'STORE' and "cheerful"."sales_date_year" = 2002 THEN "cheerful"."sales_ext_discount_amount" ELSE NULL END) * 1.0) / sum("cheerful"."sales_ext_list_price" - CASE WHEN "cheerful"."sales_sales_channel" = 'STORE' and "cheerful"."sales_date_year" = 2001 THEN "cheerful"."sales_ext_discount_amount" ELSE NULL END)
	ELSE 0.0
	END )
),
abundant as (
SELECT
    "sales_customer_customers"."C_CUSTOMER_ID" as "sales_customer_text_id",
    "sales_customer_customers"."C_FIRST_NAME" as "sales_customer_first_name",
    "sales_customer_customers"."C_LAST_NAME" as "sales_customer_last_name",
    "sales_customer_customers"."C_PREFERRED_CUST_FLAG" as "sales_customer_preferred_cust_flag",
    "thoughtful"."sales_customer_id" as "sales_customer_id"
FROM
    "thoughtful"
    INNER JOIN "memory"."date_dim" as "sales_date_date" on "thoughtful"."sales_date_id" = "sales_date_date"."D_DATE_SK"
    LEFT OUTER JOIN "memory"."customer" as "sales_customer_customers" on "thoughtful"."sales_customer_id" = "sales_customer_customers"."C_CUSTOMER_SK"
WHERE
    "sales_date_date"."D_YEAR" in (2001,2002)

GROUP BY
    1,
    2,
    3,
    4,
    5,
    "sales_date_date"."D_YEAR",
    "thoughtful"."sales_sales_channel")
SELECT
    "abundant"."sales_customer_text_id" as "sales_customer_text_id",
    "abundant"."sales_customer_first_name" as "sales_customer_first_name",
    "abundant"."sales_customer_last_name" as "sales_customer_last_name",
    "abundant"."sales_customer_preferred_cust_flag" as "sales_customer_preferred_cust_flag"
FROM
    "abundant"
    INNER JOIN "uneven" on "abundant"."sales_customer_id" = "uneven"."sales_customer_id"
GROUP BY
    1,
    2,
    3,
    4
ORDER BY 
    "abundant"."sales_customer_text_id" asc nulls first,
    "abundant"."sales_customer_first_name" asc nulls first,
    "abundant"."sales_customer_last_name" asc nulls first,
    "abundant"."sales_customer_preferred_cust_flag" asc nulls first
LIMIT (100)
```
