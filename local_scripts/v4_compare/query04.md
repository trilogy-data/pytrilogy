# Query 04

**Status:** `match`

| Stage | Result |
| --- | --- |
| v4 SQL generation | OK |
| v4 execution | OK (6 rows) |
| reference execution | OK (6 rows) |
| results identical | YES |

## Result comparison

v4 rows: 6 (6 distinct)
ref rows: 6 (6 distinct)

## SQL size + execution time

| Source | Chars | Lines | Exec (min of 4) |
| --- | --- | --- | --- |
| v4 | 5970 | 95 | 103.54 ms |
| reference | 12153 | 278 | 465.05 ms |
| v4 / ref | 0.49x | 0.34x | 0.22x |

## Preql

```
import unified_sales as sales;

const first_year <- 2001;
const second_year <- 2002;

def channel_year_total(channel, year) -> sum(
    (sales.ext_list_price - sales.ext_wholesale_cost - sales.ext_discount_amount + sales.ext_sales_price) / (2) ? sales.sales_channel = channel and sales.date.year = year
)
    by sales.customer.id;

auto store_first_year <- @channel_year_total('STORE', first_year);
auto catalog_first_year <- @channel_year_total('CATALOG', first_year);
auto web_first_year <- @channel_year_total('WEB', first_year);
auto store_second_year <- @channel_year_total('STORE', second_year);
auto catalog_second_year <- @channel_year_total('CATALOG', second_year);
auto web_second_year <- @channel_year_total('WEB', second_year);

where
    sales.date.year in (first_year, second_year)
    and store_first_year > 0
    and catalog_first_year > 0
    and web_first_year > 0
    and (case
            when catalog_first_year > 0 then catalog_second_year / catalog_first_year
            else null
        end) > (case
            when store_first_year > 0 then store_second_year / store_first_year
            else null
        end)
    and (case
            when catalog_first_year > 0 then catalog_second_year / catalog_first_year
            else null
        end) > (case
            when web_first_year > 0 then web_second_year / web_first_year
            else null
        end)
select
    sales.customer.text_id as customer_id,
    sales.customer.first_name as customer_first_name,
    sales.customer.last_name as customer_last_name,
    sales.customer.preferred_cust_flag as customer_preferred_cust_flag,
order by
    customer_id asc nulls first,
    customer_first_name asc nulls first,
    customer_last_name asc nulls first,
    customer_preferred_cust_flag asc nulls first
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
    "sales_catalog_sales_unified"."CS_EXT_SALES_PRICE" as "sales_ext_sales_price",
    "sales_catalog_sales_unified"."CS_EXT_WHOLESALE_COST" as "sales_ext_wholesale_cost",
     'CATALOG'  as "sales_sales_channel"
FROM
    "memory"."catalog_sales" as "sales_catalog_sales_unified"
UNION ALL
SELECT
    "sales_store_sales_unified"."SS_CUSTOMER_SK" as "sales_customer_id",
    "sales_store_sales_unified"."SS_SOLD_DATE_SK" as "sales_date_id",
    "sales_store_sales_unified"."SS_EXT_DISCOUNT_AMT" as "sales_ext_discount_amount",
    "sales_store_sales_unified"."SS_EXT_LIST_PRICE" as "sales_ext_list_price",
    "sales_store_sales_unified"."SS_EXT_SALES_PRICE" as "sales_ext_sales_price",
    "sales_store_sales_unified"."SS_EXT_WHOLESALE_COST" as "sales_ext_wholesale_cost",
     'STORE'  as "sales_sales_channel"
FROM
    "memory"."store_sales" as "sales_store_sales_unified"
UNION ALL
SELECT
    "sales_web_sales_unified"."WS_BILL_CUSTOMER_SK" as "sales_customer_id",
    "sales_web_sales_unified"."WS_SOLD_DATE_SK" as "sales_date_id",
    "sales_web_sales_unified"."WS_EXT_DISCOUNT_AMT" as "sales_ext_discount_amount",
    "sales_web_sales_unified"."WS_EXT_LIST_PRICE" as "sales_ext_list_price",
    "sales_web_sales_unified"."WS_EXT_SALES_PRICE" as "sales_ext_sales_price",
    "sales_web_sales_unified"."WS_EXT_WHOLESALE_COST" as "sales_ext_wholesale_cost",
     'WEB'  as "sales_sales_channel"
FROM
    "memory"."web_sales" as "sales_web_sales_unified"),
cooperative as (
SELECT
    "cheerful"."sales_customer_id" as "sales_customer_id",
    sum((( ( "cheerful"."sales_ext_list_price" - "cheerful"."sales_ext_wholesale_cost" ) - "cheerful"."sales_ext_discount_amount" ) + "cheerful"."sales_ext_sales_price") / CASE WHEN "cheerful"."sales_sales_channel" = 'CATALOG' and "sales_date_date"."D_YEAR" = 2001 THEN 2 ELSE NULL END) as "catalog_first_year",
    sum((( ( "cheerful"."sales_ext_list_price" - "cheerful"."sales_ext_wholesale_cost" ) - "cheerful"."sales_ext_discount_amount" ) + "cheerful"."sales_ext_sales_price") / CASE WHEN "cheerful"."sales_sales_channel" = 'CATALOG' and "sales_date_date"."D_YEAR" = 2002 THEN 2 ELSE NULL END) as "catalog_second_year",
    sum((( ( "cheerful"."sales_ext_list_price" - "cheerful"."sales_ext_wholesale_cost" ) - "cheerful"."sales_ext_discount_amount" ) + "cheerful"."sales_ext_sales_price") / CASE WHEN "cheerful"."sales_sales_channel" = 'STORE' and "sales_date_date"."D_YEAR" = 2001 THEN 2 ELSE NULL END) as "store_first_year",
    sum((( ( "cheerful"."sales_ext_list_price" - "cheerful"."sales_ext_wholesale_cost" ) - "cheerful"."sales_ext_discount_amount" ) + "cheerful"."sales_ext_sales_price") / CASE WHEN "cheerful"."sales_sales_channel" = 'STORE' and "sales_date_date"."D_YEAR" = 2002 THEN 2 ELSE NULL END) as "store_second_year",
    sum((( ( "cheerful"."sales_ext_list_price" - "cheerful"."sales_ext_wholesale_cost" ) - "cheerful"."sales_ext_discount_amount" ) + "cheerful"."sales_ext_sales_price") / CASE WHEN "cheerful"."sales_sales_channel" = 'WEB' and "sales_date_date"."D_YEAR" = 2001 THEN 2 ELSE NULL END) as "web_first_year",
    sum((( ( "cheerful"."sales_ext_list_price" - "cheerful"."sales_ext_wholesale_cost" ) - "cheerful"."sales_ext_discount_amount" ) + "cheerful"."sales_ext_sales_price") / CASE WHEN "cheerful"."sales_sales_channel" = 'WEB' and "sales_date_date"."D_YEAR" = 2002 THEN 2 ELSE NULL END) as "web_second_year"
FROM
    "cheerful"
    LEFT OUTER JOIN "memory"."date_dim" as "sales_date_date" on "cheerful"."sales_date_id" = "sales_date_date"."D_DATE_SK"
GROUP BY
    1),
yummy as (
SELECT
    "cooperative"."catalog_first_year" as "catalog_first_year",
    "cooperative"."catalog_second_year" as "catalog_second_year",
    "cooperative"."sales_customer_id" as "sales_customer_id",
    "cooperative"."store_first_year" as "store_first_year",
    "cooperative"."store_second_year" as "store_second_year",
    "cooperative"."web_first_year" as "web_first_year",
    "cooperative"."web_second_year" as "web_second_year",
    "sales_customer_customers"."C_CUSTOMER_ID" as "sales_customer_text_id",
    "sales_customer_customers"."C_FIRST_NAME" as "sales_customer_first_name",
    "sales_customer_customers"."C_LAST_NAME" as "sales_customer_last_name",
    "sales_customer_customers"."C_PREFERRED_CUST_FLAG" as "sales_customer_preferred_cust_flag"
FROM
    "cooperative"
    LEFT OUTER JOIN "memory"."customer" as "sales_customer_customers" on "cooperative"."sales_customer_id" = "sales_customer_customers"."C_CUSTOMER_SK"
WHERE
    "cooperative"."store_first_year" > 0
)
SELECT
    "yummy"."sales_customer_first_name" as "customer_first_name",
    "yummy"."sales_customer_text_id" as "customer_id",
    "yummy"."sales_customer_last_name" as "customer_last_name",
    "yummy"."sales_customer_preferred_cust_flag" as "customer_preferred_cust_flag"
FROM
    "yummy"
WHERE
    "yummy"."catalog_first_year" > 0 and "yummy"."web_first_year" > 0 and ( CASE
	WHEN "yummy"."catalog_first_year" > 0 THEN "yummy"."catalog_second_year" / "yummy"."catalog_first_year"
	ELSE null
	END ) > ( CASE
	WHEN "yummy"."store_first_year" > 0 THEN "yummy"."store_second_year" / "yummy"."store_first_year"
	ELSE null
	END ) and ( CASE
	WHEN "yummy"."catalog_first_year" > 0 THEN "yummy"."catalog_second_year" / "yummy"."catalog_first_year"
	ELSE null
	END ) > ( CASE
	WHEN "yummy"."web_first_year" > 0 THEN "yummy"."web_second_year" / "yummy"."web_first_year"
	ELSE null
	END )

ORDER BY 
    "customer_id" asc nulls first,
    "customer_first_name" asc nulls first,
    "customer_last_name" asc nulls first,
    "customer_preferred_cust_flag" asc nulls first
LIMIT (100)
```

## Reference SQL (zquery log)

```sql
WITH 
cheerful as (
SELECT
    "sales_catalog_sales_unified"."CS_BILL_CUSTOMER_SK" as "sales_customer_id",
    "sales_catalog_sales_unified"."CS_SOLD_DATE_SK" as "sales_date_id",
    "sales_catalog_sales_unified"."CS_EXT_DISCOUNT_AMT" as "sales_ext_discount_amount",
    "sales_catalog_sales_unified"."CS_EXT_LIST_PRICE" as "sales_ext_list_price",
    "sales_catalog_sales_unified"."CS_EXT_SALES_PRICE" as "sales_ext_sales_price",
    "sales_catalog_sales_unified"."CS_EXT_WHOLESALE_COST" as "sales_ext_wholesale_cost",
     'CATALOG'  as "sales_sales_channel",
    "sales_date_date"."D_YEAR" as "sales_date_year"
FROM
    "memory"."catalog_sales" as "sales_catalog_sales_unified"
    INNER JOIN "memory"."date_dim" as "sales_date_date" on "sales_catalog_sales_unified"."CS_SOLD_DATE_SK" = "sales_date_date"."D_DATE_SK"
WHERE
    "sales_date_date"."D_YEAR" in (2001,2002)

UNION ALL
SELECT
    "sales_store_sales_unified"."SS_CUSTOMER_SK" as "sales_customer_id",
    "sales_store_sales_unified"."SS_SOLD_DATE_SK" as "sales_date_id",
    "sales_store_sales_unified"."SS_EXT_DISCOUNT_AMT" as "sales_ext_discount_amount",
    "sales_store_sales_unified"."SS_EXT_LIST_PRICE" as "sales_ext_list_price",
    "sales_store_sales_unified"."SS_EXT_SALES_PRICE" as "sales_ext_sales_price",
    "sales_store_sales_unified"."SS_EXT_WHOLESALE_COST" as "sales_ext_wholesale_cost",
     'STORE'  as "sales_sales_channel",
    "sales_date_date"."D_YEAR" as "sales_date_year"
FROM
    "memory"."store_sales" as "sales_store_sales_unified"
    INNER JOIN "memory"."date_dim" as "sales_date_date" on "sales_store_sales_unified"."SS_SOLD_DATE_SK" = "sales_date_date"."D_DATE_SK"
WHERE
    "sales_date_date"."D_YEAR" in (2001,2002)

UNION ALL
SELECT
    "sales_web_sales_unified"."WS_BILL_CUSTOMER_SK" as "sales_customer_id",
    "sales_web_sales_unified"."WS_SOLD_DATE_SK" as "sales_date_id",
    "sales_web_sales_unified"."WS_EXT_DISCOUNT_AMT" as "sales_ext_discount_amount",
    "sales_web_sales_unified"."WS_EXT_LIST_PRICE" as "sales_ext_list_price",
    "sales_web_sales_unified"."WS_EXT_SALES_PRICE" as "sales_ext_sales_price",
    "sales_web_sales_unified"."WS_EXT_WHOLESALE_COST" as "sales_ext_wholesale_cost",
     'WEB'  as "sales_sales_channel",
    "sales_date_date"."D_YEAR" as "sales_date_year"
FROM
    "memory"."web_sales" as "sales_web_sales_unified"
    INNER JOIN "memory"."date_dim" as "sales_date_date" on "sales_web_sales_unified"."WS_SOLD_DATE_SK" = "sales_date_date"."D_DATE_SK"
WHERE
    "sales_date_date"."D_YEAR" in (2001,2002)
),
thoughtful as (
SELECT
    "cheerful"."sales_customer_id" as "sales_customer_id",
    "cheerful"."sales_date_id" as "sales_date_id"
FROM
    "cheerful"
GROUP BY
    1,
    2),
uneven as (
SELECT
    "cheerful"."sales_customer_id" as "sales_customer_id",
    "cheerful"."sales_date_year" as "sales_date_year",
    "cheerful"."sales_ext_discount_amount" as "sales_ext_discount_amount",
    "cheerful"."sales_ext_list_price" as "sales_ext_list_price",
    "cheerful"."sales_ext_sales_price" as "sales_ext_sales_price",
    "cheerful"."sales_ext_wholesale_cost" as "sales_ext_wholesale_cost",
    "cheerful"."sales_sales_channel" as "sales_sales_channel"
FROM
    "cheerful"),
abundant as (
SELECT
    "sales_customer_customers"."C_CUSTOMER_ID" as "sales_customer_text_id",
    "sales_customer_customers"."C_FIRST_NAME" as "sales_customer_first_name",
    "sales_customer_customers"."C_LAST_NAME" as "sales_customer_last_name",
    "sales_customer_customers"."C_PREFERRED_CUST_FLAG" as "sales_customer_preferred_cust_flag",
    "sales_date_date"."D_YEAR" as "sales_date_year",
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
    6),
charming as (
SELECT
    "uneven"."sales_customer_id" as "sales_customer_id",
    sum((( ( "uneven"."sales_ext_list_price" - "uneven"."sales_ext_wholesale_cost" ) - "uneven"."sales_ext_discount_amount" ) + "uneven"."sales_ext_sales_price") / CASE WHEN "uneven"."sales_sales_channel" = 'CATALOG' and "uneven"."sales_date_year" = 2001 THEN 2 ELSE NULL END) as "catalog_first_year"
FROM
    "uneven"
GROUP BY
    1
HAVING
    "catalog_first_year" > 0
),
kaput as (
SELECT
    "uneven"."sales_customer_id" as "sales_customer_id",
    sum((( ( "uneven"."sales_ext_list_price" - "uneven"."sales_ext_wholesale_cost" ) - "uneven"."sales_ext_discount_amount" ) + "uneven"."sales_ext_sales_price") / CASE WHEN "uneven"."sales_sales_channel" = 'CATALOG' and "uneven"."sales_date_year" = 2002 THEN 2 ELSE NULL END) as "catalog_second_year"
FROM
    "uneven"
WHERE
    "uneven"."sales_date_year" in (2001,2002)

GROUP BY
    1),
macho as (
SELECT
    "uneven"."sales_customer_id" as "sales_customer_id",
    sum((( ( "uneven"."sales_ext_list_price" - "uneven"."sales_ext_wholesale_cost" ) - "uneven"."sales_ext_discount_amount" ) + "uneven"."sales_ext_sales_price") / CASE WHEN "uneven"."sales_sales_channel" = 'STORE' and "uneven"."sales_date_year" = 2001 THEN 2 ELSE NULL END) as "store_first_year"
FROM
    "uneven"
WHERE
    "uneven"."sales_date_year" in (2001,2002)

GROUP BY
    1
HAVING
    "store_first_year" > 0
),
abhorrent as (
SELECT
    "uneven"."sales_customer_id" as "sales_customer_id",
    sum((( ( "uneven"."sales_ext_list_price" - "uneven"."sales_ext_wholesale_cost" ) - "uneven"."sales_ext_discount_amount" ) + "uneven"."sales_ext_sales_price") / CASE WHEN "uneven"."sales_sales_channel" = 'STORE' and "uneven"."sales_date_year" = 2002 THEN 2 ELSE NULL END) as "store_second_year"
FROM
    "uneven"
WHERE
    "uneven"."sales_date_year" in (2001,2002)

GROUP BY
    1),
concerned as (
SELECT
    "uneven"."sales_customer_id" as "sales_customer_id",
    sum((( ( "uneven"."sales_ext_list_price" - "uneven"."sales_ext_wholesale_cost" ) - "uneven"."sales_ext_discount_amount" ) + "uneven"."sales_ext_sales_price") / CASE WHEN "uneven"."sales_sales_channel" = 'WEB' and "uneven"."sales_date_year" = 2001 THEN 2 ELSE NULL END) as "web_first_year"
FROM
    "uneven"
WHERE
    "uneven"."sales_date_year" in (2001,2002)

GROUP BY
    1),
yummy as (
SELECT
    "uneven"."sales_customer_id" as "sales_customer_id",
    sum((( ( "uneven"."sales_ext_list_price" - "uneven"."sales_ext_wholesale_cost" ) - "uneven"."sales_ext_discount_amount" ) + "uneven"."sales_ext_sales_price") / CASE WHEN "uneven"."sales_sales_channel" = 'WEB' and "uneven"."sales_date_year" = 2002 THEN 2 ELSE NULL END) as "web_second_year"
FROM
    "uneven"
WHERE
    "uneven"."sales_date_year" in (2001,2002)

GROUP BY
    1),
vacuous as (
SELECT
    "abundant"."sales_customer_first_name" as "sales_customer_first_name",
    "abundant"."sales_customer_last_name" as "sales_customer_last_name",
    "abundant"."sales_customer_preferred_cust_flag" as "sales_customer_preferred_cust_flag",
    "abundant"."sales_customer_text_id" as "sales_customer_text_id",
    "abundant"."sales_date_year" as "sales_date_year",
    "yummy"."web_second_year" as "web_second_year",
    coalesce("abundant"."sales_customer_id","yummy"."sales_customer_id") as "sales_customer_id"
FROM
    "abundant"
    INNER JOIN "yummy" on "abundant"."sales_customer_id" is not distinct from "yummy"."sales_customer_id"
WHERE
    "abundant"."sales_date_year" in (2001,2002)
),
sparkling as (
SELECT
    "concerned"."web_first_year" as "web_first_year",
    "vacuous"."sales_customer_first_name" as "sales_customer_first_name",
    "vacuous"."sales_customer_last_name" as "sales_customer_last_name",
    "vacuous"."sales_customer_preferred_cust_flag" as "sales_customer_preferred_cust_flag",
    "vacuous"."sales_customer_text_id" as "sales_customer_text_id",
    "vacuous"."sales_date_year" as "sales_date_year",
    "vacuous"."web_second_year" as "web_second_year",
    coalesce("concerned"."sales_customer_id","vacuous"."sales_customer_id") as "sales_customer_id"
FROM
    "vacuous"
    FULL JOIN "concerned" on "vacuous"."sales_customer_id" is not distinct from "concerned"."sales_customer_id"
WHERE
    "vacuous"."sales_date_year" in (2001,2002)
),
late as (
SELECT
    "abhorrent"."store_second_year" as "store_second_year",
    "sparkling"."sales_customer_first_name" as "sales_customer_first_name",
    "sparkling"."sales_customer_last_name" as "sales_customer_last_name",
    "sparkling"."sales_customer_preferred_cust_flag" as "sales_customer_preferred_cust_flag",
    "sparkling"."sales_customer_text_id" as "sales_customer_text_id",
    "sparkling"."sales_date_year" as "sales_date_year",
    "sparkling"."web_first_year" as "web_first_year",
    "sparkling"."web_second_year" as "web_second_year",
    coalesce("abhorrent"."sales_customer_id","sparkling"."sales_customer_id") as "sales_customer_id"
FROM
    "sparkling"
    FULL JOIN "abhorrent" on "sparkling"."sales_customer_id" is not distinct from "abhorrent"."sales_customer_id"
WHERE
    "sparkling"."sales_date_year" in (2001,2002)
),
friendly as (
SELECT
    "late"."sales_customer_first_name" as "sales_customer_first_name",
    "late"."sales_customer_last_name" as "sales_customer_last_name",
    "late"."sales_customer_preferred_cust_flag" as "sales_customer_preferred_cust_flag",
    "late"."sales_customer_text_id" as "sales_customer_text_id",
    "late"."sales_date_year" as "sales_date_year",
    "late"."store_second_year" as "store_second_year",
    "late"."web_first_year" as "web_first_year",
    "late"."web_second_year" as "web_second_year",
    "macho"."store_first_year" as "store_first_year",
    coalesce("late"."sales_customer_id","macho"."sales_customer_id") as "sales_customer_id"
FROM
    "late"
    INNER JOIN "macho" on "late"."sales_customer_id" is not distinct from "macho"."sales_customer_id"
WHERE
    "late"."sales_date_year" in (2001,2002)
),
busy as (
SELECT
    "friendly"."sales_customer_first_name" as "sales_customer_first_name",
    "friendly"."sales_customer_last_name" as "sales_customer_last_name",
    "friendly"."sales_customer_preferred_cust_flag" as "sales_customer_preferred_cust_flag",
    "friendly"."sales_customer_text_id" as "sales_customer_text_id",
    "friendly"."store_first_year" as "store_first_year",
    "friendly"."store_second_year" as "store_second_year",
    "friendly"."web_first_year" as "web_first_year",
    "friendly"."web_second_year" as "web_second_year",
    "kaput"."catalog_second_year" as "catalog_second_year",
    coalesce("friendly"."sales_customer_id","kaput"."sales_customer_id") as "sales_customer_id"
FROM
    "friendly"
    LEFT OUTER JOIN "kaput" on "friendly"."sales_customer_id" is not distinct from "kaput"."sales_customer_id"
WHERE
    "friendly"."sales_date_year" in (2001,2002) and "friendly"."store_first_year" > 0
)
SELECT
    "busy"."sales_customer_text_id" as "customer_id",
    "busy"."sales_customer_first_name" as "customer_first_name",
    "busy"."sales_customer_last_name" as "customer_last_name",
    "busy"."sales_customer_preferred_cust_flag" as "customer_preferred_cust_flag"
FROM
    "busy"
    INNER JOIN "charming" on "busy"."sales_customer_id" is not distinct from "charming"."sales_customer_id"
WHERE
    "busy"."web_first_year" > 0 and ( CASE
	WHEN "charming"."catalog_first_year" > 0 THEN "busy"."catalog_second_year" / "charming"."catalog_first_year"
	ELSE null
	END ) > ( CASE
	WHEN "busy"."store_first_year" > 0 THEN "busy"."store_second_year" / "busy"."store_first_year"
	ELSE null
	END ) and ( CASE
	WHEN "charming"."catalog_first_year" > 0 THEN "busy"."catalog_second_year" / "charming"."catalog_first_year"
	ELSE null
	END ) > ( CASE
	WHEN "busy"."web_first_year" > 0 THEN "busy"."web_second_year" / "busy"."web_first_year"
	ELSE null
	END )

GROUP BY
    1,
    2,
    3,
    4
ORDER BY 
    "customer_id" asc nulls first,
    "customer_first_name" asc nulls first,
    "customer_last_name" asc nulls first,
    "customer_preferred_cust_flag" asc nulls first
LIMIT (100)
```
