# Query 23

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
| v4 | 6261 | 169 | 13.48 ms |
| reference | 7843 | 178 | 20.80 ms |
| v4 / ref | 0.80x | 0.95x | 0.65x |

## Preql

```
import all_sales as sales;

# frequent_ss_items: items with at least one (truncated_desc, item.id, sold_date) combo having > 4 store_sales rows in 2000-2003
auto sales.item.desc_truncated <- substring(sales.item.desc, 1, 30);
auto customer_total_in_window <- sum(sales.quantity * sales.sales_price) by sales.billing_customer.id;

rowset frequent_items <- where
    sales.sales_channel = 'STORE' and sales.date.year in (2000, 2001, 2002, 2003)
select
    sales.item.id as frequent_item_id,
    --ss_combo_count,
having
    ss_combo_count > 4

;

# max_store_sales: scalar max over customers of total ss_qty*ss_price for years 2000-2003
auto customer_total_overall <- sum(sales.quantity * sales.sales_price) by sales.billing_customer.id;

rowset max_total <- where
    sales.billing_customer.id is not null
    and sales.sales_channel = 'STORE'
    and sales.date.year in (2000, 2001, 2002, 2003)
select
    max(customer_total_in_window) as cmax,
;

# best_ss_customer: customers whose lifetime ss_qty*ss_price > 50% of cmax (no date filter)
auto ss_combo_count <- count(sales.order_id) by sales.item.desc_truncated, sales.item.id, sales.date.date;

rowset best_customers <- where
    sales.billing_customer.id is not null and sales.sales_channel = 'STORE'
select
    sales.billing_customer.id as best_customer_id,
    --customer_total_overall,
    --max_total.cmax,
having
    customer_total_overall > 0.5 * max_total.cmax

;

# Final: sum (qty * list_price) by (last, first, channel) for Feb 2000, items in frequent, customers in best
where
    sales.date.year = 2000
    and sales.date.month_of_year = 2
    and sales.item.id in frequent_items.frequent_item_id
    and sales.billing_customer.id in best_customers.best_customer_id
select
    sales.billing_customer.last_name as c_last_name,
    sales.billing_customer.first_name as c_first_name,
    sum((sales.quantity * sales.list_price) ? sales.sales_channel in ('WEB', 'CATALOG')) as sales_total,
having
    sales_total > 0

order by
    c_last_name asc nulls first,
    c_first_name asc nulls first,
    sales_total asc nulls first
limit 100
;
```

## v4 generated SQL

```sql
WITH 
charming as (
SELECT
    "sales_store_sales_unified"."SS_CUSTOMER_SK" as "sales_billing_customer_id",
    sum("sales_store_sales_unified"."SS_QUANTITY" * "sales_store_sales_unified"."SS_SALES_PRICE") as "customer_total_overall"
FROM
    "memory"."store_sales" as "sales_store_sales_unified"
WHERE
    "sales_store_sales_unified"."SS_CUSTOMER_SK" is not null

GROUP BY
    1),
thoughtful as (
SELECT
    "sales_store_sales_unified"."SS_CUSTOMER_SK" as "sales_billing_customer_id",
    "sales_store_sales_unified"."SS_SOLD_DATE_SK" as "sales_date_id",
    "sales_store_sales_unified"."SS_ITEM_SK" as "sales_item_id",
    "sales_store_sales_unified"."SS_TICKET_NUMBER" as "sales_order_id",
    "sales_store_sales_unified"."SS_QUANTITY" as "sales_quantity",
     'STORE'  as "sales_sales_channel",
    "sales_store_sales_unified"."SS_SALES_PRICE" as "sales_sales_price",
    "sales_store_sales_unified"."SS_LIST_PRICE" as "sales_list_price"
FROM
    "memory"."store_sales" as "sales_store_sales_unified"),
juicy as (
SELECT
    "sales_store_returns_unified"."SR_ITEM_SK" as "sales_item_id",
    "sales_store_returns_unified"."SR_TICKET_NUMBER" as "sales_order_id",
     'STORE'  as "sales_sales_channel"
FROM
    "memory"."store_returns" as "sales_store_returns_unified"),
concerned as (
SELECT
    "sales_item_items"."I_ITEM_SK" as "sales_item_id",
    SUBSTRING("sales_item_items"."I_ITEM_DESC",1,30) as "sales_item_desc_truncated"
FROM
    "memory"."item" as "sales_item_items"),
premium as (
SELECT
    "charming"."customer_total_overall" as "customer_total_overall",
    "charming"."sales_billing_customer_id" as "_best_customers_best_customer_id"
FROM
    "charming"),
scrawny as (
SELECT
    sum("thoughtful"."sales_quantity" * "thoughtful"."sales_sales_price") as "customer_total_in_window"
FROM
    "thoughtful"
    INNER JOIN "memory"."date_dim" as "sales_date_date" on "thoughtful"."sales_date_id" = "sales_date_date"."D_DATE_SK"
WHERE
    "thoughtful"."sales_billing_customer_id" is not null and "sales_date_date"."D_YEAR" in (2000,2001,2002,2003)

GROUP BY
    "thoughtful"."sales_billing_customer_id"),
vacuous as (
SELECT
    "thoughtful"."sales_item_id" as "sales_item_id",
    "thoughtful"."sales_order_id" as "sales_order_id",
    cast("sales_date_date"."D_DATE" as date) as "sales_date_date"
FROM
    "thoughtful"
    INNER JOIN "memory"."date_dim" as "sales_date_date" on "thoughtful"."sales_date_id" = "sales_date_date"."D_DATE_SK"
    LEFT OUTER JOIN "juicy" on "thoughtful"."sales_item_id" = "juicy"."sales_item_id" AND "thoughtful"."sales_order_id" = "juicy"."sales_order_id" AND "thoughtful"."sales_sales_channel" = "juicy"."sales_sales_channel"
WHERE
    "sales_date_date"."D_YEAR" in (2000,2001,2002,2003)
),
kaput as (
SELECT
    max("scrawny"."customer_total_in_window") as "_max_total_cmax"
FROM
    "scrawny"),
sparkling as (
SELECT
    "concerned"."sales_item_desc_truncated" as "sales_item_desc_truncated",
    "concerned"."sales_item_id" as "sales_item_id",
    "vacuous"."sales_date_date" as "sales_date_date",
    "vacuous"."sales_order_id" as "sales_order_id"
FROM
    "concerned"
    LEFT OUTER JOIN "vacuous" on "concerned"."sales_item_id" = "vacuous"."sales_item_id"
GROUP BY
    1,
    2,
    3,
    4),
busy as (
SELECT
    "kaput"."_max_total_cmax" as "max_total_cmax"
FROM
    "kaput"),
sweltering as (
SELECT
    "sparkling"."sales_item_id" as "_frequent_items_frequent_item_id"
FROM
    "sparkling"
GROUP BY
    1,
    "sparkling"."sales_date_date",
    "sparkling"."sales_item_desc_truncated"
HAVING
    count("sparkling"."sales_order_id") > 4
),
puzzled as (
SELECT
    "premium"."_best_customers_best_customer_id" as "_best_customers_best_customer_id"
FROM
    "premium"
    INNER JOIN "busy" on 1=1
WHERE
    "premium"."customer_total_overall" > 0.5 * "busy"."max_total_cmax"
),
late as (
SELECT
    "sweltering"."_frequent_items_frequent_item_id" as "_frequent_items_frequent_item_id"
FROM
    "sweltering"),
waggish as (
SELECT
    "puzzled"."_best_customers_best_customer_id" as "_best_customers_best_customer_id"
FROM
    "puzzled"),
macho as (
SELECT
    "late"."_frequent_items_frequent_item_id" as "frequent_items_frequent_item_id"
FROM
    "late"),
rambunctious as (
SELECT
    "waggish"."_best_customers_best_customer_id" as "best_customers_best_customer_id"
FROM
    "waggish"),
questionable as (
SELECT
    "sales_billing_customer_customers"."C_FIRST_NAME" as "sales_billing_customer_first_name",
    "sales_billing_customer_customers"."C_LAST_NAME" as "sales_billing_customer_last_name",
    "thoughtful"."sales_list_price" as "sales_list_price",
    "thoughtful"."sales_quantity" as "sales_quantity",
    "thoughtful"."sales_sales_channel" as "sales_sales_channel"
FROM
    "thoughtful"
    INNER JOIN "memory"."date_dim" as "sales_date_date" on "thoughtful"."sales_date_id" = "sales_date_date"."D_DATE_SK"
    INNER JOIN "memory"."customer" as "sales_billing_customer_customers" on "thoughtful"."sales_billing_customer_id" = "sales_billing_customer_customers"."C_CUSTOMER_SK"
WHERE
    "sales_date_date"."D_YEAR" = 2000 and "sales_date_date"."D_MOY" = 2 and "thoughtful"."sales_item_id" in (select macho."frequent_items_frequent_item_id" from macho where macho."frequent_items_frequent_item_id" is not null) and "thoughtful"."sales_billing_customer_id" in (select rambunctious."best_customers_best_customer_id" from rambunctious where rambunctious."best_customers_best_customer_id" is not null)
),
puffy as (
SELECT
    "questionable"."sales_billing_customer_first_name" as "sales_billing_customer_first_name",
    "questionable"."sales_billing_customer_last_name" as "sales_billing_customer_last_name",
    sum(CASE WHEN "questionable"."sales_sales_channel" in ('WEB','CATALOG') THEN "questionable"."sales_quantity" * "questionable"."sales_list_price" ELSE NULL END) as "sales_total"
FROM
    "questionable"
GROUP BY
    1,
    2
HAVING
    "sales_total" > 0
)
SELECT
    "puffy"."sales_billing_customer_last_name" as "c_last_name",
    "puffy"."sales_billing_customer_first_name" as "c_first_name",
    "puffy"."sales_total" as "sales_total"
FROM
    "puffy"
ORDER BY 
    "c_last_name" asc nulls first,
    "c_first_name" asc nulls first,
    "puffy"."sales_total" asc nulls first
LIMIT (100)
```

## Reference SQL (zquery log)

```sql
WITH 
concerned as (
SELECT
     'STORE'  as "sales_sales_channel",
    "sales_store_sales_unified"."SS_CUSTOMER_SK" as "sales_billing_customer_id",
    "sales_store_sales_unified"."SS_QUANTITY" as "sales_quantity",
    "sales_store_sales_unified"."SS_SALES_PRICE" as "sales_sales_price",
    "sales_store_sales_unified"."SS_SOLD_DATE_SK" as "sales_date_id"
FROM
    "memory"."store_sales" as "sales_store_sales_unified"
WHERE
    "sales_store_sales_unified"."SS_CUSTOMER_SK" is not null
),
questionable as (
SELECT
    "sales_date_date"."D_DATE_SK" as "sales_date_id",
    cast("sales_date_date"."D_DATE" as date) as "sales_date_date"
FROM
    "memory"."date_dim" as "sales_date_date"
WHERE
    "sales_date_date"."D_YEAR" in (2000,2001,2002,2003)
),
sweltering as (
SELECT
    "concerned"."sales_billing_customer_id" as "_best_customers_best_customer_id",
    sum("concerned"."sales_quantity" * "concerned"."sales_sales_price") as "customer_total_overall"
FROM
    "concerned"
GROUP BY
    1),
young as (
SELECT
    sum("concerned"."sales_quantity" * "concerned"."sales_sales_price") as "customer_total_in_window"
FROM
    "concerned"
    INNER JOIN "questionable" on "concerned"."sales_date_id" = "questionable"."sales_date_id"
WHERE
    "concerned"."sales_sales_channel" = 'STORE'

GROUP BY
    "concerned"."sales_billing_customer_id"),
uneven as (
SELECT
    "questionable"."sales_date_date" as "sales_date_date",
    "sales_item_items"."I_ITEM_SK" as "sales_item_id",
    "sales_store_sales_unified"."SS_TICKET_NUMBER" as "sales_order_id",
    SUBSTRING("sales_item_items"."I_ITEM_DESC",1,30) as "sales_item_desc_truncated"
FROM
    "memory"."store_sales" as "sales_store_sales_unified"
    INNER JOIN "questionable" on "sales_store_sales_unified"."SS_SOLD_DATE_SK" = "questionable"."sales_date_id"
    LEFT OUTER JOIN "memory"."item" as "sales_item_items" on "sales_store_sales_unified"."SS_ITEM_SK" = "sales_item_items"."I_ITEM_SK"
WHERE
     'STORE'  = 'STORE'

GROUP BY
    1,
    2,
    3,
    4),
abhorrent as (
SELECT
    max("young"."customer_total_in_window") as "max_total_cmax"
FROM
    "young"),
yummy as (
SELECT
    "uneven"."sales_item_id" as "_frequent_items_frequent_item_id"
FROM
    "uneven"
GROUP BY
    1,
    "uneven"."sales_date_date",
    "uneven"."sales_item_desc_truncated"
HAVING
    count("uneven"."sales_order_id") > 4
),
late as (
SELECT
    "sweltering"."_best_customers_best_customer_id" as "_best_customers_best_customer_id"
FROM
    "sweltering"
    INNER JOIN "abhorrent" on 1=1
WHERE
    "sweltering"."customer_total_overall" > 0.5 * "abhorrent"."max_total_cmax"
),
juicy as (
SELECT
    "yummy"."_frequent_items_frequent_item_id" as "frequent_items_frequent_item_id"
FROM
    "yummy"
GROUP BY
    1),
macho as (
SELECT
    "late"."_best_customers_best_customer_id" as "best_customers_best_customer_id"
FROM
    "late"),
thoughtful as (
SELECT
    "sales_catalog_sales_unified"."CS_BILL_CUSTOMER_SK" as "sales_billing_customer_id",
    "sales_catalog_sales_unified"."CS_ITEM_SK" as "sales_item_id",
    "sales_catalog_sales_unified"."CS_LIST_PRICE" as "sales_list_price",
    "sales_catalog_sales_unified"."CS_ORDER_NUMBER" as "sales_order_id",
    "sales_catalog_sales_unified"."CS_QUANTITY" as "sales_quantity",
     'CATALOG'  as "sales_sales_channel"
FROM
    "memory"."catalog_sales" as "sales_catalog_sales_unified"
    INNER JOIN "memory"."customer" as "sales_billing_customer_customers" on "sales_catalog_sales_unified"."CS_BILL_CUSTOMER_SK" = "sales_billing_customer_customers"."C_CUSTOMER_SK"
    INNER JOIN "memory"."date_dim" as "sales_date_date" on "sales_catalog_sales_unified"."CS_SOLD_DATE_SK" = "sales_date_date"."D_DATE_SK"
WHERE
    "sales_catalog_sales_unified"."CS_ITEM_SK" in (select juicy."frequent_items_frequent_item_id" from juicy where juicy."frequent_items_frequent_item_id" is not null) and "sales_catalog_sales_unified"."CS_BILL_CUSTOMER_SK" in (select macho."best_customers_best_customer_id" from macho where macho."best_customers_best_customer_id" is not null) and "sales_date_date"."D_YEAR" = 2000 and "sales_date_date"."D_MOY" = 2

UNION ALL
SELECT
    "sales_store_sales_unified"."SS_CUSTOMER_SK" as "sales_billing_customer_id",
    "sales_store_sales_unified"."SS_ITEM_SK" as "sales_item_id",
    "sales_store_sales_unified"."SS_LIST_PRICE" as "sales_list_price",
    "sales_store_sales_unified"."SS_TICKET_NUMBER" as "sales_order_id",
    "sales_store_sales_unified"."SS_QUANTITY" as "sales_quantity",
     'STORE'  as "sales_sales_channel"
FROM
    "memory"."store_sales" as "sales_store_sales_unified"
    INNER JOIN "memory"."customer" as "sales_billing_customer_customers" on "sales_store_sales_unified"."SS_CUSTOMER_SK" = "sales_billing_customer_customers"."C_CUSTOMER_SK"
    INNER JOIN "memory"."date_dim" as "sales_date_date" on "sales_store_sales_unified"."SS_SOLD_DATE_SK" = "sales_date_date"."D_DATE_SK"
WHERE
    "sales_store_sales_unified"."SS_ITEM_SK" in (select juicy."frequent_items_frequent_item_id" from juicy where juicy."frequent_items_frequent_item_id" is not null) and "sales_store_sales_unified"."SS_CUSTOMER_SK" in (select macho."best_customers_best_customer_id" from macho where macho."best_customers_best_customer_id" is not null) and "sales_date_date"."D_YEAR" = 2000 and "sales_date_date"."D_MOY" = 2

UNION ALL
SELECT
    "sales_web_sales_unified"."WS_BILL_CUSTOMER_SK" as "sales_billing_customer_id",
    "sales_web_sales_unified"."WS_ITEM_SK" as "sales_item_id",
    "sales_web_sales_unified"."WS_LIST_PRICE" as "sales_list_price",
    "sales_web_sales_unified"."WS_ORDER_NUMBER" as "sales_order_id",
    "sales_web_sales_unified"."WS_QUANTITY" as "sales_quantity",
     'WEB'  as "sales_sales_channel"
FROM
    "memory"."web_sales" as "sales_web_sales_unified"
    INNER JOIN "memory"."customer" as "sales_billing_customer_customers" on "sales_web_sales_unified"."WS_BILL_CUSTOMER_SK" = "sales_billing_customer_customers"."C_CUSTOMER_SK"
    INNER JOIN "memory"."date_dim" as "sales_date_date" on "sales_web_sales_unified"."WS_SOLD_DATE_SK" = "sales_date_date"."D_DATE_SK"
WHERE
    "sales_web_sales_unified"."WS_ITEM_SK" in (select juicy."frequent_items_frequent_item_id" from juicy where juicy."frequent_items_frequent_item_id" is not null) and "sales_web_sales_unified"."WS_BILL_CUSTOMER_SK" in (select macho."best_customers_best_customer_id" from macho where macho."best_customers_best_customer_id" is not null) and "sales_date_date"."D_YEAR" = 2000 and "sales_date_date"."D_MOY" = 2
),
scrawny as (
SELECT
    "sales_billing_customer_customers"."C_FIRST_NAME" as "sales_billing_customer_first_name",
    "sales_billing_customer_customers"."C_LAST_NAME" as "sales_billing_customer_last_name",
    "thoughtful"."sales_quantity" * "thoughtful"."sales_list_price" as "_virt_func_multiply_8507033399516423",
    "thoughtful"."sales_sales_channel" as "sales_sales_channel"
FROM
    "thoughtful"
    LEFT OUTER JOIN "memory"."customer" as "sales_billing_customer_customers" on "thoughtful"."sales_billing_customer_id" = "sales_billing_customer_customers"."C_CUSTOMER_SK"
WHERE
    "thoughtful"."sales_item_id" in (select juicy."frequent_items_frequent_item_id" from juicy where juicy."frequent_items_frequent_item_id" is not null)

GROUP BY
    1,
    2,
    3,
    4,
    "thoughtful"."sales_item_id",
    "thoughtful"."sales_order_id")
SELECT
    "scrawny"."sales_billing_customer_last_name" as "c_last_name",
    "scrawny"."sales_billing_customer_first_name" as "c_first_name",
    sum(CASE WHEN "scrawny"."sales_sales_channel" in ('WEB','CATALOG') THEN "scrawny"."_virt_func_multiply_8507033399516423" ELSE NULL END) as "sales_total"
FROM
    "scrawny"
GROUP BY
    1,
    2
HAVING
    "sales_total" > 0

ORDER BY 
    "c_last_name" asc nulls first,
    "c_first_name" asc nulls first,
    "sales_total" asc nulls first
LIMIT (100)
```
