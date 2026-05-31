# Query 23

**Status:** `match`

| Stage | Result |
| --- | --- |
| v4 SQL generation | OK |
| v4 execution | OK (4 rows) |
| reference execution | OK (4 rows) |
| results identical | YES |

## Result comparison

v4 rows: 4 (4 distinct)
ref rows: 4 (4 distinct)

## SQL size + execution time

| Source | Chars | Lines | Exec (min of 4) |
| --- | --- | --- | --- |
| v4 | 7310 | 169 | 634.00 ms |
| reference | 7657 | 178 | 642.58 ms |
| v4 / ref | 0.95x | 0.95x | 0.99x |

## Preql

```
import unified_sales as sales;

# frequent_ss_items: items with at least one (truncated_desc, item.id, sold_date) combo having > 4 store_sales rows in 2000-2003
auto sales.item.desc_truncated <- substring(sales.item.desc, 1, 30);
auto customer_total_in_window <- sum(sales.quantity * sales.sales_price) by sales.customer.id;

rowset frequent_items <- where
    sales.sales_channel = 'STORE' and sales.date.year in (2000, 2001, 2002, 2003)
select
    sales.item.id as frequent_item_id,
    --ss_combo_count,
having
    ss_combo_count > 4

;

# max_store_sales: scalar max over customers of total ss_qty*ss_price for years 2000-2003
auto customer_total_overall <- sum(sales.quantity * sales.sales_price) by sales.customer.id;

rowset max_total <- where
    sales.customer.id is not null
    and sales.sales_channel = 'STORE'
    and sales.date.year in (2000, 2001, 2002, 2003)
select
    max(customer_total_in_window) as cmax,
;

# best_ss_customer: customers whose lifetime ss_qty*ss_price > 50% of cmax (no date filter)
auto ss_combo_count <- count(sales.order_id) by sales.item.desc_truncated, sales.item.id, sales.date.date;

rowset best_customers <- where
    sales.customer.id is not null and sales.sales_channel = 'STORE'
select
    sales.customer.id as best_customer_id,
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
    and sales.customer.id in best_customers.best_customer_id
select
    sales.customer.last_name as c_last_name,
    sales.customer.first_name as c_first_name,
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
concerned as (
SELECT
     'STORE'  as "sales_sales_channel",
    "sales_store_sales_unified"."SS_CUSTOMER_SK" as "sales_customer_id",
    "sales_store_sales_unified"."SS_QUANTITY" as "sales_quantity",
    "sales_store_sales_unified"."SS_SALES_PRICE" as "sales_sales_price",
    "sales_store_sales_unified"."SS_SOLD_DATE_SK" as "sales_date_id"
FROM
    "memory"."store_sales" as "sales_store_sales_unified"
WHERE
    "sales_store_sales_unified"."SS_CUSTOMER_SK" is not null
),
abundant as (
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
    "concerned"."sales_customer_id" as "_best_customers_best_customer_id",
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
    INNER JOIN "abundant" on "concerned"."sales_date_id" = "abundant"."sales_date_id"
WHERE
    "concerned"."sales_sales_channel" = 'STORE'

GROUP BY
    "concerned"."sales_customer_id"),
yummy as (
SELECT
    "abundant"."sales_date_date" as "sales_date_date",
    "sales_item_items"."I_ITEM_SK" as "sales_item_id",
    "sales_store_sales_unified"."SS_TICKET_NUMBER" as "sales_order_id",
    SUBSTRING("sales_item_items"."I_ITEM_DESC",1,30) as "sales_item_desc_truncated"
FROM
    "memory"."store_sales" as "sales_store_sales_unified"
    INNER JOIN "abundant" on "sales_store_sales_unified"."SS_SOLD_DATE_SK" = "abundant"."sales_date_id"
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
juicy as (
SELECT
    "yummy"."sales_item_id" as "_frequent_items_frequent_item_id",
    count("yummy"."sales_order_id") as "ss_combo_count"
FROM
    "yummy"
GROUP BY
    1,
    "yummy"."sales_date_date",
    "yummy"."sales_item_desc_truncated"),
late as (
SELECT
    "sweltering"."_best_customers_best_customer_id" as "_best_customers_best_customer_id"
FROM
    "sweltering"
    INNER JOIN "abhorrent" on 1=1
WHERE
    "sweltering"."customer_total_overall" > 0.5 * "abhorrent"."max_total_cmax"
),
vacuous as (
SELECT
    "juicy"."_frequent_items_frequent_item_id" as "frequent_items_frequent_item_id"
FROM
    "juicy"
WHERE
    "juicy"."ss_combo_count" > 4
),
macho as (
SELECT
    "late"."_best_customers_best_customer_id" as "best_customers_best_customer_id"
FROM
    "late"),
cheerful as (
SELECT
    "sales_catalog_sales_unified"."CS_BILL_CUSTOMER_SK" as "sales_customer_id",
    "sales_catalog_sales_unified"."CS_LIST_PRICE" as "sales_list_price",
    "sales_catalog_sales_unified"."CS_QUANTITY" as "sales_quantity",
     'CATALOG'  as "sales_sales_channel"
FROM
    "memory"."catalog_sales" as "sales_catalog_sales_unified"
    INNER JOIN "memory"."customer" as "sales_customer_customers" on "sales_catalog_sales_unified"."CS_BILL_CUSTOMER_SK" = "sales_customer_customers"."C_CUSTOMER_SK"
    INNER JOIN "memory"."date_dim" as "sales_date_date" on "sales_catalog_sales_unified"."CS_SOLD_DATE_SK" = "sales_date_date"."D_DATE_SK"
WHERE
    "sales_catalog_sales_unified"."CS_ITEM_SK" in (select vacuous."frequent_items_frequent_item_id" from vacuous where vacuous."frequent_items_frequent_item_id" is not null) and "sales_catalog_sales_unified"."CS_BILL_CUSTOMER_SK" in (select macho."best_customers_best_customer_id" from macho where macho."best_customers_best_customer_id" is not null) and "sales_date_date"."D_YEAR" = 2000 and "sales_date_date"."D_MOY" = 2

UNION ALL
SELECT
    "sales_store_sales_unified"."SS_CUSTOMER_SK" as "sales_customer_id",
    "sales_store_sales_unified"."SS_LIST_PRICE" as "sales_list_price",
    "sales_store_sales_unified"."SS_QUANTITY" as "sales_quantity",
     'STORE'  as "sales_sales_channel"
FROM
    "memory"."store_sales" as "sales_store_sales_unified"
    INNER JOIN "memory"."customer" as "sales_customer_customers" on "sales_store_sales_unified"."SS_CUSTOMER_SK" = "sales_customer_customers"."C_CUSTOMER_SK"
    INNER JOIN "memory"."date_dim" as "sales_date_date" on "sales_store_sales_unified"."SS_SOLD_DATE_SK" = "sales_date_date"."D_DATE_SK"
WHERE
    "sales_store_sales_unified"."SS_ITEM_SK" in (select vacuous."frequent_items_frequent_item_id" from vacuous where vacuous."frequent_items_frequent_item_id" is not null) and "sales_store_sales_unified"."SS_CUSTOMER_SK" in (select macho."best_customers_best_customer_id" from macho where macho."best_customers_best_customer_id" is not null) and "sales_date_date"."D_YEAR" = 2000 and "sales_date_date"."D_MOY" = 2

UNION ALL
SELECT
    "sales_web_sales_unified"."WS_BILL_CUSTOMER_SK" as "sales_customer_id",
    "sales_web_sales_unified"."WS_LIST_PRICE" as "sales_list_price",
    "sales_web_sales_unified"."WS_QUANTITY" as "sales_quantity",
     'WEB'  as "sales_sales_channel"
FROM
    "memory"."web_sales" as "sales_web_sales_unified"
    INNER JOIN "memory"."customer" as "sales_customer_customers" on "sales_web_sales_unified"."WS_BILL_CUSTOMER_SK" = "sales_customer_customers"."C_CUSTOMER_SK"
    INNER JOIN "memory"."date_dim" as "sales_date_date" on "sales_web_sales_unified"."WS_SOLD_DATE_SK" = "sales_date_date"."D_DATE_SK"
WHERE
    "sales_web_sales_unified"."WS_ITEM_SK" in (select vacuous."frequent_items_frequent_item_id" from vacuous where vacuous."frequent_items_frequent_item_id" is not null) and "sales_web_sales_unified"."WS_BILL_CUSTOMER_SK" in (select macho."best_customers_best_customer_id" from macho where macho."best_customers_best_customer_id" is not null) and "sales_date_date"."D_YEAR" = 2000 and "sales_date_date"."D_MOY" = 2
),
questionable as (
SELECT
    "cheerful"."sales_list_price" as "sales_list_price",
    "cheerful"."sales_quantity" as "sales_quantity",
    "cheerful"."sales_sales_channel" as "sales_sales_channel",
    "sales_customer_customers"."C_FIRST_NAME" as "sales_customer_first_name",
    "sales_customer_customers"."C_LAST_NAME" as "sales_customer_last_name"
FROM
    "cheerful"
    LEFT OUTER JOIN "memory"."customer" as "sales_customer_customers" on "cheerful"."sales_customer_id" = "sales_customer_customers"."C_CUSTOMER_SK"),
scrawny as (
SELECT
    "questionable"."sales_customer_first_name" as "sales_customer_first_name",
    "questionable"."sales_customer_last_name" as "sales_customer_last_name",
    sum(CASE WHEN "questionable"."sales_sales_channel" in ('WEB','CATALOG') THEN "questionable"."sales_quantity" * "questionable"."sales_list_price" ELSE NULL END) as "sales_total"
FROM
    "questionable"
GROUP BY
    1,
    2)
SELECT
    "scrawny"."sales_customer_last_name" as "c_last_name",
    "scrawny"."sales_customer_first_name" as "c_first_name",
    "scrawny"."sales_total" as "sales_total"
FROM
    "scrawny"
WHERE
    "scrawny"."sales_total" > 0

ORDER BY 
    "c_last_name" asc nulls first,
    "c_first_name" asc nulls first,
    "scrawny"."sales_total" asc nulls first
LIMIT (100)
```

## Reference SQL (zquery log)

```sql
WITH 
concerned as (
SELECT
     'STORE'  as "sales_sales_channel",
    "sales_store_sales_unified"."SS_CUSTOMER_SK" as "sales_customer_id",
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
    "concerned"."sales_customer_id" as "_best_customers_best_customer_id",
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
    "concerned"."sales_customer_id"),
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
cheerful as (
SELECT
    "sales_catalog_sales_unified"."CS_BILL_CUSTOMER_SK" as "sales_customer_id",
    "sales_catalog_sales_unified"."CS_ITEM_SK" as "sales_item_id",
    "sales_catalog_sales_unified"."CS_LIST_PRICE" as "sales_list_price",
    "sales_catalog_sales_unified"."CS_ORDER_NUMBER" as "sales_order_id",
    "sales_catalog_sales_unified"."CS_QUANTITY" as "sales_quantity",
     'CATALOG'  as "sales_sales_channel"
FROM
    "memory"."catalog_sales" as "sales_catalog_sales_unified"
    INNER JOIN "memory"."customer" as "sales_customer_customers" on "sales_catalog_sales_unified"."CS_BILL_CUSTOMER_SK" = "sales_customer_customers"."C_CUSTOMER_SK"
    INNER JOIN "memory"."date_dim" as "sales_date_date" on "sales_catalog_sales_unified"."CS_SOLD_DATE_SK" = "sales_date_date"."D_DATE_SK"
WHERE
    "sales_catalog_sales_unified"."CS_ITEM_SK" in (select juicy."frequent_items_frequent_item_id" from juicy where juicy."frequent_items_frequent_item_id" is not null) and "sales_catalog_sales_unified"."CS_BILL_CUSTOMER_SK" in (select macho."best_customers_best_customer_id" from macho where macho."best_customers_best_customer_id" is not null) and "sales_date_date"."D_YEAR" = 2000 and "sales_date_date"."D_MOY" = 2

UNION ALL
SELECT
    "sales_store_sales_unified"."SS_CUSTOMER_SK" as "sales_customer_id",
    "sales_store_sales_unified"."SS_ITEM_SK" as "sales_item_id",
    "sales_store_sales_unified"."SS_LIST_PRICE" as "sales_list_price",
    "sales_store_sales_unified"."SS_TICKET_NUMBER" as "sales_order_id",
    "sales_store_sales_unified"."SS_QUANTITY" as "sales_quantity",
     'STORE'  as "sales_sales_channel"
FROM
    "memory"."store_sales" as "sales_store_sales_unified"
    INNER JOIN "memory"."customer" as "sales_customer_customers" on "sales_store_sales_unified"."SS_CUSTOMER_SK" = "sales_customer_customers"."C_CUSTOMER_SK"
    INNER JOIN "memory"."date_dim" as "sales_date_date" on "sales_store_sales_unified"."SS_SOLD_DATE_SK" = "sales_date_date"."D_DATE_SK"
WHERE
    "sales_store_sales_unified"."SS_ITEM_SK" in (select juicy."frequent_items_frequent_item_id" from juicy where juicy."frequent_items_frequent_item_id" is not null) and "sales_store_sales_unified"."SS_CUSTOMER_SK" in (select macho."best_customers_best_customer_id" from macho where macho."best_customers_best_customer_id" is not null) and "sales_date_date"."D_YEAR" = 2000 and "sales_date_date"."D_MOY" = 2

UNION ALL
SELECT
    "sales_web_sales_unified"."WS_BILL_CUSTOMER_SK" as "sales_customer_id",
    "sales_web_sales_unified"."WS_ITEM_SK" as "sales_item_id",
    "sales_web_sales_unified"."WS_LIST_PRICE" as "sales_list_price",
    "sales_web_sales_unified"."WS_ORDER_NUMBER" as "sales_order_id",
    "sales_web_sales_unified"."WS_QUANTITY" as "sales_quantity",
     'WEB'  as "sales_sales_channel"
FROM
    "memory"."web_sales" as "sales_web_sales_unified"
    INNER JOIN "memory"."customer" as "sales_customer_customers" on "sales_web_sales_unified"."WS_BILL_CUSTOMER_SK" = "sales_customer_customers"."C_CUSTOMER_SK"
    INNER JOIN "memory"."date_dim" as "sales_date_date" on "sales_web_sales_unified"."WS_SOLD_DATE_SK" = "sales_date_date"."D_DATE_SK"
WHERE
    "sales_web_sales_unified"."WS_ITEM_SK" in (select juicy."frequent_items_frequent_item_id" from juicy where juicy."frequent_items_frequent_item_id" is not null) and "sales_web_sales_unified"."WS_BILL_CUSTOMER_SK" in (select macho."best_customers_best_customer_id" from macho where macho."best_customers_best_customer_id" is not null) and "sales_date_date"."D_YEAR" = 2000 and "sales_date_date"."D_MOY" = 2
),
scrawny as (
SELECT
    "cheerful"."sales_quantity" * "cheerful"."sales_list_price" as "_virt_func_multiply_8507033399516423",
    "cheerful"."sales_sales_channel" as "sales_sales_channel",
    "sales_customer_customers"."C_FIRST_NAME" as "sales_customer_first_name",
    "sales_customer_customers"."C_LAST_NAME" as "sales_customer_last_name"
FROM
    "cheerful"
    LEFT OUTER JOIN "memory"."customer" as "sales_customer_customers" on "cheerful"."sales_customer_id" = "sales_customer_customers"."C_CUSTOMER_SK"
WHERE
    "cheerful"."sales_item_id" in (select juicy."frequent_items_frequent_item_id" from juicy where juicy."frequent_items_frequent_item_id" is not null)

GROUP BY
    1,
    2,
    3,
    4,
    "cheerful"."sales_item_id",
    "cheerful"."sales_order_id")
SELECT
    "scrawny"."sales_customer_last_name" as "c_last_name",
    "scrawny"."sales_customer_first_name" as "c_first_name",
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
