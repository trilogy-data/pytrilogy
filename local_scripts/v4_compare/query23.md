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
| v4 | 8080 | 193 | 313.64 ms |
| reference | 8246 | 210 | 441.74 ms |
| v4 / ref | 0.98x | 0.92x | 0.71x |

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
abundant as (
SELECT
    "sales_date_date"."D_DATE_SK" as "sales_date_id",
    cast("sales_date_date"."D_DATE" as date) as "sales_date_date"
FROM
    "memory"."date_dim" as "sales_date_date"
WHERE
    "sales_date_date"."D_YEAR" in (2000,2001,2002,2003)
),
uneven as (
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
friendly as (
SELECT
    "sales_item_items"."I_ITEM_SK" as "sales_item_id",
    SUBSTRING("sales_item_items"."I_ITEM_DESC",1,30) as "sales_item_desc_truncated"
FROM
    "memory"."item" as "sales_item_items"),
scrawny as (
SELECT
    "abundant"."sales_date_date" as "sales_date_date",
    "sales_store_sales_unified"."SS_ITEM_SK" as "sales_item_id",
    "sales_store_sales_unified"."SS_TICKET_NUMBER" as "sales_order_id"
FROM
    "memory"."store_sales" as "sales_store_sales_unified"
    INNER JOIN "abundant" on "sales_store_sales_unified"."SS_SOLD_DATE_SK" = "abundant"."sales_date_id"
WHERE
     'STORE'  = 'STORE'

GROUP BY
    1,
    2,
    3),
sparkling as (
SELECT
    "uneven"."sales_billing_customer_id" as "sales_billing_customer_id",
    sum("uneven"."sales_quantity" * "uneven"."sales_sales_price") as "customer_total_overall"
FROM
    "uneven"
GROUP BY
    1),
yummy as (
SELECT
    sum("uneven"."sales_quantity" * "uneven"."sales_sales_price") as "customer_total_in_window"
FROM
    "uneven"
    INNER JOIN "abundant" on "uneven"."sales_date_id" = "abundant"."sales_date_id"
WHERE
    "uneven"."sales_sales_channel" = 'STORE'

GROUP BY
    "uneven"."sales_billing_customer_id"),
divergent as (
SELECT
    "friendly"."sales_item_id" as "_frequent_items_frequent_item_id"
FROM
    "friendly"
    LEFT OUTER JOIN "scrawny" on "friendly"."sales_item_id" = "scrawny"."sales_item_id"
GROUP BY
    1,
    "friendly"."sales_item_desc_truncated",
    "scrawny"."sales_date_date"
HAVING
    count("scrawny"."sales_order_id") > 4
),
abhorrent as (
SELECT
    "sparkling"."customer_total_overall" as "customer_total_overall",
    "sparkling"."sales_billing_customer_id" as "_best_customers_best_customer_id"
FROM
    "sparkling"),
vacuous as (
SELECT
    max("yummy"."customer_total_in_window") as "_max_total_cmax"
FROM
    "yummy"),
protective as (
SELECT
    "divergent"."_frequent_items_frequent_item_id" as "_frequent_items_frequent_item_id"
FROM
    "divergent"),
young as (
SELECT
    "vacuous"."_max_total_cmax" as "max_total_cmax"
FROM
    "vacuous"),
premium as (
SELECT
    "protective"."_frequent_items_frequent_item_id" as "frequent_items_frequent_item_id"
FROM
    "protective"),
sweltering as (
SELECT
    "abhorrent"."_best_customers_best_customer_id" as "_best_customers_best_customer_id"
FROM
    "abhorrent"
    INNER JOIN "young" on 1=1
WHERE
    "abhorrent"."customer_total_overall" > 0.5 * "young"."max_total_cmax"
),
late as (
SELECT
    "sweltering"."_best_customers_best_customer_id" as "_best_customers_best_customer_id"
FROM
    "sweltering"),
macho as (
SELECT
    "late"."_best_customers_best_customer_id" as "best_customers_best_customer_id"
FROM
    "late"),
thoughtful as (
SELECT
    "sales_catalog_sales_unified"."CS_BILL_CUSTOMER_SK" as "sales_billing_customer_id",
    "sales_catalog_sales_unified"."CS_LIST_PRICE" as "sales_list_price",
    "sales_catalog_sales_unified"."CS_QUANTITY" as "sales_quantity",
     'CATALOG'  as "sales_sales_channel"
FROM
    "memory"."catalog_sales" as "sales_catalog_sales_unified"
    INNER JOIN "memory"."customer" as "sales_billing_customer_customers" on "sales_catalog_sales_unified"."CS_BILL_CUSTOMER_SK" = "sales_billing_customer_customers"."C_CUSTOMER_SK"
    INNER JOIN "memory"."date_dim" as "sales_date_date" on "sales_catalog_sales_unified"."CS_SOLD_DATE_SK" = "sales_date_date"."D_DATE_SK"
WHERE
    "sales_catalog_sales_unified"."CS_ITEM_SK" in (select premium."frequent_items_frequent_item_id" from premium where premium."frequent_items_frequent_item_id" is not null) and "sales_catalog_sales_unified"."CS_BILL_CUSTOMER_SK" in (select macho."best_customers_best_customer_id" from macho where macho."best_customers_best_customer_id" is not null) and "sales_date_date"."D_YEAR" = 2000 and "sales_date_date"."D_MOY" = 2

UNION ALL
SELECT
    "sales_store_sales_unified"."SS_CUSTOMER_SK" as "sales_billing_customer_id",
    "sales_store_sales_unified"."SS_LIST_PRICE" as "sales_list_price",
    "sales_store_sales_unified"."SS_QUANTITY" as "sales_quantity",
     'STORE'  as "sales_sales_channel"
FROM
    "memory"."store_sales" as "sales_store_sales_unified"
    INNER JOIN "memory"."customer" as "sales_billing_customer_customers" on "sales_store_sales_unified"."SS_CUSTOMER_SK" = "sales_billing_customer_customers"."C_CUSTOMER_SK"
    INNER JOIN "memory"."date_dim" as "sales_date_date" on "sales_store_sales_unified"."SS_SOLD_DATE_SK" = "sales_date_date"."D_DATE_SK"
WHERE
    "sales_store_sales_unified"."SS_ITEM_SK" in (select premium."frequent_items_frequent_item_id" from premium where premium."frequent_items_frequent_item_id" is not null) and "sales_store_sales_unified"."SS_CUSTOMER_SK" in (select macho."best_customers_best_customer_id" from macho where macho."best_customers_best_customer_id" is not null) and "sales_date_date"."D_YEAR" = 2000 and "sales_date_date"."D_MOY" = 2

UNION ALL
SELECT
    "sales_web_sales_unified"."WS_BILL_CUSTOMER_SK" as "sales_billing_customer_id",
    "sales_web_sales_unified"."WS_LIST_PRICE" as "sales_list_price",
    "sales_web_sales_unified"."WS_QUANTITY" as "sales_quantity",
     'WEB'  as "sales_sales_channel"
FROM
    "memory"."web_sales" as "sales_web_sales_unified"
    INNER JOIN "memory"."customer" as "sales_billing_customer_customers" on "sales_web_sales_unified"."WS_BILL_CUSTOMER_SK" = "sales_billing_customer_customers"."C_CUSTOMER_SK"
    INNER JOIN "memory"."date_dim" as "sales_date_date" on "sales_web_sales_unified"."WS_SOLD_DATE_SK" = "sales_date_date"."D_DATE_SK"
WHERE
    "sales_web_sales_unified"."WS_ITEM_SK" in (select premium."frequent_items_frequent_item_id" from premium where premium."frequent_items_frequent_item_id" is not null) and "sales_web_sales_unified"."WS_BILL_CUSTOMER_SK" in (select macho."best_customers_best_customer_id" from macho where macho."best_customers_best_customer_id" is not null) and "sales_date_date"."D_YEAR" = 2000 and "sales_date_date"."D_MOY" = 2
),
questionable as (
SELECT
    "sales_billing_customer_customers"."C_FIRST_NAME" as "sales_billing_customer_first_name",
    "sales_billing_customer_customers"."C_LAST_NAME" as "sales_billing_customer_last_name",
    "thoughtful"."sales_list_price" as "sales_list_price",
    "thoughtful"."sales_quantity" as "sales_quantity",
    "thoughtful"."sales_sales_channel" as "sales_sales_channel"
FROM
    "thoughtful"
    LEFT OUTER JOIN "memory"."customer" as "sales_billing_customer_customers" on "thoughtful"."sales_billing_customer_id" = "sales_billing_customer_customers"."C_CUSTOMER_SK"),
puzzled as (
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
    "puzzled"."sales_billing_customer_last_name" as "c_last_name",
    "puzzled"."sales_billing_customer_first_name" as "c_first_name",
    "puzzled"."sales_total" as "sales_total"
FROM
    "puzzled"
ORDER BY 
    "c_last_name" asc nulls first,
    "c_first_name" asc nulls first,
    "puzzled"."sales_total" asc nulls first
LIMIT (100)
```

## Reference SQL (zquery log)

```sql
WITH 
uneven as (
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
abundant as (
SELECT
    "sales_date_date"."D_DATE_SK" as "sales_date_id"
FROM
    "memory"."date_dim" as "sales_date_date"
WHERE
    "sales_date_date"."D_YEAR" in (2000,2001,2002,2003)
),
scrawny as (
SELECT
    "sales_store_sales_unified"."SS_ITEM_SK" as "sales_item_id",
    "sales_store_sales_unified"."SS_TICKET_NUMBER" as "sales_order_id",
    cast("sales_date_date"."D_DATE" as date) as "sales_date_date"
FROM
    "memory"."store_sales" as "sales_store_sales_unified"
    INNER JOIN "memory"."date_dim" as "sales_date_date" on "sales_store_sales_unified"."SS_SOLD_DATE_SK" = "sales_date_date"."D_DATE_SK"
WHERE
     'STORE'  = 'STORE' and "sales_date_date"."D_YEAR" in (2000,2001,2002,2003)

GROUP BY
    1,
    2,
    3,
    "sales_date_date"."D_DATE_SK"),
friendly as (
SELECT
    "sales_item_items"."I_ITEM_SK" as "sales_item_id",
    SUBSTRING("sales_item_items"."I_ITEM_DESC",1,30) as "sales_item_desc_truncated"
FROM
    "memory"."item" as "sales_item_items"),
sparkling as (
SELECT
    "uneven"."sales_billing_customer_id" as "sales_billing_customer_id",
    sum("uneven"."sales_quantity" * "uneven"."sales_sales_price") as "customer_total_overall"
FROM
    "uneven"
GROUP BY
    1),
yummy as (
SELECT
    sum("uneven"."sales_quantity" * "uneven"."sales_sales_price") as "customer_total_in_window"
FROM
    "uneven"
    INNER JOIN "abundant" on "uneven"."sales_date_id" = "abundant"."sales_date_id"
WHERE
    "uneven"."sales_sales_channel" = 'STORE'

GROUP BY
    "uneven"."sales_billing_customer_id"),
divergent as (
SELECT
    "friendly"."sales_item_desc_truncated" as "sales_item_desc_truncated",
    "friendly"."sales_item_id" as "sales_item_id",
    "scrawny"."sales_date_date" as "sales_date_date",
    "scrawny"."sales_order_id" as "sales_order_id"
FROM
    "friendly"
    LEFT OUTER JOIN "scrawny" on "friendly"."sales_item_id" = "scrawny"."sales_item_id"
GROUP BY
    1,
    2,
    3,
    4),
abhorrent as (
SELECT
    "sparkling"."customer_total_overall" as "customer_total_overall",
    "sparkling"."sales_billing_customer_id" as "_best_customers_best_customer_id"
FROM
    "sparkling"),
vacuous as (
SELECT
    max("yummy"."customer_total_in_window") as "_max_total_cmax"
FROM
    "yummy"),
charming as (
SELECT
    "divergent"."sales_item_id" as "_frequent_items_frequent_item_id"
FROM
    "divergent"
GROUP BY
    1,
    "divergent"."sales_date_date",
    "divergent"."sales_item_desc_truncated"
HAVING
    count("divergent"."sales_order_id") > 4
),
young as (
SELECT
    "vacuous"."_max_total_cmax" as "max_total_cmax"
FROM
    "vacuous"),
protective as (
SELECT
    "charming"."_frequent_items_frequent_item_id" as "_frequent_items_frequent_item_id"
FROM
    "charming"),
sweltering as (
SELECT
    "abhorrent"."_best_customers_best_customer_id" as "_best_customers_best_customer_id"
FROM
    "abhorrent"
    INNER JOIN "young" on 1=1
WHERE
    "abhorrent"."customer_total_overall" > 0.5 * "young"."max_total_cmax"
),
premium as (
SELECT
    "protective"."_frequent_items_frequent_item_id" as "frequent_items_frequent_item_id"
FROM
    "protective"),
late as (
SELECT
    "sweltering"."_best_customers_best_customer_id" as "_best_customers_best_customer_id"
FROM
    "sweltering"),
macho as (
SELECT
    "late"."_best_customers_best_customer_id" as "best_customers_best_customer_id"
FROM
    "late"),
thoughtful as (
SELECT
    "sales_catalog_sales_unified"."CS_BILL_CUSTOMER_SK" as "sales_billing_customer_id",
    "sales_catalog_sales_unified"."CS_SOLD_DATE_SK" as "sales_date_id",
    "sales_catalog_sales_unified"."CS_LIST_PRICE" as "sales_list_price",
    "sales_catalog_sales_unified"."CS_QUANTITY" as "sales_quantity",
     'CATALOG'  as "sales_sales_channel"
FROM
    "memory"."catalog_sales" as "sales_catalog_sales_unified"
    INNER JOIN "memory"."customer" as "sales_billing_customer_customers" on "sales_catalog_sales_unified"."CS_BILL_CUSTOMER_SK" = "sales_billing_customer_customers"."C_CUSTOMER_SK"
WHERE
    "sales_catalog_sales_unified"."CS_ITEM_SK" in (select premium."frequent_items_frequent_item_id" from premium where premium."frequent_items_frequent_item_id" is not null) and "sales_catalog_sales_unified"."CS_BILL_CUSTOMER_SK" in (select macho."best_customers_best_customer_id" from macho where macho."best_customers_best_customer_id" is not null)

UNION ALL
SELECT
    "sales_store_sales_unified"."SS_CUSTOMER_SK" as "sales_billing_customer_id",
    "sales_store_sales_unified"."SS_SOLD_DATE_SK" as "sales_date_id",
    "sales_store_sales_unified"."SS_LIST_PRICE" as "sales_list_price",
    "sales_store_sales_unified"."SS_QUANTITY" as "sales_quantity",
     'STORE'  as "sales_sales_channel"
FROM
    "memory"."store_sales" as "sales_store_sales_unified"
    INNER JOIN "memory"."customer" as "sales_billing_customer_customers" on "sales_store_sales_unified"."SS_CUSTOMER_SK" = "sales_billing_customer_customers"."C_CUSTOMER_SK"
WHERE
    "sales_store_sales_unified"."SS_ITEM_SK" in (select premium."frequent_items_frequent_item_id" from premium where premium."frequent_items_frequent_item_id" is not null) and "sales_store_sales_unified"."SS_CUSTOMER_SK" in (select macho."best_customers_best_customer_id" from macho where macho."best_customers_best_customer_id" is not null)

UNION ALL
SELECT
    "sales_web_sales_unified"."WS_BILL_CUSTOMER_SK" as "sales_billing_customer_id",
    "sales_web_sales_unified"."WS_SOLD_DATE_SK" as "sales_date_id",
    "sales_web_sales_unified"."WS_LIST_PRICE" as "sales_list_price",
    "sales_web_sales_unified"."WS_QUANTITY" as "sales_quantity",
     'WEB'  as "sales_sales_channel"
FROM
    "memory"."web_sales" as "sales_web_sales_unified"
    INNER JOIN "memory"."customer" as "sales_billing_customer_customers" on "sales_web_sales_unified"."WS_BILL_CUSTOMER_SK" = "sales_billing_customer_customers"."C_CUSTOMER_SK"
WHERE
    "sales_web_sales_unified"."WS_ITEM_SK" in (select premium."frequent_items_frequent_item_id" from premium where premium."frequent_items_frequent_item_id" is not null) and "sales_web_sales_unified"."WS_BILL_CUSTOMER_SK" in (select macho."best_customers_best_customer_id" from macho where macho."best_customers_best_customer_id" is not null)
),
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
    LEFT OUTER JOIN "memory"."customer" as "sales_billing_customer_customers" on "thoughtful"."sales_billing_customer_id" = "sales_billing_customer_customers"."C_CUSTOMER_SK"
WHERE
    "sales_date_date"."D_YEAR" = 2000 and "sales_date_date"."D_MOY" = 2
),
puzzled as (
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
    "puzzled"."sales_billing_customer_last_name" as "c_last_name",
    "puzzled"."sales_billing_customer_first_name" as "c_first_name",
    "puzzled"."sales_total" as "sales_total"
FROM
    "puzzled"
ORDER BY 
    "c_last_name" asc nulls first,
    "c_first_name" asc nulls first,
    "puzzled"."sales_total" asc nulls first
LIMIT (100)
```
