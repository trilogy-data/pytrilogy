# Query 06

**Status:** `match`

| Stage | Result |
| --- | --- |
| v4 SQL generation | OK |
| v4 execution | OK (46 rows) |
| reference execution | OK (46 rows) |
| results identical | YES |

## Result comparison

v4 rows: 46 (46 distinct)
ref rows: 46 (46 distinct)

## SQL size + execution time

| Source | Chars | Lines | Exec (min of 4) |
| --- | --- | --- | --- |
| v4 | 2967 | 59 | 42.08 ms |
| reference | 2203 | 44 | 28.51 ms |
| v4 / ref | 1.35x | 1.34x | 1.48x |

## Preql

```
#List all the states with at least 10 customers who during a given month bought items with the price tag at least
#20% higher than the average price of items in the same category##
import store_sales as store_sales;

where
    store_sales.date.year = 2001
    and store_sales.item.category is not null
    and store_sales.date.month_of_year = 1
    and store_sales.item.current_price > 1.2 * avg(store_sales.item.current_price) by store_sales.item.category
    and store_sales.customer.id is not null
    and store_sales.customer.address.id is not null
select
    store_sales.customer.address.state,
    sum(store_sales.row_counter) as customer_count,
having
    customer_count >= 10

order by
    customer_count asc nulls first,
    store_sales.customer.address.state asc nulls first
;
```

## v4 generated SQL

```sql
WITH 
cooperative as (
SELECT
    "store_sales_customer_address_customer_address"."CA_ADDRESS_SK" as "store_sales_customer_address_id",
    "store_sales_customer_address_customer_address"."CA_STATE" as "store_sales_customer_address_state",
    "store_sales_item_items"."I_CATEGORY" as "store_sales_item_category",
    "store_sales_item_items"."I_CURRENT_PRICE" as "store_sales_item_current_price",
    "store_sales_item_items"."I_ITEM_SK" as "store_sales_item_id",
    "store_sales_store_sales"."SS_TICKET_NUMBER" as "store_sales_ticket_number",
    1 as "store_sales_row_counter"
FROM
    "memory"."store_sales" as "store_sales_store_sales"
    INNER JOIN "memory"."date_dim" as "store_sales_date_date" on "store_sales_store_sales"."SS_SOLD_DATE_SK" = "store_sales_date_date"."D_DATE_SK"
    INNER JOIN "memory"."item" as "store_sales_item_items" on "store_sales_store_sales"."SS_ITEM_SK" = "store_sales_item_items"."I_ITEM_SK"
    INNER JOIN "memory"."customer" as "store_sales_customer_customers" on "store_sales_store_sales"."SS_CUSTOMER_SK" = "store_sales_customer_customers"."C_CUSTOMER_SK"
    LEFT OUTER JOIN "memory"."customer_address" as "store_sales_customer_address_customer_address" on "store_sales_customer_customers"."C_CURRENT_ADDR_SK" = "store_sales_customer_address_customer_address"."CA_ADDRESS_SK"
WHERE
    "store_sales_date_date"."D_YEAR" = 2001 and "store_sales_item_items"."I_CATEGORY" is not null and "store_sales_date_date"."D_MOY" = 1 and "store_sales_store_sales"."SS_CUSTOMER_SK" is not null
),
questionable as (
SELECT
    "store_sales_item_items"."I_CATEGORY" as "store_sales_item_category",
    avg("store_sales_item_items"."I_CURRENT_PRICE") as "_virt_agg_avg_2054483076469165"
FROM
    "memory"."item" as "store_sales_item_items"
GROUP BY
    1),
abundant as (
SELECT
    "cooperative"."store_sales_customer_address_state" as "store_sales_customer_address_state",
    "cooperative"."store_sales_row_counter" as "store_sales_row_counter"
FROM
    "cooperative"
    INNER JOIN "questionable" on "cooperative"."store_sales_item_category" is not distinct from "questionable"."store_sales_item_category"
WHERE
    "cooperative"."store_sales_item_current_price" > 1.2 * "questionable"."_virt_agg_avg_2054483076469165"

GROUP BY
    1,
    2,
    "cooperative"."store_sales_customer_address_id",
    "cooperative"."store_sales_item_current_price",
    "cooperative"."store_sales_item_id",
    "cooperative"."store_sales_ticket_number",
    "questionable"."_virt_agg_avg_2054483076469165",
    coalesce("cooperative"."store_sales_item_category","questionable"."store_sales_item_category"))
SELECT
    "abundant"."store_sales_customer_address_state" as "store_sales_customer_address_state",
    sum("abundant"."store_sales_row_counter") as "customer_count"
FROM
    "abundant"
GROUP BY
    1
HAVING
    "customer_count" >= 10

ORDER BY 
    "customer_count" asc nulls first,
    "abundant"."store_sales_customer_address_state" asc nulls first
```

## Reference SQL (zquery log)

```sql
WITH 
cooperative as (
SELECT
    "store_sales_customer_address_customer_address"."CA_STATE" as "store_sales_customer_address_state",
    "store_sales_item_items"."I_CATEGORY" as "store_sales_item_category",
    "store_sales_item_items"."I_CURRENT_PRICE" as "store_sales_item_current_price",
    1 as "store_sales_row_counter"
FROM
    "memory"."store_sales" as "store_sales_store_sales"
    INNER JOIN "memory"."date_dim" as "store_sales_date_date" on "store_sales_store_sales"."SS_SOLD_DATE_SK" = "store_sales_date_date"."D_DATE_SK"
    INNER JOIN "memory"."item" as "store_sales_item_items" on "store_sales_store_sales"."SS_ITEM_SK" = "store_sales_item_items"."I_ITEM_SK"
    INNER JOIN "memory"."customer" as "store_sales_customer_customers" on "store_sales_store_sales"."SS_CUSTOMER_SK" = "store_sales_customer_customers"."C_CUSTOMER_SK"
    LEFT OUTER JOIN "memory"."customer_address" as "store_sales_customer_address_customer_address" on "store_sales_customer_customers"."C_CURRENT_ADDR_SK" = "store_sales_customer_address_customer_address"."CA_ADDRESS_SK"
WHERE
    "store_sales_date_date"."D_YEAR" = 2001 and "store_sales_item_items"."I_CATEGORY" is not null and "store_sales_date_date"."D_MOY" = 1 and "store_sales_store_sales"."SS_CUSTOMER_SK" is not null
),
questionable as (
SELECT
    "store_sales_item_items"."I_CATEGORY" as "store_sales_item_category",
    avg("store_sales_item_items"."I_CURRENT_PRICE") as "_virt_agg_avg_2054483076469165"
FROM
    "memory"."item" as "store_sales_item_items"
WHERE
    "store_sales_item_items"."I_CATEGORY" is not null

GROUP BY
    1)
SELECT
    "cooperative"."store_sales_customer_address_state" as "store_sales_customer_address_state",
    sum("cooperative"."store_sales_row_counter") as "customer_count"
FROM
    "cooperative"
    INNER JOIN "questionable" on "cooperative"."store_sales_item_category" is not distinct from "questionable"."store_sales_item_category"
WHERE
    "cooperative"."store_sales_item_current_price" > 1.2 * "questionable"."_virt_agg_avg_2054483076469165"

GROUP BY
    1
HAVING
    "customer_count" >= 10

ORDER BY 
    "customer_count" asc nulls first,
    "cooperative"."store_sales_customer_address_state" asc nulls first
```
