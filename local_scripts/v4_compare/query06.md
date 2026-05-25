# Query 06

**Status:** `mismatch`

| Stage | Result |
| --- | --- |
| v4 SQL generation | OK |
| v4 execution | OK (52 rows) |
| reference execution | OK (46 rows) |
| results identical | NO |

## Result comparison

v4 rows: 52 (52 distinct)
ref rows: 46 (46 distinct)
only in v4 (showing up to 5 of 52):
  1x  (11, 'DC')
  1x  (23, 'HI')
  1x  (28, 'DE')
  1x  (42, 'RI')
  1x  (91, 'CT')
only in ref (showing up to 5 of 46):
  1x  (11, 'WY')
  1x  (16, 'VT')
  1x  (17, 'ME')
  1x  (19, 'MD')
  1x  (19, 'NJ')

## SQL size

| Source | Chars | Lines |
| --- | --- | --- |
| v4 | 3772 | 93 |
| reference | 2203 | 44 |
| v4 / ref | 1.71x | 2.11x |

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
thoughtful as (
SELECT
    "store_sales_store_sales"."SS_CUSTOMER_SK" as "store_sales_customer_id",
    "store_sales_store_sales"."SS_ITEM_SK" as "store_sales_item_id",
    "store_sales_store_sales"."SS_SOLD_DATE_SK" as "store_sales_date_id",
    1 as "store_sales_row_counter"
FROM
    "memory"."store_sales" as "store_sales_store_sales"),
cooperative as (
SELECT
    "thoughtful"."store_sales_customer_id" as "store_sales_customer_id",
    "thoughtful"."store_sales_date_id" as "store_sales_date_id",
    "thoughtful"."store_sales_item_id" as "store_sales_item_id",
    "thoughtful"."store_sales_row_counter" as "store_sales_row_counter"
FROM
    "thoughtful"
GROUP BY
    1,
    2,
    3,
    4),
cheerful as (
SELECT
    "store_sales_item_items"."I_CATEGORY" as "store_sales_item_category",
    "store_sales_item_items"."I_CURRENT_PRICE" as "store_sales_item_current_price",
    "store_sales_item_items"."I_ITEM_SK" as "store_sales_item_id"
FROM
    "memory"."item" as "store_sales_item_items"),
wakeful as (
SELECT
    "store_sales_date_date"."D_DATE_SK" as "store_sales_date_id",
    "store_sales_date_date"."D_MOY" as "store_sales_date_month_of_year",
    "store_sales_date_date"."D_YEAR" as "store_sales_date_year"
FROM
    "memory"."date_dim" as "store_sales_date_date"),
highfalutin as (
SELECT
    "store_sales_customer_customers"."C_CURRENT_ADDR_SK" as "store_sales_customer_address_id",
    "store_sales_customer_customers"."C_CUSTOMER_SK" as "store_sales_customer_id"
FROM
    "memory"."customer" as "store_sales_customer_customers"),
quizzical as (
SELECT
    "store_sales_customer_address_customer_address"."CA_ADDRESS_SK" as "store_sales_customer_address_id",
    "store_sales_customer_address_customer_address"."CA_STATE" as "store_sales_customer_address_state"
FROM
    "memory"."customer_address" as "store_sales_customer_address_customer_address"),
questionable as (
SELECT
    "cheerful"."store_sales_item_category" as "store_sales_item_category",
    "cheerful"."store_sales_item_current_price" as "store_sales_item_current_price",
    "cooperative"."store_sales_customer_id" as "store_sales_customer_id",
    "cooperative"."store_sales_row_counter" as "store_sales_row_counter",
    "quizzical"."store_sales_customer_address_state" as "store_sales_customer_address_state",
    "wakeful"."store_sales_date_month_of_year" as "store_sales_date_month_of_year",
    "wakeful"."store_sales_date_year" as "store_sales_date_year"
FROM
    "cooperative"
    LEFT OUTER JOIN "wakeful" on "cooperative"."store_sales_date_id" = "wakeful"."store_sales_date_id"
    INNER JOIN "cheerful" on "cooperative"."store_sales_item_id" = "cheerful"."store_sales_item_id"
    LEFT OUTER JOIN "highfalutin" on "cooperative"."store_sales_customer_id" = "highfalutin"."store_sales_customer_id"
    LEFT OUTER JOIN "quizzical" on "highfalutin"."store_sales_customer_address_id" = "quizzical"."store_sales_customer_address_id"
WHERE
    "wakeful"."store_sales_date_year" = 2001 and "cheerful"."store_sales_item_category" is not null and "wakeful"."store_sales_date_month_of_year" = 1 and "cooperative"."store_sales_customer_id" is not null

GROUP BY
    1,
    2,
    3,
    4,
    5,
    6,
    7),
abundant as (
SELECT
    "questionable"."store_sales_customer_address_state" as "store_sales_customer_address_state",
    sum("questionable"."store_sales_row_counter") as "customer_count"
FROM
    "questionable"
GROUP BY
    1)
SELECT
    "abundant"."store_sales_customer_address_state" as "store_sales_customer_address_state",
    "abundant"."customer_count" as "customer_count"
FROM
    "abundant"
WHERE
    "abundant"."customer_count" >= 10

ORDER BY 
    "abundant"."customer_count" asc nulls first,
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
