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
| v4 | 3739 | 75 | 49.24 ms |
| reference | 2439 | 44 | 23.30 ms |
| v4 / ref | 1.53x | 1.70x | 2.11x |

## Preql

```
#List all the states with at least 10 customers who during a given month bought items with the price tag at least
#20% higher than the average price of items in the same category##
import physical_sales as physical_sales;

where
    physical_sales.date.year = 2001
    and physical_sales.item.category is not null
    and physical_sales.date.month_of_year = 1
    and physical_sales.item.current_price > 1.2 * avg(physical_sales.item.current_price) by physical_sales.item.category
    and physical_sales.billing_customer.id is not null
    and physical_sales.billing_customer.address.id is not null
select
    physical_sales.billing_customer.address.state,
    physical_sales.line_item_count
having
    physical_sales.line_item_count >= 10

order by
    physical_sales.line_item_count asc nulls first,
    physical_sales.billing_customer.address.state asc nulls first
;
```

## v4 generated SQL

```sql
WITH 
cooperative as (
SELECT
    "physical_sales_billing_customer_address_customer_address"."CA_ADDRESS_SK" as "physical_sales_billing_customer_address_id",
    "physical_sales_billing_customer_address_customer_address"."CA_STATE" as "physical_sales_billing_customer_address_state",
    "physical_sales_item_items"."I_CATEGORY" as "physical_sales_item_category",
    "physical_sales_item_items"."I_CURRENT_PRICE" as "physical_sales_item_current_price",
    "physical_sales_item_items"."I_ITEM_SK" as "physical_sales_item_id",
    "physical_sales_store_sales"."SS_TICKET_NUMBER" as "physical_sales_ticket_number",
    1 as "physical_sales_row_counter"
FROM
    "memory"."store_sales" as "physical_sales_store_sales"
    INNER JOIN "memory"."date_dim" as "physical_sales_date_date" on "physical_sales_store_sales"."SS_SOLD_DATE_SK" = "physical_sales_date_date"."D_DATE_SK"
    INNER JOIN "memory"."item" as "physical_sales_item_items" on "physical_sales_store_sales"."SS_ITEM_SK" = "physical_sales_item_items"."I_ITEM_SK"
    INNER JOIN "memory"."customer" as "physical_sales_billing_customer_customers" on "physical_sales_store_sales"."SS_CUSTOMER_SK" = "physical_sales_billing_customer_customers"."C_CUSTOMER_SK"
    LEFT OUTER JOIN "memory"."customer_address" as "physical_sales_billing_customer_address_customer_address" on "physical_sales_billing_customer_customers"."C_CURRENT_ADDR_SK" = "physical_sales_billing_customer_address_customer_address"."CA_ADDRESS_SK"
WHERE
    "physical_sales_date_date"."D_YEAR" = 2001 and "physical_sales_item_items"."I_CATEGORY" is not null and "physical_sales_date_date"."D_MOY" = 1 and "physical_sales_store_sales"."SS_CUSTOMER_SK" is not null
),
questionable as (
SELECT
    "physical_sales_item_items"."I_CATEGORY" as "physical_sales_item_category",
    avg("physical_sales_item_items"."I_CURRENT_PRICE") as "_virt_agg_avg_8857095867163344"
FROM
    "memory"."item" as "physical_sales_item_items"
WHERE
    "physical_sales_item_items"."I_CATEGORY" is not null

GROUP BY
    1),
yummy as (
SELECT
    "cooperative"."physical_sales_billing_customer_address_state" as "physical_sales_billing_customer_address_state",
    "cooperative"."physical_sales_item_id" as "physical_sales_item_id",
    "cooperative"."physical_sales_row_counter" as "physical_sales_row_counter",
    "cooperative"."physical_sales_ticket_number" as "physical_sales_ticket_number"
FROM
    "cooperative"
    INNER JOIN "questionable" on "cooperative"."physical_sales_item_category" is not distinct from "questionable"."physical_sales_item_category"
WHERE
    "cooperative"."physical_sales_item_current_price" > 1.2 * "questionable"."_virt_agg_avg_8857095867163344"

GROUP BY
    1,
    2,
    3,
    4,
    "cooperative"."physical_sales_billing_customer_address_id",
    "cooperative"."physical_sales_item_current_price",
    "questionable"."_virt_agg_avg_8857095867163344",
    coalesce("cooperative"."physical_sales_item_category","questionable"."physical_sales_item_category")),
juicy as (
SELECT
    "yummy"."physical_sales_billing_customer_address_state" as "physical_sales_billing_customer_address_state",
    "yummy"."physical_sales_row_counter" as "physical_sales_row_counter"
FROM
    "yummy"
GROUP BY
    1,
    2,
    "yummy"."physical_sales_item_id",
    "yummy"."physical_sales_ticket_number")
SELECT
    "juicy"."physical_sales_billing_customer_address_state" as "physical_sales_billing_customer_address_state",
    sum("juicy"."physical_sales_row_counter") as "physical_sales_line_item_count"
FROM
    "juicy"
GROUP BY
    1
HAVING
    "physical_sales_line_item_count" >= 10

ORDER BY 
    "physical_sales_line_item_count" asc nulls first,
    "juicy"."physical_sales_billing_customer_address_state" asc nulls first
```

## Reference SQL (zquery log)

```sql
WITH 
cooperative as (
SELECT
    "physical_sales_billing_customer_address_customer_address"."CA_STATE" as "physical_sales_billing_customer_address_state",
    "physical_sales_item_items"."I_CATEGORY" as "physical_sales_item_category",
    "physical_sales_item_items"."I_CURRENT_PRICE" as "physical_sales_item_current_price",
    1 as "physical_sales_row_counter"
FROM
    "memory"."store_sales" as "physical_sales_store_sales"
    INNER JOIN "memory"."date_dim" as "physical_sales_date_date" on "physical_sales_store_sales"."SS_SOLD_DATE_SK" = "physical_sales_date_date"."D_DATE_SK"
    INNER JOIN "memory"."item" as "physical_sales_item_items" on "physical_sales_store_sales"."SS_ITEM_SK" = "physical_sales_item_items"."I_ITEM_SK"
    INNER JOIN "memory"."customer" as "physical_sales_billing_customer_customers" on "physical_sales_store_sales"."SS_CUSTOMER_SK" = "physical_sales_billing_customer_customers"."C_CUSTOMER_SK"
    LEFT OUTER JOIN "memory"."customer_address" as "physical_sales_billing_customer_address_customer_address" on "physical_sales_billing_customer_customers"."C_CURRENT_ADDR_SK" = "physical_sales_billing_customer_address_customer_address"."CA_ADDRESS_SK"
WHERE
    "physical_sales_date_date"."D_YEAR" = 2001 and "physical_sales_item_items"."I_CATEGORY" is not null and "physical_sales_date_date"."D_MOY" = 1 and "physical_sales_store_sales"."SS_CUSTOMER_SK" is not null
),
questionable as (
SELECT
    "physical_sales_item_items"."I_CATEGORY" as "physical_sales_item_category",
    avg("physical_sales_item_items"."I_CURRENT_PRICE") as "_virt_agg_avg_8857095867163344"
FROM
    "memory"."item" as "physical_sales_item_items"
WHERE
    "physical_sales_item_items"."I_CATEGORY" is not null

GROUP BY
    1)
SELECT
    "cooperative"."physical_sales_billing_customer_address_state" as "physical_sales_billing_customer_address_state",
    sum("cooperative"."physical_sales_row_counter") as "physical_sales_line_item_count"
FROM
    "cooperative"
    INNER JOIN "questionable" on "cooperative"."physical_sales_item_category" is not distinct from "questionable"."physical_sales_item_category"
WHERE
    "cooperative"."physical_sales_item_current_price" > 1.2 * "questionable"."_virt_agg_avg_8857095867163344"

GROUP BY
    1
HAVING
    "physical_sales_line_item_count" >= 10

ORDER BY 
    "physical_sales_line_item_count" asc nulls first,
    "cooperative"."physical_sales_billing_customer_address_state" asc nulls first
```
