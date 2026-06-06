# Query 24

**Status:** `match`

| Stage | Result |
| --- | --- |
| v4 SQL generation | OK |
| v4 execution | OK (1 rows) |
| reference execution | OK (1 rows) |
| results identical | YES |

## Result comparison

v4 rows: 1 (1 distinct)
ref rows: 1 (1 distinct)

## SQL size + execution time

| Source | Chars | Lines | Exec (min of 4) |
| --- | --- | --- | --- |
| v4 | 4227 | 62 | 58.69 ms |
| reference | 3925 | 52 | 52.54 ms |
| v4 / ref | 1.08x | 1.19x | 1.12x |

## Preql

```
import physical_sales as physical_sales;

where
    physical_sales.store.market = 8
    and physical_sales.customer.birth_country != upper(physical_sales.customer.address.country)
    and physical_sales.is_returned is True
    and physical_sales.store.zip = physical_sales.customer.address.zip
select
    physical_sales.customer.last_name,
    physical_sales.customer.first_name,
    physical_sales.store.name,
    --avg(
            sum(physical_sales.net_paid)
                by physical_sales.customer.id, physical_sales.item.id, physical_sales.store.id
        )
            by * as avg_store_customer_sales,
    sum(physical_sales.net_paid ? physical_sales.item.color = 'peach') as peach_sales,
having
    peach_sales > 0.05 * avg_store_customer_sales

;
```

## v4 generated SQL

```sql
WITH 
juicy as (
SELECT
    sum("physical_sales_store_sales"."SS_NET_PAID") as "_virt_agg_sum_5757395975543459"
FROM
    "memory"."store_sales" as "physical_sales_store_sales"
    INNER JOIN "memory"."store_returns" as "physical_sales_store_returns" on "physical_sales_store_sales"."SS_ITEM_SK" = "physical_sales_store_returns"."SR_ITEM_SK" AND "physical_sales_store_sales"."SS_TICKET_NUMBER" = "physical_sales_store_returns"."SR_TICKET_NUMBER"
    INNER JOIN "memory"."store" as "physical_sales_store_store" on "physical_sales_store_sales"."SS_STORE_SK" = "physical_sales_store_store"."S_STORE_SK"
    INNER JOIN "memory"."customer" as "physical_sales_customer_customers" on "physical_sales_store_sales"."SS_CUSTOMER_SK" = "physical_sales_customer_customers"."C_CUSTOMER_SK"
    INNER JOIN "memory"."customer_address" as "physical_sales_customer_address_customer_address" on "physical_sales_customer_customers"."C_CURRENT_ADDR_SK" = "physical_sales_customer_address_customer_address"."CA_ADDRESS_SK"
WHERE
    "physical_sales_store_store"."S_MARKET_ID" = 8 and "physical_sales_customer_customers"."C_BIRTH_COUNTRY" != UPPER("physical_sales_customer_address_customer_address"."CA_COUNTRY")  and SR_RETURN_TIME_SK IS NOT NULL is True and "physical_sales_store_store"."S_ZIP" = "physical_sales_customer_address_customer_address"."CA_ZIP"

GROUP BY
    "physical_sales_customer_customers"."C_FIRST_NAME",
    "physical_sales_customer_customers"."C_LAST_NAME",
    "physical_sales_store_sales"."SS_CUSTOMER_SK",
    "physical_sales_store_sales"."SS_ITEM_SK",
    "physical_sales_store_sales"."SS_STORE_SK",
    "physical_sales_store_store"."S_STORE_NAME"),
young as (
SELECT
    1 as "__preql_internal_all_rows"
),
questionable as (
SELECT
    "physical_sales_customer_customers"."C_FIRST_NAME" as "physical_sales_customer_first_name",
    "physical_sales_customer_customers"."C_LAST_NAME" as "physical_sales_customer_last_name",
    "physical_sales_store_store"."S_STORE_NAME" as "physical_sales_store_name",
    sum(CASE WHEN "physical_sales_item_items"."I_COLOR" = 'peach' THEN "physical_sales_store_sales"."SS_NET_PAID" ELSE NULL END) as "peach_sales"
FROM
    "memory"."store_sales" as "physical_sales_store_sales"
    INNER JOIN "memory"."store" as "physical_sales_store_store" on "physical_sales_store_sales"."SS_STORE_SK" = "physical_sales_store_store"."S_STORE_SK"
    INNER JOIN "memory"."customer" as "physical_sales_customer_customers" on "physical_sales_store_sales"."SS_CUSTOMER_SK" = "physical_sales_customer_customers"."C_CUSTOMER_SK"
    INNER JOIN "memory"."store_returns" as "physical_sales_store_returns" on "physical_sales_store_sales"."SS_ITEM_SK" = "physical_sales_store_returns"."SR_ITEM_SK" AND "physical_sales_store_sales"."SS_TICKET_NUMBER" = "physical_sales_store_returns"."SR_TICKET_NUMBER"
    INNER JOIN "memory"."customer_address" as "physical_sales_customer_address_customer_address" on "physical_sales_customer_customers"."C_CURRENT_ADDR_SK" = "physical_sales_customer_address_customer_address"."CA_ADDRESS_SK"
    INNER JOIN "memory"."item" as "physical_sales_item_items" on "physical_sales_store_sales"."SS_ITEM_SK" = "physical_sales_item_items"."I_ITEM_SK"
WHERE
    "physical_sales_store_store"."S_MARKET_ID" = 8 and "physical_sales_customer_customers"."C_BIRTH_COUNTRY" != UPPER("physical_sales_customer_address_customer_address"."CA_COUNTRY")  and SR_RETURN_TIME_SK IS NOT NULL is True and "physical_sales_store_store"."S_ZIP" = "physical_sales_customer_address_customer_address"."CA_ZIP"

GROUP BY
    1,
    2,
    3),
sparkling as (
SELECT
    avg("juicy"."_virt_agg_sum_5757395975543459") as "avg_store_customer_sales"
FROM
    "young"
    FULL JOIN "juicy" on 1=1
GROUP BY
    "young"."__preql_internal_all_rows")
SELECT
    "questionable"."physical_sales_customer_last_name" as "physical_sales_customer_last_name",
    "questionable"."physical_sales_customer_first_name" as "physical_sales_customer_first_name",
    "questionable"."physical_sales_store_name" as "physical_sales_store_name",
    "questionable"."peach_sales" as "peach_sales"
FROM
    "sparkling"
    INNER JOIN "questionable" on 1=1
WHERE
    "questionable"."peach_sales" > 0.05 * "sparkling"."avg_store_customer_sales"
```

## Reference SQL (zquery log)

```sql
WITH 
yummy as (
SELECT
    sum("physical_sales_store_sales"."SS_NET_PAID") as "_virt_agg_sum_5757395975543459"
FROM
    "memory"."store_sales" as "physical_sales_store_sales"
    INNER JOIN "memory"."store_returns" as "physical_sales_store_returns" on "physical_sales_store_sales"."SS_ITEM_SK" = "physical_sales_store_returns"."SR_ITEM_SK" AND "physical_sales_store_sales"."SS_TICKET_NUMBER" = "physical_sales_store_returns"."SR_TICKET_NUMBER"
    INNER JOIN "memory"."store" as "physical_sales_store_store" on "physical_sales_store_sales"."SS_STORE_SK" = "physical_sales_store_store"."S_STORE_SK"
    INNER JOIN "memory"."customer" as "physical_sales_customer_customers" on "physical_sales_store_sales"."SS_CUSTOMER_SK" = "physical_sales_customer_customers"."C_CUSTOMER_SK"
    INNER JOIN "memory"."customer_address" as "physical_sales_customer_address_customer_address" on "physical_sales_customer_customers"."C_CURRENT_ADDR_SK" = "physical_sales_customer_address_customer_address"."CA_ADDRESS_SK"
WHERE
    "physical_sales_store_store"."S_MARKET_ID" = 8 and "physical_sales_customer_customers"."C_BIRTH_COUNTRY" != UPPER("physical_sales_customer_address_customer_address"."CA_COUNTRY")  and SR_RETURN_TIME_SK IS NOT NULL is True and "physical_sales_store_store"."S_ZIP" = "physical_sales_customer_address_customer_address"."CA_ZIP"

GROUP BY
    "physical_sales_store_sales"."SS_CUSTOMER_SK",
    "physical_sales_store_sales"."SS_ITEM_SK",
    "physical_sales_store_sales"."SS_STORE_SK"),
questionable as (
SELECT
    "physical_sales_customer_customers"."C_FIRST_NAME" as "physical_sales_customer_first_name",
    "physical_sales_customer_customers"."C_LAST_NAME" as "physical_sales_customer_last_name",
    "physical_sales_store_store"."S_STORE_NAME" as "physical_sales_store_name",
    sum(CASE WHEN "physical_sales_item_items"."I_COLOR" = 'peach' THEN "physical_sales_store_sales"."SS_NET_PAID" ELSE NULL END) as "peach_sales"
FROM
    "memory"."store_sales" as "physical_sales_store_sales"
    INNER JOIN "memory"."store" as "physical_sales_store_store" on "physical_sales_store_sales"."SS_STORE_SK" = "physical_sales_store_store"."S_STORE_SK"
    INNER JOIN "memory"."customer" as "physical_sales_customer_customers" on "physical_sales_store_sales"."SS_CUSTOMER_SK" = "physical_sales_customer_customers"."C_CUSTOMER_SK"
    INNER JOIN "memory"."store_returns" as "physical_sales_store_returns" on "physical_sales_store_sales"."SS_ITEM_SK" = "physical_sales_store_returns"."SR_ITEM_SK" AND "physical_sales_store_sales"."SS_TICKET_NUMBER" = "physical_sales_store_returns"."SR_TICKET_NUMBER"
    INNER JOIN "memory"."customer_address" as "physical_sales_customer_address_customer_address" on "physical_sales_customer_customers"."C_CURRENT_ADDR_SK" = "physical_sales_customer_address_customer_address"."CA_ADDRESS_SK"
    INNER JOIN "memory"."item" as "physical_sales_item_items" on "physical_sales_store_sales"."SS_ITEM_SK" = "physical_sales_item_items"."I_ITEM_SK"
WHERE
    "physical_sales_store_store"."S_MARKET_ID" = 8 and "physical_sales_customer_customers"."C_BIRTH_COUNTRY" != UPPER("physical_sales_customer_address_customer_address"."CA_COUNTRY")  and SR_RETURN_TIME_SK IS NOT NULL is True and "physical_sales_store_store"."S_ZIP" = "physical_sales_customer_address_customer_address"."CA_ZIP"

GROUP BY
    1,
    2,
    3),
vacuous as (
SELECT
    avg("yummy"."_virt_agg_sum_5757395975543459") as "avg_store_customer_sales"
FROM
    "yummy")
SELECT
    "questionable"."physical_sales_customer_last_name" as "physical_sales_customer_last_name",
    "questionable"."physical_sales_customer_first_name" as "physical_sales_customer_first_name",
    "questionable"."physical_sales_store_name" as "physical_sales_store_name",
    "questionable"."peach_sales" as "peach_sales"
FROM
    "vacuous"
    INNER JOIN "questionable" on 1=1
WHERE
    "questionable"."peach_sales" > 0.05 * "vacuous"."avg_store_customer_sales"
```
