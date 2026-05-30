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
| v4 | 3460 | 67 | 28.25 ms |
| reference | 3752 | 52 | 59.52 ms |
| v4 / ref | 0.92x | 1.29x | 0.47x |

## Preql

```
import store_sales as store_sales;

where
    store_sales.store.market = 8
    and store_sales.customer.birth_country != upper(store_sales.customer.address.country)
    and store_sales.is_returned is True
    and store_sales.store.zip = store_sales.customer.address.zip
select
    store_sales.customer.last_name,
    store_sales.customer.first_name,
    store_sales.store.name,
    --avg(
            sum(store_sales.net_paid)
                by store_sales.customer.id, store_sales.item.id, store_sales.store.id
        )
            by * as avg_store_customer_sales,
    sum(store_sales.net_paid ? store_sales.item.color = 'peach') as peach_sales,
having
    peach_sales > 0.05 * avg_store_customer_sales

;
```

## v4 generated SQL

```sql
WITH 
abundant as (
SELECT
    "store_sales_customer_customers"."C_FIRST_NAME" as "store_sales_customer_first_name",
    "store_sales_customer_customers"."C_LAST_NAME" as "store_sales_customer_last_name",
    "store_sales_item_items"."I_COLOR" as "store_sales_item_color",
    "store_sales_item_items"."I_ITEM_SK" as "store_sales_item_id",
    "store_sales_store_sales"."SS_CUSTOMER_SK" as "store_sales_customer_id",
    "store_sales_store_sales"."SS_NET_PAID" as "store_sales_net_paid",
    "store_sales_store_sales"."SS_STORE_SK" as "store_sales_store_id",
    "store_sales_store_store"."S_STORE_NAME" as "store_sales_store_name"
FROM
    "memory"."store_sales" as "store_sales_store_sales"
    INNER JOIN "memory"."store" as "store_sales_store_store" on "store_sales_store_sales"."SS_STORE_SK" = "store_sales_store_store"."S_STORE_SK"
    INNER JOIN "memory"."customer" as "store_sales_customer_customers" on "store_sales_store_sales"."SS_CUSTOMER_SK" = "store_sales_customer_customers"."C_CUSTOMER_SK"
    LEFT OUTER JOIN "memory"."store_returns" as "store_sales_store_returns" on "store_sales_store_sales"."SS_ITEM_SK" = "store_sales_store_returns"."SR_ITEM_SK" AND "store_sales_store_sales"."SS_TICKET_NUMBER" = "store_sales_store_returns"."SR_TICKET_NUMBER"
    INNER JOIN "memory"."customer_address" as "store_sales_customer_address_customer_address" on "store_sales_customer_customers"."C_CURRENT_ADDR_SK" = "store_sales_customer_address_customer_address"."CA_ADDRESS_SK"
    INNER JOIN "memory"."item" as "store_sales_item_items" on "store_sales_store_sales"."SS_ITEM_SK" = "store_sales_item_items"."I_ITEM_SK"
WHERE
    "store_sales_store_store"."S_MARKET_ID" = 8 and "store_sales_customer_customers"."C_BIRTH_COUNTRY" != UPPER("store_sales_customer_address_customer_address"."CA_COUNTRY")  and SR_RETURN_TIME_SK IS NOT NULL is True and "store_sales_store_store"."S_ZIP" = "store_sales_customer_address_customer_address"."CA_ZIP"
),
quizzical as (
SELECT
    1 as "__preql_internal_all_rows"
),
vacuous as (
SELECT
    "abundant"."store_sales_customer_first_name" as "store_sales_customer_first_name",
    "abundant"."store_sales_customer_last_name" as "store_sales_customer_last_name",
    "abundant"."store_sales_store_name" as "store_sales_store_name",
    sum(CASE WHEN "abundant"."store_sales_item_color" = 'peach' THEN "abundant"."store_sales_net_paid" ELSE NULL END) as "peach_sales"
FROM
    "abundant"
GROUP BY
    1,
    2,
    3),
uneven as (
SELECT
    sum("abundant"."store_sales_net_paid") as "_virt_agg_sum_1360566110228423"
FROM
    "abundant"
GROUP BY
    "abundant"."store_sales_customer_first_name",
    "abundant"."store_sales_customer_id",
    "abundant"."store_sales_customer_last_name",
    "abundant"."store_sales_item_id",
    "abundant"."store_sales_store_id",
    "abundant"."store_sales_store_name"),
yummy as (
SELECT
    avg("uneven"."_virt_agg_sum_1360566110228423") as "avg_store_customer_sales"
FROM
    "quizzical"
    FULL JOIN "uneven" on 1=1
GROUP BY
    "quizzical"."__preql_internal_all_rows")
SELECT
    "vacuous"."store_sales_customer_last_name" as "store_sales_customer_last_name",
    "vacuous"."store_sales_customer_first_name" as "store_sales_customer_first_name",
    "vacuous"."store_sales_store_name" as "store_sales_store_name",
    "vacuous"."peach_sales" as "peach_sales"
FROM
    "yummy"
    INNER JOIN "vacuous" on 1=1
WHERE
    "vacuous"."peach_sales" > 0.05 * "yummy"."avg_store_customer_sales"
```

## Reference SQL (zquery log)

```sql
WITH 
yummy as (
SELECT
    sum("store_sales_store_sales"."SS_NET_PAID") as "_virt_agg_sum_1360566110228423"
FROM
    "memory"."store_sales" as "store_sales_store_sales"
    LEFT OUTER JOIN "memory"."store_returns" as "store_sales_store_returns" on "store_sales_store_sales"."SS_ITEM_SK" = "store_sales_store_returns"."SR_ITEM_SK" AND "store_sales_store_sales"."SS_TICKET_NUMBER" = "store_sales_store_returns"."SR_TICKET_NUMBER"
    INNER JOIN "memory"."store" as "store_sales_store_store" on "store_sales_store_sales"."SS_STORE_SK" = "store_sales_store_store"."S_STORE_SK"
    INNER JOIN "memory"."customer" as "store_sales_customer_customers" on "store_sales_store_sales"."SS_CUSTOMER_SK" = "store_sales_customer_customers"."C_CUSTOMER_SK"
    INNER JOIN "memory"."customer_address" as "store_sales_customer_address_customer_address" on "store_sales_customer_customers"."C_CURRENT_ADDR_SK" = "store_sales_customer_address_customer_address"."CA_ADDRESS_SK"
WHERE
    "store_sales_store_store"."S_MARKET_ID" = 8 and "store_sales_customer_customers"."C_BIRTH_COUNTRY" != UPPER("store_sales_customer_address_customer_address"."CA_COUNTRY")  and SR_RETURN_TIME_SK IS NOT NULL is True and "store_sales_store_store"."S_ZIP" = "store_sales_customer_address_customer_address"."CA_ZIP"

GROUP BY
    "store_sales_store_sales"."SS_CUSTOMER_SK",
    "store_sales_store_sales"."SS_ITEM_SK",
    "store_sales_store_sales"."SS_STORE_SK"),
questionable as (
SELECT
    "store_sales_customer_customers"."C_FIRST_NAME" as "store_sales_customer_first_name",
    "store_sales_customer_customers"."C_LAST_NAME" as "store_sales_customer_last_name",
    "store_sales_store_store"."S_STORE_NAME" as "store_sales_store_name",
    sum(CASE WHEN "store_sales_item_items"."I_COLOR" = 'peach' THEN "store_sales_store_sales"."SS_NET_PAID" ELSE NULL END) as "peach_sales"
FROM
    "memory"."store_sales" as "store_sales_store_sales"
    INNER JOIN "memory"."store" as "store_sales_store_store" on "store_sales_store_sales"."SS_STORE_SK" = "store_sales_store_store"."S_STORE_SK"
    INNER JOIN "memory"."customer" as "store_sales_customer_customers" on "store_sales_store_sales"."SS_CUSTOMER_SK" = "store_sales_customer_customers"."C_CUSTOMER_SK"
    LEFT OUTER JOIN "memory"."store_returns" as "store_sales_store_returns" on "store_sales_store_sales"."SS_ITEM_SK" = "store_sales_store_returns"."SR_ITEM_SK" AND "store_sales_store_sales"."SS_TICKET_NUMBER" = "store_sales_store_returns"."SR_TICKET_NUMBER"
    INNER JOIN "memory"."customer_address" as "store_sales_customer_address_customer_address" on "store_sales_customer_customers"."C_CURRENT_ADDR_SK" = "store_sales_customer_address_customer_address"."CA_ADDRESS_SK"
    INNER JOIN "memory"."item" as "store_sales_item_items" on "store_sales_store_sales"."SS_ITEM_SK" = "store_sales_item_items"."I_ITEM_SK"
WHERE
    "store_sales_store_store"."S_MARKET_ID" = 8 and "store_sales_customer_customers"."C_BIRTH_COUNTRY" != UPPER("store_sales_customer_address_customer_address"."CA_COUNTRY")  and SR_RETURN_TIME_SK IS NOT NULL is True and "store_sales_store_store"."S_ZIP" = "store_sales_customer_address_customer_address"."CA_ZIP"

GROUP BY
    1,
    2,
    3),
vacuous as (
SELECT
    avg("yummy"."_virt_agg_sum_1360566110228423") as "avg_store_customer_sales"
FROM
    "yummy")
SELECT
    "questionable"."store_sales_customer_last_name" as "store_sales_customer_last_name",
    "questionable"."store_sales_customer_first_name" as "store_sales_customer_first_name",
    "questionable"."store_sales_store_name" as "store_sales_store_name",
    "questionable"."peach_sales" as "peach_sales"
FROM
    "vacuous"
    INNER JOIN "questionable" on 1=1
WHERE
    "questionable"."peach_sales" > 0.05 * "vacuous"."avg_store_customer_sales"
```
