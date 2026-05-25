# Query 24

**Status:** `exec_fail`

| Stage | Result |
| --- | --- |
| v4 SQL generation | OK |
| v4 execution | FAILED |
| reference execution | OK (1 rows) |

## Result comparison

_at least one side did not produce rows._

## SQL size

| Source | Chars | Lines |
| --- | --- | --- |
| v4 | 6560 | 148 |
| reference | 3752 | 52 |
| v4 / ref | 1.75x | 2.85x |

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
questionable as (
SELECT
    "store_sales_store_sales"."SS_CUSTOMER_SK" as "store_sales_customer_id",
    "store_sales_store_sales"."SS_ITEM_SK" as "store_sales_item_id",
    "store_sales_store_sales"."SS_NET_PAID" as "store_sales_net_paid",
    "store_sales_store_sales"."SS_STORE_SK" as "store_sales_store_id",
    "store_sales_store_sales"."SS_TICKET_NUMBER" as "store_sales_ticket_number"
FROM
    "memory"."store_sales" as "store_sales_store_sales"),
cooperative as (
SELECT
    "store_sales_store_returns"."SR_ITEM_SK" as "store_sales_item_id",
    "store_sales_store_returns"."SR_TICKET_NUMBER" as "store_sales_ticket_number",
    SR_RETURN_TIME_SK IS NOT NULL as "store_sales_is_returned"
FROM
    "memory"."store_returns" as "store_sales_store_returns"),
thoughtful as (
SELECT
    "store_sales_store_store"."S_MARKET_ID" as "store_sales_store_market",
    "store_sales_store_store"."S_STORE_NAME" as "store_sales_store_name",
    "store_sales_store_store"."S_STORE_SK" as "store_sales_store_id",
    "store_sales_store_store"."S_ZIP" as "store_sales_store_zip"
FROM
    "memory"."store" as "store_sales_store_store"),
cheerful as (
SELECT
    "store_sales_item_items"."I_COLOR" as "store_sales_item_color",
    "store_sales_item_items"."I_ITEM_SK" as "store_sales_item_id"
FROM
    "memory"."item" as "store_sales_item_items"),
wakeful as (
SELECT
    "store_sales_customer_customers"."C_BIRTH_COUNTRY" as "store_sales_customer_birth_country",
    "store_sales_customer_customers"."C_CURRENT_ADDR_SK" as "store_sales_customer_address_id",
    "store_sales_customer_customers"."C_CUSTOMER_SK" as "store_sales_customer_id",
    "store_sales_customer_customers"."C_FIRST_NAME" as "store_sales_customer_first_name",
    "store_sales_customer_customers"."C_LAST_NAME" as "store_sales_customer_last_name"
FROM
    "memory"."customer" as "store_sales_customer_customers"),
highfalutin as (
SELECT
    "store_sales_customer_address_customer_address"."CA_ADDRESS_SK" as "store_sales_customer_address_id",
    "store_sales_customer_address_customer_address"."CA_COUNTRY" as "store_sales_customer_address_country",
    "store_sales_customer_address_customer_address"."CA_ZIP" as "store_sales_customer_address_zip"
FROM
    "memory"."customer_address" as "store_sales_customer_address_customer_address"),
abundant as (
SELECT
    "cheerful"."store_sales_item_color" as "store_sales_item_color",
    "cheerful"."store_sales_item_id" as "store_sales_item_id",
    "cooperative"."store_sales_is_returned" as "store_sales_is_returned",
    "highfalutin"."store_sales_customer_address_country" as "store_sales_customer_address_country",
    "highfalutin"."store_sales_customer_address_zip" as "store_sales_customer_address_zip",
    "questionable"."store_sales_customer_id" as "store_sales_customer_id",
    "questionable"."store_sales_net_paid" as "store_sales_net_paid",
    "questionable"."store_sales_store_id" as "store_sales_store_id",
    "thoughtful"."store_sales_store_market" as "store_sales_store_market",
    "thoughtful"."store_sales_store_name" as "store_sales_store_name",
    "thoughtful"."store_sales_store_zip" as "store_sales_store_zip",
    "wakeful"."store_sales_customer_birth_country" as "store_sales_customer_birth_country",
    "wakeful"."store_sales_customer_first_name" as "store_sales_customer_first_name",
    "wakeful"."store_sales_customer_last_name" as "store_sales_customer_last_name"
FROM
    "questionable"
    LEFT OUTER JOIN "thoughtful" on "questionable"."store_sales_store_id" = "thoughtful"."store_sales_store_id"
    LEFT OUTER JOIN "wakeful" on "questionable"."store_sales_customer_id" = "wakeful"."store_sales_customer_id"
    LEFT OUTER JOIN "cooperative" on "questionable"."store_sales_item_id" = "cooperative"."store_sales_item_id" AND "questionable"."store_sales_ticket_number" = "cooperative"."store_sales_ticket_number"
    LEFT OUTER JOIN "highfalutin" on "wakeful"."store_sales_customer_address_id" = "highfalutin"."store_sales_customer_address_id"
    INNER JOIN "cheerful" on "questionable"."store_sales_item_id" = "cheerful"."store_sales_item_id"
WHERE
    "thoughtful"."store_sales_store_market" = 8 and "wakeful"."store_sales_customer_birth_country" != UPPER("highfalutin"."store_sales_customer_address_country")  and "cooperative"."store_sales_is_returned" is True and "thoughtful"."store_sales_store_zip" = "highfalutin"."store_sales_customer_address_zip"

GROUP BY
    1,
    2,
    3,
    4,
    5,
    6,
    7,
    8,
    9,
    10,
    11,
    12,
    13,
    14),
juicy as (
SELECT
    CASE WHEN "abundant"."store_sales_item_color" = 'peach' THEN "abundant"."store_sales_net_paid" ELSE NULL END as "_virt_filter_net_paid_7736053037874424"
FROM
    "abundant"),
vacuous as (
SELECT
    "abundant"."store_sales_customer_first_name" as "store_sales_customer_first_name",
    "abundant"."store_sales_customer_last_name" as "store_sales_customer_last_name",
    "abundant"."store_sales_store_name" as "store_sales_store_name",
    sum("juicy"."_virt_filter_net_paid_7736053037874424") as "peach_sales"
FROM
    "abundant"
GROUP BY
    1,
    2,
    3),
uneven as (
SELECT
    "abundant"."store_sales_customer_id" as "store_sales_customer_id",
    "abundant"."store_sales_item_id" as "store_sales_item_id",
    "abundant"."store_sales_store_id" as "store_sales_store_id",
    sum("abundant"."store_sales_net_paid") as "_virt_agg_sum_1360566110228423"
FROM
    "abundant"
GROUP BY
    1,
    2,
    3),
quizzical as (
SELECT
    1 as "__preql_internal_all_rows"
),
yummy as (
SELECT
    "quizzical"."__preql_internal_all_rows" as "__preql_internal_all_rows",
    avg("uneven"."_virt_agg_sum_1360566110228423") as "avg_store_customer_sales"
FROM
    "quizzical"
GROUP BY
    1),
concerned as (
SELECT
    "vacuous"."peach_sales" as "peach_sales",
    "vacuous"."store_sales_customer_first_name" as "store_sales_customer_first_name",
    "vacuous"."store_sales_customer_last_name" as "store_sales_customer_last_name",
    "vacuous"."store_sales_store_name" as "store_sales_store_name",
    "yummy"."avg_store_customer_sales" as "avg_store_customer_sales"
FROM
    "yummy"
    FULL JOIN "vacuous" on 1=1)
SELECT
    "concerned"."store_sales_customer_last_name" as "store_sales_customer_last_name",
    "concerned"."store_sales_customer_first_name" as "store_sales_customer_first_name",
    "concerned"."store_sales_store_name" as "store_sales_store_name",
    "concerned"."peach_sales" as "peach_sales"
FROM
    "concerned"
WHERE
    "concerned"."peach_sales" > 0.05 * "concerned"."avg_store_customer_sales"
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

## v4 execution error

```
Traceback (most recent call last):
  File "C:\Users\ethan\coding_projects\pytrilogy\local_scripts\discovery_v4_compare.py", line 161, in run_one
    result.v4_rows = execute(con, v4_sql)
                     ~~~~~~~^^^^^^^^^^^^^
  File "C:\Users\ethan\coding_projects\pytrilogy\local_scripts\discovery_v4_compare.py", line 102, in execute
    cursor = con.execute(sql)
_duckdb.BinderException: Binder Error: Referenced table "juicy" not found!
Candidate tables: "abundant"

LINE 99:     sum("juicy"."_virt_filter_net_paid_7736053037874424") as "peach...
                 ^
```
