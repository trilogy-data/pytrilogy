# Query 25

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
| v4 | 2390 | 31 | 43.84 ms |
| reference | 6854 | 114 | 120.73 ms |
| v4 / ref | 0.35x | 0.27x | 0.36x |

## Preql

```
import catalog_store_returns as analysis;

where
    analysis.store_sale_date.year = 2001
    and analysis.store_sale_date.month_of_year = 4
    and analysis.store_return_date.year = 2001
    and analysis.store_return_date.month_of_year between 4 and 10
    and analysis.catalog_date.year = 2001
    and analysis.catalog_date.month_of_year between 4 and 10
    and analysis.is_returned
select
    analysis.item.name,
    analysis.item.desc,
    analysis.store.text_id,
    analysis.store.name,
    sum(analysis.store_net_profit) as store_sales_profit,
    sum(analysis.store_return_net_loss) as store_returns_loss,
    sum(analysis.catalog_net_profit) as catalog_sales_profit,
order by
    analysis.item.name asc,
    analysis.item.desc asc,
    analysis.store.text_id asc,
    analysis.store.name asc
limit 100
;
```

## v4 generated SQL

```sql
SELECT
    "analysis_item_items"."I_ITEM_DESC" as "analysis_item_desc",
    "analysis_item_items"."I_ITEM_ID" as "analysis_item_name",
    "analysis_store_store"."S_STORE_NAME" as "analysis_store_name",
    "analysis_store_store"."S_STORE_ID" as "analysis_store_text_id",
    sum("analysis_catalog_sales"."CS_NET_PROFIT") as "catalog_sales_profit",
    sum("analysis_store_returns"."SR_NET_LOSS") as "store_returns_loss",
    sum("analysis_store_sales"."SS_NET_PROFIT") as "store_sales_profit"
FROM
    "memory"."store_sales" as "analysis_store_sales"
    LEFT OUTER JOIN "memory"."store" as "analysis_store_store" on "analysis_store_sales"."SS_STORE_SK" = "analysis_store_store"."S_STORE_SK"
    INNER JOIN "memory"."store_returns" as "analysis_store_returns" on "analysis_store_sales"."SS_ITEM_SK" = "analysis_store_returns"."SR_ITEM_SK" AND "analysis_store_sales"."SS_TICKET_NUMBER" = "analysis_store_returns"."SR_TICKET_NUMBER"
    INNER JOIN "memory"."date_dim" as "analysis_store_sale_date_date" on "analysis_store_sales"."SS_SOLD_DATE_SK" = "analysis_store_sale_date_date"."D_DATE_SK"
    INNER JOIN "memory"."date_dim" as "analysis_store_return_date_date" on "analysis_store_returns"."SR_RETURNED_DATE_SK" = "analysis_store_return_date_date"."D_DATE_SK"
    INNER JOIN "memory"."catalog_sales" as "analysis_catalog_sales" on "analysis_store_sales"."SS_CUSTOMER_SK" = "analysis_catalog_sales"."CS_BILL_CUSTOMER_SK" AND "analysis_store_sales"."SS_ITEM_SK" = "analysis_catalog_sales"."CS_ITEM_SK"
    INNER JOIN "memory"."date_dim" as "analysis_catalog_date_date" on "analysis_catalog_sales"."CS_SOLD_DATE_SK" = "analysis_catalog_date_date"."D_DATE_SK"
    INNER JOIN "memory"."item" as "analysis_item_items" on "analysis_store_sales"."SS_ITEM_SK" = "analysis_item_items"."I_ITEM_SK"
WHERE
    "analysis_store_sale_date_date"."D_YEAR" = 2001 and "analysis_store_sale_date_date"."D_MOY" = 4 and "analysis_store_return_date_date"."D_YEAR" = 2001 and "analysis_store_return_date_date"."D_MOY" BETWEEN 4 AND 10 and "analysis_catalog_date_date"."D_YEAR" = 2001 and "analysis_catalog_date_date"."D_MOY" BETWEEN 4 AND 10 and SR_RETURN_TIME_SK IS NOT NULL

GROUP BY
    1,
    2,
    3,
    4
ORDER BY 
    "analysis_item_items"."I_ITEM_ID" asc,
    "analysis_item_items"."I_ITEM_DESC" asc,
    "analysis_store_store"."S_STORE_ID" asc,
    "analysis_store_store"."S_STORE_NAME" asc
LIMIT (100)
```

## Reference SQL (zquery log)

```sql
WITH 
wakeful as (
SELECT
    "analysis_catalog_sales"."CS_BILL_CUSTOMER_SK" as "analysis_customer_id",
    "analysis_catalog_sales"."CS_ITEM_SK" as "analysis_item_id",
    "analysis_catalog_sales"."CS_SOLD_DATE_SK" as "analysis_catalog_date_id"
FROM
    "memory"."catalog_sales" as "analysis_catalog_sales"
GROUP BY
    1,
    2,
    3),
vacuous as (
SELECT
    "analysis_catalog_sales"."CS_NET_PROFIT" as "analysis_catalog_net_profit",
    "analysis_item_items"."I_ITEM_DESC" as "analysis_item_desc",
    "analysis_item_items"."I_ITEM_ID" as "analysis_item_name",
    "analysis_store_store"."S_STORE_ID" as "analysis_store_text_id",
    "analysis_store_store"."S_STORE_NAME" as "analysis_store_name"
FROM
    "memory"."store_sales" as "analysis_store_sales"
    LEFT OUTER JOIN "memory"."store" as "analysis_store_store" on "analysis_store_sales"."SS_STORE_SK" = "analysis_store_store"."S_STORE_SK"
    INNER JOIN "memory"."store_returns" as "analysis_store_returns" on "analysis_store_sales"."SS_ITEM_SK" = "analysis_store_returns"."SR_ITEM_SK" AND "analysis_store_sales"."SS_TICKET_NUMBER" = "analysis_store_returns"."SR_TICKET_NUMBER"
    INNER JOIN "memory"."date_dim" as "analysis_store_sale_date_date" on "analysis_store_sales"."SS_SOLD_DATE_SK" = "analysis_store_sale_date_date"."D_DATE_SK"
    INNER JOIN "memory"."date_dim" as "analysis_store_return_date_date" on "analysis_store_returns"."SR_RETURNED_DATE_SK" = "analysis_store_return_date_date"."D_DATE_SK"
    INNER JOIN "memory"."catalog_sales" as "analysis_catalog_sales" on "analysis_store_sales"."SS_CUSTOMER_SK" = "analysis_catalog_sales"."CS_BILL_CUSTOMER_SK" AND "analysis_store_sales"."SS_ITEM_SK" = "analysis_catalog_sales"."CS_ITEM_SK"
    INNER JOIN "memory"."date_dim" as "analysis_catalog_date_date" on "analysis_catalog_sales"."CS_SOLD_DATE_SK" = "analysis_catalog_date_date"."D_DATE_SK"
    INNER JOIN "memory"."item" as "analysis_item_items" on "analysis_store_sales"."SS_ITEM_SK" = "analysis_item_items"."I_ITEM_SK"
WHERE
    "analysis_store_sale_date_date"."D_YEAR" = 2001 and "analysis_store_sale_date_date"."D_MOY" = 4 and "analysis_store_return_date_date"."D_YEAR" = 2001 and "analysis_store_return_date_date"."D_MOY" BETWEEN 4 AND 10 and "analysis_catalog_date_date"."D_YEAR" = 2001 and "analysis_catalog_date_date"."D_MOY" BETWEEN 4 AND 10 and SR_RETURN_TIME_SK IS NOT NULL

GROUP BY
    1,
    2,
    3,
    4,
    5,
    "analysis_catalog_sales"."CS_ITEM_SK",
    "analysis_catalog_sales"."CS_ORDER_NUMBER"),
yummy as (
SELECT
    "analysis_item_items"."I_ITEM_DESC" as "analysis_item_desc",
    "analysis_item_items"."I_ITEM_ID" as "analysis_item_name",
    "analysis_store_returns"."SR_NET_LOSS" as "analysis_store_return_net_loss",
    "analysis_store_sales"."SS_NET_PROFIT" as "analysis_store_net_profit",
    "analysis_store_store"."S_STORE_ID" as "analysis_store_text_id",
    "analysis_store_store"."S_STORE_NAME" as "analysis_store_name"
FROM
    "memory"."store_sales" as "analysis_store_sales"
    LEFT OUTER JOIN "memory"."store" as "analysis_store_store" on "analysis_store_sales"."SS_STORE_SK" = "analysis_store_store"."S_STORE_SK"
    INNER JOIN "memory"."store_returns" as "analysis_store_returns" on "analysis_store_sales"."SS_ITEM_SK" = "analysis_store_returns"."SR_ITEM_SK" AND "analysis_store_sales"."SS_TICKET_NUMBER" = "analysis_store_returns"."SR_TICKET_NUMBER"
    INNER JOIN "memory"."date_dim" as "analysis_store_sale_date_date" on "analysis_store_sales"."SS_SOLD_DATE_SK" = "analysis_store_sale_date_date"."D_DATE_SK"
    INNER JOIN "memory"."date_dim" as "analysis_store_return_date_date" on "analysis_store_returns"."SR_RETURNED_DATE_SK" = "analysis_store_return_date_date"."D_DATE_SK"
    INNER JOIN "wakeful" on "analysis_store_sales"."SS_CUSTOMER_SK" = "wakeful"."analysis_customer_id" AND "analysis_store_sales"."SS_ITEM_SK" = "wakeful"."analysis_item_id"
    INNER JOIN "memory"."date_dim" as "analysis_catalog_date_date" on "wakeful"."analysis_catalog_date_id" = "analysis_catalog_date_date"."D_DATE_SK"
    INNER JOIN "memory"."item" as "analysis_item_items" on "wakeful"."analysis_item_id" = "analysis_item_items"."I_ITEM_SK"
WHERE
    "analysis_store_sale_date_date"."D_YEAR" = 2001 and "analysis_store_sale_date_date"."D_MOY" = 4 and "analysis_store_return_date_date"."D_YEAR" = 2001 and "analysis_store_return_date_date"."D_MOY" BETWEEN 4 AND 10 and "analysis_catalog_date_date"."D_YEAR" = 2001 and "analysis_catalog_date_date"."D_MOY" BETWEEN 4 AND 10 and SR_RETURN_TIME_SK IS NOT NULL

GROUP BY
    1,
    2,
    3,
    4,
    5,
    6,
    "analysis_store_sales"."SS_TICKET_NUMBER",
    "wakeful"."analysis_item_id"),
concerned as (
SELECT
    "vacuous"."analysis_item_desc" as "analysis_item_desc",
    "vacuous"."analysis_item_name" as "analysis_item_name",
    "vacuous"."analysis_store_name" as "analysis_store_name",
    "vacuous"."analysis_store_text_id" as "analysis_store_text_id",
    sum("vacuous"."analysis_catalog_net_profit") as "catalog_sales_profit"
FROM
    "vacuous"
GROUP BY
    1,
    2,
    3,
    4),
juicy as (
SELECT
    "yummy"."analysis_item_desc" as "analysis_item_desc",
    "yummy"."analysis_item_name" as "analysis_item_name",
    "yummy"."analysis_store_name" as "analysis_store_name",
    "yummy"."analysis_store_text_id" as "analysis_store_text_id",
    sum("yummy"."analysis_store_net_profit") as "store_sales_profit",
    sum("yummy"."analysis_store_return_net_loss") as "store_returns_loss"
FROM
    "yummy"
GROUP BY
    1,
    2,
    3,
    4)
SELECT
    coalesce("concerned"."analysis_item_name","juicy"."analysis_item_name") as "analysis_item_name",
    coalesce("concerned"."analysis_item_desc","juicy"."analysis_item_desc") as "analysis_item_desc",
    coalesce("concerned"."analysis_store_text_id","juicy"."analysis_store_text_id") as "analysis_store_text_id",
    coalesce("concerned"."analysis_store_name","juicy"."analysis_store_name") as "analysis_store_name",
    "juicy"."store_sales_profit" as "store_sales_profit",
    "juicy"."store_returns_loss" as "store_returns_loss",
    "concerned"."catalog_sales_profit" as "catalog_sales_profit"
FROM
    "concerned"
    INNER JOIN "juicy" on "concerned"."analysis_item_desc" is not distinct from "juicy"."analysis_item_desc" AND "concerned"."analysis_item_name" = "juicy"."analysis_item_name" AND "concerned"."analysis_store_name" is not distinct from "juicy"."analysis_store_name" AND "concerned"."analysis_store_text_id" is not distinct from "juicy"."analysis_store_text_id"
ORDER BY 
    coalesce("concerned"."analysis_item_name","juicy"."analysis_item_name") asc,
    coalesce("concerned"."analysis_item_desc","juicy"."analysis_item_desc") asc,
    coalesce("concerned"."analysis_store_text_id","juicy"."analysis_store_text_id") asc,
    coalesce("concerned"."analysis_store_name","juicy"."analysis_store_name") asc
LIMIT (100)
```
