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

## SQL size

| Source | Chars | Lines |
| --- | --- | --- |
| v4 | 6642 | 141 |
| reference | 7129 | 114 |
| v4 / ref | 0.93x | 1.24x |

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
WITH 
uneven as (
SELECT
    "analysis_store_sales"."SS_CUSTOMER_SK" as "analysis_customer_id",
    "analysis_store_sales"."SS_ITEM_SK" as "analysis_item_id",
    "analysis_store_sales"."SS_NET_PROFIT" as "analysis_store_net_profit",
    "analysis_store_sales"."SS_SOLD_DATE_SK" as "analysis_store_sale_date_id",
    "analysis_store_sales"."SS_STORE_SK" as "analysis_store_id",
    "analysis_store_sales"."SS_TICKET_NUMBER" as "analysis_ticket_number"
FROM
    "memory"."store_sales" as "analysis_store_sales"),
abundant as (
SELECT
    "analysis_store_sale_date_date"."D_DATE_SK" as "analysis_store_sale_date_id",
    "analysis_store_sale_date_date"."D_MOY" as "analysis_store_sale_date_month_of_year",
    "analysis_store_sale_date_date"."D_YEAR" as "analysis_store_sale_date_year"
FROM
    "memory"."date_dim" as "analysis_store_sale_date_date"),
questionable as (
SELECT
    "analysis_store_returns"."SR_CUSTOMER_SK" as "analysis_customer_id",
    "analysis_store_returns"."SR_ITEM_SK" as "analysis_item_id",
    "analysis_store_returns"."SR_NET_LOSS" as "analysis_store_return_net_loss",
    "analysis_store_returns"."SR_RETURNED_DATE_SK" as "analysis_store_return_date_id",
    "analysis_store_returns"."SR_TICKET_NUMBER" as "analysis_ticket_number",
    SR_RETURN_TIME_SK IS NOT NULL as "analysis_is_returned"
FROM
    "memory"."store_returns" as "analysis_store_returns"),
cooperative as (
SELECT
    "analysis_store_return_date_date"."D_DATE_SK" as "analysis_store_return_date_id",
    "analysis_store_return_date_date"."D_MOY" as "analysis_store_return_date_month_of_year",
    "analysis_store_return_date_date"."D_YEAR" as "analysis_store_return_date_year"
FROM
    "memory"."date_dim" as "analysis_store_return_date_date"),
thoughtful as (
SELECT
    "analysis_store_store"."S_STORE_ID" as "analysis_store_text_id",
    "analysis_store_store"."S_STORE_NAME" as "analysis_store_name",
    "analysis_store_store"."S_STORE_SK" as "analysis_store_id"
FROM
    "memory"."store" as "analysis_store_store"),
cheerful as (
SELECT
    "analysis_item_items"."I_ITEM_DESC" as "analysis_item_desc",
    "analysis_item_items"."I_ITEM_ID" as "analysis_item_name",
    "analysis_item_items"."I_ITEM_SK" as "analysis_item_id"
FROM
    "memory"."item" as "analysis_item_items"),
highfalutin as (
SELECT
    "analysis_catalog_sales"."CS_BILL_CUSTOMER_SK" as "analysis_customer_id",
    "analysis_catalog_sales"."CS_ITEM_SK" as "analysis_item_id",
    "analysis_catalog_sales"."CS_NET_PROFIT" as "analysis_catalog_net_profit",
    "analysis_catalog_sales"."CS_SOLD_DATE_SK" as "analysis_catalog_date_id"
FROM
    "memory"."catalog_sales" as "analysis_catalog_sales"),
wakeful as (
SELECT
    "highfalutin"."analysis_catalog_date_id" as "analysis_catalog_date_id",
    "highfalutin"."analysis_catalog_net_profit" as "analysis_catalog_net_profit",
    "highfalutin"."analysis_customer_id" as "analysis_customer_id",
    "highfalutin"."analysis_item_id" as "analysis_item_id"
FROM
    "highfalutin"
GROUP BY
    1,
    2,
    3,
    4),
quizzical as (
SELECT
    "analysis_catalog_date_date"."D_DATE_SK" as "analysis_catalog_date_id",
    "analysis_catalog_date_date"."D_MOY" as "analysis_catalog_date_month_of_year",
    "analysis_catalog_date_date"."D_YEAR" as "analysis_catalog_date_year"
FROM
    "memory"."date_dim" as "analysis_catalog_date_date"),
yummy as (
SELECT
    "abundant"."analysis_store_sale_date_month_of_year" as "analysis_store_sale_date_month_of_year",
    "abundant"."analysis_store_sale_date_year" as "analysis_store_sale_date_year",
    "cheerful"."analysis_item_desc" as "analysis_item_desc",
    "cheerful"."analysis_item_name" as "analysis_item_name",
    "cooperative"."analysis_store_return_date_month_of_year" as "analysis_store_return_date_month_of_year",
    "cooperative"."analysis_store_return_date_year" as "analysis_store_return_date_year",
    "questionable"."analysis_is_returned" as "analysis_is_returned",
    "questionable"."analysis_store_return_net_loss" as "analysis_store_return_net_loss",
    "quizzical"."analysis_catalog_date_month_of_year" as "analysis_catalog_date_month_of_year",
    "quizzical"."analysis_catalog_date_year" as "analysis_catalog_date_year",
    "thoughtful"."analysis_store_name" as "analysis_store_name",
    "thoughtful"."analysis_store_text_id" as "analysis_store_text_id",
    "uneven"."analysis_store_net_profit" as "analysis_store_net_profit",
    "wakeful"."analysis_catalog_net_profit" as "analysis_catalog_net_profit"
FROM
    "uneven"
    LEFT OUTER JOIN "thoughtful" on "uneven"."analysis_store_id" = "thoughtful"."analysis_store_id"
    LEFT OUTER JOIN "questionable" on "uneven"."analysis_item_id" = "questionable"."analysis_item_id" AND "uneven"."analysis_ticket_number" = "questionable"."analysis_ticket_number"
    LEFT OUTER JOIN "abundant" on "uneven"."analysis_store_sale_date_id" = "abundant"."analysis_store_sale_date_id"
    LEFT OUTER JOIN "cooperative" on "questionable"."analysis_store_return_date_id" = "cooperative"."analysis_store_return_date_id"
    FULL JOIN "wakeful" on "questionable"."analysis_customer_id" = "wakeful"."analysis_customer_id" AND "questionable"."analysis_item_id" = "wakeful"."analysis_item_id"
    LEFT OUTER JOIN "quizzical" on "wakeful"."analysis_catalog_date_id" = "quizzical"."analysis_catalog_date_id"
    LEFT OUTER JOIN "cheerful" on "wakeful"."analysis_item_id" = "cheerful"."analysis_item_id"
WHERE
    "abundant"."analysis_store_sale_date_year" = 2001 and "abundant"."analysis_store_sale_date_month_of_year" = 4 and "cooperative"."analysis_store_return_date_year" = 2001 and "cooperative"."analysis_store_return_date_month_of_year" BETWEEN 4 AND 10 and "quizzical"."analysis_catalog_date_year" = 2001 and "quizzical"."analysis_catalog_date_month_of_year" BETWEEN 4 AND 10 and "questionable"."analysis_is_returned"

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
    14)
SELECT
    sum("yummy"."analysis_store_net_profit") as "store_sales_profit",
    sum("yummy"."analysis_store_return_net_loss") as "store_returns_loss",
    sum("yummy"."analysis_catalog_net_profit") as "catalog_sales_profit",
    "yummy"."analysis_store_name" as "analysis_store_name",
    "yummy"."analysis_store_text_id" as "analysis_store_text_id",
    "yummy"."analysis_item_desc" as "analysis_item_desc",
    "yummy"."analysis_item_name" as "analysis_item_name"
FROM
    "yummy"
GROUP BY
    4,
    5,
    6,
    7
ORDER BY 
    "yummy"."analysis_item_name" asc,
    "yummy"."analysis_item_desc" asc,
    "yummy"."analysis_store_text_id" asc,
    "yummy"."analysis_store_name" asc
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
    INNER JOIN "memory"."catalog_sales" as "analysis_catalog_sales" on "analysis_store_returns"."SR_CUSTOMER_SK" = "analysis_catalog_sales"."CS_BILL_CUSTOMER_SK" AND "analysis_store_returns"."SR_ITEM_SK" = "analysis_catalog_sales"."CS_ITEM_SK"
    INNER JOIN "memory"."date_dim" as "analysis_catalog_date_date" on "analysis_catalog_sales"."CS_SOLD_DATE_SK" = "analysis_catalog_date_date"."D_DATE_SK"
    LEFT OUTER JOIN "memory"."item" as "analysis_item_items" on "analysis_catalog_sales"."CS_ITEM_SK" = "analysis_item_items"."I_ITEM_SK"
WHERE
    "analysis_store_sale_date_date"."D_YEAR" = 2001 and "analysis_store_sale_date_date"."D_MOY" = 4 and "analysis_store_return_date_date"."D_YEAR" = 2001 and "analysis_store_return_date_date"."D_MOY" BETWEEN 4 AND 10 and "analysis_catalog_date_date"."D_YEAR" = 2001 and "analysis_catalog_date_date"."D_MOY" BETWEEN 4 AND 10 and SR_RETURN_TIME_SK IS NOT NULL

GROUP BY
    1,
    2,
    3,
    4,
    5,
    "analysis_catalog_sales"."CS_ORDER_NUMBER",
    coalesce("analysis_catalog_sales"."CS_ITEM_SK","analysis_item_items"."I_ITEM_SK","analysis_store_returns"."SR_ITEM_SK","analysis_store_sales"."SS_ITEM_SK")),
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
    INNER JOIN "wakeful" on "analysis_store_returns"."SR_CUSTOMER_SK" = "wakeful"."analysis_customer_id" AND "analysis_store_returns"."SR_ITEM_SK" = "wakeful"."analysis_item_id"
    INNER JOIN "memory"."date_dim" as "analysis_catalog_date_date" on "wakeful"."analysis_catalog_date_id" = "analysis_catalog_date_date"."D_DATE_SK"
    LEFT OUTER JOIN "memory"."item" as "analysis_item_items" on "wakeful"."analysis_item_id" = "analysis_item_items"."I_ITEM_SK"
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
    coalesce("analysis_item_items"."I_ITEM_SK","analysis_store_returns"."SR_ITEM_SK","analysis_store_sales"."SS_ITEM_SK","wakeful"."analysis_item_id")),
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
    "juicy"
    INNER JOIN "concerned" on "juicy"."analysis_item_desc" is not distinct from "concerned"."analysis_item_desc" AND "juicy"."analysis_item_name" is not distinct from "concerned"."analysis_item_name" AND "juicy"."analysis_store_name" is not distinct from "concerned"."analysis_store_name" AND "juicy"."analysis_store_text_id" is not distinct from "concerned"."analysis_store_text_id"
ORDER BY 
    coalesce("concerned"."analysis_item_name","juicy"."analysis_item_name") asc,
    coalesce("concerned"."analysis_item_desc","juicy"."analysis_item_desc") asc,
    coalesce("concerned"."analysis_store_text_id","juicy"."analysis_store_text_id") asc,
    coalesce("concerned"."analysis_store_name","juicy"."analysis_store_name") asc
LIMIT (100)
```
