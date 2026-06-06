# Query 25

**Status:** `mismatch`

| Stage | Result |
| --- | --- |
| v4 SQL generation | OK |
| v4 execution | OK (15 rows) |
| reference execution | OK (0 rows) |
| results identical | NO |

## Result comparison

v4 rows: 15 (15 distinct)
ref rows: 0 (0 distinct)
only in v4 (showing up to 5 of 15):
  1x  ('Especially other jeans must not say less difficult men. Thousands should not advance serious systems. Unchanged situations need physical actions. Double g', 'AAAAAAAAACAAAAAA', 'ought', 'AAAAAAAABAAAAAAA', Decimal('-31148.35000000'), Decimal('101.61000000'), Decimal('-857.58000000'))
  1x  ('Public, annual h', 'AAAAAAAAAEAAAAAA', 'ought', 'AAAAAAAABAAAAAAA', Decimal('-9126.32000000'), Decimal('68.32000000'), Decimal('-155.04000000'))
  1x  ('Alone other sales could interpret slightly relations. Fast social police give just british centuries. There sacred channels go so industrial, original systems; women might approach black members. C', 'AAAAAAAAAHAAAAAA', 'ought', 'AAAAAAAABAAAAAAA', Decimal('-202.75000000'), Decimal('1010.18000000'), Decimal('-2795.98000000'))
  1x  ('Great, tiny animals adopt then outcomes. Terms sweep less dry, physical signs. National, black terms adapt for a reasons; groups shall ', 'AAAAAAAAAIAAAAAA', 'ought', 'AAAAAAAABAAAAAAA', Decimal('9575.37000000'), None, Decimal('-607.24000000'))
  1x  ('Tonnes alter. Towns get schools', 'AAAAAAAABDAAAAAA', 'ought', 'AAAAAAAABAAAAAAA', Decimal('3999.86000000'), Decimal('1759.56000000'), Decimal('-797.94000000'))

## SQL size + execution time

| Source | Chars | Lines | Exec (min of 4) |
| --- | --- | --- | --- |
| v4 | 5698 | 118 | 28.41 ms |
| reference | 6893 | 114 | 11.25 ms |
| v4 / ref | 0.83x | 1.04x | 2.53x |

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
    analysis.item.text_id,
    analysis.item.desc,
    analysis.store.text_id,
    analysis.store.name,
    sum(analysis.store_net_profit) as store_sales_profit,
    sum(analysis.store_return_net_loss) as store_returns_loss,
    sum(analysis.catalog_net_profit) as catalog_sales_profit,
order by
    analysis.item.text_id asc,
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
    "analysis_catalog_sales"."CS_ITEM_SK" as "analysis_item_id",
    "analysis_catalog_sales"."CS_NET_PROFIT" as "analysis_catalog_net_profit",
    "analysis_catalog_sales"."CS_ORDER_NUMBER" as "analysis_catalog_order_number",
    "analysis_item_items"."I_ITEM_DESC" as "analysis_item_desc",
    "analysis_item_items"."I_ITEM_ID" as "analysis_item_text_id",
    "analysis_store_returns"."SR_NET_LOSS" as "analysis_store_return_net_loss",
    "analysis_store_sales"."SS_NET_PROFIT" as "analysis_store_net_profit",
    "analysis_store_sales"."SS_TICKET_NUMBER" as "analysis_ticket_number",
    "analysis_store_store"."S_STORE_ID" as "analysis_store_text_id",
    "analysis_store_store"."S_STORE_NAME" as "analysis_store_name"
FROM
    "memory"."store_sales" as "analysis_store_sales"
    LEFT OUTER JOIN "memory"."store" as "analysis_store_store" on "analysis_store_sales"."SS_STORE_SK" = "analysis_store_store"."S_STORE_SK"
    INNER JOIN "memory"."store_returns" as "analysis_store_returns" on "analysis_store_sales"."SS_ITEM_SK" = "analysis_store_returns"."SR_ITEM_SK" AND "analysis_store_sales"."SS_TICKET_NUMBER" = "analysis_store_returns"."SR_TICKET_NUMBER"
    INNER JOIN "memory"."date_dim" as "analysis_store_sale_date_date" on "analysis_store_sales"."SS_SOLD_DATE_SK" = "analysis_store_sale_date_date"."D_DATE_SK"
    INNER JOIN "memory"."date_dim" as "analysis_store_return_date_date" on "analysis_store_returns"."SR_RETURNED_DATE_SK" = "analysis_store_return_date_date"."D_DATE_SK"
    INNER JOIN "memory"."catalog_sales" as "analysis_catalog_sales" on "analysis_store_sales"."SS_ITEM_SK" = "analysis_catalog_sales"."CS_ITEM_SK"
    INNER JOIN "memory"."item" as "analysis_item_items" on "analysis_store_sales"."SS_ITEM_SK" = "analysis_item_items"."I_ITEM_SK"
    INNER JOIN "memory"."date_dim" as "analysis_catalog_date_date" on "analysis_catalog_sales"."CS_SOLD_DATE_SK" = "analysis_catalog_date_date"."D_DATE_SK"
WHERE
    "analysis_store_sale_date_date"."D_YEAR" = 2001 and "analysis_store_sale_date_date"."D_MOY" = 4 and "analysis_store_return_date_date"."D_YEAR" = 2001 and "analysis_store_return_date_date"."D_MOY" BETWEEN 4 AND 10 and "analysis_catalog_date_date"."D_YEAR" = 2001 and "analysis_catalog_date_date"."D_MOY" BETWEEN 4 AND 10 and SR_RETURN_TIME_SK IS NOT NULL

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
    10),
vacuous as (
SELECT
    "uneven"."analysis_item_desc" as "analysis_item_desc",
    "uneven"."analysis_item_text_id" as "analysis_item_text_id",
    "uneven"."analysis_store_name" as "analysis_store_name",
    "uneven"."analysis_store_net_profit" as "analysis_store_net_profit",
    "uneven"."analysis_store_return_net_loss" as "analysis_store_return_net_loss",
    "uneven"."analysis_store_text_id" as "analysis_store_text_id"
FROM
    "uneven"
GROUP BY
    1,
    2,
    3,
    4,
    5,
    6,
    "uneven"."analysis_item_id",
    "uneven"."analysis_ticket_number"),
yummy as (
SELECT
    "uneven"."analysis_catalog_net_profit" as "analysis_catalog_net_profit",
    "uneven"."analysis_item_desc" as "analysis_item_desc",
    "uneven"."analysis_item_text_id" as "analysis_item_text_id",
    "uneven"."analysis_store_name" as "analysis_store_name",
    "uneven"."analysis_store_text_id" as "analysis_store_text_id"
FROM
    "uneven"
GROUP BY
    1,
    2,
    3,
    4,
    5,
    "uneven"."analysis_catalog_order_number",
    "uneven"."analysis_item_id"),
concerned as (
SELECT
    "vacuous"."analysis_item_desc" as "analysis_item_desc",
    "vacuous"."analysis_item_text_id" as "analysis_item_text_id",
    "vacuous"."analysis_store_name" as "analysis_store_name",
    "vacuous"."analysis_store_text_id" as "analysis_store_text_id",
    sum("vacuous"."analysis_store_net_profit") as "store_sales_profit",
    sum("vacuous"."analysis_store_return_net_loss") as "store_returns_loss"
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
    "yummy"."analysis_item_text_id" as "analysis_item_text_id",
    "yummy"."analysis_store_name" as "analysis_store_name",
    "yummy"."analysis_store_text_id" as "analysis_store_text_id",
    sum("yummy"."analysis_catalog_net_profit") as "catalog_sales_profit"
FROM
    "yummy"
GROUP BY
    1,
    2,
    3,
    4)
SELECT
    coalesce("concerned"."analysis_item_text_id","juicy"."analysis_item_text_id") as "analysis_item_text_id",
    coalesce("concerned"."analysis_item_desc","juicy"."analysis_item_desc") as "analysis_item_desc",
    coalesce("concerned"."analysis_store_text_id","juicy"."analysis_store_text_id") as "analysis_store_text_id",
    coalesce("concerned"."analysis_store_name","juicy"."analysis_store_name") as "analysis_store_name",
    "concerned"."store_sales_profit" as "store_sales_profit",
    "concerned"."store_returns_loss" as "store_returns_loss",
    "juicy"."catalog_sales_profit" as "catalog_sales_profit"
FROM
    "concerned"
    INNER JOIN "juicy" on "concerned"."analysis_item_desc" is not distinct from "juicy"."analysis_item_desc" AND "concerned"."analysis_item_text_id" = "juicy"."analysis_item_text_id" AND "concerned"."analysis_store_name" is not distinct from "juicy"."analysis_store_name" AND "concerned"."analysis_store_text_id" is not distinct from "juicy"."analysis_store_text_id"
ORDER BY 
    coalesce("concerned"."analysis_item_text_id","juicy"."analysis_item_text_id") asc,
    coalesce("concerned"."analysis_item_desc","juicy"."analysis_item_desc") asc,
    coalesce("concerned"."analysis_store_text_id","juicy"."analysis_store_text_id") asc,
    coalesce("concerned"."analysis_store_name","juicy"."analysis_store_name") asc
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
    "analysis_item_items"."I_ITEM_ID" as "analysis_item_text_id",
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
    "analysis_item_items"."I_ITEM_ID" as "analysis_item_text_id",
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
    "vacuous"."analysis_item_text_id" as "analysis_item_text_id",
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
    "yummy"."analysis_item_text_id" as "analysis_item_text_id",
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
    coalesce("concerned"."analysis_item_text_id","juicy"."analysis_item_text_id") as "analysis_item_text_id",
    coalesce("concerned"."analysis_item_desc","juicy"."analysis_item_desc") as "analysis_item_desc",
    coalesce("concerned"."analysis_store_text_id","juicy"."analysis_store_text_id") as "analysis_store_text_id",
    coalesce("concerned"."analysis_store_name","juicy"."analysis_store_name") as "analysis_store_name",
    "juicy"."store_sales_profit" as "store_sales_profit",
    "juicy"."store_returns_loss" as "store_returns_loss",
    "concerned"."catalog_sales_profit" as "catalog_sales_profit"
FROM
    "concerned"
    INNER JOIN "juicy" on "concerned"."analysis_item_desc" is not distinct from "juicy"."analysis_item_desc" AND "concerned"."analysis_item_text_id" = "juicy"."analysis_item_text_id" AND "concerned"."analysis_store_name" is not distinct from "juicy"."analysis_store_name" AND "concerned"."analysis_store_text_id" is not distinct from "juicy"."analysis_store_text_id"
ORDER BY 
    coalesce("concerned"."analysis_item_text_id","juicy"."analysis_item_text_id") asc,
    coalesce("concerned"."analysis_item_desc","juicy"."analysis_item_desc") asc,
    coalesce("concerned"."analysis_store_text_id","juicy"."analysis_store_text_id") asc,
    coalesce("concerned"."analysis_store_name","juicy"."analysis_store_name") asc
LIMIT (100)
```
