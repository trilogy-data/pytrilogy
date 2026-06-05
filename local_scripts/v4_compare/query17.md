# Query 17

**Status:** `match`

| Stage | Result |
| --- | --- |
| v4 SQL generation | OK |
| v4 execution | OK (0 rows) |
| reference execution | OK (0 rows) |
| results identical | YES |

## Result comparison

v4 rows: 0 (0 distinct)
ref rows: 0 (0 distinct)

## SQL size + execution time

| Source | Chars | Lines | Exec (min of 4) |
| --- | --- | --- | --- |
| v4 | 8300 | 143 | 34.88 ms |
| reference | 8300 | 143 | 34.78 ms |
| v4 / ref | 1.00x | 1.00x | 1.00x |

## Preql

```
import catalog_store_returns as analysis;

where
    analysis.store_sale_date.quarter_name = '2001Q1'
    and analysis.store_return_date.quarter_name in ('2001Q1', '2001Q2', '2001Q3')
    and analysis.catalog_date.quarter_name in ('2001Q1', '2001Q2', '2001Q3')
    and analysis.is_returned
select
    analysis.item.text_id,
    analysis.item.desc,
    analysis.store.state,
    count(analysis.store_quantity) as store_sales_quantitycount,
    avg(analysis.store_quantity) as store_sales_quantityave,
    stddev(analysis.store_quantity) as store_sales_quantitystdev,
    stddev(analysis.store_quantity) / avg(analysis.store_quantity) as store_sales_quantitycov,
    count(analysis.store_return_quantity) as store_returns_quantitycount,
    avg(analysis.store_return_quantity) as store_returns_quantityave,
    stddev(analysis.store_return_quantity) as store_returns_quantitystdev,
    stddev(analysis.store_return_quantity) / avg(analysis.store_return_quantity) as store_returns_quantitycov,
    count(analysis.catalog_quantity) as catalog_sales_quantitycount,
    avg(analysis.catalog_quantity) as catalog_sales_quantityave,
    stddev(analysis.catalog_quantity) as catalog_sales_quantitystdev,
    stddev(analysis.catalog_quantity) / avg(analysis.catalog_quantity) as catalog_sales_quantitycov,
order by
    analysis.item.text_id asc nulls first,
    analysis.item.desc asc nulls first,
    analysis.store.state asc nulls first
limit 100
;
```

## v4 generated SQL

```sql
WITH 
uneven as (
SELECT
    "analysis_catalog_sales"."CS_ITEM_SK" as "analysis_item_id",
    "analysis_catalog_sales"."CS_ORDER_NUMBER" as "analysis_catalog_order_number",
    "analysis_catalog_sales"."CS_QUANTITY" as "analysis_catalog_quantity",
    "analysis_item_items"."I_ITEM_DESC" as "analysis_item_desc",
    "analysis_item_items"."I_ITEM_ID" as "analysis_item_text_id",
    "analysis_store_returns"."SR_RETURN_QUANTITY" as "analysis_store_return_quantity",
    "analysis_store_sales"."SS_QUANTITY" as "analysis_store_quantity",
    "analysis_store_sales"."SS_TICKET_NUMBER" as "analysis_ticket_number",
    "analysis_store_store"."S_STATE" as "analysis_store_state"
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
    "analysis_store_sale_date_date"."D_QUARTER_NAME" = '2001Q1' and "analysis_store_return_date_date"."D_QUARTER_NAME" in ('2001Q1','2001Q2','2001Q3') and "analysis_catalog_date_date"."D_QUARTER_NAME" in ('2001Q1','2001Q2','2001Q3') and SR_RETURN_TIME_SK IS NOT NULL
),
concerned as (
SELECT
    "uneven"."analysis_item_desc" as "analysis_item_desc",
    "uneven"."analysis_item_text_id" as "analysis_item_text_id",
    "uneven"."analysis_store_quantity" as "analysis_store_quantity",
    "uneven"."analysis_store_return_quantity" as "analysis_store_return_quantity",
    "uneven"."analysis_store_state" as "analysis_store_state"
FROM
    "uneven"
GROUP BY
    1,
    2,
    3,
    4,
    5,
    "uneven"."analysis_item_id",
    "uneven"."analysis_ticket_number"),
yummy as (
SELECT
    "uneven"."analysis_catalog_quantity" as "analysis_catalog_quantity",
    "uneven"."analysis_item_desc" as "analysis_item_desc",
    "uneven"."analysis_item_text_id" as "analysis_item_text_id",
    "uneven"."analysis_store_state" as "analysis_store_state"
FROM
    "uneven"
GROUP BY
    1,
    2,
    3,
    4,
    "uneven"."analysis_catalog_order_number",
    "uneven"."analysis_item_id"),
young as (
SELECT
    "concerned"."analysis_item_desc" as "analysis_item_desc",
    "concerned"."analysis_item_text_id" as "analysis_item_text_id",
    "concerned"."analysis_store_state" as "analysis_store_state",
    avg("concerned"."analysis_store_quantity") as "_virt_agg_avg_7518273379920258",
    avg("concerned"."analysis_store_quantity") as "store_sales_quantityave",
    avg("concerned"."analysis_store_return_quantity") as "_virt_agg_avg_8572613716165371",
    avg("concerned"."analysis_store_return_quantity") as "store_returns_quantityave",
    count("concerned"."analysis_store_quantity") as "store_sales_quantitycount",
    count("concerned"."analysis_store_return_quantity") as "store_returns_quantitycount",
    stddev_samp("concerned"."analysis_store_quantity") as "_virt_agg_stddev_8948125603328408",
    stddev_samp("concerned"."analysis_store_quantity") as "store_sales_quantitystdev",
    stddev_samp("concerned"."analysis_store_return_quantity") as "_virt_agg_stddev_2955055239782943",
    stddev_samp("concerned"."analysis_store_return_quantity") as "store_returns_quantitystdev"
FROM
    "concerned"
GROUP BY
    1,
    2,
    3),
juicy as (
SELECT
    "yummy"."analysis_item_desc" as "analysis_item_desc",
    "yummy"."analysis_item_text_id" as "analysis_item_text_id",
    "yummy"."analysis_store_state" as "analysis_store_state",
    avg("yummy"."analysis_catalog_quantity") as "_virt_agg_avg_1688371525139287",
    avg("yummy"."analysis_catalog_quantity") as "catalog_sales_quantityave",
    count("yummy"."analysis_catalog_quantity") as "catalog_sales_quantitycount",
    stddev_samp("yummy"."analysis_catalog_quantity") as "_virt_agg_stddev_2693366057110854",
    stddev_samp("yummy"."analysis_catalog_quantity") as "catalog_sales_quantitystdev"
FROM
    "yummy"
GROUP BY
    1,
    2,
    3),
sparkling as (
SELECT
    "young"."_virt_agg_stddev_2955055239782943" / "young"."_virt_agg_avg_8572613716165371" as "store_returns_quantitycov",
    "young"."_virt_agg_stddev_8948125603328408" / "young"."_virt_agg_avg_7518273379920258" as "store_sales_quantitycov",
    "young"."analysis_item_desc" as "analysis_item_desc",
    "young"."analysis_item_text_id" as "analysis_item_text_id",
    "young"."analysis_store_state" as "analysis_store_state",
    "young"."store_returns_quantityave" as "store_returns_quantityave",
    "young"."store_returns_quantitycount" as "store_returns_quantitycount",
    "young"."store_returns_quantitystdev" as "store_returns_quantitystdev",
    "young"."store_sales_quantityave" as "store_sales_quantityave",
    "young"."store_sales_quantitycount" as "store_sales_quantitycount",
    "young"."store_sales_quantitystdev" as "store_sales_quantitystdev"
FROM
    "young"),
vacuous as (
SELECT
    "juicy"."_virt_agg_stddev_2693366057110854" / "juicy"."_virt_agg_avg_1688371525139287" as "catalog_sales_quantitycov",
    "juicy"."analysis_item_desc" as "analysis_item_desc",
    "juicy"."analysis_item_text_id" as "analysis_item_text_id",
    "juicy"."analysis_store_state" as "analysis_store_state",
    "juicy"."catalog_sales_quantityave" as "catalog_sales_quantityave",
    "juicy"."catalog_sales_quantitycount" as "catalog_sales_quantitycount",
    "juicy"."catalog_sales_quantitystdev" as "catalog_sales_quantitystdev"
FROM
    "juicy")
SELECT
    coalesce("sparkling"."analysis_item_text_id","vacuous"."analysis_item_text_id") as "analysis_item_text_id",
    coalesce("sparkling"."analysis_item_desc","vacuous"."analysis_item_desc") as "analysis_item_desc",
    coalesce("sparkling"."analysis_store_state","vacuous"."analysis_store_state") as "analysis_store_state",
    "sparkling"."store_sales_quantitycount" as "store_sales_quantitycount",
    "sparkling"."store_sales_quantityave" as "store_sales_quantityave",
    "sparkling"."store_sales_quantitystdev" as "store_sales_quantitystdev",
    "sparkling"."store_sales_quantitycov" as "store_sales_quantitycov",
    "sparkling"."store_returns_quantitycount" as "store_returns_quantitycount",
    "sparkling"."store_returns_quantityave" as "store_returns_quantityave",
    "sparkling"."store_returns_quantitystdev" as "store_returns_quantitystdev",
    "sparkling"."store_returns_quantitycov" as "store_returns_quantitycov",
    coalesce("vacuous"."catalog_sales_quantitycount",0) as "catalog_sales_quantitycount",
    "vacuous"."catalog_sales_quantityave" as "catalog_sales_quantityave",
    "vacuous"."catalog_sales_quantitystdev" as "catalog_sales_quantitystdev",
    "vacuous"."catalog_sales_quantitycov" as "catalog_sales_quantitycov"
FROM
    "sparkling"
    FULL JOIN "vacuous" on "sparkling"."analysis_item_desc" is not distinct from "vacuous"."analysis_item_desc" AND "sparkling"."analysis_item_text_id" = "vacuous"."analysis_item_text_id" AND "sparkling"."analysis_store_state" = "vacuous"."analysis_store_state"
ORDER BY 
    coalesce("sparkling"."analysis_item_text_id","vacuous"."analysis_item_text_id") asc nulls first,
    coalesce("sparkling"."analysis_item_desc","vacuous"."analysis_item_desc") asc nulls first,
    coalesce("sparkling"."analysis_store_state","vacuous"."analysis_store_state") asc nulls first
LIMIT (100)
```

## Reference SQL (zquery log)

```sql
WITH 
uneven as (
SELECT
    "analysis_catalog_sales"."CS_ITEM_SK" as "analysis_item_id",
    "analysis_catalog_sales"."CS_ORDER_NUMBER" as "analysis_catalog_order_number",
    "analysis_catalog_sales"."CS_QUANTITY" as "analysis_catalog_quantity",
    "analysis_item_items"."I_ITEM_DESC" as "analysis_item_desc",
    "analysis_item_items"."I_ITEM_ID" as "analysis_item_text_id",
    "analysis_store_returns"."SR_RETURN_QUANTITY" as "analysis_store_return_quantity",
    "analysis_store_sales"."SS_QUANTITY" as "analysis_store_quantity",
    "analysis_store_sales"."SS_TICKET_NUMBER" as "analysis_ticket_number",
    "analysis_store_store"."S_STATE" as "analysis_store_state"
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
    "analysis_store_sale_date_date"."D_QUARTER_NAME" = '2001Q1' and "analysis_store_return_date_date"."D_QUARTER_NAME" in ('2001Q1','2001Q2','2001Q3') and "analysis_catalog_date_date"."D_QUARTER_NAME" in ('2001Q1','2001Q2','2001Q3') and SR_RETURN_TIME_SK IS NOT NULL
),
concerned as (
SELECT
    "uneven"."analysis_item_desc" as "analysis_item_desc",
    "uneven"."analysis_item_text_id" as "analysis_item_text_id",
    "uneven"."analysis_store_quantity" as "analysis_store_quantity",
    "uneven"."analysis_store_return_quantity" as "analysis_store_return_quantity",
    "uneven"."analysis_store_state" as "analysis_store_state"
FROM
    "uneven"
GROUP BY
    1,
    2,
    3,
    4,
    5,
    "uneven"."analysis_item_id",
    "uneven"."analysis_ticket_number"),
yummy as (
SELECT
    "uneven"."analysis_catalog_quantity" as "analysis_catalog_quantity",
    "uneven"."analysis_item_desc" as "analysis_item_desc",
    "uneven"."analysis_item_text_id" as "analysis_item_text_id",
    "uneven"."analysis_store_state" as "analysis_store_state"
FROM
    "uneven"
GROUP BY
    1,
    2,
    3,
    4,
    "uneven"."analysis_catalog_order_number",
    "uneven"."analysis_item_id"),
young as (
SELECT
    "concerned"."analysis_item_desc" as "analysis_item_desc",
    "concerned"."analysis_item_text_id" as "analysis_item_text_id",
    "concerned"."analysis_store_state" as "analysis_store_state",
    avg("concerned"."analysis_store_quantity") as "_virt_agg_avg_7518273379920258",
    avg("concerned"."analysis_store_quantity") as "store_sales_quantityave",
    avg("concerned"."analysis_store_return_quantity") as "_virt_agg_avg_8572613716165371",
    avg("concerned"."analysis_store_return_quantity") as "store_returns_quantityave",
    count("concerned"."analysis_store_quantity") as "store_sales_quantitycount",
    count("concerned"."analysis_store_return_quantity") as "store_returns_quantitycount",
    stddev_samp("concerned"."analysis_store_quantity") as "_virt_agg_stddev_8948125603328408",
    stddev_samp("concerned"."analysis_store_quantity") as "store_sales_quantitystdev",
    stddev_samp("concerned"."analysis_store_return_quantity") as "_virt_agg_stddev_2955055239782943",
    stddev_samp("concerned"."analysis_store_return_quantity") as "store_returns_quantitystdev"
FROM
    "concerned"
GROUP BY
    1,
    2,
    3),
juicy as (
SELECT
    "yummy"."analysis_item_desc" as "analysis_item_desc",
    "yummy"."analysis_item_text_id" as "analysis_item_text_id",
    "yummy"."analysis_store_state" as "analysis_store_state",
    avg("yummy"."analysis_catalog_quantity") as "_virt_agg_avg_1688371525139287",
    avg("yummy"."analysis_catalog_quantity") as "catalog_sales_quantityave",
    count("yummy"."analysis_catalog_quantity") as "catalog_sales_quantitycount",
    stddev_samp("yummy"."analysis_catalog_quantity") as "_virt_agg_stddev_2693366057110854",
    stddev_samp("yummy"."analysis_catalog_quantity") as "catalog_sales_quantitystdev"
FROM
    "yummy"
GROUP BY
    1,
    2,
    3),
sparkling as (
SELECT
    "young"."_virt_agg_stddev_2955055239782943" / "young"."_virt_agg_avg_8572613716165371" as "store_returns_quantitycov",
    "young"."_virt_agg_stddev_8948125603328408" / "young"."_virt_agg_avg_7518273379920258" as "store_sales_quantitycov",
    "young"."analysis_item_desc" as "analysis_item_desc",
    "young"."analysis_item_text_id" as "analysis_item_text_id",
    "young"."analysis_store_state" as "analysis_store_state",
    "young"."store_returns_quantityave" as "store_returns_quantityave",
    "young"."store_returns_quantitycount" as "store_returns_quantitycount",
    "young"."store_returns_quantitystdev" as "store_returns_quantitystdev",
    "young"."store_sales_quantityave" as "store_sales_quantityave",
    "young"."store_sales_quantitycount" as "store_sales_quantitycount",
    "young"."store_sales_quantitystdev" as "store_sales_quantitystdev"
FROM
    "young"),
vacuous as (
SELECT
    "juicy"."_virt_agg_stddev_2693366057110854" / "juicy"."_virt_agg_avg_1688371525139287" as "catalog_sales_quantitycov",
    "juicy"."analysis_item_desc" as "analysis_item_desc",
    "juicy"."analysis_item_text_id" as "analysis_item_text_id",
    "juicy"."analysis_store_state" as "analysis_store_state",
    "juicy"."catalog_sales_quantityave" as "catalog_sales_quantityave",
    "juicy"."catalog_sales_quantitycount" as "catalog_sales_quantitycount",
    "juicy"."catalog_sales_quantitystdev" as "catalog_sales_quantitystdev"
FROM
    "juicy")
SELECT
    coalesce("sparkling"."analysis_item_text_id","vacuous"."analysis_item_text_id") as "analysis_item_text_id",
    coalesce("sparkling"."analysis_item_desc","vacuous"."analysis_item_desc") as "analysis_item_desc",
    coalesce("sparkling"."analysis_store_state","vacuous"."analysis_store_state") as "analysis_store_state",
    "sparkling"."store_sales_quantitycount" as "store_sales_quantitycount",
    "sparkling"."store_sales_quantityave" as "store_sales_quantityave",
    "sparkling"."store_sales_quantitystdev" as "store_sales_quantitystdev",
    "sparkling"."store_sales_quantitycov" as "store_sales_quantitycov",
    "sparkling"."store_returns_quantitycount" as "store_returns_quantitycount",
    "sparkling"."store_returns_quantityave" as "store_returns_quantityave",
    "sparkling"."store_returns_quantitystdev" as "store_returns_quantitystdev",
    "sparkling"."store_returns_quantitycov" as "store_returns_quantitycov",
    coalesce("vacuous"."catalog_sales_quantitycount",0) as "catalog_sales_quantitycount",
    "vacuous"."catalog_sales_quantityave" as "catalog_sales_quantityave",
    "vacuous"."catalog_sales_quantitystdev" as "catalog_sales_quantitystdev",
    "vacuous"."catalog_sales_quantitycov" as "catalog_sales_quantitycov"
FROM
    "sparkling"
    FULL JOIN "vacuous" on "sparkling"."analysis_item_desc" is not distinct from "vacuous"."analysis_item_desc" AND "sparkling"."analysis_item_text_id" = "vacuous"."analysis_item_text_id" AND "sparkling"."analysis_store_state" = "vacuous"."analysis_store_state"
ORDER BY 
    coalesce("sparkling"."analysis_item_text_id","vacuous"."analysis_item_text_id") asc nulls first,
    coalesce("sparkling"."analysis_item_desc","vacuous"."analysis_item_desc") asc nulls first,
    coalesce("sparkling"."analysis_store_state","vacuous"."analysis_store_state") asc nulls first
LIMIT (100)
```
