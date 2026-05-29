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
| v4 | 4572 | 60 | 33.07 ms |
| reference | 8323 | 125 | 91.67 ms |
| v4 / ref | 0.55x | 0.48x | 0.36x |

## Preql

```
import catalog_store_returns as analysis;

where
    analysis.store_sale_date.quarter_name = '2001Q1'
    and analysis.store_return_date.quarter_name in ('2001Q1', '2001Q2', '2001Q3')
    and analysis.catalog_date.quarter_name in ('2001Q1', '2001Q2', '2001Q3')
    and analysis.is_returned
select
    analysis.item.name,
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
    analysis.item.name asc nulls first,
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
    "analysis_item_items"."I_ITEM_DESC" as "analysis_item_desc",
    "analysis_item_items"."I_ITEM_ID" as "analysis_item_name",
    "analysis_store_store"."S_STATE" as "analysis_store_state",
    avg("analysis_catalog_sales"."CS_QUANTITY") as "_virt_agg_avg_1688371525139287",
    avg("analysis_catalog_sales"."CS_QUANTITY") as "catalog_sales_quantityave",
    avg("analysis_store_returns"."SR_RETURN_QUANTITY") as "_virt_agg_avg_8572613716165371",
    avg("analysis_store_returns"."SR_RETURN_QUANTITY") as "store_returns_quantityave",
    avg("analysis_store_sales"."SS_QUANTITY") as "_virt_agg_avg_7518273379920258",
    avg("analysis_store_sales"."SS_QUANTITY") as "store_sales_quantityave",
    count("analysis_catalog_sales"."CS_QUANTITY") as "catalog_sales_quantitycount",
    count("analysis_store_returns"."SR_RETURN_QUANTITY") as "store_returns_quantitycount",
    count("analysis_store_sales"."SS_QUANTITY") as "store_sales_quantitycount",
    stddev_samp("analysis_catalog_sales"."CS_QUANTITY") as "_virt_agg_stddev_2693366057110854",
    stddev_samp("analysis_catalog_sales"."CS_QUANTITY") as "catalog_sales_quantitystdev",
    stddev_samp("analysis_store_returns"."SR_RETURN_QUANTITY") as "_virt_agg_stddev_2955055239782943",
    stddev_samp("analysis_store_returns"."SR_RETURN_QUANTITY") as "store_returns_quantitystdev",
    stddev_samp("analysis_store_sales"."SS_QUANTITY") as "_virt_agg_stddev_8948125603328408",
    stddev_samp("analysis_store_sales"."SS_QUANTITY") as "store_sales_quantitystdev"
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
    "analysis_store_sale_date_date"."D_QUARTER_NAME" = '2001Q1' and "analysis_store_return_date_date"."D_QUARTER_NAME" in ('2001Q1','2001Q2','2001Q3') and "analysis_catalog_date_date"."D_QUARTER_NAME" in ('2001Q1','2001Q2','2001Q3') and SR_RETURN_TIME_SK IS NOT NULL

GROUP BY
    1,
    2,
    3)
SELECT
    "uneven"."analysis_item_name" as "analysis_item_name",
    "uneven"."analysis_item_desc" as "analysis_item_desc",
    "uneven"."analysis_store_state" as "analysis_store_state",
    "uneven"."store_sales_quantitycount" as "store_sales_quantitycount",
    "uneven"."store_sales_quantityave" as "store_sales_quantityave",
    "uneven"."store_sales_quantitystdev" as "store_sales_quantitystdev",
    "uneven"."_virt_agg_stddev_8948125603328408" / "uneven"."_virt_agg_avg_7518273379920258" as "store_sales_quantitycov",
    "uneven"."store_returns_quantitycount" as "store_returns_quantitycount",
    "uneven"."store_returns_quantityave" as "store_returns_quantityave",
    "uneven"."store_returns_quantitystdev" as "store_returns_quantitystdev",
    "uneven"."_virt_agg_stddev_2955055239782943" / "uneven"."_virt_agg_avg_8572613716165371" as "store_returns_quantitycov",
    "uneven"."catalog_sales_quantitycount" as "catalog_sales_quantitycount",
    "uneven"."catalog_sales_quantityave" as "catalog_sales_quantityave",
    "uneven"."catalog_sales_quantitystdev" as "catalog_sales_quantitystdev",
    "uneven"."_virt_agg_stddev_2693366057110854" / "uneven"."_virt_agg_avg_1688371525139287" as "catalog_sales_quantitycov"
FROM
    "uneven"
ORDER BY 
    "uneven"."analysis_item_name" asc nulls first,
    "uneven"."analysis_item_desc" asc nulls first,
    "uneven"."analysis_store_state" asc nulls first
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
    "analysis_catalog_sales"."CS_QUANTITY" as "analysis_catalog_quantity",
    "analysis_item_items"."I_ITEM_DESC" as "analysis_item_desc",
    "analysis_item_items"."I_ITEM_ID" as "analysis_item_name",
    "analysis_store_store"."S_STATE" as "analysis_store_state"
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
    "analysis_store_sale_date_date"."D_QUARTER_NAME" = '2001Q1' and "analysis_store_return_date_date"."D_QUARTER_NAME" in ('2001Q1','2001Q2','2001Q3') and "analysis_catalog_date_date"."D_QUARTER_NAME" in ('2001Q1','2001Q2','2001Q3') and SR_RETURN_TIME_SK IS NOT NULL

GROUP BY
    1,
    2,
    3,
    4,
    "analysis_catalog_sales"."CS_ORDER_NUMBER",
    coalesce("analysis_catalog_sales"."CS_ITEM_SK","analysis_item_items"."I_ITEM_SK","analysis_store_returns"."SR_ITEM_SK","analysis_store_sales"."SS_ITEM_SK")),
yummy as (
SELECT
    "analysis_item_items"."I_ITEM_DESC" as "analysis_item_desc",
    "analysis_item_items"."I_ITEM_ID" as "analysis_item_name",
    "analysis_store_returns"."SR_RETURN_QUANTITY" as "analysis_store_return_quantity",
    "analysis_store_sales"."SS_QUANTITY" as "analysis_store_quantity",
    "analysis_store_store"."S_STATE" as "analysis_store_state"
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
    "analysis_store_sale_date_date"."D_QUARTER_NAME" = '2001Q1' and "analysis_store_return_date_date"."D_QUARTER_NAME" in ('2001Q1','2001Q2','2001Q3') and "analysis_catalog_date_date"."D_QUARTER_NAME" in ('2001Q1','2001Q2','2001Q3') and SR_RETURN_TIME_SK IS NOT NULL

GROUP BY
    1,
    2,
    3,
    4,
    5,
    "analysis_store_sales"."SS_TICKET_NUMBER",
    coalesce("analysis_item_items"."I_ITEM_SK","analysis_store_returns"."SR_ITEM_SK","analysis_store_sales"."SS_ITEM_SK","wakeful"."analysis_item_id")),
concerned as (
SELECT
    "vacuous"."analysis_item_desc" as "analysis_item_desc",
    "vacuous"."analysis_item_name" as "analysis_item_name",
    "vacuous"."analysis_store_state" as "analysis_store_state",
    avg("vacuous"."analysis_catalog_quantity") as "_virt_agg_avg_1688371525139287",
    avg("vacuous"."analysis_catalog_quantity") as "catalog_sales_quantityave",
    count("vacuous"."analysis_catalog_quantity") as "catalog_sales_quantitycount",
    stddev_samp("vacuous"."analysis_catalog_quantity") as "_virt_agg_stddev_2693366057110854",
    stddev_samp("vacuous"."analysis_catalog_quantity") as "catalog_sales_quantitystdev"
FROM
    "vacuous"
GROUP BY
    1,
    2,
    3),
juicy as (
SELECT
    "yummy"."analysis_item_desc" as "analysis_item_desc",
    "yummy"."analysis_item_name" as "analysis_item_name",
    "yummy"."analysis_store_state" as "analysis_store_state",
    avg("yummy"."analysis_store_quantity") as "_virt_agg_avg_7518273379920258",
    avg("yummy"."analysis_store_quantity") as "store_sales_quantityave",
    avg("yummy"."analysis_store_return_quantity") as "_virt_agg_avg_8572613716165371",
    avg("yummy"."analysis_store_return_quantity") as "store_returns_quantityave",
    count("yummy"."analysis_store_quantity") as "store_sales_quantitycount",
    count("yummy"."analysis_store_return_quantity") as "store_returns_quantitycount",
    stddev_samp("yummy"."analysis_store_quantity") as "_virt_agg_stddev_8948125603328408",
    stddev_samp("yummy"."analysis_store_quantity") as "store_sales_quantitystdev",
    stddev_samp("yummy"."analysis_store_return_quantity") as "_virt_agg_stddev_2955055239782943",
    stddev_samp("yummy"."analysis_store_return_quantity") as "store_returns_quantitystdev"
FROM
    "yummy"
GROUP BY
    1,
    2,
    3)
SELECT
    coalesce("concerned"."analysis_item_name","juicy"."analysis_item_name") as "analysis_item_name",
    coalesce("concerned"."analysis_item_desc","juicy"."analysis_item_desc") as "analysis_item_desc",
    coalesce("concerned"."analysis_store_state","juicy"."analysis_store_state") as "analysis_store_state",
    "juicy"."store_sales_quantitycount" as "store_sales_quantitycount",
    "juicy"."store_sales_quantityave" as "store_sales_quantityave",
    "juicy"."store_sales_quantitystdev" as "store_sales_quantitystdev",
    "juicy"."_virt_agg_stddev_8948125603328408" / "juicy"."_virt_agg_avg_7518273379920258" as "store_sales_quantitycov",
    "juicy"."store_returns_quantitycount" as "store_returns_quantitycount",
    "juicy"."store_returns_quantityave" as "store_returns_quantityave",
    "juicy"."store_returns_quantitystdev" as "store_returns_quantitystdev",
    "juicy"."_virt_agg_stddev_2955055239782943" / "juicy"."_virt_agg_avg_8572613716165371" as "store_returns_quantitycov",
    coalesce("concerned"."catalog_sales_quantitycount",0) as "catalog_sales_quantitycount",
    "concerned"."catalog_sales_quantityave" as "catalog_sales_quantityave",
    "concerned"."catalog_sales_quantitystdev" as "catalog_sales_quantitystdev",
    "concerned"."_virt_agg_stddev_2693366057110854" / "concerned"."_virt_agg_avg_1688371525139287" as "catalog_sales_quantitycov"
FROM
    "juicy"
    INNER JOIN "concerned" on "juicy"."analysis_item_desc" is not distinct from "concerned"."analysis_item_desc" AND "juicy"."analysis_item_name" is not distinct from "concerned"."analysis_item_name" AND "juicy"."analysis_store_state" is not distinct from "concerned"."analysis_store_state"
ORDER BY 
    coalesce("concerned"."analysis_item_name","juicy"."analysis_item_name") asc nulls first,
    coalesce("concerned"."analysis_item_desc","juicy"."analysis_item_desc") asc nulls first,
    coalesce("concerned"."analysis_store_state","juicy"."analysis_store_state") asc nulls first
LIMIT (100)
```
