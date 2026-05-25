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

## SQL size

| Source | Chars | Lines |
| --- | --- | --- |
| v4 | 9577 | 174 |
| reference | 8323 | 125 |
| v4 / ref | 1.15x | 1.39x |

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
    "analysis_store_sales"."SS_CUSTOMER_SK" as "analysis_customer_id",
    "analysis_store_sales"."SS_ITEM_SK" as "analysis_item_id",
    "analysis_store_sales"."SS_QUANTITY" as "analysis_store_quantity",
    "analysis_store_sales"."SS_SOLD_DATE_SK" as "analysis_store_sale_date_id",
    "analysis_store_sales"."SS_STORE_SK" as "analysis_store_id",
    "analysis_store_sales"."SS_TICKET_NUMBER" as "analysis_ticket_number"
FROM
    "memory"."store_sales" as "analysis_store_sales"),
abundant as (
SELECT
    "analysis_store_sale_date_date"."D_DATE_SK" as "analysis_store_sale_date_id",
    "analysis_store_sale_date_date"."D_QUARTER_NAME" as "analysis_store_sale_date_quarter_name"
FROM
    "memory"."date_dim" as "analysis_store_sale_date_date"),
questionable as (
SELECT
    "analysis_store_returns"."SR_CUSTOMER_SK" as "analysis_customer_id",
    "analysis_store_returns"."SR_ITEM_SK" as "analysis_item_id",
    "analysis_store_returns"."SR_RETURNED_DATE_SK" as "analysis_store_return_date_id",
    "analysis_store_returns"."SR_RETURN_QUANTITY" as "analysis_store_return_quantity",
    "analysis_store_returns"."SR_TICKET_NUMBER" as "analysis_ticket_number",
    SR_RETURN_TIME_SK IS NOT NULL as "analysis_is_returned"
FROM
    "memory"."store_returns" as "analysis_store_returns"),
cooperative as (
SELECT
    "analysis_store_return_date_date"."D_DATE_SK" as "analysis_store_return_date_id",
    "analysis_store_return_date_date"."D_QUARTER_NAME" as "analysis_store_return_date_quarter_name"
FROM
    "memory"."date_dim" as "analysis_store_return_date_date"),
thoughtful as (
SELECT
    "analysis_store_store"."S_STATE" as "analysis_store_state",
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
    "analysis_catalog_sales"."CS_QUANTITY" as "analysis_catalog_quantity",
    "analysis_catalog_sales"."CS_SOLD_DATE_SK" as "analysis_catalog_date_id"
FROM
    "memory"."catalog_sales" as "analysis_catalog_sales"),
wakeful as (
SELECT
    "highfalutin"."analysis_catalog_date_id" as "analysis_catalog_date_id",
    "highfalutin"."analysis_catalog_quantity" as "analysis_catalog_quantity",
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
    "analysis_catalog_date_date"."D_QUARTER_NAME" as "analysis_catalog_date_quarter_name"
FROM
    "memory"."date_dim" as "analysis_catalog_date_date"),
yummy as (
SELECT
    "abundant"."analysis_store_sale_date_quarter_name" as "analysis_store_sale_date_quarter_name",
    "cheerful"."analysis_item_desc" as "analysis_item_desc",
    "cheerful"."analysis_item_name" as "analysis_item_name",
    "cooperative"."analysis_store_return_date_quarter_name" as "analysis_store_return_date_quarter_name",
    "questionable"."analysis_is_returned" as "analysis_is_returned",
    "questionable"."analysis_store_return_quantity" as "analysis_store_return_quantity",
    "quizzical"."analysis_catalog_date_quarter_name" as "analysis_catalog_date_quarter_name",
    "thoughtful"."analysis_store_state" as "analysis_store_state",
    "uneven"."analysis_store_quantity" as "analysis_store_quantity",
    "wakeful"."analysis_catalog_quantity" as "analysis_catalog_quantity"
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
    "abundant"."analysis_store_sale_date_quarter_name" = '2001Q1' and "cooperative"."analysis_store_return_date_quarter_name" in ('2001Q1','2001Q2','2001Q3') and "quizzical"."analysis_catalog_date_quarter_name" in ('2001Q1','2001Q2','2001Q3') and "questionable"."analysis_is_returned"

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
juicy as (
SELECT
    "yummy"."analysis_item_desc" as "analysis_item_desc",
    "yummy"."analysis_item_name" as "analysis_item_name",
    "yummy"."analysis_store_state" as "analysis_store_state",
    avg("yummy"."analysis_catalog_quantity") as "_virt_agg_avg_1688371525139287",
    avg("yummy"."analysis_catalog_quantity") as "catalog_sales_quantityave",
    avg("yummy"."analysis_store_quantity") as "_virt_agg_avg_7518273379920258",
    avg("yummy"."analysis_store_quantity") as "store_sales_quantityave",
    avg("yummy"."analysis_store_return_quantity") as "_virt_agg_avg_8572613716165371",
    avg("yummy"."analysis_store_return_quantity") as "store_returns_quantityave",
    count("yummy"."analysis_catalog_quantity") as "catalog_sales_quantitycount",
    count("yummy"."analysis_store_quantity") as "store_sales_quantitycount",
    count("yummy"."analysis_store_return_quantity") as "store_returns_quantitycount",
    stddev_samp("yummy"."analysis_catalog_quantity") as "_virt_agg_stddev_2693366057110854",
    stddev_samp("yummy"."analysis_catalog_quantity") as "catalog_sales_quantitystdev",
    stddev_samp("yummy"."analysis_store_quantity") as "_virt_agg_stddev_8948125603328408",
    stddev_samp("yummy"."analysis_store_quantity") as "store_sales_quantitystdev",
    stddev_samp("yummy"."analysis_store_return_quantity") as "_virt_agg_stddev_2955055239782943",
    stddev_samp("yummy"."analysis_store_return_quantity") as "store_returns_quantitystdev"
FROM
    "yummy"
GROUP BY
    1,
    2,
    3),
vacuous as (
SELECT
    "juicy"."_virt_agg_avg_1688371525139287" as "_virt_agg_avg_1688371525139287",
    "juicy"."_virt_agg_avg_7518273379920258" as "_virt_agg_avg_7518273379920258",
    "juicy"."_virt_agg_avg_8572613716165371" as "_virt_agg_avg_8572613716165371",
    "juicy"."_virt_agg_stddev_2693366057110854" / "juicy"."_virt_agg_avg_1688371525139287" as "catalog_sales_quantitycov",
    "juicy"."_virt_agg_stddev_2693366057110854" as "_virt_agg_stddev_2693366057110854",
    "juicy"."_virt_agg_stddev_2955055239782943" / "juicy"."_virt_agg_avg_8572613716165371" as "store_returns_quantitycov",
    "juicy"."_virt_agg_stddev_2955055239782943" as "_virt_agg_stddev_2955055239782943",
    "juicy"."_virt_agg_stddev_8948125603328408" / "juicy"."_virt_agg_avg_7518273379920258" as "store_sales_quantitycov",
    "juicy"."_virt_agg_stddev_8948125603328408" as "_virt_agg_stddev_8948125603328408",
    "juicy"."analysis_item_desc" as "analysis_item_desc",
    "juicy"."analysis_item_name" as "analysis_item_name",
    "juicy"."analysis_store_state" as "analysis_store_state"
FROM
    "juicy")
SELECT
    coalesce("juicy"."analysis_item_name","vacuous"."analysis_item_name") as "analysis_item_name",
    coalesce("juicy"."analysis_item_desc","vacuous"."analysis_item_desc") as "analysis_item_desc",
    coalesce("juicy"."analysis_store_state","vacuous"."analysis_store_state") as "analysis_store_state",
    coalesce("juicy"."store_sales_quantitycount",0) as "store_sales_quantitycount",
    "juicy"."store_sales_quantityave" as "store_sales_quantityave",
    "juicy"."store_sales_quantitystdev" as "store_sales_quantitystdev",
    "vacuous"."store_sales_quantitycov" as "store_sales_quantitycov",
    coalesce("juicy"."store_returns_quantitycount",0) as "store_returns_quantitycount",
    "juicy"."store_returns_quantityave" as "store_returns_quantityave",
    "juicy"."store_returns_quantitystdev" as "store_returns_quantitystdev",
    "vacuous"."store_returns_quantitycov" as "store_returns_quantitycov",
    coalesce("juicy"."catalog_sales_quantitycount",0) as "catalog_sales_quantitycount",
    "juicy"."catalog_sales_quantityave" as "catalog_sales_quantityave",
    "juicy"."catalog_sales_quantitystdev" as "catalog_sales_quantitystdev",
    "vacuous"."catalog_sales_quantitycov" as "catalog_sales_quantitycov"
FROM
    "vacuous"
    FULL JOIN "juicy" on "vacuous"."analysis_item_desc" is not distinct from "juicy"."analysis_item_desc" AND "vacuous"."analysis_item_name" is not distinct from "juicy"."analysis_item_name" AND "vacuous"."analysis_store_state" is not distinct from "juicy"."analysis_store_state"
ORDER BY 
    coalesce("juicy"."analysis_item_name","vacuous"."analysis_item_name") asc nulls first,
    coalesce("juicy"."analysis_item_desc","vacuous"."analysis_item_desc") asc nulls first,
    coalesce("juicy"."analysis_store_state","vacuous"."analysis_store_state") asc nulls first
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
