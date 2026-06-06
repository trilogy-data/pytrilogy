# Query 72

**Status:** `match`

| Stage | Result |
| --- | --- |
| v4 SQL generation | OK |
| v4 execution | OK (100 rows) |
| reference execution | OK (100 rows) |
| results identical | YES |

## Result comparison

v4 rows: 100 (100 distinct)
ref rows: 100 (100 distinct)

## SQL size + execution time

| Source | Chars | Lines | Exec (min of 4) |
| --- | --- | --- | --- |
| v4 | 7595 | 143 | 305.56 ms |
| reference | 6283 | 115 | 4.243 s |
| v4 / ref | 1.21x | 1.24x | 0.07x |

## Preql

```
import catalog_sales as cs;
import inventory as inv;

merge inv.item.id into cs.item.id;

where
    cs.sold_date.year = 1999
    and cs.bill_household_demographic.buy_potential = '>10000'
    and cs.bill_customer_demographic.marital_status = 'D'
    and inv.date.week_seq = cs.sold_date.week_seq
    and cs.days_to_ship > 5
    and inv.quantity_on_hand < cs.quantity
select
    cs.item.desc as item_desc,
    inv.warehouse.name as warehouse_name,
    cs.sold_date.week_seq as week_seq,
    sum(
            case
                when cs.promotion.id is null then 1
                else 0
            end
        ) as no_promo,
    sum(
            case
                when cs.promotion.id is not null then 1
                else 0
            end
        ) as promo,
    count(cs.order_number) as total_cnt,
order by
    total_cnt desc nulls first,
    item_desc asc nulls first,
    warehouse_name asc nulls first,
    week_seq asc nulls first
limit 100
;
```

## v4 generated SQL

```sql
WITH 
yummy as (
SELECT
    "cs_catalog_sales"."CS_SHIP_DATE_SK" as "cs_ship_date_id",
    "cs_catalog_sales"."CS_SOLD_DATE_SK" as "cs_sold_date_id"
FROM
    "memory"."catalog_sales" as "cs_catalog_sales"
GROUP BY
    1,
    2),
vacuous as (
SELECT
    "yummy"."cs_ship_date_id" as "cs_ship_date_id",
    "yummy"."cs_sold_date_id" as "cs_sold_date_id",
    date_diff('day', cast("cs_sold_date_date"."D_DATE" as date), cast("cs_ship_date_date"."D_DATE" as date)) as "cs_days_to_ship"
FROM
    "yummy"
    LEFT OUTER JOIN "memory"."date_dim" as "cs_ship_date_date" on "yummy"."cs_ship_date_id" = "cs_ship_date_date"."D_DATE_SK"
    INNER JOIN "memory"."date_dim" as "cs_sold_date_date" on "yummy"."cs_sold_date_id" = "cs_sold_date_date"."D_DATE_SK"
WHERE
    "cs_sold_date_date"."D_YEAR" = 1999
),
late as (
SELECT
    "cs_catalog_sales"."CS_ORDER_NUMBER" as "cs_order_number",
    "cs_item_items"."I_ITEM_DESC" as "cs_item_desc",
    "cs_sold_date_date"."D_WEEK_SEQ" as "cs_sold_date_week_seq",
    "inv_warehouse_warehouse"."w_warehouse_name" as "inv_warehouse_name"
FROM
    "memory"."inventory" as "inv_warehouse_inventory"
    INNER JOIN "memory"."date_dim" as "inv_date_date" on "inv_warehouse_inventory"."inv_date_sk" = "inv_date_date"."D_DATE_SK"
    LEFT OUTER JOIN "memory"."warehouse" as "inv_warehouse_warehouse" on "inv_warehouse_inventory"."inv_warehouse_sk" = "inv_warehouse_warehouse"."w_warehouse_sk"
    INNER JOIN "memory"."catalog_sales" as "cs_catalog_sales" on "inv_warehouse_inventory"."inv_item_sk" = "cs_catalog_sales"."CS_ITEM_SK"
    INNER JOIN "memory"."item" as "cs_item_items" on "inv_warehouse_inventory"."inv_item_sk" = "cs_item_items"."I_ITEM_SK"
    INNER JOIN "memory"."date_dim" as "cs_sold_date_date" on "cs_catalog_sales"."CS_SOLD_DATE_SK" = "cs_sold_date_date"."D_DATE_SK"
    INNER JOIN "memory"."customer_demographics" as "cs_bill_customer_demographic_customer_demographics" on "cs_catalog_sales"."CS_BILL_CDEMO_SK" = "cs_bill_customer_demographic_customer_demographics"."CD_DEMO_SK"
    INNER JOIN "memory"."household_demographics" as "cs_bill_household_demographic_household_demographics" on "cs_catalog_sales"."CS_BILL_HDEMO_SK" = "cs_bill_household_demographic_household_demographics"."HD_DEMO_SK"
    INNER JOIN "vacuous" on "cs_catalog_sales"."CS_SHIP_DATE_SK" is not distinct from "vacuous"."cs_ship_date_id" AND "cs_catalog_sales"."CS_SOLD_DATE_SK" is not distinct from "vacuous"."cs_sold_date_id"
WHERE
    "cs_sold_date_date"."D_YEAR" = 1999 and "cs_bill_household_demographic_household_demographics"."HD_BUY_POTENTIAL" = '>10000' and "cs_bill_customer_demographic_customer_demographics"."CD_MARITAL_STATUS" = 'D' and "inv_date_date"."D_WEEK_SEQ" = "cs_sold_date_date"."D_WEEK_SEQ" and "inv_warehouse_inventory"."inv_quantity_on_hand" < "cs_catalog_sales"."CS_QUANTITY" and "vacuous"."cs_days_to_ship" > 5

GROUP BY
    1,
    2,
    3,
    4),
uneven as (
SELECT
    "cs_catalog_sales"."CS_PROMO_SK" as "cs_promotion_id",
    "cs_item_items"."I_ITEM_DESC" as "cs_item_desc",
    "cs_sold_date_date"."D_WEEK_SEQ" as "cs_sold_date_week_seq",
    "inv_warehouse_warehouse"."w_warehouse_name" as "inv_warehouse_name"
FROM
    "memory"."inventory" as "inv_warehouse_inventory"
    INNER JOIN "memory"."date_dim" as "inv_date_date" on "inv_warehouse_inventory"."inv_date_sk" = "inv_date_date"."D_DATE_SK"
    LEFT OUTER JOIN "memory"."warehouse" as "inv_warehouse_warehouse" on "inv_warehouse_inventory"."inv_warehouse_sk" = "inv_warehouse_warehouse"."w_warehouse_sk"
    INNER JOIN "memory"."catalog_sales" as "cs_catalog_sales" on "inv_warehouse_inventory"."inv_item_sk" = "cs_catalog_sales"."CS_ITEM_SK"
    INNER JOIN "memory"."item" as "cs_item_items" on "inv_warehouse_inventory"."inv_item_sk" = "cs_item_items"."I_ITEM_SK"
    INNER JOIN "memory"."date_dim" as "cs_sold_date_date" on "cs_catalog_sales"."CS_SOLD_DATE_SK" = "cs_sold_date_date"."D_DATE_SK"
    INNER JOIN "memory"."customer_demographics" as "cs_bill_customer_demographic_customer_demographics" on "cs_catalog_sales"."CS_BILL_CDEMO_SK" = "cs_bill_customer_demographic_customer_demographics"."CD_DEMO_SK"
    INNER JOIN "memory"."household_demographics" as "cs_bill_household_demographic_household_demographics" on "cs_catalog_sales"."CS_BILL_HDEMO_SK" = "cs_bill_household_demographic_household_demographics"."HD_DEMO_SK"
    INNER JOIN "vacuous" on "cs_catalog_sales"."CS_SHIP_DATE_SK" is not distinct from "vacuous"."cs_ship_date_id" AND "cs_catalog_sales"."CS_SOLD_DATE_SK" is not distinct from "vacuous"."cs_sold_date_id"
WHERE
    "cs_sold_date_date"."D_YEAR" = 1999 and "cs_bill_household_demographic_household_demographics"."HD_BUY_POTENTIAL" = '>10000' and "cs_bill_customer_demographic_customer_demographics"."CD_MARITAL_STATUS" = 'D' and "inv_date_date"."D_WEEK_SEQ" = "cs_sold_date_date"."D_WEEK_SEQ" and "inv_warehouse_inventory"."inv_quantity_on_hand" < "cs_catalog_sales"."CS_QUANTITY" and "vacuous"."cs_days_to_ship" > 5

GROUP BY
    1,
    2,
    3,
    4,
    "cs_bill_customer_demographic_customer_demographics"."CD_MARITAL_STATUS",
    "cs_bill_household_demographic_household_demographics"."HD_BUY_POTENTIAL",
    "cs_catalog_sales"."CS_BILL_CDEMO_SK",
    "cs_catalog_sales"."CS_BILL_HDEMO_SK",
    "cs_catalog_sales"."CS_ITEM_SK",
    "cs_catalog_sales"."CS_ORDER_NUMBER",
    "cs_catalog_sales"."CS_QUANTITY",
    "cs_sold_date_date"."D_YEAR",
    "inv_date_date"."D_DATE_SK",
    "inv_date_date"."D_WEEK_SEQ",
    "inv_warehouse_inventory"."inv_quantity_on_hand",
    "inv_warehouse_inventory"."inv_warehouse_sk",
    coalesce("cs_catalog_sales"."CS_SHIP_DATE_SK","vacuous"."cs_ship_date_id"),
    coalesce("cs_catalog_sales"."CS_SOLD_DATE_SK","vacuous"."cs_sold_date_id")),
kaput as (
SELECT
    "late"."cs_item_desc" as "cs_item_desc",
    "late"."cs_sold_date_week_seq" as "cs_sold_date_week_seq",
    "late"."inv_warehouse_name" as "inv_warehouse_name",
    count("late"."cs_order_number") as "total_cnt"
FROM
    "late"
GROUP BY
    1,
    2,
    3),
young as (
SELECT
    "uneven"."cs_item_desc" as "cs_item_desc",
    "uneven"."cs_sold_date_week_seq" as "cs_sold_date_week_seq",
    "uneven"."inv_warehouse_name" as "inv_warehouse_name",
    sum(CASE
	WHEN "uneven"."cs_promotion_id" is not null THEN 1
	ELSE 0
	END) as "promo",
    sum(CASE
	WHEN "uneven"."cs_promotion_id" is null THEN 1
	ELSE 0
	END) as "no_promo"
FROM
    "uneven"
GROUP BY
    1,
    2,
    3),
sweltering as (
SELECT
    "young"."cs_item_desc" as "cs_item_desc",
    "young"."cs_item_desc" as "item_desc",
    "young"."cs_sold_date_week_seq" as "cs_sold_date_week_seq",
    "young"."cs_sold_date_week_seq" as "week_seq",
    "young"."inv_warehouse_name" as "inv_warehouse_name",
    "young"."inv_warehouse_name" as "warehouse_name",
    "young"."no_promo" as "no_promo",
    "young"."promo" as "promo"
FROM
    "young")
SELECT
    "sweltering"."item_desc" as "item_desc",
    "sweltering"."warehouse_name" as "warehouse_name",
    "sweltering"."week_seq" as "week_seq",
    "sweltering"."no_promo" as "no_promo",
    "sweltering"."promo" as "promo",
    "kaput"."total_cnt" as "total_cnt"
FROM
    "kaput"
    FULL JOIN "sweltering" on "kaput"."cs_item_desc" is not distinct from "sweltering"."cs_item_desc" AND "kaput"."cs_sold_date_week_seq" = "sweltering"."cs_sold_date_week_seq" AND "kaput"."inv_warehouse_name" is not distinct from "sweltering"."inv_warehouse_name"
ORDER BY 
    "kaput"."total_cnt" desc nulls first,
    "sweltering"."item_desc" asc nulls first,
    "sweltering"."warehouse_name" asc nulls first,
    "sweltering"."week_seq" asc nulls first
LIMIT (100)
```

## Reference SQL (zquery log)

```sql
WITH 
cheerful as (
SELECT
    "cs_catalog_sales"."CS_BILL_CDEMO_SK" as "cs_bill_customer_demographic_id",
    "cs_catalog_sales"."CS_BILL_HDEMO_SK" as "cs_bill_household_demographic_id",
    "cs_catalog_sales"."CS_ITEM_SK" as "cs_item_id",
    "cs_catalog_sales"."CS_PROMO_SK" as "cs_promotion_id",
    "cs_catalog_sales"."CS_QUANTITY" as "cs_quantity",
    "cs_catalog_sales"."CS_SHIP_DATE_SK" as "cs_ship_date_id",
    "cs_catalog_sales"."CS_SOLD_DATE_SK" as "cs_sold_date_id"
FROM
    "memory"."catalog_sales" as "cs_catalog_sales"
GROUP BY
    1,
    2,
    3,
    4,
    5,
    6,
    7),
concerned as (
SELECT
    "cs_catalog_sales"."CS_ORDER_NUMBER" as "cs_order_number",
    "cs_item_items"."I_ITEM_DESC" as "cs_item_desc",
    "cs_sold_date_date"."D_WEEK_SEQ" as "cs_sold_date_week_seq",
    "inv_warehouse_warehouse"."w_warehouse_name" as "inv_warehouse_name"
FROM
    "memory"."inventory" as "inv_warehouse_inventory"
    INNER JOIN "memory"."date_dim" as "inv_date_date" on "inv_warehouse_inventory"."inv_date_sk" = "inv_date_date"."D_DATE_SK"
    LEFT OUTER JOIN "memory"."warehouse" as "inv_warehouse_warehouse" on "inv_warehouse_inventory"."inv_warehouse_sk" = "inv_warehouse_warehouse"."w_warehouse_sk"
    INNER JOIN "memory"."catalog_sales" as "cs_catalog_sales" on "inv_warehouse_inventory"."inv_item_sk" = "cs_catalog_sales"."CS_ITEM_SK"
    INNER JOIN "memory"."item" as "cs_item_items" on "inv_warehouse_inventory"."inv_item_sk" = "cs_item_items"."I_ITEM_SK"
    LEFT OUTER JOIN "memory"."date_dim" as "cs_ship_date_date" on "cs_catalog_sales"."CS_SHIP_DATE_SK" = "cs_ship_date_date"."D_DATE_SK"
    INNER JOIN "memory"."date_dim" as "cs_sold_date_date" on "cs_catalog_sales"."CS_SOLD_DATE_SK" = "cs_sold_date_date"."D_DATE_SK"
    INNER JOIN "memory"."customer_demographics" as "cs_bill_customer_demographic_customer_demographics" on "cs_catalog_sales"."CS_BILL_CDEMO_SK" = "cs_bill_customer_demographic_customer_demographics"."CD_DEMO_SK"
    INNER JOIN "memory"."household_demographics" as "cs_bill_household_demographic_household_demographics" on "cs_catalog_sales"."CS_BILL_HDEMO_SK" = "cs_bill_household_demographic_household_demographics"."HD_DEMO_SK"
WHERE
    "cs_sold_date_date"."D_YEAR" = 1999 and "cs_bill_household_demographic_household_demographics"."HD_BUY_POTENTIAL" = '>10000' and "cs_bill_customer_demographic_customer_demographics"."CD_MARITAL_STATUS" = 'D' and "inv_date_date"."D_WEEK_SEQ" = "cs_sold_date_date"."D_WEEK_SEQ" and date_diff('day', cast("cs_sold_date_date"."D_DATE" as date), cast("cs_ship_date_date"."D_DATE" as date)) > 5 and "inv_warehouse_inventory"."inv_quantity_on_hand" < "cs_catalog_sales"."CS_QUANTITY"

GROUP BY
    1,
    2,
    3,
    4),
juicy as (
SELECT
    "cheerful"."cs_promotion_id" as "cs_promotion_id",
    "cs_item_items"."I_ITEM_DESC" as "cs_item_desc",
    "cs_sold_date_date"."D_WEEK_SEQ" as "cs_sold_date_week_seq",
    "inv_warehouse_warehouse"."w_warehouse_name" as "inv_warehouse_name"
FROM
    "memory"."inventory" as "inv_warehouse_inventory"
    INNER JOIN "memory"."date_dim" as "inv_date_date" on "inv_warehouse_inventory"."inv_date_sk" = "inv_date_date"."D_DATE_SK"
    LEFT OUTER JOIN "memory"."warehouse" as "inv_warehouse_warehouse" on "inv_warehouse_inventory"."inv_warehouse_sk" = "inv_warehouse_warehouse"."w_warehouse_sk"
    INNER JOIN "cheerful" on "inv_warehouse_inventory"."inv_item_sk" = "cheerful"."cs_item_id"
    INNER JOIN "memory"."item" as "cs_item_items" on "cheerful"."cs_item_id" = "cs_item_items"."I_ITEM_SK"
    LEFT OUTER JOIN "memory"."date_dim" as "cs_ship_date_date" on "cheerful"."cs_ship_date_id" = "cs_ship_date_date"."D_DATE_SK"
    INNER JOIN "memory"."date_dim" as "cs_sold_date_date" on "cheerful"."cs_sold_date_id" = "cs_sold_date_date"."D_DATE_SK"
    INNER JOIN "memory"."customer_demographics" as "cs_bill_customer_demographic_customer_demographics" on "cheerful"."cs_bill_customer_demographic_id" = "cs_bill_customer_demographic_customer_demographics"."CD_DEMO_SK"
    INNER JOIN "memory"."household_demographics" as "cs_bill_household_demographic_household_demographics" on "cheerful"."cs_bill_household_demographic_id" = "cs_bill_household_demographic_household_demographics"."HD_DEMO_SK"
WHERE
    "cs_sold_date_date"."D_YEAR" = 1999 and "cs_bill_household_demographic_household_demographics"."HD_BUY_POTENTIAL" = '>10000' and "cs_bill_customer_demographic_customer_demographics"."CD_MARITAL_STATUS" = 'D' and "inv_date_date"."D_WEEK_SEQ" = "cs_sold_date_date"."D_WEEK_SEQ" and date_diff('day', cast("cs_sold_date_date"."D_DATE" as date), cast("cs_ship_date_date"."D_DATE" as date)) > 5 and "inv_warehouse_inventory"."inv_quantity_on_hand" < "cheerful"."cs_quantity"

GROUP BY
    1,
    2,
    3,
    4),
young as (
SELECT
    "concerned"."cs_item_desc" as "cs_item_desc",
    "concerned"."cs_sold_date_week_seq" as "cs_sold_date_week_seq",
    "concerned"."inv_warehouse_name" as "inv_warehouse_name",
    count("concerned"."cs_order_number") as "total_cnt"
FROM
    "concerned"
GROUP BY
    1,
    2,
    3),
vacuous as (
SELECT
    "juicy"."cs_item_desc" as "cs_item_desc",
    "juicy"."cs_sold_date_week_seq" as "cs_sold_date_week_seq",
    "juicy"."inv_warehouse_name" as "inv_warehouse_name",
    sum(CASE
	WHEN "juicy"."cs_promotion_id" is not null THEN 1
	ELSE 0
	END) as "promo",
    sum(CASE
	WHEN "juicy"."cs_promotion_id" is null THEN 1
	ELSE 0
	END) as "no_promo"
FROM
    "juicy"
GROUP BY
    1,
    2,
    3)
SELECT
    coalesce("vacuous"."cs_item_desc","young"."cs_item_desc") as "item_desc",
    coalesce("vacuous"."inv_warehouse_name","young"."inv_warehouse_name") as "warehouse_name",
    coalesce("vacuous"."cs_sold_date_week_seq","young"."cs_sold_date_week_seq") as "week_seq",
    "vacuous"."no_promo" as "no_promo",
    "vacuous"."promo" as "promo",
    "young"."total_cnt" as "total_cnt"
FROM
    "young"
    INNER JOIN "vacuous" on "young"."cs_item_desc" is not distinct from "vacuous"."cs_item_desc" AND "young"."cs_sold_date_week_seq" = "vacuous"."cs_sold_date_week_seq" AND "young"."inv_warehouse_name" is not distinct from "vacuous"."inv_warehouse_name"
ORDER BY 
    "young"."total_cnt" desc nulls first,
    "item_desc" asc nulls first,
    "warehouse_name" asc nulls first,
    "week_seq" asc nulls first
LIMIT (100)
```
