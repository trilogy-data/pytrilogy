# Query 72

**Status:** `mismatch`

| Stage | Result |
| --- | --- |
| v4 SQL generation | OK |
| v4 execution | OK (100 rows) |
| reference execution | OK (100 rows) |
| results identical | NO |

## Result comparison

v4 rows: 100 (1 distinct)
ref rows: 100 (100 distinct)
only in v4 (showing up to 5 of 1):
  99x  (None, 0, 2, 2, 'Of course ot', 5207)
only in ref (showing up to 5 of 99):
  1x  ('Alone rights cannot w', 0, 2, 2, 'Social, royal laws m', 5204)
  1x  ('Authorities offer complete, ', 0, 2, 2, 'Social, royal laws m', 5212)
  1x  ('Available, major villages may use long over a daughters. Involved personnel sleep weak police. Physical names may lose extra arr', 0, 2, 2, 'Terms overcome instr', 5217)
  1x  ('Businesses gain never early physical officials. More labour others would respect. Contemporary stones enhance courts. Sexual taxes might think. Times will hold neither traditional ', 0, 2, 2, 'Of course ot', 5198)
  1x  ('Businesses gain never early physical officials. More labour others would respect. Contemporary stones enhance courts. Sexual taxes might think. Times will hold neither traditional ', 0, 2, 2, 'Terms overcome instr', 5198)

## SQL size + execution time

| Source | Chars | Lines | Exec (min of 4) |
| --- | --- | --- | --- |
| v4 | 4251 | 91 | 536.32 ms |
| reference | 6297 | 115 | 671.51 ms |
| v4 / ref | 0.68x | 0.79x | 0.80x |

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
    and date_diff(cs.sold_date.date, cs.ship_date.date, day) > 5
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
    "cs_catalog_sales"."CS_ORDER_NUMBER" as "cs_order_number",
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
    INNER JOIN "memory"."date_dim" as "cs_ship_date_date" on "cs_catalog_sales"."CS_SHIP_DATE_SK" = "cs_ship_date_date"."D_DATE_SK"
    INNER JOIN "memory"."date_dim" as "cs_sold_date_date" on "cs_catalog_sales"."CS_SOLD_DATE_SK" = "cs_sold_date_date"."D_DATE_SK"
    INNER JOIN "memory"."customer_demographics" as "cs_bill_customer_demographic_customer_demographics" on "cs_catalog_sales"."CS_BILL_CDEMO_SK" = "cs_bill_customer_demographic_customer_demographics"."CD_DEMO_SK"
    INNER JOIN "memory"."household_demographics" as "cs_bill_household_demographic_household_demographics" on "cs_catalog_sales"."CS_BILL_HDEMO_SK" = "cs_bill_household_demographic_household_demographics"."HD_DEMO_SK"
WHERE
    "cs_sold_date_date"."D_YEAR" = 1999 and "cs_bill_household_demographic_household_demographics"."HD_BUY_POTENTIAL" = '>10000' and "cs_bill_customer_demographic_customer_demographics"."CD_MARITAL_STATUS" = 'D' and "inv_date_date"."D_WEEK_SEQ" = "cs_sold_date_date"."D_WEEK_SEQ" and date_diff('day', cast("cs_sold_date_date"."D_DATE" as date), cast("cs_ship_date_date"."D_DATE" as date)) > 5 and "inv_warehouse_inventory"."inv_quantity_on_hand" < "cs_catalog_sales"."CS_QUANTITY"

GROUP BY
    1,
    2,
    3,
    4,
    5,
    "cs_bill_customer_demographic_customer_demographics"."CD_MARITAL_STATUS",
    "cs_bill_household_demographic_household_demographics"."HD_BUY_POTENTIAL",
    "cs_catalog_sales"."CS_QUANTITY",
    "cs_sold_date_date"."D_YEAR",
    "inv_date_date"."D_WEEK_SEQ",
    "inv_warehouse_inventory"."inv_quantity_on_hand",
    cast("cs_ship_date_date"."D_DATE" as date),
    cast("cs_sold_date_date"."D_DATE" as date)),
young as (
SELECT
    "yummy"."cs_item_desc" as "cs_item_desc",
    "yummy"."cs_sold_date_week_seq" as "cs_sold_date_week_seq",
    "yummy"."inv_warehouse_name" as "inv_warehouse_name",
    count("yummy"."cs_order_number") as "total_cnt",
    sum(CASE
	WHEN "yummy"."cs_promotion_id" is not null THEN 1
	ELSE 0
	END) as "promo",
    sum(CASE
	WHEN "yummy"."cs_promotion_id" is null THEN 1
	ELSE 0
	END) as "no_promo"
FROM
    "yummy"
GROUP BY
    1,
    2,
    3),
concerned as (
SELECT
    "yummy"."inv_warehouse_name" as "inv_warehouse_name",
    "yummy"."inv_warehouse_name" as "warehouse_name"
FROM
    "yummy"),
vacuous as (
SELECT
    "yummy"."cs_sold_date_week_seq" as "cs_sold_date_week_seq",
    "yummy"."cs_sold_date_week_seq" as "week_seq"
FROM
    "yummy"),
juicy as (
SELECT
    "yummy"."cs_item_desc" as "cs_item_desc",
    "yummy"."cs_item_desc" as "item_desc"
FROM
    "yummy")
SELECT
    "juicy"."item_desc" as "item_desc",
    "concerned"."warehouse_name" as "warehouse_name",
    "vacuous"."week_seq" as "week_seq",
    "young"."no_promo" as "no_promo",
    "young"."promo" as "promo",
    "young"."total_cnt" as "total_cnt"
FROM
    "young"
    FULL JOIN "juicy" on "young"."cs_item_desc" is not distinct from "juicy"."cs_item_desc"
    FULL JOIN "concerned" on "young"."inv_warehouse_name" is not distinct from "concerned"."inv_warehouse_name"
    RIGHT OUTER JOIN "vacuous" on "young"."cs_sold_date_week_seq" = "vacuous"."cs_sold_date_week_seq"
ORDER BY 
    "young"."total_cnt" desc nulls first,
    "juicy"."item_desc" asc nulls first,
    "concerned"."warehouse_name" asc nulls first,
    "vacuous"."week_seq" asc nulls first
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
    INNER JOIN "memory"."date_dim" as "cs_ship_date_date" on "cs_catalog_sales"."CS_SHIP_DATE_SK" = "cs_ship_date_date"."D_DATE_SK"
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
    INNER JOIN "memory"."date_dim" as "cs_ship_date_date" on "cheerful"."cs_ship_date_id" = "cs_ship_date_date"."D_DATE_SK"
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
    coalesce("young"."total_cnt",0) as "total_cnt"
FROM
    "vacuous"
    INNER JOIN "young" on "vacuous"."cs_item_desc" is not distinct from "young"."cs_item_desc" AND "vacuous"."cs_sold_date_week_seq" = "young"."cs_sold_date_week_seq" AND "vacuous"."inv_warehouse_name" is not distinct from "young"."inv_warehouse_name"
ORDER BY 
    coalesce("young"."total_cnt",0) desc nulls first,
    "item_desc" asc nulls first,
    "warehouse_name" asc nulls first,
    "week_seq" asc nulls first
LIMIT (100)
```
