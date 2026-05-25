# Query 72

**Status:** `mismatch`

| Stage | Result |
| --- | --- |
| v4 SQL generation | OK |
| v4 execution | OK (100 rows) |
| reference execution | OK (100 rows) |
| results identical | NO |

## Result comparison

v4 rows: 100 (84 distinct)
ref rows: 100 (100 distinct)
only in v4 (showing up to 5 of 16):
  1x  (None, 0, 2, 2, 'Of course ot', 5207)
  1x  ('Alone rights cannot w', 0, 2, 2, 'Social, royal laws m', 5204)
  1x  ('Authorities offer complete, ', 0, 2, 2, 'Social, royal laws m', 5212)
  1x  ('Available, major villages may use long over a daughters. Involved personnel sleep weak police. Physical names may lose extra arr', 0, 2, 2, 'Terms overcome instr', 5217)
  1x  ('Businesses gain never early physical officials. More labour others would respect. Contemporary stones enhance courts. Sexual taxes might think. Times will hold neither traditional ', 0, 2, 2, 'Of course ot', 5198)
only in ref (showing up to 5 of 16):
  1x  ('Almost leading hills access frequently. Awkward schools increase today for a items. Linguistic cells see below that strategic representati', 0, 1, 1, 'Terms overcome instr', 5205)
  1x  ('Almost mild levels could not prove there coming, different seconds; culturally conservative products relax from a others. Ready days permit. Even good pictures provide forces. Weekly, good rules raise', 0, 1, 1, 'Of course ot', 5196)
  1x  ('Alone large policies would drown more impossible shelves. Interests make very children. Local qualities facilitate most countries; objectives can agree', 0, 1, 1, 'Of course ot', 5210)
  1x  ('Alone large policies would drown more impossible shelves. Interests make very children. Local qualities facilitate most countries; objectives can agree', 0, 1, 1, 'Terms overcome instr', 5210)
  1x  ('Alone windows will not fashion. Evenly small foods live sooner large plants. Criminal journalists should not ring closely medical, numerous books. Parti', 0, 1, 1, 'Conventional childr', 5216)

## SQL size

| Source | Chars | Lines |
| --- | --- | --- |
| v4 | 7215 | 163 |
| reference | 6297 | 115 |
| v4 / ref | 1.15x | 1.42x |

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
uneven as (
SELECT
    "inv_warehouse_inventory"."inv_date_sk" as "inv_date_id",
    "inv_warehouse_inventory"."inv_item_sk" as "cs_item_id",
    "inv_warehouse_inventory"."inv_quantity_on_hand" as "inv_quantity_on_hand",
    "inv_warehouse_inventory"."inv_warehouse_sk" as "inv_warehouse_id"
FROM
    "memory"."inventory" as "inv_warehouse_inventory"),
abundant as (
SELECT
    "inv_warehouse_warehouse"."w_warehouse_name" as "inv_warehouse_name",
    "inv_warehouse_warehouse"."w_warehouse_sk" as "inv_warehouse_id"
FROM
    "memory"."warehouse" as "inv_warehouse_warehouse"),
questionable as (
SELECT
    "inv_date_date"."D_DATE_SK" as "inv_date_id",
    "inv_date_date"."D_WEEK_SEQ" as "inv_date_week_seq"
FROM
    "memory"."date_dim" as "inv_date_date"),
cooperative as (
SELECT
    "cs_sold_date_date"."D_DATE_SK" as "cs_sold_date_id",
    "cs_sold_date_date"."D_WEEK_SEQ" as "cs_sold_date_week_seq",
    "cs_sold_date_date"."D_YEAR" as "cs_sold_date_year",
    cast("cs_sold_date_date"."D_DATE" as date) as "cs_sold_date_date"
FROM
    "memory"."date_dim" as "cs_sold_date_date"),
thoughtful as (
SELECT
    "cs_ship_date_date"."D_DATE_SK" as "cs_ship_date_id",
    cast("cs_ship_date_date"."D_DATE" as date) as "cs_ship_date_date"
FROM
    "memory"."date_dim" as "cs_ship_date_date"),
cheerful as (
SELECT
    "cs_item_items"."I_ITEM_DESC" as "cs_item_desc",
    "cs_item_items"."I_ITEM_SK" as "cs_item_id"
FROM
    "memory"."item" as "cs_item_items"),
wakeful as (
SELECT
    "cs_catalog_sales"."CS_BILL_CDEMO_SK" as "cs_bill_customer_demographic_id",
    "cs_catalog_sales"."CS_BILL_HDEMO_SK" as "cs_bill_household_demographic_id",
    "cs_catalog_sales"."CS_ITEM_SK" as "cs_item_id",
    "cs_catalog_sales"."CS_ORDER_NUMBER" as "cs_order_number",
    "cs_catalog_sales"."CS_PROMO_SK" as "cs_promotion_id",
    "cs_catalog_sales"."CS_QUANTITY" as "cs_quantity",
    "cs_catalog_sales"."CS_SHIP_DATE_SK" as "cs_ship_date_id",
    "cs_catalog_sales"."CS_SOLD_DATE_SK" as "cs_sold_date_id"
FROM
    "memory"."catalog_sales" as "cs_catalog_sales"),
highfalutin as (
SELECT
    "cs_bill_household_demographic_household_demographics"."HD_BUY_POTENTIAL" as "cs_bill_household_demographic_buy_potential",
    "cs_bill_household_demographic_household_demographics"."HD_DEMO_SK" as "cs_bill_household_demographic_id"
FROM
    "memory"."household_demographics" as "cs_bill_household_demographic_household_demographics"),
quizzical as (
SELECT
    "cs_bill_customer_demographic_customer_demographics"."CD_DEMO_SK" as "cs_bill_customer_demographic_id",
    "cs_bill_customer_demographic_customer_demographics"."CD_MARITAL_STATUS" as "cs_bill_customer_demographic_marital_status"
FROM
    "memory"."customer_demographics" as "cs_bill_customer_demographic_customer_demographics"),
yummy as (
SELECT
    "abundant"."inv_warehouse_name" as "inv_warehouse_name",
    "cheerful"."cs_item_desc" as "cs_item_desc",
    "cooperative"."cs_sold_date_date" as "cs_sold_date_date",
    "cooperative"."cs_sold_date_week_seq" as "cs_sold_date_week_seq",
    "cooperative"."cs_sold_date_year" as "cs_sold_date_year",
    "highfalutin"."cs_bill_household_demographic_buy_potential" as "cs_bill_household_demographic_buy_potential",
    "questionable"."inv_date_week_seq" as "inv_date_week_seq",
    "quizzical"."cs_bill_customer_demographic_marital_status" as "cs_bill_customer_demographic_marital_status",
    "thoughtful"."cs_ship_date_date" as "cs_ship_date_date",
    "uneven"."inv_quantity_on_hand" as "inv_quantity_on_hand",
    "wakeful"."cs_order_number" as "cs_order_number",
    "wakeful"."cs_promotion_id" as "cs_promotion_id",
    "wakeful"."cs_quantity" as "cs_quantity"
FROM
    "uneven"
    INNER JOIN "questionable" on "uneven"."inv_date_id" = "questionable"."inv_date_id"
    LEFT OUTER JOIN "abundant" on "uneven"."inv_warehouse_id" = "abundant"."inv_warehouse_id"
    INNER JOIN "wakeful" on "uneven"."cs_item_id" = "wakeful"."cs_item_id"
    INNER JOIN "cheerful" on "uneven"."cs_item_id" = "cheerful"."cs_item_id"
    LEFT OUTER JOIN "thoughtful" on "wakeful"."cs_ship_date_id" = "thoughtful"."cs_ship_date_id"
    LEFT OUTER JOIN "cooperative" on "wakeful"."cs_sold_date_id" = "cooperative"."cs_sold_date_id"
    LEFT OUTER JOIN "quizzical" on "wakeful"."cs_bill_customer_demographic_id" = "quizzical"."cs_bill_customer_demographic_id"
    LEFT OUTER JOIN "highfalutin" on "wakeful"."cs_bill_household_demographic_id" = "highfalutin"."cs_bill_household_demographic_id"
WHERE
    "cooperative"."cs_sold_date_year" = 1999 and "highfalutin"."cs_bill_household_demographic_buy_potential" = '>10000' and "quizzical"."cs_bill_customer_demographic_marital_status" = 'D' and "questionable"."inv_date_week_seq" = "cooperative"."cs_sold_date_week_seq" and date_diff('day', "cooperative"."cs_sold_date_date", "thoughtful"."cs_ship_date_date") > 5 and "uneven"."inv_quantity_on_hand" < "wakeful"."cs_quantity"

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
    13),
vacuous as (
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
juicy as (
SELECT
    "yummy"."cs_bill_customer_demographic_marital_status" as "cs_bill_customer_demographic_marital_status",
    "yummy"."cs_bill_household_demographic_buy_potential" as "cs_bill_household_demographic_buy_potential",
    "yummy"."cs_item_desc" as "cs_item_desc",
    "yummy"."cs_item_desc" as "item_desc",
    "yummy"."cs_order_number" as "cs_order_number",
    "yummy"."cs_promotion_id" as "cs_promotion_id",
    "yummy"."cs_quantity" as "cs_quantity",
    "yummy"."cs_ship_date_date" as "cs_ship_date_date",
    "yummy"."cs_sold_date_date" as "cs_sold_date_date",
    "yummy"."cs_sold_date_week_seq" as "cs_sold_date_week_seq",
    "yummy"."cs_sold_date_week_seq" as "week_seq",
    "yummy"."cs_sold_date_year" as "cs_sold_date_year",
    "yummy"."inv_date_week_seq" as "inv_date_week_seq",
    "yummy"."inv_quantity_on_hand" as "inv_quantity_on_hand",
    "yummy"."inv_warehouse_name" as "inv_warehouse_name",
    "yummy"."inv_warehouse_name" as "warehouse_name"
FROM
    "yummy")
SELECT
    "juicy"."item_desc" as "item_desc",
    "juicy"."warehouse_name" as "warehouse_name",
    "juicy"."week_seq" as "week_seq",
    "vacuous"."no_promo" as "no_promo",
    "vacuous"."promo" as "promo",
    coalesce("vacuous"."total_cnt",0) as "total_cnt"
FROM
    "juicy"
    FULL JOIN "vacuous" on "juicy"."cs_item_desc" is not distinct from "vacuous"."cs_item_desc" AND "juicy"."cs_sold_date_week_seq" = "vacuous"."cs_sold_date_week_seq" AND "juicy"."inv_warehouse_name" is not distinct from "vacuous"."inv_warehouse_name"
ORDER BY 
    coalesce("vacuous"."total_cnt",0) desc nulls first,
    "juicy"."item_desc" asc nulls first,
    "juicy"."warehouse_name" asc nulls first,
    "juicy"."week_seq" asc nulls first
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
