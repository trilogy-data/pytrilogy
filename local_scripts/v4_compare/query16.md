# Query 16

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
| v4 | 5373 | 119 | 111.71 ms |
| reference | 3728 | 83 | 57.33 ms |
| v4 / ref | 1.44x | 1.43x | 1.95x |

## Preql

```
import catalog_sales as cs;
import catalog_returns as cr;

auto multi_warehouse_sales <- cs.order_number ? count(cs.warehouse.id) by cs.order_number > 1;

where
    cs.ship_date.date between '2002-02-01'::date and '2002-04-02'::date
    and cs.customer_address.state = 'GA'
    and cs.call_center.county = 'Williamson County'
    and cs.order_number not in cr.order_number
    and cs.order_number in multi_warehouse_sales
select
    count(cs.order_number) as order_count,
    sum(cs.ext_ship_cost) as total_shipping_cost,
    sum(cs.net_profit) as total_net_profit,
order by
    order_count desc
limit 100
;
```

## v4 generated SQL

```sql
WITH 
uneven as (
SELECT
    "cs_catalog_sales"."CS_ORDER_NUMBER" as "cs_order_number",
    "cs_catalog_sales"."CS_WAREHOUSE_SK" as "cs_warehouse_id"
FROM
    "memory"."catalog_sales" as "cs_catalog_sales"
GROUP BY
    1,
    2),
quizzical as (
SELECT
    "cr_catalog_returns"."CR_ORDER_NUMBER" as "cr_order_number"
FROM
    "memory"."catalog_returns" as "cr_catalog_returns"
GROUP BY
    1),
yummy as (
SELECT
    "uneven"."cs_order_number" as "cs_order_number",
    count("uneven"."cs_warehouse_id") as "_virt_agg_count_7777088585630721"
FROM
    "uneven"
GROUP BY
    1
HAVING
    "_virt_agg_count_7777088585630721" > 1
),
vacuous as (
SELECT
    CASE WHEN "yummy"."_virt_agg_count_7777088585630721" > 1 THEN "yummy"."cs_order_number" ELSE NULL END as "multi_warehouse_sales"
FROM
    "yummy"),
thoughtful as (
SELECT
    "cs_catalog_sales"."CS_CALL_CENTER_SK" as "cs_call_center_id",
    "cs_catalog_sales"."CS_ORDER_NUMBER" as "cs_order_number",
    "cs_catalog_sales"."CS_SHIP_ADDR_SK" as "cs_customer_address_id",
    "cs_catalog_sales"."CS_SHIP_DATE_SK" as "cs_ship_date_id"
FROM
    "memory"."catalog_sales" as "cs_catalog_sales"
WHERE
    "cs_catalog_sales"."CS_ORDER_NUMBER" not in (select quizzical."cr_order_number" from quizzical where quizzical."cr_order_number" is not null) and "cs_catalog_sales"."CS_ORDER_NUMBER" in (select vacuous."multi_warehouse_sales" from vacuous where vacuous."multi_warehouse_sales" is not null)

GROUP BY
    1,
    2,
    3,
    4),
abhorrent as (
SELECT
    "cs_catalog_sales"."CS_EXT_SHIP_COST" as "cs_ext_ship_cost",
    "cs_catalog_sales"."CS_ITEM_SK" as "cs_item_id",
    "cs_catalog_sales"."CS_NET_PROFIT" as "cs_net_profit",
    "cs_catalog_sales"."CS_ORDER_NUMBER" as "cs_order_number"
FROM
    "memory"."catalog_sales" as "cs_catalog_sales"
    INNER JOIN "memory"."date_dim" as "cs_ship_date_date" on "cs_catalog_sales"."CS_SHIP_DATE_SK" = "cs_ship_date_date"."D_DATE_SK"
    INNER JOIN "memory"."call_center" as "cs_call_center_call_center" on "cs_catalog_sales"."CS_CALL_CENTER_SK" = "cs_call_center_call_center"."CC_CALL_CENTER_SK"
    INNER JOIN "memory"."customer_address" as "cs_customer_address_customer_address" on "cs_catalog_sales"."CS_SHIP_ADDR_SK" = "cs_customer_address_customer_address"."CA_ADDRESS_SK"
WHERE
    cast("cs_ship_date_date"."D_DATE" as date) BETWEEN date '2002-02-01' AND date '2002-04-02' and "cs_customer_address_customer_address"."CA_STATE" = 'GA' and "cs_call_center_call_center"."CC_COUNTY" = 'Williamson County' and "cs_catalog_sales"."CS_ORDER_NUMBER" not in (select quizzical."cr_order_number" from quizzical where quizzical."cr_order_number" is not null) and "cs_catalog_sales"."CS_ORDER_NUMBER" in (select vacuous."multi_warehouse_sales" from vacuous where vacuous."multi_warehouse_sales" is not null)
),
abundant as (
SELECT
    "thoughtful"."cs_order_number" as "cs_order_number"
FROM
    "thoughtful"
    INNER JOIN "memory"."date_dim" as "cs_ship_date_date" on "thoughtful"."cs_ship_date_id" = "cs_ship_date_date"."D_DATE_SK"
    INNER JOIN "memory"."call_center" as "cs_call_center_call_center" on "thoughtful"."cs_call_center_id" = "cs_call_center_call_center"."CC_CALL_CENTER_SK"
    INNER JOIN "memory"."customer_address" as "cs_customer_address_customer_address" on "thoughtful"."cs_customer_address_id" = "cs_customer_address_customer_address"."CA_ADDRESS_SK"
WHERE
    cast("cs_ship_date_date"."D_DATE" as date) BETWEEN date '2002-02-01' AND date '2002-04-02' and "cs_customer_address_customer_address"."CA_STATE" = 'GA' and "cs_call_center_call_center"."CC_COUNTY" = 'Williamson County' and "thoughtful"."cs_order_number" not in (select quizzical."cr_order_number" from quizzical where quizzical."cr_order_number" is not null) and "thoughtful"."cs_order_number" in (select vacuous."multi_warehouse_sales" from vacuous where vacuous."multi_warehouse_sales" is not null)
),
sweltering as (
SELECT
    "abhorrent"."cs_ext_ship_cost" as "cs_ext_ship_cost",
    "abhorrent"."cs_net_profit" as "cs_net_profit"
FROM
    "abhorrent"
WHERE
    "abhorrent"."cs_order_number" not in (select quizzical."cr_order_number" from quizzical where quizzical."cr_order_number" is not null) and "abhorrent"."cs_order_number" in (select vacuous."multi_warehouse_sales" from vacuous where vacuous."multi_warehouse_sales" is not null)

GROUP BY
    1,
    2,
    "abhorrent"."cs_item_id",
    "abhorrent"."cs_order_number"),
concerned as (
SELECT
    "abundant"."cs_order_number" as "cs_order_number"
FROM
    "abundant"
WHERE
    "abundant"."cs_order_number" not in (select quizzical."cr_order_number" from quizzical where quizzical."cr_order_number" is not null) and "abundant"."cs_order_number" in (select vacuous."multi_warehouse_sales" from vacuous where vacuous."multi_warehouse_sales" is not null)

GROUP BY
    1),
macho as (
SELECT
    sum("sweltering"."cs_ext_ship_cost") as "total_shipping_cost",
    sum("sweltering"."cs_net_profit") as "total_net_profit"
FROM
    "sweltering"),
sparkling as (
SELECT
    count("concerned"."cs_order_number") as "order_count"
FROM
    "concerned")
SELECT
    coalesce("sparkling"."order_count",0) as "order_count",
    "macho"."total_shipping_cost" as "total_shipping_cost",
    "macho"."total_net_profit" as "total_net_profit"
FROM
    "sparkling"
    FULL JOIN "macho" on 1=1
ORDER BY 
    coalesce("sparkling"."order_count",0) desc
LIMIT (100)
```

## Reference SQL (zquery log)

```sql
WITH 
cooperative as (
SELECT
    "cs_catalog_sales"."CS_ORDER_NUMBER" as "cs_order_number",
    "cs_catalog_sales"."CS_WAREHOUSE_SK" as "cs_warehouse_id"
FROM
    "memory"."catalog_sales" as "cs_catalog_sales"
GROUP BY
    1,
    2),
quizzical as (
SELECT
    "cr_catalog_returns"."CR_ORDER_NUMBER" as "cr_order_number"
FROM
    "memory"."catalog_returns" as "cr_catalog_returns"
GROUP BY
    1),
questionable as (
SELECT
    "cooperative"."cs_order_number" as "multi_warehouse_sales"
FROM
    "cooperative"
GROUP BY
    1
HAVING
    count("cooperative"."cs_warehouse_id") > 1
),
abundant as (
SELECT
    "questionable"."multi_warehouse_sales" as "multi_warehouse_sales"
FROM
    "questionable"),
thoughtful as (
SELECT
    "cs_catalog_sales"."CS_ORDER_NUMBER" as "cs_order_number"
FROM
    "memory"."catalog_sales" as "cs_catalog_sales"
    INNER JOIN "memory"."date_dim" as "cs_ship_date_date" on "cs_catalog_sales"."CS_SHIP_DATE_SK" = "cs_ship_date_date"."D_DATE_SK"
    INNER JOIN "memory"."call_center" as "cs_call_center_call_center" on "cs_catalog_sales"."CS_CALL_CENTER_SK" = "cs_call_center_call_center"."CC_CALL_CENTER_SK"
    INNER JOIN "memory"."customer_address" as "cs_customer_address_customer_address" on "cs_catalog_sales"."CS_SHIP_ADDR_SK" = "cs_customer_address_customer_address"."CA_ADDRESS_SK"
WHERE
    cast("cs_ship_date_date"."D_DATE" as date) BETWEEN date '2002-02-01' AND date '2002-04-02' and "cs_call_center_call_center"."CC_COUNTY" = 'Williamson County' and "cs_customer_address_customer_address"."CA_STATE" = 'GA' and "cs_catalog_sales"."CS_ORDER_NUMBER" not in (select quizzical."cr_order_number" from quizzical where quizzical."cr_order_number" is not null) and "cs_catalog_sales"."CS_ORDER_NUMBER" in (select abundant."multi_warehouse_sales" from abundant where abundant."multi_warehouse_sales" is not null)

GROUP BY
    1),
concerned as (
SELECT
    "cs_catalog_sales"."CS_EXT_SHIP_COST" as "cs_ext_ship_cost",
    "cs_catalog_sales"."CS_NET_PROFIT" as "cs_net_profit"
FROM
    "memory"."catalog_sales" as "cs_catalog_sales"
    INNER JOIN "memory"."date_dim" as "cs_ship_date_date" on "cs_catalog_sales"."CS_SHIP_DATE_SK" = "cs_ship_date_date"."D_DATE_SK"
    INNER JOIN "memory"."call_center" as "cs_call_center_call_center" on "cs_catalog_sales"."CS_CALL_CENTER_SK" = "cs_call_center_call_center"."CC_CALL_CENTER_SK"
    INNER JOIN "memory"."customer_address" as "cs_customer_address_customer_address" on "cs_catalog_sales"."CS_SHIP_ADDR_SK" = "cs_customer_address_customer_address"."CA_ADDRESS_SK"
WHERE
    cast("cs_ship_date_date"."D_DATE" as date) BETWEEN date '2002-02-01' AND date '2002-04-02' and "cs_customer_address_customer_address"."CA_STATE" = 'GA' and "cs_call_center_call_center"."CC_COUNTY" = 'Williamson County' and "cs_catalog_sales"."CS_ORDER_NUMBER" not in (select quizzical."cr_order_number" from quizzical where quizzical."cr_order_number" is not null) and "cs_catalog_sales"."CS_ORDER_NUMBER" in (select abundant."multi_warehouse_sales" from abundant where abundant."multi_warehouse_sales" is not null)

GROUP BY
    1,
    2,
    "cs_catalog_sales"."CS_ITEM_SK",
    "cs_catalog_sales"."CS_ORDER_NUMBER"),
vacuous as (
SELECT
    count("thoughtful"."cs_order_number") as "order_count"
FROM
    "thoughtful"),
young as (
SELECT
    sum("concerned"."cs_ext_ship_cost") as "total_shipping_cost",
    sum("concerned"."cs_net_profit") as "total_net_profit"
FROM
    "concerned")
SELECT
    coalesce("vacuous"."order_count",0) as "order_count",
    "young"."total_shipping_cost" as "total_shipping_cost",
    "young"."total_net_profit" as "total_net_profit"
FROM
    "vacuous"
    FULL JOIN "young" on 1=1
ORDER BY 
    coalesce("vacuous"."order_count",0) desc
LIMIT (100)
```
