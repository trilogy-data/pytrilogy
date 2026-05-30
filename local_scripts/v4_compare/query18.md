# Query 18

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
| v4 | 5820 | 106 | 119.43 ms |
| reference | 7284 | 111 | 90.22 ms |
| v4 / ref | 0.80x | 0.95x | 1.32x |

## Preql

```
import catalog_sales as cs;

# Group dim-property values by the row grain (order_number, item.id) so they
# aren't dedup'd by their source key before averaging.
auto row_birth_year <- group(cs.bill_customer.birth_year) by cs.order_number, cs.item.id;
auto row_dep_count <- group(cs.bill_customer_demographic.dependent_count) by cs.order_number, cs.item.id;

def rollup_avg(metric) -> avg(metric::numeric(12,2))
    by rollup cs.item.name, cs.bill_customer.address.country, cs.bill_customer.address.state, cs.bill_customer.address.county;

where
    cs.bill_customer_demographic.gender = 'F'
    and cs.bill_customer_demographic.education_status = 'Unknown'
    and cs.bill_customer.demographics.id is not null
    and cs.bill_customer.birth_month in (1, 6, 8, 9, 12, 2)
    and cs.date.year = 1998
    and cs.bill_customer.address.state in ('MS', 'IN', 'ND', 'OK', 'NM', 'VA')
select
    cs.item.name,
    cs.bill_customer.address.country,
    cs.bill_customer.address.state,
    cs.bill_customer.address.county,
    @rollup_avg(cs.quantity) as agg1,
    @rollup_avg(cs.list_price) as agg2,
    @rollup_avg(cs.coupon_amt) as agg3,
    @rollup_avg(cs.sales_price) as agg4,
    @rollup_avg(cs.net_profit) as agg5,
    @rollup_avg(row_birth_year) as agg6,
    @rollup_avg(row_dep_count) as agg7,
order by
    cs.bill_customer.address.country asc nulls first,
    cs.bill_customer.address.state asc nulls first,
    cs.bill_customer.address.county asc nulls first,
    cs.item.name asc nulls first
limit 100
;
```

## v4 generated SQL

```sql
WITH 
questionable as (
SELECT
    "cs_bill_customer_address_customer_address"."CA_COUNTRY" as "cs_bill_customer_address_country",
    "cs_bill_customer_address_customer_address"."CA_COUNTY" as "cs_bill_customer_address_county",
    "cs_bill_customer_address_customer_address"."CA_STATE" as "cs_bill_customer_address_state",
    "cs_bill_customer_customers"."C_BIRTH_YEAR" as "cs_bill_customer_birth_year",
    "cs_bill_customer_demographic_customer_demographics"."CD_DEP_COUNT" as "cs_bill_customer_demographic_dependent_count",
    "cs_catalog_sales"."CS_COUPON_AMT" as "cs_coupon_amt",
    "cs_catalog_sales"."CS_ITEM_SK" as "cs_item_id",
    "cs_catalog_sales"."CS_LIST_PRICE" as "cs_list_price",
    "cs_catalog_sales"."CS_NET_PROFIT" as "cs_net_profit",
    "cs_catalog_sales"."CS_ORDER_NUMBER" as "cs_order_number",
    "cs_catalog_sales"."CS_QUANTITY" as "cs_quantity",
    "cs_catalog_sales"."CS_SALES_PRICE" as "cs_sales_price",
    "cs_item_items"."I_ITEM_ID" as "cs_item_name"
FROM
    "memory"."catalog_sales" as "cs_catalog_sales"
    INNER JOIN "memory"."date_dim" as "cs_date_date" on "cs_catalog_sales"."CS_SOLD_DATE_SK" = "cs_date_date"."D_DATE_SK"
    INNER JOIN "memory"."item" as "cs_item_items" on "cs_catalog_sales"."CS_ITEM_SK" = "cs_item_items"."I_ITEM_SK"
    INNER JOIN "memory"."customer" as "cs_bill_customer_customers" on "cs_catalog_sales"."CS_BILL_CUSTOMER_SK" = "cs_bill_customer_customers"."C_CUSTOMER_SK"
    INNER JOIN "memory"."customer_address" as "cs_bill_customer_address_customer_address" on "cs_bill_customer_customers"."C_CURRENT_ADDR_SK" = "cs_bill_customer_address_customer_address"."CA_ADDRESS_SK"
    INNER JOIN "memory"."customer_demographics" as "cs_bill_customer_demographic_customer_demographics" on "cs_catalog_sales"."CS_BILL_CDEMO_SK" = "cs_bill_customer_demographic_customer_demographics"."CD_DEMO_SK"
WHERE
    "cs_bill_customer_demographic_customer_demographics"."CD_GENDER" = 'F' and "cs_bill_customer_demographic_customer_demographics"."CD_EDUCATION_STATUS" = 'Unknown' and "cs_bill_customer_customers"."C_CURRENT_CDEMO_SK" is not null and "cs_bill_customer_customers"."C_BIRTH_MONTH" in (1,6,8,9,12,2) and "cs_date_date"."D_YEAR" = 1998 and "cs_bill_customer_address_customer_address"."CA_STATE" in ('MS','IN','ND','OK','NM','VA')
),
uneven as (
SELECT
    "questionable"."cs_bill_customer_demographic_dependent_count" as "cs_bill_customer_demographic_dependent_count",
    "questionable"."cs_bill_customer_demographic_dependent_count" as "row_dep_count",
    "questionable"."cs_item_id" as "cs_item_id",
    "questionable"."cs_order_number" as "cs_order_number"
FROM
    "questionable"
GROUP BY
    1,
    3,
    4,
    "questionable"."cs_coupon_amt",
    "questionable"."cs_item_name",
    "questionable"."cs_list_price",
    "questionable"."cs_net_profit",
    "questionable"."cs_quantity",
    "questionable"."cs_sales_price"),
abundant as (
SELECT
    "questionable"."cs_bill_customer_birth_year" as "row_birth_year",
    "questionable"."cs_coupon_amt" as "cs_coupon_amt",
    "questionable"."cs_item_id" as "cs_item_id",
    "questionable"."cs_item_name" as "cs_item_name",
    "questionable"."cs_list_price" as "cs_list_price",
    "questionable"."cs_net_profit" as "cs_net_profit",
    "questionable"."cs_order_number" as "cs_order_number",
    "questionable"."cs_quantity" as "cs_quantity",
    "questionable"."cs_sales_price" as "cs_sales_price"
FROM
    "questionable"
GROUP BY
    1,
    2,
    3,
    4,
    5,
    6,
    7,
    8,
    9),
yummy as (
SELECT
    "abundant"."cs_coupon_amt" as "cs_coupon_amt",
    "abundant"."cs_item_name" as "cs_item_name",
    "abundant"."cs_list_price" as "cs_list_price",
    "abundant"."cs_net_profit" as "cs_net_profit",
    "abundant"."cs_quantity" as "cs_quantity",
    "abundant"."cs_sales_price" as "cs_sales_price",
    "abundant"."row_birth_year" as "row_birth_year",
    "questionable"."cs_bill_customer_address_country" as "cs_bill_customer_address_country",
    "questionable"."cs_bill_customer_address_county" as "cs_bill_customer_address_county",
    "questionable"."cs_bill_customer_address_state" as "cs_bill_customer_address_state",
    "uneven"."row_dep_count" as "row_dep_count"
FROM
    "abundant"
    FULL JOIN "questionable" on "abundant"."cs_item_id" = "questionable"."cs_item_id" AND "abundant"."cs_order_number" = "questionable"."cs_order_number"
    FULL JOIN "uneven" on "questionable"."cs_bill_customer_demographic_dependent_count" is not distinct from "uneven"."cs_bill_customer_demographic_dependent_count" AND coalesce("questionable"."cs_item_id", "abundant"."cs_item_id") = "uneven"."cs_item_id" AND coalesce("questionable"."cs_order_number", "abundant"."cs_order_number") = "uneven"."cs_order_number")
SELECT
    "yummy"."cs_bill_customer_address_country" as "cs_bill_customer_address_country",
    "yummy"."cs_bill_customer_address_county" as "cs_bill_customer_address_county",
    "yummy"."cs_bill_customer_address_state" as "cs_bill_customer_address_state",
    "yummy"."cs_item_name" as "cs_item_name",
    avg(cast("yummy"."cs_quantity" as numeric(12,2))) as "agg1",
    avg(cast("yummy"."cs_list_price" as numeric(12,2))) as "agg2",
    avg(cast("yummy"."cs_coupon_amt" as numeric(12,2))) as "agg3",
    avg(cast("yummy"."cs_sales_price" as numeric(12,2))) as "agg4",
    avg(cast("yummy"."cs_net_profit" as numeric(12,2))) as "agg5",
    avg(cast("yummy"."row_birth_year" as numeric(12,2))) as "agg6",
    avg(cast("yummy"."row_dep_count" as numeric(12,2))) as "agg7"
FROM
    "yummy"
GROUP BY
    ROLLUP (4, 1, 3, 2)
ORDER BY 
    "yummy"."cs_bill_customer_address_country" asc nulls first,
    "yummy"."cs_bill_customer_address_state" asc nulls first,
    "yummy"."cs_bill_customer_address_county" asc nulls first,
    "yummy"."cs_item_name" asc nulls first
LIMIT (100)
```

## Reference SQL (zquery log)

```sql
WITH 
cooperative as (
SELECT
    "cs_bill_customer_customers"."C_BIRTH_YEAR" as "cs_bill_customer_birth_year",
    "cs_bill_customer_demographic_customer_demographics"."CD_DEP_COUNT" as "cs_bill_customer_demographic_dependent_count",
    "cs_catalog_sales"."CS_ITEM_SK" as "cs_item_id",
    "cs_catalog_sales"."CS_ORDER_NUMBER" as "cs_order_number"
FROM
    "memory"."catalog_sales" as "cs_catalog_sales"
    INNER JOIN "memory"."date_dim" as "cs_date_date" on "cs_catalog_sales"."CS_SOLD_DATE_SK" = "cs_date_date"."D_DATE_SK"
    INNER JOIN "memory"."customer" as "cs_bill_customer_customers" on "cs_catalog_sales"."CS_BILL_CUSTOMER_SK" = "cs_bill_customer_customers"."C_CUSTOMER_SK"
    INNER JOIN "memory"."customer_address" as "cs_bill_customer_address_customer_address" on "cs_bill_customer_customers"."C_CURRENT_ADDR_SK" = "cs_bill_customer_address_customer_address"."CA_ADDRESS_SK"
    INNER JOIN "memory"."customer_demographics" as "cs_bill_customer_demographic_customer_demographics" on "cs_catalog_sales"."CS_BILL_CDEMO_SK" = "cs_bill_customer_demographic_customer_demographics"."CD_DEMO_SK"
WHERE
    "cs_bill_customer_demographic_customer_demographics"."CD_GENDER" = 'F' and "cs_bill_customer_demographic_customer_demographics"."CD_EDUCATION_STATUS" = 'Unknown' and "cs_bill_customer_customers"."C_CURRENT_CDEMO_SK" is not null and "cs_bill_customer_customers"."C_BIRTH_MONTH" in (1,6,8,9,12,2) and "cs_date_date"."D_YEAR" = 1998 and "cs_bill_customer_address_customer_address"."CA_STATE" in ('MS','IN','ND','OK','NM','VA')
),
yummy as (
SELECT
    "cs_bill_customer_address_customer_address"."CA_COUNTRY" as "cs_bill_customer_address_country",
    "cs_bill_customer_address_customer_address"."CA_COUNTY" as "cs_bill_customer_address_county",
    "cs_bill_customer_address_customer_address"."CA_STATE" as "cs_bill_customer_address_state",
    "cs_bill_customer_customers"."C_BIRTH_YEAR" as "cs_bill_customer_birth_year",
    "cs_catalog_sales"."CS_COUPON_AMT" as "cs_coupon_amt",
    "cs_catalog_sales"."CS_ITEM_SK" as "cs_item_id",
    "cs_catalog_sales"."CS_LIST_PRICE" as "cs_list_price",
    "cs_catalog_sales"."CS_NET_PROFIT" as "cs_net_profit",
    "cs_catalog_sales"."CS_ORDER_NUMBER" as "cs_order_number",
    "cs_catalog_sales"."CS_QUANTITY" as "cs_quantity",
    "cs_catalog_sales"."CS_SALES_PRICE" as "cs_sales_price",
    "cs_item_items"."I_ITEM_ID" as "cs_item_name"
FROM
    "memory"."catalog_sales" as "cs_catalog_sales"
    INNER JOIN "memory"."date_dim" as "cs_date_date" on "cs_catalog_sales"."CS_SOLD_DATE_SK" = "cs_date_date"."D_DATE_SK"
    INNER JOIN "memory"."item" as "cs_item_items" on "cs_catalog_sales"."CS_ITEM_SK" = "cs_item_items"."I_ITEM_SK"
    INNER JOIN "memory"."customer" as "cs_bill_customer_customers" on "cs_catalog_sales"."CS_BILL_CUSTOMER_SK" = "cs_bill_customer_customers"."C_CUSTOMER_SK"
    INNER JOIN "memory"."customer_address" as "cs_bill_customer_address_customer_address" on "cs_bill_customer_customers"."C_CURRENT_ADDR_SK" = "cs_bill_customer_address_customer_address"."CA_ADDRESS_SK"
    INNER JOIN "memory"."customer_demographics" as "cs_bill_customer_demographic_customer_demographics" on "cs_catalog_sales"."CS_BILL_CDEMO_SK" = "cs_bill_customer_demographic_customer_demographics"."CD_DEMO_SK"
WHERE
    "cs_bill_customer_demographic_customer_demographics"."CD_GENDER" = 'F' and "cs_bill_customer_demographic_customer_demographics"."CD_EDUCATION_STATUS" = 'Unknown' and "cs_bill_customer_customers"."C_CURRENT_CDEMO_SK" is not null and "cs_bill_customer_customers"."C_BIRTH_MONTH" in (1,6,8,9,12,2) and "cs_date_date"."D_YEAR" = 1998 and "cs_bill_customer_address_customer_address"."CA_STATE" in ('MS','IN','ND','OK','NM','VA')
),
abundant as (
SELECT
    "cooperative"."cs_bill_customer_demographic_dependent_count" as "row_dep_count",
    "cooperative"."cs_item_id" as "cs_item_id",
    "cooperative"."cs_order_number" as "cs_order_number"
FROM
    "cooperative"),
questionable as (
SELECT
    "cooperative"."cs_bill_customer_birth_year" as "cs_bill_customer_birth_year",
    "cooperative"."cs_bill_customer_birth_year" as "row_birth_year",
    "cooperative"."cs_item_id" as "cs_item_id",
    "cooperative"."cs_order_number" as "cs_order_number"
FROM
    "cooperative"),
juicy as (
SELECT
    "abundant"."row_dep_count" as "row_dep_count",
    "yummy"."cs_bill_customer_address_country" as "cs_bill_customer_address_country",
    "yummy"."cs_bill_customer_address_county" as "cs_bill_customer_address_county",
    "yummy"."cs_bill_customer_address_state" as "cs_bill_customer_address_state",
    "yummy"."cs_bill_customer_birth_year" as "cs_bill_customer_birth_year",
    "yummy"."cs_coupon_amt" as "cs_coupon_amt",
    "yummy"."cs_item_name" as "cs_item_name",
    "yummy"."cs_list_price" as "cs_list_price",
    "yummy"."cs_net_profit" as "cs_net_profit",
    "yummy"."cs_quantity" as "cs_quantity",
    "yummy"."cs_sales_price" as "cs_sales_price",
    coalesce("abundant"."cs_item_id","yummy"."cs_item_id") as "cs_item_id",
    coalesce("abundant"."cs_order_number","yummy"."cs_order_number") as "cs_order_number"
FROM
    "abundant"
    FULL JOIN "yummy" on "abundant"."cs_item_id" = "yummy"."cs_item_id" AND "abundant"."cs_order_number" = "yummy"."cs_order_number"),
vacuous as (
SELECT
    "juicy"."cs_bill_customer_address_country" as "cs_bill_customer_address_country",
    "juicy"."cs_bill_customer_address_county" as "cs_bill_customer_address_county",
    "juicy"."cs_bill_customer_address_state" as "cs_bill_customer_address_state",
    "juicy"."cs_coupon_amt" as "cs_coupon_amt",
    "juicy"."cs_item_name" as "cs_item_name",
    "juicy"."cs_list_price" as "cs_list_price",
    "juicy"."cs_net_profit" as "cs_net_profit",
    "juicy"."cs_quantity" as "cs_quantity",
    "juicy"."cs_sales_price" as "cs_sales_price",
    "juicy"."row_dep_count" as "row_dep_count",
    "questionable"."row_birth_year" as "row_birth_year"
FROM
    "juicy"
    FULL JOIN "questionable" on "juicy"."cs_bill_customer_birth_year" is not distinct from "questionable"."cs_bill_customer_birth_year" AND "juicy"."cs_item_id" = "questionable"."cs_item_id" AND "juicy"."cs_order_number" = "questionable"."cs_order_number")
SELECT
    "vacuous"."cs_item_name" as "cs_item_name",
    "vacuous"."cs_bill_customer_address_country" as "cs_bill_customer_address_country",
    "vacuous"."cs_bill_customer_address_state" as "cs_bill_customer_address_state",
    "vacuous"."cs_bill_customer_address_county" as "cs_bill_customer_address_county",
    avg(cast("vacuous"."cs_quantity" as numeric(12,2))) as "agg1",
    avg(cast("vacuous"."cs_list_price" as numeric(12,2))) as "agg2",
    avg(cast("vacuous"."cs_coupon_amt" as numeric(12,2))) as "agg3",
    avg(cast("vacuous"."cs_sales_price" as numeric(12,2))) as "agg4",
    avg(cast("vacuous"."cs_net_profit" as numeric(12,2))) as "agg5",
    avg(cast("vacuous"."row_birth_year" as numeric(12,2))) as "agg6",
    avg(cast("vacuous"."row_dep_count" as numeric(12,2))) as "agg7"
FROM
    "vacuous"
GROUP BY
    ROLLUP (1, 2, 3, 4)
ORDER BY 
    "vacuous"."cs_bill_customer_address_country" asc nulls first,
    "vacuous"."cs_bill_customer_address_state" asc nulls first,
    "vacuous"."cs_bill_customer_address_county" asc nulls first,
    "vacuous"."cs_item_name" asc nulls first
LIMIT (100)
```
