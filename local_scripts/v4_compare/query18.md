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
| v4 | 6301 | 100 | 84.59 ms |
| reference | 6745 | 102 | 69.65 ms |
| v4 / ref | 0.93x | 0.98x | 1.21x |

## Preql

```
import catalog_sales as cs;

# Group dim-property values by the row grain (order_number, item.id) so they
# aren't dedup'd by their source key before averaging.
auto row_birth_year <- group(cs.bill_customer.birth_year) by cs.order_number, cs.item.id;
auto row_dep_count <- group(cs.bill_customer_demographic.dependent_count) by cs.order_number, cs.item.id;

def rollup_avg(metric) -> avg(metric::numeric(12,2))
    by rollup cs.item.text_id, cs.bill_customer.address.country, cs.bill_customer.address.state, cs.bill_customer.address.county;

where
    cs.bill_customer_demographic.gender = 'F'
    and cs.bill_customer_demographic.education_status = 'Unknown'
    and cs.bill_customer.demographics.id is not null
    and cs.bill_customer.birth_month in (1, 6, 8, 9, 12, 2)
    and cs.date.year = 1998
    and cs.bill_customer.address.state in ('MS', 'IN', 'ND', 'OK', 'NM', 'VA')
select
    cs.item.text_id,
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
    cs.item.text_id asc nulls first
limit 100
;
```

## v4 generated SQL

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
    "cs_bill_customer_customers"."C_BIRTH_YEAR" as "row_birth_year",
    "cs_bill_customer_demographic_customer_demographics"."CD_DEP_COUNT" as "row_dep_count",
    "cs_catalog_sales"."CS_COUPON_AMT" as "cs_coupon_amt",
    "cs_catalog_sales"."CS_ITEM_SK" as "cs_item_id",
    "cs_catalog_sales"."CS_LIST_PRICE" as "cs_list_price",
    "cs_catalog_sales"."CS_NET_PROFIT" as "cs_net_profit",
    "cs_catalog_sales"."CS_ORDER_NUMBER" as "cs_order_number",
    "cs_catalog_sales"."CS_QUANTITY" as "cs_quantity",
    "cs_catalog_sales"."CS_SALES_PRICE" as "cs_sales_price",
    "cs_item_items"."I_ITEM_ID" as "cs_item_text_id"
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
    "cooperative"."cs_item_id" as "cs_item_id",
    "cooperative"."cs_order_number" as "cs_order_number"
FROM
    "cooperative"
GROUP BY
    1,
    2,
    "cooperative"."cs_bill_customer_demographic_dependent_count"),
questionable as (
SELECT
    "cooperative"."cs_item_id" as "cs_item_id",
    "cooperative"."cs_order_number" as "cs_order_number"
FROM
    "cooperative"
GROUP BY
    1,
    2,
    "cooperative"."cs_bill_customer_birth_year"),
juicy as (
SELECT
    "yummy"."cs_bill_customer_address_country" as "cs_bill_customer_address_country",
    "yummy"."cs_bill_customer_address_county" as "cs_bill_customer_address_county",
    "yummy"."cs_bill_customer_address_state" as "cs_bill_customer_address_state",
    "yummy"."cs_coupon_amt" as "cs_coupon_amt",
    "yummy"."cs_item_text_id" as "cs_item_text_id",
    "yummy"."cs_list_price" as "cs_list_price",
    "yummy"."cs_net_profit" as "cs_net_profit",
    "yummy"."cs_quantity" as "cs_quantity",
    "yummy"."cs_sales_price" as "cs_sales_price",
    "yummy"."row_birth_year" as "row_birth_year",
    "yummy"."row_dep_count" as "row_dep_count"
FROM
    "yummy"
    LEFT OUTER JOIN "abundant" on "yummy"."cs_item_id" = "abundant"."cs_item_id" AND "yummy"."cs_order_number" = "abundant"."cs_order_number"
    LEFT OUTER JOIN "questionable" on "yummy"."cs_item_id" = "questionable"."cs_item_id" AND "yummy"."cs_order_number" = "questionable"."cs_order_number")
SELECT
    "juicy"."cs_item_text_id" as "cs_item_text_id",
    "juicy"."cs_bill_customer_address_country" as "cs_bill_customer_address_country",
    "juicy"."cs_bill_customer_address_state" as "cs_bill_customer_address_state",
    "juicy"."cs_bill_customer_address_county" as "cs_bill_customer_address_county",
    avg(cast("juicy"."cs_quantity" as numeric(12,2))) as "agg1",
    avg(cast("juicy"."cs_list_price" as numeric(12,2))) as "agg2",
    avg(cast("juicy"."cs_coupon_amt" as numeric(12,2))) as "agg3",
    avg(cast("juicy"."cs_sales_price" as numeric(12,2))) as "agg4",
    avg(cast("juicy"."cs_net_profit" as numeric(12,2))) as "agg5",
    avg(cast("juicy"."row_birth_year" as numeric(12,2))) as "agg6",
    avg(cast("juicy"."row_dep_count" as numeric(12,2))) as "agg7"
FROM
    "juicy"
GROUP BY
    ROLLUP (1, 2, 3, 4)
ORDER BY 
    "juicy"."cs_bill_customer_address_country" asc nulls first,
    "juicy"."cs_bill_customer_address_state" asc nulls first,
    "juicy"."cs_bill_customer_address_county" asc nulls first,
    "juicy"."cs_item_text_id" asc nulls first
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
uneven as (
SELECT
    "cs_bill_customer_address_customer_address"."CA_COUNTRY" as "cs_bill_customer_address_country",
    "cs_bill_customer_address_customer_address"."CA_COUNTY" as "cs_bill_customer_address_county",
    "cs_bill_customer_address_customer_address"."CA_STATE" as "cs_bill_customer_address_state",
    "cs_catalog_sales"."CS_COUPON_AMT" as "cs_coupon_amt",
    "cs_catalog_sales"."CS_ITEM_SK" as "cs_item_id",
    "cs_catalog_sales"."CS_LIST_PRICE" as "cs_list_price",
    "cs_catalog_sales"."CS_NET_PROFIT" as "cs_net_profit",
    "cs_catalog_sales"."CS_ORDER_NUMBER" as "cs_order_number",
    "cs_catalog_sales"."CS_QUANTITY" as "cs_quantity",
    "cs_catalog_sales"."CS_SALES_PRICE" as "cs_sales_price",
    "cs_item_items"."I_ITEM_ID" as "cs_item_text_id"
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
questionable as (
SELECT
    "cooperative"."cs_bill_customer_birth_year" as "row_birth_year",
    "cooperative"."cs_bill_customer_demographic_dependent_count" as "row_dep_count",
    "cooperative"."cs_item_id" as "cs_item_id",
    "cooperative"."cs_order_number" as "cs_order_number"
FROM
    "cooperative"),
yummy as (
SELECT
    "questionable"."row_dep_count" as "row_dep_count",
    "uneven"."cs_bill_customer_address_country" as "cs_bill_customer_address_country",
    "uneven"."cs_bill_customer_address_county" as "cs_bill_customer_address_county",
    "uneven"."cs_bill_customer_address_state" as "cs_bill_customer_address_state",
    "uneven"."cs_coupon_amt" as "cs_coupon_amt",
    "uneven"."cs_item_id" as "cs_item_id",
    "uneven"."cs_item_text_id" as "cs_item_text_id",
    "uneven"."cs_list_price" as "cs_list_price",
    "uneven"."cs_net_profit" as "cs_net_profit",
    "uneven"."cs_order_number" as "cs_order_number",
    "uneven"."cs_quantity" as "cs_quantity",
    "uneven"."cs_sales_price" as "cs_sales_price"
FROM
    "uneven"
    LEFT OUTER JOIN "questionable" on "uneven"."cs_item_id" = "questionable"."cs_item_id" AND "uneven"."cs_order_number" = "questionable"."cs_order_number"),
juicy as (
SELECT
    "questionable"."row_birth_year" as "row_birth_year",
    "yummy"."cs_bill_customer_address_country" as "cs_bill_customer_address_country",
    "yummy"."cs_bill_customer_address_county" as "cs_bill_customer_address_county",
    "yummy"."cs_bill_customer_address_state" as "cs_bill_customer_address_state",
    "yummy"."cs_coupon_amt" as "cs_coupon_amt",
    "yummy"."cs_item_text_id" as "cs_item_text_id",
    "yummy"."cs_list_price" as "cs_list_price",
    "yummy"."cs_net_profit" as "cs_net_profit",
    "yummy"."cs_quantity" as "cs_quantity",
    "yummy"."cs_sales_price" as "cs_sales_price",
    "yummy"."row_dep_count" as "row_dep_count"
FROM
    "yummy"
    LEFT OUTER JOIN "questionable" on "yummy"."cs_item_id" = "questionable"."cs_item_id" AND "yummy"."cs_order_number" = "questionable"."cs_order_number")
SELECT
    "juicy"."cs_item_text_id" as "cs_item_text_id",
    "juicy"."cs_bill_customer_address_country" as "cs_bill_customer_address_country",
    "juicy"."cs_bill_customer_address_state" as "cs_bill_customer_address_state",
    "juicy"."cs_bill_customer_address_county" as "cs_bill_customer_address_county",
    avg(cast("juicy"."cs_quantity" as numeric(12,2))) as "agg1",
    avg(cast("juicy"."cs_list_price" as numeric(12,2))) as "agg2",
    avg(cast("juicy"."cs_coupon_amt" as numeric(12,2))) as "agg3",
    avg(cast("juicy"."cs_sales_price" as numeric(12,2))) as "agg4",
    avg(cast("juicy"."cs_net_profit" as numeric(12,2))) as "agg5",
    avg(cast("juicy"."row_birth_year" as numeric(12,2))) as "agg6",
    avg(cast("juicy"."row_dep_count" as numeric(12,2))) as "agg7"
FROM
    "juicy"
GROUP BY
    ROLLUP (1, 2, 3, 4)
ORDER BY 
    "juicy"."cs_bill_customer_address_country" asc nulls first,
    "juicy"."cs_bill_customer_address_state" asc nulls first,
    "juicy"."cs_bill_customer_address_county" asc nulls first,
    "juicy"."cs_item_text_id" asc nulls first
LIMIT (100)
```
