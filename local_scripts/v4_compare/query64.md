# Query 64

**Status:** `match`

| Stage | Result |
| --- | --- |
| v4 SQL generation | OK |
| v4 execution | OK (2 rows) |
| reference execution | OK (2 rows) |
| results identical | YES |

## Result comparison

v4 rows: 2 (2 distinct)
ref rows: 2 (2 distinct)

## SQL size + execution time

| Source | Chars | Lines | Exec (min of 4) |
| --- | --- | --- | --- |
| v4 | 31311 | 486 | 330.76 ms |
| reference | 16331 | 244 | 301.64 ms |
| v4 / ref | 1.92x | 1.99x | 1.10x |

## Preql

```
import physical_sales as ss;
import catalog_returns as cr;

# cs_ui: items where catalog sale (sum of cs_ext_list_price) exceeds twice
# the catalog refund (sum of refunded_cash + reversed_charge + store_credit)
# for items that have a matching catalog_return. catalog_sales is reached
# through cr.sales (cr already imports catalog_sales as sales) so we avoid
# the cross-import merge dance.
auto cs_ui_refund_amt <- coalesce(cr.refunded_cash, 0) + coalesce(cr.reversed_charge, 0) + coalesce(cr.store_credit, 0);
auto cs_ui_sale <- sum(cr.sales.ext_list_price) by cr.sales.item.id;
auto cs_ui_refund <- sum(cs_ui_refund_amt) by cr.sales.item.id;

rowset cs_ui <- where
    cs_ui_sale > 2 * cs_ui_refund
select
    cr.sales.item.id as cs_ui_item_id,
;

# Row-level filter common to both year slices. Excludes the cross-CDEMO
# marital_status inequality â€” deferring that to the outer aggregate keeps
# it out of the customer_demographics join planning, which otherwise
# triggers a LEFT->INNER upgrade that DuckDB plans catastrophically (q64
# went from 0.05s -> 18s in PR #549's null-safe promotion).
def year_row_filter(yr) -> ss.item.id in cs_ui.cs_ui_item_id
and ss.date.year = yr
and ss.is_returned
and ss.item.color in ('purple', 'burlywood', 'indian', 'spring', 'floral', 'medium')
and ss.item.current_price between 65 and 74
and ss.billing_customer.id is not null
and ss.store.id is not null
and ss.sale_address.id is not null
and ss.billing_customer.address.id is not null;

# Row-grain projection for each year. Both marital_status columns are
# selected here so the outer aggregate can apply the inequality.
rowset ss_rows_99 <- where
    @year_row_filter(1999) = True
select
    ss.ticket_number,
    ss.item.id,
    ss.item.product_name,
    ss.store.name,
    ss.store.zip,
    ss.sale_address.street_number,
    ss.sale_address.street_name,
    ss.sale_address.city,
    ss.sale_address.zip,
    ss.billing_customer.address.street_number,
    ss.billing_customer.address.street_name,
    ss.billing_customer.address.city,
    ss.billing_customer.address.zip,
    ss.date.year,
    ss.billing_customer.first_sales_date.year,
    ss.billing_customer.first_shipto_date.year,
    ss.wholesale_cost,
    ss.list_price,
    ss.coupon_amt,
    ss.customer_demographic.marital_status,
    ss.billing_customer.demographics.marital_status,
;

rowset ss_rows_00 <- where
    @year_row_filter(2000) = True
select
    ss.ticket_number,
    ss.item.id,
    ss.store.name,
    ss.store.zip,
    ss.date.year,
    ss.wholesale_cost,
    ss.list_price,
    ss.coupon_amt,
    ss.customer_demographic.marital_status,
    ss.billing_customer.demographics.marital_status,
;

rowset q64_results <- where
    ss_rows_99.ss.customer_demographic.marital_status != ss_rows_99.ss.billing_customer.demographics.marital_status
select
    ss_rows_99.ss.item.id as item_sk_99,
    ss_rows_99.ss.store.name as s_name_99,
    ss_rows_99.ss.store.zip as s_zip_99,
    ss_rows_99.ss.item.product_name as p_name_99,
    ss_rows_99.ss.sale_address.street_number as b_sn_99,
    ss_rows_99.ss.sale_address.street_name as b_str_99,
    ss_rows_99.ss.sale_address.city as b_city_99,
    ss_rows_99.ss.sale_address.zip as b_zip_99,
    ss_rows_99.ss.billing_customer.address.street_number as c_sn_99,
    ss_rows_99.ss.billing_customer.address.street_name as c_str_99,
    ss_rows_99.ss.billing_customer.address.city as c_city_99,
    ss_rows_99.ss.billing_customer.address.zip as c_zip_99,
    ss_rows_99.ss.date.year as syear_99,
    ss_rows_99.ss.billing_customer.first_sales_date.year as fsyear_99,
    ss_rows_99.ss.billing_customer.first_shipto_date.year as s2year_99,
    count(ss_rows_99.ss.ticket_number) as cnt_99,
    sum(ss_rows_99.ss.wholesale_cost) as s1_99,
    sum(ss_rows_99.ss.list_price) as s2_99,
    sum(ss_rows_99.ss.coupon_amt) as s3_99,
merge
where
    ss_rows_00.ss.customer_demographic.marital_status != ss_rows_00.ss.billing_customer.demographics.marital_status
select
    ss_rows_00.ss.item.id as item_sk_00,
    ss_rows_00.ss.store.name as s_name_00,
    ss_rows_00.ss.store.zip as s_zip_00,
    ss_rows_00.ss.date.year as syear_00,
    count(ss_rows_00.ss.ticket_number) as cnt_00,
    sum(ss_rows_00.ss.wholesale_cost) as s1_00,
    sum(ss_rows_00.ss.list_price) as s2_00,
    sum(ss_rows_00.ss.coupon_amt) as s3_00,
align
    item_sk: item_sk_99, item_sk_00
    and s_name: s_name_99, s_name_00
    and s_zip: s_zip_99, s_zip_00
;

where
    q64_results.cnt_00 <= q64_results.cnt_99
select
    q64_results.p_name_99,
    q64_results.s_name,
    q64_results.s_zip,
    q64_results.b_sn_99,
    q64_results.b_str_99,
    q64_results.b_city_99,
    q64_results.b_zip_99,
    q64_results.c_sn_99,
    q64_results.c_str_99,
    q64_results.c_city_99,
    q64_results.c_zip_99,
    q64_results.syear_99,
    q64_results.cnt_99,
    q64_results.s1_99,
    q64_results.s2_99,
    q64_results.s3_99,
    q64_results.s1_00,
    q64_results.s2_00,
    q64_results.s3_00,
    q64_results.syear_00,
    q64_results.cnt_00,
order by
    q64_results.p_name_99 asc nulls first,
    q64_results.s_name asc nulls first,
    q64_results.cnt_00 asc nulls first,
    q64_results.s1_99 asc nulls first,
    q64_results.s1_00 asc nulls first
;
```

## v4 generated SQL

```sql
WITH 
wakeful as (
SELECT
    "cr_sales_catalog_sales"."CS_ITEM_SK" as "cr_sales_item_id"
FROM
    "memory"."catalog_sales" as "cr_sales_catalog_sales"
    INNER JOIN "memory"."catalog_returns" as "cr_catalog_returns" on "cr_sales_catalog_sales"."CS_ORDER_NUMBER" = "cr_catalog_returns"."CR_ORDER_NUMBER"
GROUP BY
    1
HAVING
    sum("cr_sales_catalog_sales"."CS_EXT_LIST_PRICE") > 2 * sum(( coalesce("cr_catalog_returns"."CR_REFUNDED_CASH",0) + coalesce("cr_catalog_returns"."CR_REVERSED_CHARGE",0) ) + coalesce("cr_catalog_returns"."CR_STORE_CREDIT",0))
),
cooperative as (
SELECT
    "wakeful"."cr_sales_item_id" as "_cs_ui_cs_ui_item_id"
FROM
    "wakeful"),
questionable as (
SELECT
    "cooperative"."_cs_ui_cs_ui_item_id" as "cs_ui_cs_ui_item_id"
FROM
    "cooperative"),
waggish as (
SELECT
    "ss_billing_customer_demographics_customer_demographics"."CD_MARITAL_STATUS" as "ss_billing_customer_demographics_marital_status",
    "ss_customer_demographic_customer_demographics"."CD_MARITAL_STATUS" as "ss_customer_demographic_marital_status",
    "ss_date_date"."D_YEAR" as "ss_date_year",
    "ss_item_items"."I_ITEM_SK" as "ss_item_id",
    "ss_store_sales"."SS_COUPON_AMT" as "ss_coupon_amt",
    "ss_store_sales"."SS_LIST_PRICE" as "ss_list_price",
    "ss_store_sales"."SS_TICKET_NUMBER" as "ss_ticket_number",
    "ss_store_sales"."SS_WHOLESALE_COST" as "ss_wholesale_cost",
    "ss_store_store"."S_STORE_NAME" as "ss_store_name",
    "ss_store_store"."S_ZIP" as "ss_store_zip"
FROM
    "memory"."store_sales" as "ss_store_sales"
    INNER JOIN "memory"."date_dim" as "ss_date_date" on "ss_store_sales"."SS_SOLD_DATE_SK" = "ss_date_date"."D_DATE_SK"
    INNER JOIN "memory"."store" as "ss_store_store" on "ss_store_sales"."SS_STORE_SK" = "ss_store_store"."S_STORE_SK"
    INNER JOIN "memory"."store_returns" as "ss_store_returns" on "ss_store_sales"."SS_ITEM_SK" = "ss_store_returns"."SR_ITEM_SK" AND "ss_store_sales"."SS_TICKET_NUMBER" = "ss_store_returns"."SR_TICKET_NUMBER"
    INNER JOIN "memory"."customer" as "ss_billing_customer_customers" on "ss_store_sales"."SS_CUSTOMER_SK" = "ss_billing_customer_customers"."C_CUSTOMER_SK"
    LEFT OUTER JOIN "memory"."customer_demographics" as "ss_customer_demographic_customer_demographics" on "ss_store_sales"."SS_CDEMO_SK" = "ss_customer_demographic_customer_demographics"."CD_DEMO_SK"
    LEFT OUTER JOIN "memory"."customer_demographics" as "ss_billing_customer_demographics_customer_demographics" on "ss_billing_customer_customers"."C_CURRENT_CDEMO_SK" = "ss_billing_customer_demographics_customer_demographics"."CD_DEMO_SK"
    INNER JOIN "memory"."item" as "ss_item_items" on "ss_store_sales"."SS_ITEM_SK" = "ss_item_items"."I_ITEM_SK"
WHERE
    "ss_date_date"."D_YEAR" = 2000 and SR_RETURN_TIME_SK IS NOT NULL and "ss_item_items"."I_COLOR" in ('purple','burlywood','indian','spring','floral','medium') and "ss_item_items"."I_CURRENT_PRICE" BETWEEN 65 AND 74 and "ss_store_sales"."SS_CUSTOMER_SK" is not null and "ss_store_sales"."SS_STORE_SK" is not null and "ss_store_sales"."SS_ADDR_SK" is not null and "ss_billing_customer_customers"."C_CURRENT_ADDR_SK" is not null and "ss_item_items"."I_ITEM_SK" in (select questionable."cs_ui_cs_ui_item_id" from questionable where questionable."cs_ui_cs_ui_item_id" is not null)
),
scrawny as (
SELECT
    "ss_billing_customer_address_customer_address"."CA_CITY" as "ss_billing_customer_address_city",
    "ss_billing_customer_address_customer_address"."CA_STREET_NAME" as "ss_billing_customer_address_street_name",
    "ss_billing_customer_address_customer_address"."CA_STREET_NUMBER" as "ss_billing_customer_address_street_number",
    "ss_billing_customer_address_customer_address"."CA_ZIP" as "ss_billing_customer_address_zip",
    "ss_billing_customer_demographics_customer_demographics"."CD_MARITAL_STATUS" as "ss_billing_customer_demographics_marital_status",
    "ss_billing_customer_first_sales_date_date"."D_YEAR" as "ss_billing_customer_first_sales_date_year",
    "ss_billing_customer_first_shipto_date_date"."D_YEAR" as "ss_billing_customer_first_shipto_date_year",
    "ss_customer_demographic_customer_demographics"."CD_MARITAL_STATUS" as "ss_customer_demographic_marital_status",
    "ss_date_date"."D_YEAR" as "ss_date_year",
    "ss_item_items"."I_ITEM_SK" as "ss_item_id",
    "ss_item_items"."I_PRODUCT_NAME" as "ss_item_product_name",
    "ss_sale_address_customer_address"."CA_CITY" as "ss_sale_address_city",
    "ss_sale_address_customer_address"."CA_STREET_NAME" as "ss_sale_address_street_name",
    "ss_sale_address_customer_address"."CA_STREET_NUMBER" as "ss_sale_address_street_number",
    "ss_sale_address_customer_address"."CA_ZIP" as "ss_sale_address_zip",
    "ss_store_sales"."SS_COUPON_AMT" as "ss_coupon_amt",
    "ss_store_sales"."SS_LIST_PRICE" as "ss_list_price",
    "ss_store_sales"."SS_TICKET_NUMBER" as "ss_ticket_number",
    "ss_store_sales"."SS_WHOLESALE_COST" as "ss_wholesale_cost",
    "ss_store_store"."S_STORE_NAME" as "ss_store_name",
    "ss_store_store"."S_ZIP" as "ss_store_zip"
FROM
    "memory"."store_sales" as "ss_store_sales"
    INNER JOIN "memory"."date_dim" as "ss_date_date" on "ss_store_sales"."SS_SOLD_DATE_SK" = "ss_date_date"."D_DATE_SK"
    INNER JOIN "memory"."store" as "ss_store_store" on "ss_store_sales"."SS_STORE_SK" = "ss_store_store"."S_STORE_SK"
    INNER JOIN "memory"."store_returns" as "ss_store_returns" on "ss_store_sales"."SS_ITEM_SK" = "ss_store_returns"."SR_ITEM_SK" AND "ss_store_sales"."SS_TICKET_NUMBER" = "ss_store_returns"."SR_TICKET_NUMBER"
    INNER JOIN "memory"."customer_address" as "ss_sale_address_customer_address" on "ss_store_sales"."SS_ADDR_SK" = "ss_sale_address_customer_address"."CA_ADDRESS_SK"
    INNER JOIN "memory"."customer" as "ss_billing_customer_customers" on "ss_store_sales"."SS_CUSTOMER_SK" = "ss_billing_customer_customers"."C_CUSTOMER_SK"
    LEFT OUTER JOIN "memory"."customer_demographics" as "ss_customer_demographic_customer_demographics" on "ss_store_sales"."SS_CDEMO_SK" = "ss_customer_demographic_customer_demographics"."CD_DEMO_SK"
    INNER JOIN "memory"."customer_address" as "ss_billing_customer_address_customer_address" on "ss_billing_customer_customers"."C_CURRENT_ADDR_SK" = "ss_billing_customer_address_customer_address"."CA_ADDRESS_SK"
    LEFT OUTER JOIN "memory"."customer_demographics" as "ss_billing_customer_demographics_customer_demographics" on "ss_billing_customer_customers"."C_CURRENT_CDEMO_SK" = "ss_billing_customer_demographics_customer_demographics"."CD_DEMO_SK"
    LEFT OUTER JOIN "memory"."date_dim" as "ss_billing_customer_first_sales_date_date" on "ss_billing_customer_customers"."C_FIRST_SALES_DATE_SK" = "ss_billing_customer_first_sales_date_date"."D_DATE_SK"
    LEFT OUTER JOIN "memory"."date_dim" as "ss_billing_customer_first_shipto_date_date" on "ss_billing_customer_customers"."C_FIRST_SHIPTO_DATE_SK" = "ss_billing_customer_first_shipto_date_date"."D_DATE_SK"
    INNER JOIN "memory"."item" as "ss_item_items" on "ss_store_sales"."SS_ITEM_SK" = "ss_item_items"."I_ITEM_SK"
WHERE
    "ss_date_date"."D_YEAR" = 1999 and SR_RETURN_TIME_SK IS NOT NULL and "ss_item_items"."I_COLOR" in ('purple','burlywood','indian','spring','floral','medium') and "ss_item_items"."I_CURRENT_PRICE" BETWEEN 65 AND 74 and "ss_store_sales"."SS_CUSTOMER_SK" is not null and "ss_store_sales"."SS_STORE_SK" is not null and "ss_store_sales"."SS_ADDR_SK" is not null and "ss_billing_customer_address_customer_address"."CA_ADDRESS_SK" is not null and "ss_item_items"."I_ITEM_SK" in (select questionable."cs_ui_cs_ui_item_id" from questionable where questionable."cs_ui_cs_ui_item_id" is not null)
),
rambunctious as (
SELECT
    "waggish"."ss_billing_customer_demographics_marital_status" as "ss_billing_customer_demographics_marital_status",
    "waggish"."ss_coupon_amt" as "ss_coupon_amt",
    "waggish"."ss_customer_demographic_marital_status" as "ss_customer_demographic_marital_status",
    "waggish"."ss_date_year" as "ss_date_year",
    "waggish"."ss_item_id" as "ss_item_id",
    "waggish"."ss_list_price" as "ss_list_price",
    "waggish"."ss_store_name" as "ss_store_name",
    "waggish"."ss_store_zip" as "ss_store_zip",
    "waggish"."ss_ticket_number" as "ss_ticket_number",
    "waggish"."ss_wholesale_cost" as "ss_wholesale_cost"
FROM
    "waggish"),
friendly as (
SELECT
    "scrawny"."ss_billing_customer_address_city" as "ss_billing_customer_address_city",
    "scrawny"."ss_billing_customer_address_street_name" as "ss_billing_customer_address_street_name",
    "scrawny"."ss_billing_customer_address_street_number" as "ss_billing_customer_address_street_number",
    "scrawny"."ss_billing_customer_address_zip" as "ss_billing_customer_address_zip",
    "scrawny"."ss_billing_customer_demographics_marital_status" as "ss_billing_customer_demographics_marital_status",
    "scrawny"."ss_billing_customer_first_sales_date_year" as "ss_billing_customer_first_sales_date_year",
    "scrawny"."ss_billing_customer_first_shipto_date_year" as "ss_billing_customer_first_shipto_date_year",
    "scrawny"."ss_coupon_amt" as "ss_coupon_amt",
    "scrawny"."ss_customer_demographic_marital_status" as "ss_customer_demographic_marital_status",
    "scrawny"."ss_date_year" as "ss_date_year",
    "scrawny"."ss_item_id" as "ss_item_id",
    "scrawny"."ss_item_product_name" as "ss_item_product_name",
    "scrawny"."ss_list_price" as "ss_list_price",
    "scrawny"."ss_sale_address_city" as "ss_sale_address_city",
    "scrawny"."ss_sale_address_street_name" as "ss_sale_address_street_name",
    "scrawny"."ss_sale_address_street_number" as "ss_sale_address_street_number",
    "scrawny"."ss_sale_address_zip" as "ss_sale_address_zip",
    "scrawny"."ss_store_name" as "ss_store_name",
    "scrawny"."ss_store_zip" as "ss_store_zip",
    "scrawny"."ss_ticket_number" as "ss_ticket_number",
    "scrawny"."ss_wholesale_cost" as "ss_wholesale_cost"
FROM
    "scrawny"),
puffy as (
SELECT
    "rambunctious"."ss_billing_customer_demographics_marital_status" as "ss_rows_00_ss_billing_customer_demographics_marital_status",
    "rambunctious"."ss_coupon_amt" as "ss_rows_00_ss_coupon_amt",
    "rambunctious"."ss_customer_demographic_marital_status" as "ss_rows_00_ss_customer_demographic_marital_status",
    "rambunctious"."ss_date_year" as "ss_rows_00_ss_date_year",
    "rambunctious"."ss_item_id" as "ss_rows_00_ss_item_id",
    "rambunctious"."ss_list_price" as "ss_rows_00_ss_list_price",
    "rambunctious"."ss_store_name" as "ss_rows_00_ss_store_name",
    "rambunctious"."ss_store_zip" as "ss_rows_00_ss_store_zip",
    "rambunctious"."ss_ticket_number" as "ss_rows_00_ss_ticket_number",
    "rambunctious"."ss_wholesale_cost" as "ss_rows_00_ss_wholesale_cost"
FROM
    "rambunctious"),
kaput as (
SELECT
    "friendly"."ss_billing_customer_address_city" as "ss_rows_99_ss_billing_customer_address_city",
    "friendly"."ss_billing_customer_address_street_name" as "ss_rows_99_ss_billing_customer_address_street_name",
    "friendly"."ss_billing_customer_address_street_number" as "ss_rows_99_ss_billing_customer_address_street_number",
    "friendly"."ss_billing_customer_address_zip" as "ss_rows_99_ss_billing_customer_address_zip",
    "friendly"."ss_billing_customer_demographics_marital_status" as "ss_rows_99_ss_billing_customer_demographics_marital_status",
    "friendly"."ss_billing_customer_first_sales_date_year" as "ss_rows_99_ss_billing_customer_first_sales_date_year",
    "friendly"."ss_billing_customer_first_shipto_date_year" as "ss_rows_99_ss_billing_customer_first_shipto_date_year",
    "friendly"."ss_coupon_amt" as "ss_rows_99_ss_coupon_amt",
    "friendly"."ss_customer_demographic_marital_status" as "ss_rows_99_ss_customer_demographic_marital_status",
    "friendly"."ss_date_year" as "ss_rows_99_ss_date_year",
    "friendly"."ss_item_id" as "ss_rows_99_ss_item_id",
    "friendly"."ss_item_product_name" as "ss_rows_99_ss_item_product_name",
    "friendly"."ss_list_price" as "ss_rows_99_ss_list_price",
    "friendly"."ss_sale_address_city" as "ss_rows_99_ss_sale_address_city",
    "friendly"."ss_sale_address_street_name" as "ss_rows_99_ss_sale_address_street_name",
    "friendly"."ss_sale_address_street_number" as "ss_rows_99_ss_sale_address_street_number",
    "friendly"."ss_sale_address_zip" as "ss_rows_99_ss_sale_address_zip",
    "friendly"."ss_store_name" as "ss_rows_99_ss_store_name",
    "friendly"."ss_store_zip" as "ss_rows_99_ss_store_zip",
    "friendly"."ss_ticket_number" as "ss_rows_99_ss_ticket_number",
    "friendly"."ss_wholesale_cost" as "ss_rows_99_ss_wholesale_cost"
FROM
    "friendly"),
hard as (
SELECT
    "puffy"."ss_rows_00_ss_coupon_amt" as "ss_rows_00_ss_coupon_amt",
    "puffy"."ss_rows_00_ss_date_year" as "ss_rows_00_ss_date_year",
    "puffy"."ss_rows_00_ss_item_id" as "ss_rows_00_ss_item_id",
    "puffy"."ss_rows_00_ss_list_price" as "ss_rows_00_ss_list_price",
    "puffy"."ss_rows_00_ss_store_name" as "ss_rows_00_ss_store_name",
    "puffy"."ss_rows_00_ss_store_zip" as "ss_rows_00_ss_store_zip",
    "puffy"."ss_rows_00_ss_ticket_number" as "ss_rows_00_ss_ticket_number",
    "puffy"."ss_rows_00_ss_wholesale_cost" as "ss_rows_00_ss_wholesale_cost"
FROM
    "puffy"
WHERE
    "puffy"."ss_rows_00_ss_customer_demographic_marital_status" != "puffy"."ss_rows_00_ss_billing_customer_demographics_marital_status"
),
divergent as (
SELECT
    "kaput"."ss_rows_99_ss_billing_customer_address_city" as "ss_rows_99_ss_billing_customer_address_city",
    "kaput"."ss_rows_99_ss_billing_customer_address_street_name" as "ss_rows_99_ss_billing_customer_address_street_name",
    "kaput"."ss_rows_99_ss_billing_customer_address_street_number" as "ss_rows_99_ss_billing_customer_address_street_number",
    "kaput"."ss_rows_99_ss_billing_customer_address_zip" as "ss_rows_99_ss_billing_customer_address_zip",
    "kaput"."ss_rows_99_ss_billing_customer_first_sales_date_year" as "ss_rows_99_ss_billing_customer_first_sales_date_year",
    "kaput"."ss_rows_99_ss_billing_customer_first_shipto_date_year" as "ss_rows_99_ss_billing_customer_first_shipto_date_year",
    "kaput"."ss_rows_99_ss_coupon_amt" as "ss_rows_99_ss_coupon_amt",
    "kaput"."ss_rows_99_ss_date_year" as "ss_rows_99_ss_date_year",
    "kaput"."ss_rows_99_ss_item_id" as "ss_rows_99_ss_item_id",
    "kaput"."ss_rows_99_ss_item_product_name" as "ss_rows_99_ss_item_product_name",
    "kaput"."ss_rows_99_ss_list_price" as "ss_rows_99_ss_list_price",
    "kaput"."ss_rows_99_ss_sale_address_city" as "ss_rows_99_ss_sale_address_city",
    "kaput"."ss_rows_99_ss_sale_address_street_name" as "ss_rows_99_ss_sale_address_street_name",
    "kaput"."ss_rows_99_ss_sale_address_street_number" as "ss_rows_99_ss_sale_address_street_number",
    "kaput"."ss_rows_99_ss_sale_address_zip" as "ss_rows_99_ss_sale_address_zip",
    "kaput"."ss_rows_99_ss_store_name" as "ss_rows_99_ss_store_name",
    "kaput"."ss_rows_99_ss_store_zip" as "ss_rows_99_ss_store_zip",
    "kaput"."ss_rows_99_ss_ticket_number" as "ss_rows_99_ss_ticket_number",
    "kaput"."ss_rows_99_ss_wholesale_cost" as "ss_rows_99_ss_wholesale_cost"
FROM
    "kaput"
WHERE
    "kaput"."ss_rows_99_ss_customer_demographic_marital_status" != "kaput"."ss_rows_99_ss_billing_customer_demographics_marital_status"
),
yellow as (
SELECT
    "hard"."ss_rows_00_ss_date_year" as "ss_rows_00_ss_date_year",
    "hard"."ss_rows_00_ss_item_id" as "ss_rows_00_ss_item_id",
    "hard"."ss_rows_00_ss_store_name" as "ss_rows_00_ss_store_name",
    "hard"."ss_rows_00_ss_store_zip" as "ss_rows_00_ss_store_zip",
    "hard"."ss_rows_00_ss_ticket_number" as "ss_rows_00_ss_ticket_number"
FROM
    "hard"
GROUP BY
    1,
    2,
    3,
    4,
    5),
sedate as (
SELECT
    "hard"."ss_rows_00_ss_date_year" as "ss_rows_00_ss_date_year",
    "hard"."ss_rows_00_ss_item_id" as "ss_rows_00_ss_item_id",
    "hard"."ss_rows_00_ss_store_name" as "ss_rows_00_ss_store_name",
    "hard"."ss_rows_00_ss_store_zip" as "ss_rows_00_ss_store_zip",
    sum("hard"."ss_rows_00_ss_coupon_amt") as "_q64_results_s3_00",
    sum("hard"."ss_rows_00_ss_list_price") as "_q64_results_s2_00",
    sum("hard"."ss_rows_00_ss_wholesale_cost") as "_q64_results_s1_00"
FROM
    "hard"
GROUP BY
    1,
    2,
    3,
    4),
protective as (
SELECT
    "divergent"."ss_rows_99_ss_billing_customer_address_city" as "ss_rows_99_ss_billing_customer_address_city",
    "divergent"."ss_rows_99_ss_billing_customer_address_street_name" as "ss_rows_99_ss_billing_customer_address_street_name",
    "divergent"."ss_rows_99_ss_billing_customer_address_street_number" as "ss_rows_99_ss_billing_customer_address_street_number",
    "divergent"."ss_rows_99_ss_billing_customer_address_zip" as "ss_rows_99_ss_billing_customer_address_zip",
    "divergent"."ss_rows_99_ss_billing_customer_first_sales_date_year" as "ss_rows_99_ss_billing_customer_first_sales_date_year",
    "divergent"."ss_rows_99_ss_billing_customer_first_shipto_date_year" as "ss_rows_99_ss_billing_customer_first_shipto_date_year",
    "divergent"."ss_rows_99_ss_date_year" as "ss_rows_99_ss_date_year",
    "divergent"."ss_rows_99_ss_item_id" as "ss_rows_99_ss_item_id",
    "divergent"."ss_rows_99_ss_sale_address_city" as "ss_rows_99_ss_sale_address_city",
    "divergent"."ss_rows_99_ss_sale_address_street_name" as "ss_rows_99_ss_sale_address_street_name",
    "divergent"."ss_rows_99_ss_sale_address_street_number" as "ss_rows_99_ss_sale_address_street_number",
    "divergent"."ss_rows_99_ss_sale_address_zip" as "ss_rows_99_ss_sale_address_zip",
    "divergent"."ss_rows_99_ss_store_name" as "ss_rows_99_ss_store_name",
    "divergent"."ss_rows_99_ss_store_zip" as "ss_rows_99_ss_store_zip",
    "divergent"."ss_rows_99_ss_ticket_number" as "ss_rows_99_ss_ticket_number"
FROM
    "divergent"
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
    13,
    14,
    15),
charming as (
SELECT
    "divergent"."ss_rows_99_ss_billing_customer_address_city" as "ss_rows_99_ss_billing_customer_address_city",
    "divergent"."ss_rows_99_ss_billing_customer_address_street_name" as "ss_rows_99_ss_billing_customer_address_street_name",
    "divergent"."ss_rows_99_ss_billing_customer_address_street_number" as "ss_rows_99_ss_billing_customer_address_street_number",
    "divergent"."ss_rows_99_ss_billing_customer_address_zip" as "ss_rows_99_ss_billing_customer_address_zip",
    "divergent"."ss_rows_99_ss_billing_customer_first_sales_date_year" as "ss_rows_99_ss_billing_customer_first_sales_date_year",
    "divergent"."ss_rows_99_ss_billing_customer_first_shipto_date_year" as "ss_rows_99_ss_billing_customer_first_shipto_date_year",
    "divergent"."ss_rows_99_ss_date_year" as "ss_rows_99_ss_date_year",
    "divergent"."ss_rows_99_ss_item_id" as "ss_rows_99_ss_item_id",
    "divergent"."ss_rows_99_ss_sale_address_city" as "ss_rows_99_ss_sale_address_city",
    "divergent"."ss_rows_99_ss_sale_address_street_name" as "ss_rows_99_ss_sale_address_street_name",
    "divergent"."ss_rows_99_ss_sale_address_street_number" as "ss_rows_99_ss_sale_address_street_number",
    "divergent"."ss_rows_99_ss_sale_address_zip" as "ss_rows_99_ss_sale_address_zip",
    "divergent"."ss_rows_99_ss_store_name" as "ss_rows_99_ss_store_name",
    "divergent"."ss_rows_99_ss_store_zip" as "ss_rows_99_ss_store_zip",
    sum("divergent"."ss_rows_99_ss_coupon_amt") as "_q64_results_s3_99",
    sum("divergent"."ss_rows_99_ss_list_price") as "_q64_results_s2_99",
    sum("divergent"."ss_rows_99_ss_wholesale_cost") as "_q64_results_s1_99"
FROM
    "divergent"
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
    13,
    14),
busy as (
SELECT
    "divergent"."ss_rows_99_ss_billing_customer_address_city" as "_q64_results_c_city_99",
    "divergent"."ss_rows_99_ss_billing_customer_address_street_name" as "_q64_results_c_str_99",
    "divergent"."ss_rows_99_ss_billing_customer_address_street_number" as "_q64_results_c_sn_99",
    "divergent"."ss_rows_99_ss_billing_customer_address_zip" as "_q64_results_c_zip_99",
    "divergent"."ss_rows_99_ss_billing_customer_first_sales_date_year" as "_q64_results_fsyear_99",
    "divergent"."ss_rows_99_ss_billing_customer_first_shipto_date_year" as "_q64_results_s2year_99",
    "divergent"."ss_rows_99_ss_date_year" as "_q64_results_syear_99",
    "divergent"."ss_rows_99_ss_item_id" as "_q64_results_item_sk_99",
    "divergent"."ss_rows_99_ss_item_product_name" as "_q64_results_p_name_99",
    "divergent"."ss_rows_99_ss_sale_address_city" as "_q64_results_b_city_99",
    "divergent"."ss_rows_99_ss_sale_address_street_name" as "_q64_results_b_str_99",
    "divergent"."ss_rows_99_ss_sale_address_street_number" as "_q64_results_b_sn_99",
    "divergent"."ss_rows_99_ss_sale_address_zip" as "_q64_results_b_zip_99",
    "divergent"."ss_rows_99_ss_store_name" as "_q64_results_s_name_99",
    "divergent"."ss_rows_99_ss_store_zip" as "_q64_results_s_zip_99"
FROM
    "divergent"),
resonant as (
SELECT
    "yellow"."ss_rows_00_ss_date_year" as "ss_rows_00_ss_date_year",
    "yellow"."ss_rows_00_ss_item_id" as "ss_rows_00_ss_item_id",
    "yellow"."ss_rows_00_ss_store_name" as "ss_rows_00_ss_store_name",
    "yellow"."ss_rows_00_ss_store_zip" as "ss_rows_00_ss_store_zip",
    count("yellow"."ss_rows_00_ss_ticket_number") as "_q64_results_cnt_00"
FROM
    "yellow"
GROUP BY
    1,
    2,
    3,
    4),
premium as (
SELECT
    "protective"."ss_rows_99_ss_billing_customer_address_city" as "ss_rows_99_ss_billing_customer_address_city",
    "protective"."ss_rows_99_ss_billing_customer_address_street_name" as "ss_rows_99_ss_billing_customer_address_street_name",
    "protective"."ss_rows_99_ss_billing_customer_address_street_number" as "ss_rows_99_ss_billing_customer_address_street_number",
    "protective"."ss_rows_99_ss_billing_customer_address_zip" as "ss_rows_99_ss_billing_customer_address_zip",
    "protective"."ss_rows_99_ss_billing_customer_first_sales_date_year" as "ss_rows_99_ss_billing_customer_first_sales_date_year",
    "protective"."ss_rows_99_ss_billing_customer_first_shipto_date_year" as "ss_rows_99_ss_billing_customer_first_shipto_date_year",
    "protective"."ss_rows_99_ss_date_year" as "ss_rows_99_ss_date_year",
    "protective"."ss_rows_99_ss_item_id" as "ss_rows_99_ss_item_id",
    "protective"."ss_rows_99_ss_sale_address_city" as "ss_rows_99_ss_sale_address_city",
    "protective"."ss_rows_99_ss_sale_address_street_name" as "ss_rows_99_ss_sale_address_street_name",
    "protective"."ss_rows_99_ss_sale_address_street_number" as "ss_rows_99_ss_sale_address_street_number",
    "protective"."ss_rows_99_ss_sale_address_zip" as "ss_rows_99_ss_sale_address_zip",
    "protective"."ss_rows_99_ss_store_name" as "ss_rows_99_ss_store_name",
    "protective"."ss_rows_99_ss_store_zip" as "ss_rows_99_ss_store_zip",
    count("protective"."ss_rows_99_ss_ticket_number") as "_q64_results_cnt_99"
FROM
    "protective"
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
    13,
    14),
dapper as (
SELECT
    "resonant"."_q64_results_cnt_00" as "_q64_results_cnt_00",
    "resonant"."ss_rows_00_ss_date_year" as "_q64_results_syear_00",
    "resonant"."ss_rows_00_ss_item_id" as "_q64_results_item_sk_00",
    "resonant"."ss_rows_00_ss_store_name" as "_q64_results_s_name_00",
    "resonant"."ss_rows_00_ss_store_zip" as "_q64_results_s_zip_00"
FROM
    "resonant"),
puzzled as (
SELECT
    "busy"."_q64_results_b_city_99" as "_q64_results_b_city_99",
    "busy"."_q64_results_b_sn_99" as "_q64_results_b_sn_99",
    "busy"."_q64_results_b_str_99" as "_q64_results_b_str_99",
    "busy"."_q64_results_b_zip_99" as "_q64_results_b_zip_99",
    "busy"."_q64_results_c_city_99" as "_q64_results_c_city_99",
    "busy"."_q64_results_c_sn_99" as "_q64_results_c_sn_99",
    "busy"."_q64_results_c_str_99" as "_q64_results_c_str_99",
    "busy"."_q64_results_c_zip_99" as "_q64_results_c_zip_99",
    "busy"."_q64_results_item_sk_99" as "item_sk",
    "busy"."_q64_results_p_name_99" as "_q64_results_p_name_99",
    "busy"."_q64_results_s_name_99" as "s_name",
    "busy"."_q64_results_s_zip_99" as "s_zip",
    "busy"."_q64_results_syear_99" as "_q64_results_syear_99",
    "charming"."_q64_results_s1_99" as "_q64_results_s1_99",
    "charming"."_q64_results_s2_99" as "_q64_results_s2_99",
    "charming"."_q64_results_s3_99" as "_q64_results_s3_99",
    "premium"."_q64_results_cnt_99" as "_q64_results_cnt_99"
FROM
    "premium"
    INNER JOIN "charming" on "premium"."ss_rows_99_ss_billing_customer_address_city" = "charming"."ss_rows_99_ss_billing_customer_address_city" AND "premium"."ss_rows_99_ss_billing_customer_address_street_name" = "charming"."ss_rows_99_ss_billing_customer_address_street_name" AND "premium"."ss_rows_99_ss_billing_customer_address_street_number" = "charming"."ss_rows_99_ss_billing_customer_address_street_number" AND "premium"."ss_rows_99_ss_billing_customer_address_zip" = "charming"."ss_rows_99_ss_billing_customer_address_zip" AND "premium"."ss_rows_99_ss_billing_customer_first_sales_date_year" = "charming"."ss_rows_99_ss_billing_customer_first_sales_date_year" AND "premium"."ss_rows_99_ss_billing_customer_first_shipto_date_year" = "charming"."ss_rows_99_ss_billing_customer_first_shipto_date_year" AND "premium"."ss_rows_99_ss_date_year" = "charming"."ss_rows_99_ss_date_year" AND "premium"."ss_rows_99_ss_item_id" = "charming"."ss_rows_99_ss_item_id" AND "premium"."ss_rows_99_ss_sale_address_city" = "charming"."ss_rows_99_ss_sale_address_city" AND "premium"."ss_rows_99_ss_sale_address_street_name" = "charming"."ss_rows_99_ss_sale_address_street_name" AND "premium"."ss_rows_99_ss_sale_address_street_number" = "charming"."ss_rows_99_ss_sale_address_street_number" AND "premium"."ss_rows_99_ss_sale_address_zip" = "charming"."ss_rows_99_ss_sale_address_zip" AND "premium"."ss_rows_99_ss_store_name" = "charming"."ss_rows_99_ss_store_name" AND "premium"."ss_rows_99_ss_store_zip" = "charming"."ss_rows_99_ss_store_zip"
    INNER JOIN "busy" on "premium"."ss_rows_99_ss_billing_customer_address_city" = "busy"."_q64_results_c_city_99" AND "premium"."ss_rows_99_ss_billing_customer_address_street_name" = "busy"."_q64_results_c_str_99" AND "premium"."ss_rows_99_ss_billing_customer_address_street_number" = "busy"."_q64_results_c_sn_99" AND "premium"."ss_rows_99_ss_billing_customer_address_zip" = "busy"."_q64_results_c_zip_99" AND "premium"."ss_rows_99_ss_billing_customer_first_sales_date_year" = "busy"."_q64_results_fsyear_99" AND "premium"."ss_rows_99_ss_billing_customer_first_shipto_date_year" = "busy"."_q64_results_s2year_99" AND "premium"."ss_rows_99_ss_date_year" = "busy"."_q64_results_syear_99" AND "premium"."ss_rows_99_ss_item_id" = "busy"."_q64_results_item_sk_99" AND "premium"."ss_rows_99_ss_sale_address_city" = "busy"."_q64_results_b_city_99" AND "premium"."ss_rows_99_ss_sale_address_street_name" = "busy"."_q64_results_b_str_99" AND "premium"."ss_rows_99_ss_sale_address_street_number" = "busy"."_q64_results_b_sn_99" AND "premium"."ss_rows_99_ss_sale_address_zip" = "busy"."_q64_results_b_zip_99" AND "premium"."ss_rows_99_ss_store_name" = "busy"."_q64_results_s_name_99" AND "premium"."ss_rows_99_ss_store_zip" = "busy"."_q64_results_s_zip_99"),
courageous as (
SELECT
    "dapper"."_q64_results_cnt_00" as "_q64_results_cnt_00",
    "dapper"."_q64_results_item_sk_00" as "item_sk",
    "dapper"."_q64_results_s_name_00" as "s_name",
    "dapper"."_q64_results_s_zip_00" as "s_zip",
    "dapper"."_q64_results_syear_00" as "_q64_results_syear_00",
    "sedate"."_q64_results_s1_00" as "_q64_results_s1_00",
    "sedate"."_q64_results_s2_00" as "_q64_results_s2_00",
    "sedate"."_q64_results_s3_00" as "_q64_results_s3_00"
FROM
    "dapper"
    INNER JOIN "sedate" on "dapper"."_q64_results_item_sk_00" = "sedate"."ss_rows_00_ss_item_id" AND "dapper"."_q64_results_s_name_00" = "sedate"."ss_rows_00_ss_store_name" AND "dapper"."_q64_results_s_zip_00" = "sedate"."ss_rows_00_ss_store_zip" AND "dapper"."_q64_results_syear_00" = "sedate"."ss_rows_00_ss_date_year"
GROUP BY
    1,
    2,
    3,
    4,
    5,
    6,
    7,
    8),
vast as (
SELECT
    "courageous"."_q64_results_cnt_00" as "_q64_results_cnt_00",
    "courageous"."_q64_results_s1_00" as "_q64_results_s1_00",
    "courageous"."_q64_results_s2_00" as "_q64_results_s2_00",
    "courageous"."_q64_results_s3_00" as "_q64_results_s3_00",
    "courageous"."_q64_results_syear_00" as "_q64_results_syear_00",
    "puzzled"."_q64_results_b_city_99" as "_q64_results_b_city_99",
    "puzzled"."_q64_results_b_sn_99" as "_q64_results_b_sn_99",
    "puzzled"."_q64_results_b_str_99" as "_q64_results_b_str_99",
    "puzzled"."_q64_results_b_zip_99" as "_q64_results_b_zip_99",
    "puzzled"."_q64_results_c_city_99" as "_q64_results_c_city_99",
    "puzzled"."_q64_results_c_sn_99" as "_q64_results_c_sn_99",
    "puzzled"."_q64_results_c_str_99" as "_q64_results_c_str_99",
    "puzzled"."_q64_results_c_zip_99" as "_q64_results_c_zip_99",
    "puzzled"."_q64_results_cnt_99" as "_q64_results_cnt_99",
    "puzzled"."_q64_results_p_name_99" as "_q64_results_p_name_99",
    "puzzled"."_q64_results_s1_99" as "_q64_results_s1_99",
    "puzzled"."_q64_results_s2_99" as "_q64_results_s2_99",
    "puzzled"."_q64_results_s3_99" as "_q64_results_s3_99",
    "puzzled"."_q64_results_syear_99" as "_q64_results_syear_99",
    coalesce("courageous"."s_name","puzzled"."s_name") as "s_name",
    coalesce("courageous"."s_zip","puzzled"."s_zip") as "s_zip"
FROM
    "puzzled"
    FULL JOIN "courageous" on "puzzled"."item_sk" is not distinct from "courageous"."item_sk" AND "puzzled"."s_name" is not distinct from "courageous"."s_name" AND "puzzled"."s_zip" is not distinct from "courageous"."s_zip")
SELECT
    "vast"."_q64_results_p_name_99" as "q64_results_p_name_99",
    "vast"."s_name" as "q64_results_s_name",
    "vast"."s_zip" as "q64_results_s_zip",
    "vast"."_q64_results_b_sn_99" as "q64_results_b_sn_99",
    "vast"."_q64_results_b_str_99" as "q64_results_b_str_99",
    "vast"."_q64_results_b_city_99" as "q64_results_b_city_99",
    "vast"."_q64_results_b_zip_99" as "q64_results_b_zip_99",
    "vast"."_q64_results_c_sn_99" as "q64_results_c_sn_99",
    "vast"."_q64_results_c_str_99" as "q64_results_c_str_99",
    "vast"."_q64_results_c_city_99" as "q64_results_c_city_99",
    "vast"."_q64_results_c_zip_99" as "q64_results_c_zip_99",
    "vast"."_q64_results_syear_99" as "q64_results_syear_99",
    "vast"."_q64_results_cnt_99" as "q64_results_cnt_99",
    "vast"."_q64_results_s1_99" as "q64_results_s1_99",
    "vast"."_q64_results_s2_99" as "q64_results_s2_99",
    "vast"."_q64_results_s3_99" as "q64_results_s3_99",
    "vast"."_q64_results_s1_00" as "q64_results_s1_00",
    "vast"."_q64_results_s2_00" as "q64_results_s2_00",
    "vast"."_q64_results_s3_00" as "q64_results_s3_00",
    "vast"."_q64_results_syear_00" as "q64_results_syear_00",
    "vast"."_q64_results_cnt_00" as "q64_results_cnt_00"
FROM
    "vast"
WHERE
    "vast"."_q64_results_cnt_00" <= "vast"."_q64_results_cnt_99"

ORDER BY 
    "q64_results_p_name_99" asc nulls first,
    "q64_results_s_name" asc nulls first,
    "q64_results_cnt_00" asc nulls first,
    "q64_results_s1_99" asc nulls first,
    "q64_results_s1_00" asc nulls first
```

## Reference SQL (zquery log)

```sql
WITH 
wakeful as (
SELECT
    "cr_sales_catalog_sales"."CS_ITEM_SK" as "cr_sales_item_id",
    sum(( coalesce("cr_catalog_returns"."CR_REFUNDED_CASH",0) + coalesce("cr_catalog_returns"."CR_REVERSED_CHARGE",0) ) + coalesce("cr_catalog_returns"."CR_STORE_CREDIT",0)) as "cs_ui_refund"
FROM
    "memory"."catalog_sales" as "cr_sales_catalog_sales"
    INNER JOIN "memory"."catalog_returns" as "cr_catalog_returns" on "cr_sales_catalog_sales"."CS_ORDER_NUMBER" = "cr_catalog_returns"."CR_ORDER_NUMBER"
GROUP BY
    1),
thoughtful as (
SELECT
    "cr_sales_catalog_sales"."CS_ITEM_SK" as "cr_sales_item_id",
    sum("cr_sales_catalog_sales"."CS_EXT_LIST_PRICE") as "cs_ui_sale"
FROM
    "memory"."catalog_sales" as "cr_sales_catalog_sales"
GROUP BY
    1),
cooperative as (
SELECT
    "wakeful"."cr_sales_item_id" as "cs_ui_cs_ui_item_id"
FROM
    "thoughtful"
    INNER JOIN "wakeful" on "thoughtful"."cr_sales_item_id" = "wakeful"."cr_sales_item_id"
WHERE
    "thoughtful"."cs_ui_sale" > 2 * "wakeful"."cs_ui_refund"
),
divergent as (
SELECT
    "ss_date_date"."D_YEAR" as "ss_rows_00_ss_date_year",
    "ss_item_items"."I_ITEM_SK" as "ss_rows_00_ss_item_id",
    "ss_store_sales"."SS_COUPON_AMT" as "ss_rows_00_ss_coupon_amt",
    "ss_store_sales"."SS_LIST_PRICE" as "ss_rows_00_ss_list_price",
    "ss_store_sales"."SS_TICKET_NUMBER" as "ss_rows_00_ss_ticket_number",
    "ss_store_sales"."SS_WHOLESALE_COST" as "ss_rows_00_ss_wholesale_cost",
    "ss_store_store"."S_STORE_NAME" as "ss_rows_00_ss_store_name",
    "ss_store_store"."S_ZIP" as "ss_rows_00_ss_store_zip"
FROM
    "memory"."store_sales" as "ss_store_sales"
    INNER JOIN "memory"."date_dim" as "ss_date_date" on "ss_store_sales"."SS_SOLD_DATE_SK" = "ss_date_date"."D_DATE_SK"
    INNER JOIN "memory"."store" as "ss_store_store" on "ss_store_sales"."SS_STORE_SK" = "ss_store_store"."S_STORE_SK"
    INNER JOIN "memory"."store_returns" as "ss_store_returns" on "ss_store_sales"."SS_ITEM_SK" = "ss_store_returns"."SR_ITEM_SK" AND "ss_store_sales"."SS_TICKET_NUMBER" = "ss_store_returns"."SR_TICKET_NUMBER"
    INNER JOIN "memory"."customer" as "ss_billing_customer_customers" on "ss_store_sales"."SS_CUSTOMER_SK" = "ss_billing_customer_customers"."C_CUSTOMER_SK"
    LEFT OUTER JOIN "memory"."customer_demographics" as "ss_customer_demographic_customer_demographics" on "ss_store_sales"."SS_CDEMO_SK" = "ss_customer_demographic_customer_demographics"."CD_DEMO_SK"
    LEFT OUTER JOIN "memory"."customer_demographics" as "ss_billing_customer_demographics_customer_demographics" on "ss_billing_customer_customers"."C_CURRENT_CDEMO_SK" = "ss_billing_customer_demographics_customer_demographics"."CD_DEMO_SK"
    INNER JOIN "memory"."item" as "ss_item_items" on "ss_store_sales"."SS_ITEM_SK" = "ss_item_items"."I_ITEM_SK"
WHERE
    "ss_item_items"."I_ITEM_SK" in (select cooperative."cs_ui_cs_ui_item_id" from cooperative where cooperative."cs_ui_cs_ui_item_id" is not null) and "ss_date_date"."D_YEAR" = 2000 and SR_RETURN_TIME_SK IS NOT NULL and "ss_item_items"."I_COLOR" in ('purple','burlywood','indian','spring','floral','medium') and "ss_item_items"."I_CURRENT_PRICE" BETWEEN 65 AND 74 and "ss_store_sales"."SS_CUSTOMER_SK" is not null and "ss_store_sales"."SS_STORE_SK" is not null and "ss_store_sales"."SS_ADDR_SK" is not null and "ss_billing_customer_customers"."C_CURRENT_ADDR_SK" is not null and "ss_customer_demographic_customer_demographics"."CD_MARITAL_STATUS" != "ss_billing_customer_demographics_customer_demographics"."CD_MARITAL_STATUS"

GROUP BY
    1,
    2,
    3,
    4,
    5,
    6,
    7,
    8),
friendly as (
SELECT
    "ss_item_items"."I_ITEM_SK" as "ss_rows_99_ss_item_id",
    "ss_item_items"."I_PRODUCT_NAME" as "ss_rows_99_ss_item_product_name"
FROM
    "memory"."store_sales" as "ss_store_sales"
    INNER JOIN "memory"."date_dim" as "ss_date_date" on "ss_store_sales"."SS_SOLD_DATE_SK" = "ss_date_date"."D_DATE_SK"
    INNER JOIN "memory"."store" as "ss_store_store" on "ss_store_sales"."SS_STORE_SK" = "ss_store_store"."S_STORE_SK"
    INNER JOIN "memory"."store_returns" as "ss_store_returns" on "ss_store_sales"."SS_ITEM_SK" = "ss_store_returns"."SR_ITEM_SK" AND "ss_store_sales"."SS_TICKET_NUMBER" = "ss_store_returns"."SR_TICKET_NUMBER"
    INNER JOIN "memory"."customer_address" as "ss_sale_address_customer_address" on "ss_store_sales"."SS_ADDR_SK" = "ss_sale_address_customer_address"."CA_ADDRESS_SK"
    INNER JOIN "memory"."customer" as "ss_billing_customer_customers" on "ss_store_sales"."SS_CUSTOMER_SK" = "ss_billing_customer_customers"."C_CUSTOMER_SK"
    LEFT OUTER JOIN "memory"."customer_demographics" as "ss_customer_demographic_customer_demographics" on "ss_store_sales"."SS_CDEMO_SK" = "ss_customer_demographic_customer_demographics"."CD_DEMO_SK"
    INNER JOIN "memory"."customer_address" as "ss_billing_customer_address_customer_address" on "ss_billing_customer_customers"."C_CURRENT_ADDR_SK" = "ss_billing_customer_address_customer_address"."CA_ADDRESS_SK"
    LEFT OUTER JOIN "memory"."customer_demographics" as "ss_billing_customer_demographics_customer_demographics" on "ss_billing_customer_customers"."C_CURRENT_CDEMO_SK" = "ss_billing_customer_demographics_customer_demographics"."CD_DEMO_SK"
    LEFT OUTER JOIN "memory"."date_dim" as "ss_billing_customer_first_sales_date_date" on "ss_billing_customer_customers"."C_FIRST_SALES_DATE_SK" = "ss_billing_customer_first_sales_date_date"."D_DATE_SK"
    LEFT OUTER JOIN "memory"."date_dim" as "ss_billing_customer_first_shipto_date_date" on "ss_billing_customer_customers"."C_FIRST_SHIPTO_DATE_SK" = "ss_billing_customer_first_shipto_date_date"."D_DATE_SK"
    INNER JOIN "memory"."item" as "ss_item_items" on "ss_store_sales"."SS_ITEM_SK" = "ss_item_items"."I_ITEM_SK"
WHERE
    "ss_item_items"."I_ITEM_SK" in (select cooperative."cs_ui_cs_ui_item_id" from cooperative where cooperative."cs_ui_cs_ui_item_id" is not null) and "ss_date_date"."D_YEAR" = 1999 and SR_RETURN_TIME_SK IS NOT NULL and "ss_item_items"."I_COLOR" in ('purple','burlywood','indian','spring','floral','medium') and "ss_item_items"."I_CURRENT_PRICE" BETWEEN 65 AND 74 and "ss_store_sales"."SS_CUSTOMER_SK" is not null and "ss_store_sales"."SS_STORE_SK" is not null and "ss_store_sales"."SS_ADDR_SK" is not null and "ss_billing_customer_address_customer_address"."CA_ADDRESS_SK" is not null

GROUP BY
    1,
    2),
macho as (
SELECT
    "ss_billing_customer_address_customer_address"."CA_CITY" as "ss_rows_99_ss_billing_customer_address_city",
    "ss_billing_customer_address_customer_address"."CA_STREET_NAME" as "ss_rows_99_ss_billing_customer_address_street_name",
    "ss_billing_customer_address_customer_address"."CA_STREET_NUMBER" as "ss_rows_99_ss_billing_customer_address_street_number",
    "ss_billing_customer_address_customer_address"."CA_ZIP" as "ss_rows_99_ss_billing_customer_address_zip",
    "ss_billing_customer_first_sales_date_date"."D_YEAR" as "ss_rows_99_ss_billing_customer_first_sales_date_year",
    "ss_billing_customer_first_shipto_date_date"."D_YEAR" as "ss_rows_99_ss_billing_customer_first_shipto_date_year",
    "ss_date_date"."D_YEAR" as "ss_rows_99_ss_date_year",
    "ss_item_items"."I_ITEM_SK" as "ss_rows_99_ss_item_id",
    "ss_sale_address_customer_address"."CA_CITY" as "ss_rows_99_ss_sale_address_city",
    "ss_sale_address_customer_address"."CA_STREET_NAME" as "ss_rows_99_ss_sale_address_street_name",
    "ss_sale_address_customer_address"."CA_STREET_NUMBER" as "ss_rows_99_ss_sale_address_street_number",
    "ss_sale_address_customer_address"."CA_ZIP" as "ss_rows_99_ss_sale_address_zip",
    "ss_store_sales"."SS_COUPON_AMT" as "ss_rows_99_ss_coupon_amt",
    "ss_store_sales"."SS_LIST_PRICE" as "ss_rows_99_ss_list_price",
    "ss_store_sales"."SS_TICKET_NUMBER" as "ss_rows_99_ss_ticket_number",
    "ss_store_sales"."SS_WHOLESALE_COST" as "ss_rows_99_ss_wholesale_cost",
    "ss_store_store"."S_STORE_NAME" as "ss_rows_99_ss_store_name",
    "ss_store_store"."S_ZIP" as "ss_rows_99_ss_store_zip"
FROM
    "memory"."store_sales" as "ss_store_sales"
    INNER JOIN "memory"."date_dim" as "ss_date_date" on "ss_store_sales"."SS_SOLD_DATE_SK" = "ss_date_date"."D_DATE_SK"
    INNER JOIN "memory"."store" as "ss_store_store" on "ss_store_sales"."SS_STORE_SK" = "ss_store_store"."S_STORE_SK"
    INNER JOIN "memory"."store_returns" as "ss_store_returns" on "ss_store_sales"."SS_ITEM_SK" = "ss_store_returns"."SR_ITEM_SK" AND "ss_store_sales"."SS_TICKET_NUMBER" = "ss_store_returns"."SR_TICKET_NUMBER"
    INNER JOIN "memory"."customer_address" as "ss_sale_address_customer_address" on "ss_store_sales"."SS_ADDR_SK" = "ss_sale_address_customer_address"."CA_ADDRESS_SK"
    INNER JOIN "memory"."customer" as "ss_billing_customer_customers" on "ss_store_sales"."SS_CUSTOMER_SK" = "ss_billing_customer_customers"."C_CUSTOMER_SK"
    LEFT OUTER JOIN "memory"."customer_demographics" as "ss_customer_demographic_customer_demographics" on "ss_store_sales"."SS_CDEMO_SK" = "ss_customer_demographic_customer_demographics"."CD_DEMO_SK"
    INNER JOIN "memory"."customer_address" as "ss_billing_customer_address_customer_address" on "ss_billing_customer_customers"."C_CURRENT_ADDR_SK" = "ss_billing_customer_address_customer_address"."CA_ADDRESS_SK"
    LEFT OUTER JOIN "memory"."customer_demographics" as "ss_billing_customer_demographics_customer_demographics" on "ss_billing_customer_customers"."C_CURRENT_CDEMO_SK" = "ss_billing_customer_demographics_customer_demographics"."CD_DEMO_SK"
    LEFT OUTER JOIN "memory"."date_dim" as "ss_billing_customer_first_sales_date_date" on "ss_billing_customer_customers"."C_FIRST_SALES_DATE_SK" = "ss_billing_customer_first_sales_date_date"."D_DATE_SK"
    LEFT OUTER JOIN "memory"."date_dim" as "ss_billing_customer_first_shipto_date_date" on "ss_billing_customer_customers"."C_FIRST_SHIPTO_DATE_SK" = "ss_billing_customer_first_shipto_date_date"."D_DATE_SK"
    INNER JOIN "memory"."item" as "ss_item_items" on "ss_store_sales"."SS_ITEM_SK" = "ss_item_items"."I_ITEM_SK"
WHERE
    "ss_item_items"."I_ITEM_SK" in (select cooperative."cs_ui_cs_ui_item_id" from cooperative where cooperative."cs_ui_cs_ui_item_id" is not null) and "ss_date_date"."D_YEAR" = 1999 and SR_RETURN_TIME_SK IS NOT NULL and "ss_item_items"."I_COLOR" in ('purple','burlywood','indian','spring','floral','medium') and "ss_item_items"."I_CURRENT_PRICE" BETWEEN 65 AND 74 and "ss_store_sales"."SS_CUSTOMER_SK" is not null and "ss_store_sales"."SS_STORE_SK" is not null and "ss_store_sales"."SS_ADDR_SK" is not null and "ss_billing_customer_address_customer_address"."CA_ADDRESS_SK" is not null and "ss_customer_demographic_customer_demographics"."CD_MARITAL_STATUS" != "ss_billing_customer_demographics_customer_demographics"."CD_MARITAL_STATUS"

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
    13,
    14,
    15,
    16,
    17,
    18),
busy as (
SELECT
    "divergent"."ss_rows_00_ss_date_year" as "_q64_results_syear_00",
    "divergent"."ss_rows_00_ss_item_id" as "item_sk",
    "divergent"."ss_rows_00_ss_store_name" as "s_name",
    "divergent"."ss_rows_00_ss_store_zip" as "s_zip",
    count("divergent"."ss_rows_00_ss_ticket_number") as "_q64_results_cnt_00",
    sum("divergent"."ss_rows_00_ss_coupon_amt") as "_q64_results_s3_00",
    sum("divergent"."ss_rows_00_ss_list_price") as "_q64_results_s2_00",
    sum("divergent"."ss_rows_00_ss_wholesale_cost") as "_q64_results_s1_00"
FROM
    "divergent"
GROUP BY
    1,
    2,
    3,
    4),
scrawny as (
SELECT
    "macho"."ss_rows_99_ss_billing_customer_address_city" as "ss_rows_99_ss_billing_customer_address_city",
    "macho"."ss_rows_99_ss_billing_customer_address_street_name" as "ss_rows_99_ss_billing_customer_address_street_name",
    "macho"."ss_rows_99_ss_billing_customer_address_street_number" as "ss_rows_99_ss_billing_customer_address_street_number",
    "macho"."ss_rows_99_ss_billing_customer_address_zip" as "ss_rows_99_ss_billing_customer_address_zip",
    "macho"."ss_rows_99_ss_date_year" as "ss_rows_99_ss_date_year",
    "macho"."ss_rows_99_ss_item_id" as "ss_rows_99_ss_item_id",
    "macho"."ss_rows_99_ss_sale_address_city" as "ss_rows_99_ss_sale_address_city",
    "macho"."ss_rows_99_ss_sale_address_street_name" as "ss_rows_99_ss_sale_address_street_name",
    "macho"."ss_rows_99_ss_sale_address_street_number" as "ss_rows_99_ss_sale_address_street_number",
    "macho"."ss_rows_99_ss_sale_address_zip" as "ss_rows_99_ss_sale_address_zip",
    "macho"."ss_rows_99_ss_store_name" as "ss_rows_99_ss_store_name",
    "macho"."ss_rows_99_ss_store_zip" as "ss_rows_99_ss_store_zip",
    count("macho"."ss_rows_99_ss_ticket_number") as "_q64_results_cnt_99",
    sum("macho"."ss_rows_99_ss_coupon_amt") as "_q64_results_s3_99",
    sum("macho"."ss_rows_99_ss_list_price") as "_q64_results_s2_99",
    sum("macho"."ss_rows_99_ss_wholesale_cost") as "_q64_results_s1_99"
FROM
    "macho"
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
    "macho"."ss_rows_99_ss_billing_customer_first_sales_date_year",
    "macho"."ss_rows_99_ss_billing_customer_first_shipto_date_year"),
kaput as (
SELECT
    "friendly"."ss_rows_99_ss_item_product_name" as "_q64_results_p_name_99",
    "scrawny"."_q64_results_cnt_99" as "_q64_results_cnt_99",
    "scrawny"."_q64_results_s1_99" as "_q64_results_s1_99",
    "scrawny"."_q64_results_s2_99" as "_q64_results_s2_99",
    "scrawny"."_q64_results_s3_99" as "_q64_results_s3_99",
    "scrawny"."ss_rows_99_ss_billing_customer_address_city" as "_q64_results_c_city_99",
    "scrawny"."ss_rows_99_ss_billing_customer_address_street_name" as "_q64_results_c_str_99",
    "scrawny"."ss_rows_99_ss_billing_customer_address_street_number" as "_q64_results_c_sn_99",
    "scrawny"."ss_rows_99_ss_billing_customer_address_zip" as "_q64_results_c_zip_99",
    "scrawny"."ss_rows_99_ss_date_year" as "_q64_results_syear_99",
    "scrawny"."ss_rows_99_ss_item_id" as "item_sk",
    "scrawny"."ss_rows_99_ss_sale_address_city" as "_q64_results_b_city_99",
    "scrawny"."ss_rows_99_ss_sale_address_street_name" as "_q64_results_b_str_99",
    "scrawny"."ss_rows_99_ss_sale_address_street_number" as "_q64_results_b_sn_99",
    "scrawny"."ss_rows_99_ss_sale_address_zip" as "_q64_results_b_zip_99",
    "scrawny"."ss_rows_99_ss_store_name" as "s_name",
    "scrawny"."ss_rows_99_ss_store_zip" as "s_zip"
FROM
    "scrawny"
    INNER JOIN "friendly" on "scrawny"."ss_rows_99_ss_item_id" is not distinct from "friendly"."ss_rows_99_ss_item_id")
SELECT
    "kaput"."_q64_results_p_name_99" as "q64_results_p_name_99",
    coalesce("busy"."s_name","kaput"."s_name") as "q64_results_s_name",
    coalesce("busy"."s_zip","kaput"."s_zip") as "q64_results_s_zip",
    "kaput"."_q64_results_b_sn_99" as "q64_results_b_sn_99",
    "kaput"."_q64_results_b_str_99" as "q64_results_b_str_99",
    "kaput"."_q64_results_b_city_99" as "q64_results_b_city_99",
    "kaput"."_q64_results_b_zip_99" as "q64_results_b_zip_99",
    "kaput"."_q64_results_c_sn_99" as "q64_results_c_sn_99",
    "kaput"."_q64_results_c_str_99" as "q64_results_c_str_99",
    "kaput"."_q64_results_c_city_99" as "q64_results_c_city_99",
    "kaput"."_q64_results_c_zip_99" as "q64_results_c_zip_99",
    "kaput"."_q64_results_syear_99" as "q64_results_syear_99",
    "kaput"."_q64_results_cnt_99" as "q64_results_cnt_99",
    "kaput"."_q64_results_s1_99" as "q64_results_s1_99",
    "kaput"."_q64_results_s2_99" as "q64_results_s2_99",
    "kaput"."_q64_results_s3_99" as "q64_results_s3_99",
    "busy"."_q64_results_s1_00" as "q64_results_s1_00",
    "busy"."_q64_results_s2_00" as "q64_results_s2_00",
    "busy"."_q64_results_s3_00" as "q64_results_s3_00",
    "busy"."_q64_results_syear_00" as "q64_results_syear_00",
    "busy"."_q64_results_cnt_00" as "q64_results_cnt_00"
FROM
    "kaput"
    FULL JOIN "busy" on "kaput"."item_sk" is not distinct from "busy"."item_sk" AND "kaput"."s_name" is not distinct from "busy"."s_name" AND "kaput"."s_zip" is not distinct from "busy"."s_zip"
WHERE
    "busy"."_q64_results_cnt_00" <= "kaput"."_q64_results_cnt_99"

ORDER BY 
    "q64_results_p_name_99" asc nulls first,
    "q64_results_s_name" asc nulls first,
    "q64_results_cnt_00" asc nulls first,
    "q64_results_s1_99" asc nulls first,
    "q64_results_s1_00" asc nulls first
```
