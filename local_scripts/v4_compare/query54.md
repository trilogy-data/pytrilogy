# Query 54

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
| v4 | 5999 | 147 | 56.53 ms |
| reference | 4263 | 91 | 50.54 ms |
| v4 / ref | 1.41x | 1.62x | 1.12x |

## Preql

```
import all_sales as sales;
import physical_sales as ss;
import store as store;

# my_customers: customers who bought i_category='Women' & i_class='maternity'
# from catalog or web in Dec 1998
rowset my_customers <- where
    sales.sales_channel in ('CATALOG', 'WEB')
    and sales.item.category = 'Women'
    and sales.item.class = 'maternity'
    and sales.date.year = 1998
    and sales.date.month_of_year = 12
    and sales.billing_customer.id is not null
select
    sales.billing_customer.id as my_cust_id,
;

# Reference q54 cross-joins each ss row with every store matching the
# customer-address county/state. Compute as (ss revenue per customer) *
# (count of stores in customer's county/state), via 2 rowsets keyed on
# (county, state) and merged.
rowset cust_ss <- where
    ss.billing_customer.id in my_customers.my_cust_id
    and ss.date.month_seq >= 1188
    and ss.date.month_seq <= 1190
    and ss.billing_customer.id is not null
select
    ss.billing_customer.id as ss_cust_id,
    ss.billing_customer.address.county as ss_cust_county,
    ss.billing_customer.address.state as ss_cust_state,
    sum(ss.ext_sales_price) as ss_revenue,
;

rowset stores_cs <- select
    store.county as scs_county,
    store.state as scs_state,
    count(store.id) as scs_count,
;

merge cust_ss.ss_cust_county into stores_cs.scs_county;
merge cust_ss.ss_cust_state into stores_cs.scs_state;

# Materialize per-customer revenue at the (cust, county, state, count) grain
# so the downstream count is over the post-join row set.
rowset my_revenue <- select
    cust_ss.ss_cust_id as rev_cust_id,
    cust_ss.ss_revenue * stores_cs.scs_count as revenue,
;

auto segment <- round(my_revenue.revenue / 50, 0)::int;

select
    segment,
    count(my_revenue.rev_cust_id) as num_customers,
    segment * 50 as segment_base,
order by
    segment asc nulls first,
    num_customers asc nulls first,
    segment_base asc nulls first
limit 100
;
```

## v4 generated SQL

```sql
WITH 
scrawny as (
SELECT
    "store_store"."S_COUNTY" as "store_county",
    "store_store"."S_STATE" as "store_state",
    count("store_store"."S_STORE_SK") as "_stores_cs_scs_count"
FROM
    "memory"."store" as "store_store"
GROUP BY
    1,
    2),
cheerful as (
SELECT
    "sales_catalog_sales_unified"."CS_BILL_CUSTOMER_SK" as "sales_billing_customer_id"
FROM
    "memory"."catalog_sales" as "sales_catalog_sales_unified"
    INNER JOIN "memory"."date_dim" as "sales_date_date" on "sales_catalog_sales_unified"."CS_SOLD_DATE_SK" = "sales_date_date"."D_DATE_SK"
    INNER JOIN "memory"."item" as "sales_item_items" on "sales_catalog_sales_unified"."CS_ITEM_SK" = "sales_item_items"."I_ITEM_SK"
WHERE
    "sales_catalog_sales_unified"."CS_BILL_CUSTOMER_SK" is not null and "sales_date_date"."D_YEAR" = 1998 and "sales_date_date"."D_MOY" = 12 and "sales_item_items"."I_CATEGORY" = 'Women' and "sales_item_items"."I_CLASS" = 'maternity'

UNION ALL
SELECT
    "sales_web_sales_unified"."WS_BILL_CUSTOMER_SK" as "sales_billing_customer_id"
FROM
    "memory"."web_sales" as "sales_web_sales_unified"
    INNER JOIN "memory"."date_dim" as "sales_date_date" on "sales_web_sales_unified"."WS_SOLD_DATE_SK" = "sales_date_date"."D_DATE_SK"
    INNER JOIN "memory"."item" as "sales_item_items" on "sales_web_sales_unified"."WS_ITEM_SK" = "sales_item_items"."I_ITEM_SK"
WHERE
    "sales_web_sales_unified"."WS_BILL_CUSTOMER_SK" is not null and "sales_date_date"."D_YEAR" = 1998 and "sales_date_date"."D_MOY" = 12 and "sales_item_items"."I_CATEGORY" = 'Women' and "sales_item_items"."I_CLASS" = 'maternity'
),
divergent as (
SELECT
    "scrawny"."_stores_cs_scs_count" as "_stores_cs_scs_count",
    "scrawny"."store_county" as "_stores_cs_scs_county",
    "scrawny"."store_state" as "_stores_cs_scs_state"
FROM
    "scrawny"),
thoughtful as (
SELECT
    "cheerful"."sales_billing_customer_id" as "sales_billing_customer_id"
FROM
    "cheerful"
GROUP BY
    1),
busy as (
SELECT
    "divergent"."_stores_cs_scs_count" as "stores_cs_scs_count",
    "divergent"."_stores_cs_scs_county" as "stores_cs_scs_county",
    "divergent"."_stores_cs_scs_state" as "stores_cs_scs_state"
FROM
    "divergent"),
uneven as (
SELECT
    "thoughtful"."sales_billing_customer_id" as "_my_customers_my_cust_id"
FROM
    "thoughtful"),
yummy as (
SELECT
    "uneven"."_my_customers_my_cust_id" as "my_customers_my_cust_id"
FROM
    "uneven"),
sparkling as (
SELECT
    "ss_billing_customer_address_customer_address"."CA_COUNTY" as "ss_billing_customer_address_county",
    "ss_billing_customer_address_customer_address"."CA_STATE" as "ss_billing_customer_address_state",
    "ss_store_sales"."SS_CUSTOMER_SK" as "ss_billing_customer_id",
    "ss_store_sales"."SS_EXT_SALES_PRICE" as "ss_ext_sales_price"
FROM
    "memory"."store_sales" as "ss_store_sales"
    INNER JOIN "memory"."date_dim" as "ss_date_date" on "ss_store_sales"."SS_SOLD_DATE_SK" = "ss_date_date"."D_DATE_SK"
    INNER JOIN "memory"."customer" as "ss_billing_customer_customers" on "ss_store_sales"."SS_CUSTOMER_SK" = "ss_billing_customer_customers"."C_CUSTOMER_SK"
    LEFT OUTER JOIN "memory"."customer_address" as "ss_billing_customer_address_customer_address" on "ss_billing_customer_customers"."C_CURRENT_ADDR_SK" = "ss_billing_customer_address_customer_address"."CA_ADDRESS_SK"
WHERE
    "ss_date_date"."D_MONTH_SEQ" >= 1188 and "ss_date_date"."D_MONTH_SEQ" <= 1190 and "ss_store_sales"."SS_CUSTOMER_SK" is not null and "ss_store_sales"."SS_CUSTOMER_SK" in (select yummy."my_customers_my_cust_id" from yummy where yummy."my_customers_my_cust_id" is not null)
),
abhorrent as (
SELECT
    "sparkling"."ss_billing_customer_address_county" as "ss_billing_customer_address_county",
    "sparkling"."ss_billing_customer_address_state" as "ss_billing_customer_address_state",
    "sparkling"."ss_billing_customer_id" as "ss_billing_customer_id",
    sum("sparkling"."ss_ext_sales_price") as "_cust_ss_ss_revenue"
FROM
    "sparkling"
GROUP BY
    1,
    2,
    3),
late as (
SELECT
    "abhorrent"."_cust_ss_ss_revenue" as "_cust_ss_ss_revenue",
    "abhorrent"."ss_billing_customer_address_county" as "_cust_ss_ss_cust_county",
    "abhorrent"."ss_billing_customer_address_state" as "_cust_ss_ss_cust_state",
    "abhorrent"."ss_billing_customer_id" as "_cust_ss_ss_cust_id"
FROM
    "abhorrent"),
macho as (
SELECT
    "late"."_cust_ss_ss_cust_county" as "cust_ss_ss_cust_county",
    "late"."_cust_ss_ss_cust_id" as "cust_ss_ss_cust_id",
    "late"."_cust_ss_ss_cust_state" as "cust_ss_ss_cust_state",
    "late"."_cust_ss_ss_revenue" as "cust_ss_ss_revenue"
FROM
    "late"),
charming as (
SELECT
    "macho"."cust_ss_ss_cust_id" as "_my_revenue_rev_cust_id",
    "macho"."cust_ss_ss_revenue" * "busy"."stores_cs_scs_count" as "_my_revenue_revenue"
FROM
    "macho"
    INNER JOIN "busy" on "macho"."cust_ss_ss_cust_county" = "busy"."stores_cs_scs_county" AND "macho"."cust_ss_ss_cust_state" = "busy"."stores_cs_scs_state"),
premium as (
SELECT
    "charming"."_my_revenue_rev_cust_id" as "my_revenue_rev_cust_id",
    "charming"."_my_revenue_revenue" as "my_revenue_revenue"
FROM
    "charming"),
waggish as (
SELECT
    "premium"."my_revenue_rev_cust_id" as "my_revenue_rev_cust_id"
FROM
    "premium"
GROUP BY
    1),
puzzled as (
SELECT
    cast(round(( "premium"."my_revenue_revenue" ) / 50,0) as int) * 50 as "segment_base",
    cast(round(( "premium"."my_revenue_revenue" ) / 50,0) as int) as "segment"
FROM
    "premium"),
rambunctious as (
SELECT
    CASE WHEN "waggish"."my_revenue_rev_cust_id" IS NOT NULL THEN 1 ELSE 0 END as "num_customers"
FROM
    "waggish")
SELECT
    "puzzled"."segment" as "segment",
    coalesce("rambunctious"."num_customers",0) as "num_customers",
    "puzzled"."segment_base" as "segment_base"
FROM
    "rambunctious"
    FULL JOIN "puzzled" on 1=1
ORDER BY 
    "puzzled"."segment" asc nulls first,
    coalesce("rambunctious"."num_customers",0) asc nulls first,
    "puzzled"."segment_base" asc nulls first
LIMIT (100)
```

## Reference SQL (zquery log)

```sql
WITH 
cheerful as (
SELECT
    "sales_catalog_sales_unified"."CS_BILL_CUSTOMER_SK" as "sales_billing_customer_id"
FROM
    "memory"."catalog_sales" as "sales_catalog_sales_unified"
    INNER JOIN "memory"."date_dim" as "sales_date_date" on "sales_catalog_sales_unified"."CS_SOLD_DATE_SK" = "sales_date_date"."D_DATE_SK"
    INNER JOIN "memory"."item" as "sales_item_items" on "sales_catalog_sales_unified"."CS_ITEM_SK" = "sales_item_items"."I_ITEM_SK"
WHERE
    "sales_catalog_sales_unified"."CS_BILL_CUSTOMER_SK" is not null and "sales_date_date"."D_YEAR" = 1998 and "sales_date_date"."D_MOY" = 12 and "sales_item_items"."I_CATEGORY" = 'Women' and "sales_item_items"."I_CLASS" = 'maternity'

UNION ALL
SELECT
    "sales_web_sales_unified"."WS_BILL_CUSTOMER_SK" as "sales_billing_customer_id"
FROM
    "memory"."web_sales" as "sales_web_sales_unified"
    INNER JOIN "memory"."date_dim" as "sales_date_date" on "sales_web_sales_unified"."WS_SOLD_DATE_SK" = "sales_date_date"."D_DATE_SK"
    INNER JOIN "memory"."item" as "sales_item_items" on "sales_web_sales_unified"."WS_ITEM_SK" = "sales_item_items"."I_ITEM_SK"
WHERE
    "sales_web_sales_unified"."WS_BILL_CUSTOMER_SK" is not null and "sales_date_date"."D_YEAR" = 1998 and "sales_date_date"."D_MOY" = 12 and "sales_item_items"."I_CATEGORY" = 'Women' and "sales_item_items"."I_CLASS" = 'maternity'
),
sparkling as (
SELECT
    "store_store"."S_COUNTY" as "stores_cs_scs_county",
    "store_store"."S_STATE" as "stores_cs_scs_state",
    count("store_store"."S_STORE_SK") as "stores_cs_scs_count"
FROM
    "memory"."store" as "store_store"
GROUP BY
    1,
    2),
thoughtful as (
SELECT
    "cheerful"."sales_billing_customer_id" as "my_customers_my_cust_id"
FROM
    "cheerful"
GROUP BY
    1),
concerned as (
SELECT
    "ss_billing_customer_address_customer_address"."CA_COUNTY" as "cust_ss_ss_cust_county",
    "ss_billing_customer_address_customer_address"."CA_STATE" as "cust_ss_ss_cust_state",
    "ss_store_sales"."SS_CUSTOMER_SK" as "cust_ss_ss_cust_id",
    sum("ss_store_sales"."SS_EXT_SALES_PRICE") as "cust_ss_ss_revenue"
FROM
    "memory"."store_sales" as "ss_store_sales"
    INNER JOIN "memory"."date_dim" as "ss_date_date" on "ss_store_sales"."SS_SOLD_DATE_SK" = "ss_date_date"."D_DATE_SK"
    INNER JOIN "memory"."customer" as "ss_billing_customer_customers" on "ss_store_sales"."SS_CUSTOMER_SK" = "ss_billing_customer_customers"."C_CUSTOMER_SK"
    LEFT OUTER JOIN "memory"."customer_address" as "ss_billing_customer_address_customer_address" on "ss_billing_customer_customers"."C_CURRENT_ADDR_SK" = "ss_billing_customer_address_customer_address"."CA_ADDRESS_SK"
WHERE
    "ss_store_sales"."SS_CUSTOMER_SK" in (select thoughtful."my_customers_my_cust_id" from thoughtful where thoughtful."my_customers_my_cust_id" is not null) and "ss_date_date"."D_MONTH_SEQ" >= 1188 and "ss_date_date"."D_MONTH_SEQ" <= 1190 and "ss_store_sales"."SS_CUSTOMER_SK" is not null

GROUP BY
    1,
    2,
    3),
late as (
SELECT
    "concerned"."cust_ss_ss_cust_id" as "my_revenue_rev_cust_id"
FROM
    "concerned"
    INNER JOIN "sparkling" on "concerned"."cust_ss_ss_cust_county" = "sparkling"."stores_cs_scs_county" AND "concerned"."cust_ss_ss_cust_state" = "sparkling"."stores_cs_scs_state"
GROUP BY
    1),
sweltering as (
SELECT
    cast(round(( "concerned"."cust_ss_ss_revenue" * "sparkling"."stores_cs_scs_count" ) / 50,0) as int) * 50 as "segment_base",
    cast(round(( "concerned"."cust_ss_ss_revenue" * "sparkling"."stores_cs_scs_count" ) / 50,0) as int) as "segment"
FROM
    "concerned"
    INNER JOIN "sparkling" on "concerned"."cust_ss_ss_cust_county" = "sparkling"."stores_cs_scs_county" AND "concerned"."cust_ss_ss_cust_state" = "sparkling"."stores_cs_scs_state"
GROUP BY
    1,
    2),
macho as (
SELECT
    CASE WHEN "late"."my_revenue_rev_cust_id" IS NOT NULL THEN 1 ELSE 0 END as "num_customers"
FROM
    "late")
SELECT
    "sweltering"."segment" as "segment",
    coalesce("macho"."num_customers",0) as "num_customers",
    "sweltering"."segment_base" as "segment_base"
FROM
    "macho"
    FULL JOIN "sweltering" on 1=1
ORDER BY 
    "sweltering"."segment" asc nulls first,
    coalesce("macho"."num_customers",0) asc nulls first,
    "sweltering"."segment_base" asc nulls first
LIMIT (100)
```
