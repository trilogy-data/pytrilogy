# Query 75

**Status:** `match`

| Stage | Result |
| --- | --- |
| v4 SQL generation | OK |
| v4 execution | OK (8 rows) |
| reference execution | OK (8 rows) |
| results identical | YES |

## Result comparison

v4 rows: 8 (8 distinct)
ref rows: 8 (8 distinct)

## SQL size + execution time

| Source | Chars | Lines | Exec (min of 4) |
| --- | --- | --- | --- |
| v4 | 9723 | 209 | 27.02 ms |
| reference | 8613 | 184 | 30.90 ms |
| v4 / ref | 1.13x | 1.14x | 0.87x |

## Preql

```
import all_sales as sales;

# UNION DISTINCT semantics in the reference's `sales_detail` subquery dedups
# rows with identical (year, brand, class, cat, manufact, cnt_per_row,
# amt_per_row) tuples â€” including across cs/ss/ws â€” before the outer SUM.
# `deduped` rowset materializes the 7-tuple via implicit GROUP BY at SELECT
# grain; the per-year multi-select branches roll the deduped rows up to
# item-attribute grain in two parallel filtered aggregates aligned on
# (brand, class, category, manufact).
auto cnt_per_row <- sales.quantity - coalesce(sales.return_quantity, 0);
auto amt_per_row <- sales.ext_sales_price - coalesce(sales.return_amount, 0.0);

rowset deduped <- where
    sales.item.category = 'Books' and sales.date.year in (2001, 2002)
select
    sales.date.year,
    sales.item.brand_id,
    sales.item.class_id,
    sales.item.category_id,
    sales.item.manufacturer_id,
    cnt_per_row,
    amt_per_row,
;

rowset year_pair <- where
    deduped.sales.date.year = 2002
select
    deduped.sales.item.brand_id as brand_id_curr,
    deduped.sales.item.class_id as class_id_curr,
    deduped.sales.item.category_id as category_id_curr,
    deduped.sales.item.manufacturer_id as manufact_id_curr,
    sum(deduped.cnt_per_row) as curr_cnt,
    sum(deduped.amt_per_row) as curr_amt,
merge
where
    deduped.sales.date.year = 2001
select
    deduped.sales.item.brand_id as brand_id_prev,
    deduped.sales.item.class_id as class_id_prev,
    deduped.sales.item.category_id as category_id_prev,
    deduped.sales.item.manufacturer_id as manufact_id_prev,
    sum(deduped.cnt_per_row) as prev_cnt,
    sum(deduped.amt_per_row) as prev_amt,
align
    i_brand_id: brand_id_curr, brand_id_prev
    and i_class_id: class_id_curr, class_id_prev
    and i_category_id: category_id_curr, category_id_prev
    and i_manufact_id: manufact_id_curr, manufact_id_prev
;

select
    2001 as prev_year,
    2002 as year_,
    year_pair.i_brand_id,
    year_pair.i_class_id,
    year_pair.i_category_id,
    year_pair.i_manufact_id,
    year_pair.prev_cnt as prev_yr_cnt,
    year_pair.curr_cnt as curr_yr_cnt,
    year_pair.curr_cnt - year_pair.prev_cnt as sales_cnt_diff,
    year_pair.curr_amt - year_pair.prev_amt as sales_amt_diff,
having
    curr_yr_cnt::numeric(17,2) / prev_yr_cnt::numeric(17,2) < 0.9

order by
    sales_cnt_diff asc nulls first,
    sales_amt_diff asc nulls first
limit 100
;
```

## v4 generated SQL

```sql
WITH 
cheerful as (
SELECT
    "sales_catalog_returns_unified"."CR_ITEM_SK" as "sales_item_id",
    "sales_catalog_returns_unified"."CR_ORDER_NUMBER" as "sales_order_id",
    "sales_catalog_returns_unified"."CR_RETURN_AMOUNT" as "sales_return_amount",
    "sales_catalog_returns_unified"."CR_RETURN_QUANTITY" as "sales_return_quantity",
     'CATALOG'  as "sales_sales_channel"
FROM
    "memory"."catalog_returns" as "sales_catalog_returns_unified"
UNION ALL
SELECT
    "sales_store_returns_unified"."SR_ITEM_SK" as "sales_item_id",
    "sales_store_returns_unified"."SR_TICKET_NUMBER" as "sales_order_id",
    "sales_store_returns_unified"."SR_RETURN_AMT" as "sales_return_amount",
    "sales_store_returns_unified"."SR_RETURN_QUANTITY" as "sales_return_quantity",
     'STORE'  as "sales_sales_channel"
FROM
    "memory"."store_returns" as "sales_store_returns_unified"
UNION ALL
SELECT
    "sales_web_returns_unified"."WR_ITEM_SK" as "sales_item_id",
    "sales_web_returns_unified"."WR_ORDER_NUMBER" as "sales_order_id",
    "sales_web_returns_unified"."WR_RETURN_AMT" as "sales_return_amount",
    "sales_web_returns_unified"."WR_RETURN_QUANTITY" as "sales_return_quantity",
     'WEB'  as "sales_sales_channel"
FROM
    "memory"."web_returns" as "sales_web_returns_unified"),
abundant as (
SELECT
    "sales_catalog_sales_unified"."CS_EXT_SALES_PRICE" as "sales_ext_sales_price",
    "sales_catalog_sales_unified"."CS_ITEM_SK" as "sales_item_id",
    "sales_catalog_sales_unified"."CS_ORDER_NUMBER" as "sales_order_id",
    "sales_catalog_sales_unified"."CS_QUANTITY" as "sales_quantity",
     'CATALOG'  as "sales_sales_channel",
    "sales_date_date"."D_YEAR" as "sales_date_year"
FROM
    "memory"."catalog_sales" as "sales_catalog_sales_unified"
    INNER JOIN "memory"."date_dim" as "sales_date_date" on "sales_catalog_sales_unified"."CS_SOLD_DATE_SK" = "sales_date_date"."D_DATE_SK"
    INNER JOIN "memory"."item" as "sales_item_items" on "sales_catalog_sales_unified"."CS_ITEM_SK" = "sales_item_items"."I_ITEM_SK"
WHERE
    "sales_date_date"."D_YEAR" in (2001,2002) and "sales_item_items"."I_CATEGORY" = 'Books'

UNION ALL
SELECT
    "sales_store_sales_unified"."SS_EXT_SALES_PRICE" as "sales_ext_sales_price",
    "sales_store_sales_unified"."SS_ITEM_SK" as "sales_item_id",
    "sales_store_sales_unified"."SS_TICKET_NUMBER" as "sales_order_id",
    "sales_store_sales_unified"."SS_QUANTITY" as "sales_quantity",
     'STORE'  as "sales_sales_channel",
    "sales_date_date"."D_YEAR" as "sales_date_year"
FROM
    "memory"."store_sales" as "sales_store_sales_unified"
    INNER JOIN "memory"."date_dim" as "sales_date_date" on "sales_store_sales_unified"."SS_SOLD_DATE_SK" = "sales_date_date"."D_DATE_SK"
    INNER JOIN "memory"."item" as "sales_item_items" on "sales_store_sales_unified"."SS_ITEM_SK" = "sales_item_items"."I_ITEM_SK"
WHERE
    "sales_date_date"."D_YEAR" in (2001,2002) and "sales_item_items"."I_CATEGORY" = 'Books'

UNION ALL
SELECT
    "sales_web_sales_unified"."WS_EXT_SALES_PRICE" as "sales_ext_sales_price",
    "sales_web_sales_unified"."WS_ITEM_SK" as "sales_item_id",
    "sales_web_sales_unified"."WS_ORDER_NUMBER" as "sales_order_id",
    "sales_web_sales_unified"."WS_QUANTITY" as "sales_quantity",
     'WEB'  as "sales_sales_channel",
    "sales_date_date"."D_YEAR" as "sales_date_year"
FROM
    "memory"."web_sales" as "sales_web_sales_unified"
    INNER JOIN "memory"."date_dim" as "sales_date_date" on "sales_web_sales_unified"."WS_SOLD_DATE_SK" = "sales_date_date"."D_DATE_SK"
    INNER JOIN "memory"."item" as "sales_item_items" on "sales_web_sales_unified"."WS_ITEM_SK" = "sales_item_items"."I_ITEM_SK"
WHERE
    "sales_date_date"."D_YEAR" in (2001,2002) and "sales_item_items"."I_CATEGORY" = 'Books'
),
juicy as (
SELECT
    "abundant"."sales_date_year" as "sales_date_year",
    "abundant"."sales_ext_sales_price" as "sales_ext_sales_price",
    "abundant"."sales_quantity" as "sales_quantity",
    "cheerful"."sales_return_amount" as "sales_return_amount",
    "cheerful"."sales_return_quantity" as "sales_return_quantity",
    "sales_item_items"."I_BRAND_ID" as "sales_item_brand_id",
    "sales_item_items"."I_CATEGORY_ID" as "sales_item_category_id",
    "sales_item_items"."I_CLASS_ID" as "sales_item_class_id",
    "sales_item_items"."I_MANUFACT_ID" as "sales_item_manufacturer_id"
FROM
    "abundant"
    LEFT OUTER JOIN "cheerful" on "abundant"."sales_item_id" = "cheerful"."sales_item_id" AND "abundant"."sales_order_id" = "cheerful"."sales_order_id" AND "abundant"."sales_sales_channel" = "cheerful"."sales_sales_channel"
    INNER JOIN "memory"."item" as "sales_item_items" on "abundant"."sales_item_id" = "sales_item_items"."I_ITEM_SK"
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
vacuous as (
SELECT
    "juicy"."sales_date_year" as "sales_date_year",
    "juicy"."sales_ext_sales_price" - coalesce("juicy"."sales_return_amount",0.0) as "amt_per_row",
    "juicy"."sales_item_brand_id" as "sales_item_brand_id",
    "juicy"."sales_item_category_id" as "sales_item_category_id",
    "juicy"."sales_item_class_id" as "sales_item_class_id",
    "juicy"."sales_item_manufacturer_id" as "sales_item_manufacturer_id",
    "juicy"."sales_quantity" - coalesce("juicy"."sales_return_quantity",0) as "cnt_per_row"
FROM
    "juicy"),
concerned as (
SELECT
    "vacuous"."amt_per_row" as "deduped_amt_per_row",
    "vacuous"."cnt_per_row" as "deduped_cnt_per_row",
    "vacuous"."sales_date_year" as "deduped_sales_date_year",
    "vacuous"."sales_item_brand_id" as "deduped_sales_item_brand_id",
    "vacuous"."sales_item_category_id" as "deduped_sales_item_category_id",
    "vacuous"."sales_item_class_id" as "deduped_sales_item_class_id",
    "vacuous"."sales_item_manufacturer_id" as "deduped_sales_item_manufacturer_id"
FROM
    "vacuous"),
sweltering as (
SELECT
    "concerned"."deduped_sales_item_brand_id" as "deduped_sales_item_brand_id",
    "concerned"."deduped_sales_item_category_id" as "deduped_sales_item_category_id",
    "concerned"."deduped_sales_item_class_id" as "deduped_sales_item_class_id",
    "concerned"."deduped_sales_item_manufacturer_id" as "deduped_sales_item_manufacturer_id",
    sum("concerned"."deduped_amt_per_row") as "_year_pair_curr_amt",
    sum("concerned"."deduped_cnt_per_row") as "_year_pair_curr_cnt"
FROM
    "concerned"
WHERE
    "concerned"."deduped_sales_date_year" = 2002

GROUP BY
    1,
    2,
    3,
    4),
young as (
SELECT
    "concerned"."deduped_sales_item_brand_id" as "deduped_sales_item_brand_id",
    "concerned"."deduped_sales_item_category_id" as "deduped_sales_item_category_id",
    "concerned"."deduped_sales_item_class_id" as "deduped_sales_item_class_id",
    "concerned"."deduped_sales_item_manufacturer_id" as "deduped_sales_item_manufacturer_id",
    sum("concerned"."deduped_amt_per_row") as "_year_pair_prev_amt",
    sum("concerned"."deduped_cnt_per_row") as "_year_pair_prev_cnt"
FROM
    "concerned"
WHERE
    "concerned"."deduped_sales_date_year" = 2001

GROUP BY
    1,
    2,
    3,
    4),
macho as (
SELECT
    "sweltering"."_year_pair_curr_amt" as "_year_pair_curr_amt",
    "sweltering"."_year_pair_curr_cnt" as "_year_pair_curr_cnt",
    "sweltering"."deduped_sales_item_brand_id" as "i_brand_id",
    "sweltering"."deduped_sales_item_category_id" as "i_category_id",
    "sweltering"."deduped_sales_item_class_id" as "i_class_id",
    "sweltering"."deduped_sales_item_manufacturer_id" as "i_manufact_id"
FROM
    "sweltering"),
abhorrent as (
SELECT
    "young"."_year_pair_prev_amt" as "_year_pair_prev_amt",
    "young"."_year_pair_prev_cnt" as "_year_pair_prev_cnt",
    "young"."deduped_sales_item_brand_id" as "i_brand_id",
    "young"."deduped_sales_item_category_id" as "i_category_id",
    "young"."deduped_sales_item_class_id" as "i_class_id",
    "young"."deduped_sales_item_manufacturer_id" as "i_manufact_id"
FROM
    "young"),
scrawny as (
SELECT
    "abhorrent"."_year_pair_prev_amt" as "year_pair_prev_amt",
    "abhorrent"."_year_pair_prev_cnt" as "year_pair_prev_cnt",
    "macho"."_year_pair_curr_amt" as "year_pair_curr_amt",
    "macho"."_year_pair_curr_cnt" as "year_pair_curr_cnt",
    coalesce("abhorrent"."i_brand_id","macho"."i_brand_id") as "year_pair_i_brand_id",
    coalesce("abhorrent"."i_category_id","macho"."i_category_id") as "year_pair_i_category_id",
    coalesce("abhorrent"."i_class_id","macho"."i_class_id") as "year_pair_i_class_id",
    coalesce("abhorrent"."i_manufact_id","macho"."i_manufact_id") as "year_pair_i_manufact_id"
FROM
    "macho"
    FULL JOIN "abhorrent" on "macho"."i_brand_id" is not distinct from "abhorrent"."i_brand_id" AND "macho"."i_category_id" is not distinct from "abhorrent"."i_category_id" AND "macho"."i_class_id" is not distinct from "abhorrent"."i_class_id" AND "macho"."i_manufact_id" is not distinct from "abhorrent"."i_manufact_id")
SELECT
    2001 as "prev_year",
    2002 as "year_",
    "scrawny"."year_pair_i_brand_id" as "year_pair_i_brand_id",
    "scrawny"."year_pair_i_class_id" as "year_pair_i_class_id",
    "scrawny"."year_pair_i_category_id" as "year_pair_i_category_id",
    "scrawny"."year_pair_i_manufact_id" as "year_pair_i_manufact_id",
    "scrawny"."year_pair_prev_cnt" as "prev_yr_cnt",
    "scrawny"."year_pair_curr_cnt" as "curr_yr_cnt",
    "scrawny"."year_pair_curr_cnt" - "scrawny"."year_pair_prev_cnt" as "sales_cnt_diff",
    "scrawny"."year_pair_curr_amt" - "scrawny"."year_pair_prev_amt" as "sales_amt_diff"
FROM
    "scrawny"
WHERE
    cast("scrawny"."year_pair_curr_cnt" as numeric(17,2)) / cast("scrawny"."year_pair_prev_cnt" as numeric(17,2)) < 0.9

ORDER BY 
    "sales_cnt_diff" asc nulls first,
    "sales_amt_diff" asc nulls first
LIMIT (100)
```

## Reference SQL (zquery log)

```sql
WITH 
cheerful as (
SELECT
    "sales_catalog_returns_unified"."CR_ITEM_SK" as "sales_item_id",
    "sales_catalog_returns_unified"."CR_ORDER_NUMBER" as "sales_order_id",
    "sales_catalog_returns_unified"."CR_RETURN_AMOUNT" as "sales_return_amount",
    "sales_catalog_returns_unified"."CR_RETURN_QUANTITY" as "sales_return_quantity",
     'CATALOG'  as "sales_sales_channel"
FROM
    "memory"."catalog_returns" as "sales_catalog_returns_unified"
UNION ALL
SELECT
    "sales_store_returns_unified"."SR_ITEM_SK" as "sales_item_id",
    "sales_store_returns_unified"."SR_TICKET_NUMBER" as "sales_order_id",
    "sales_store_returns_unified"."SR_RETURN_AMT" as "sales_return_amount",
    "sales_store_returns_unified"."SR_RETURN_QUANTITY" as "sales_return_quantity",
     'STORE'  as "sales_sales_channel"
FROM
    "memory"."store_returns" as "sales_store_returns_unified"
UNION ALL
SELECT
    "sales_web_returns_unified"."WR_ITEM_SK" as "sales_item_id",
    "sales_web_returns_unified"."WR_ORDER_NUMBER" as "sales_order_id",
    "sales_web_returns_unified"."WR_RETURN_AMT" as "sales_return_amount",
    "sales_web_returns_unified"."WR_RETURN_QUANTITY" as "sales_return_quantity",
     'WEB'  as "sales_sales_channel"
FROM
    "memory"."web_returns" as "sales_web_returns_unified"),
abundant as (
SELECT
    "sales_catalog_sales_unified"."CS_EXT_SALES_PRICE" as "sales_ext_sales_price",
    "sales_catalog_sales_unified"."CS_ITEM_SK" as "sales_item_id",
    "sales_catalog_sales_unified"."CS_ORDER_NUMBER" as "sales_order_id",
    "sales_catalog_sales_unified"."CS_QUANTITY" as "sales_quantity",
     'CATALOG'  as "sales_sales_channel",
    "sales_date_date"."D_YEAR" as "sales_date_year"
FROM
    "memory"."catalog_sales" as "sales_catalog_sales_unified"
    INNER JOIN "memory"."date_dim" as "sales_date_date" on "sales_catalog_sales_unified"."CS_SOLD_DATE_SK" = "sales_date_date"."D_DATE_SK"
    INNER JOIN "memory"."item" as "sales_item_items" on "sales_catalog_sales_unified"."CS_ITEM_SK" = "sales_item_items"."I_ITEM_SK"
WHERE
    "sales_date_date"."D_YEAR" in (2001,2002) and "sales_item_items"."I_CATEGORY" = 'Books'

UNION ALL
SELECT
    "sales_store_sales_unified"."SS_EXT_SALES_PRICE" as "sales_ext_sales_price",
    "sales_store_sales_unified"."SS_ITEM_SK" as "sales_item_id",
    "sales_store_sales_unified"."SS_TICKET_NUMBER" as "sales_order_id",
    "sales_store_sales_unified"."SS_QUANTITY" as "sales_quantity",
     'STORE'  as "sales_sales_channel",
    "sales_date_date"."D_YEAR" as "sales_date_year"
FROM
    "memory"."store_sales" as "sales_store_sales_unified"
    INNER JOIN "memory"."date_dim" as "sales_date_date" on "sales_store_sales_unified"."SS_SOLD_DATE_SK" = "sales_date_date"."D_DATE_SK"
    INNER JOIN "memory"."item" as "sales_item_items" on "sales_store_sales_unified"."SS_ITEM_SK" = "sales_item_items"."I_ITEM_SK"
WHERE
    "sales_date_date"."D_YEAR" in (2001,2002) and "sales_item_items"."I_CATEGORY" = 'Books'

UNION ALL
SELECT
    "sales_web_sales_unified"."WS_EXT_SALES_PRICE" as "sales_ext_sales_price",
    "sales_web_sales_unified"."WS_ITEM_SK" as "sales_item_id",
    "sales_web_sales_unified"."WS_ORDER_NUMBER" as "sales_order_id",
    "sales_web_sales_unified"."WS_QUANTITY" as "sales_quantity",
     'WEB'  as "sales_sales_channel",
    "sales_date_date"."D_YEAR" as "sales_date_year"
FROM
    "memory"."web_sales" as "sales_web_sales_unified"
    INNER JOIN "memory"."date_dim" as "sales_date_date" on "sales_web_sales_unified"."WS_SOLD_DATE_SK" = "sales_date_date"."D_DATE_SK"
    INNER JOIN "memory"."item" as "sales_item_items" on "sales_web_sales_unified"."WS_ITEM_SK" = "sales_item_items"."I_ITEM_SK"
WHERE
    "sales_date_date"."D_YEAR" in (2001,2002) and "sales_item_items"."I_CATEGORY" = 'Books'
),
concerned as (
SELECT
    "abundant"."sales_ext_sales_price" - coalesce("cheerful"."sales_return_amount",0.0) as "deduped_amt_per_row",
    "abundant"."sales_quantity" - coalesce("cheerful"."sales_return_quantity",0) as "deduped_cnt_per_row",
    "sales_item_items"."I_BRAND_ID" as "deduped_sales_item_brand_id",
    "sales_item_items"."I_CATEGORY_ID" as "deduped_sales_item_category_id",
    "sales_item_items"."I_CLASS_ID" as "deduped_sales_item_class_id",
    "sales_item_items"."I_MANUFACT_ID" as "deduped_sales_item_manufacturer_id"
FROM
    "abundant"
    LEFT OUTER JOIN "cheerful" on "abundant"."sales_item_id" = "cheerful"."sales_item_id" AND "abundant"."sales_order_id" = "cheerful"."sales_order_id" AND "abundant"."sales_sales_channel" = "cheerful"."sales_sales_channel"
    INNER JOIN "memory"."item" as "sales_item_items" on "abundant"."sales_item_id" = "sales_item_items"."I_ITEM_SK"
WHERE
    "abundant"."sales_date_year" = 2002

GROUP BY
    1,
    2,
    3,
    4,
    5,
    6,
    "abundant"."sales_date_year"),
juicy as (
SELECT
    "abundant"."sales_ext_sales_price" - coalesce("cheerful"."sales_return_amount",0.0) as "deduped_amt_per_row",
    "abundant"."sales_quantity" - coalesce("cheerful"."sales_return_quantity",0) as "deduped_cnt_per_row",
    "sales_item_items"."I_BRAND_ID" as "deduped_sales_item_brand_id",
    "sales_item_items"."I_CATEGORY_ID" as "deduped_sales_item_category_id",
    "sales_item_items"."I_CLASS_ID" as "deduped_sales_item_class_id",
    "sales_item_items"."I_MANUFACT_ID" as "deduped_sales_item_manufacturer_id"
FROM
    "abundant"
    LEFT OUTER JOIN "cheerful" on "abundant"."sales_item_id" = "cheerful"."sales_item_id" AND "abundant"."sales_order_id" = "cheerful"."sales_order_id" AND "abundant"."sales_sales_channel" = "cheerful"."sales_sales_channel"
    INNER JOIN "memory"."item" as "sales_item_items" on "abundant"."sales_item_id" = "sales_item_items"."I_ITEM_SK"
WHERE
    "abundant"."sales_date_year" = 2001

GROUP BY
    1,
    2,
    3,
    4,
    5,
    6,
    "abundant"."sales_date_year"),
young as (
SELECT
    "concerned"."deduped_sales_item_brand_id" as "i_brand_id",
    "concerned"."deduped_sales_item_category_id" as "i_category_id",
    "concerned"."deduped_sales_item_class_id" as "i_class_id",
    "concerned"."deduped_sales_item_manufacturer_id" as "i_manufact_id",
    sum("concerned"."deduped_amt_per_row") as "_year_pair_curr_amt",
    sum("concerned"."deduped_cnt_per_row") as "_year_pair_curr_cnt"
FROM
    "concerned"
GROUP BY
    1,
    2,
    3,
    4),
vacuous as (
SELECT
    "juicy"."deduped_sales_item_brand_id" as "i_brand_id",
    "juicy"."deduped_sales_item_category_id" as "i_category_id",
    "juicy"."deduped_sales_item_class_id" as "i_class_id",
    "juicy"."deduped_sales_item_manufacturer_id" as "i_manufact_id",
    sum("juicy"."deduped_amt_per_row") as "_year_pair_prev_amt",
    sum("juicy"."deduped_cnt_per_row") as "_year_pair_prev_cnt"
FROM
    "juicy"
GROUP BY
    1,
    2,
    3,
    4),
sparkling as (
SELECT
    "vacuous"."_year_pair_prev_cnt" as "prev_yr_cnt",
    "young"."_year_pair_curr_amt" - "vacuous"."_year_pair_prev_amt" as "sales_amt_diff",
    "young"."_year_pair_curr_cnt" - "vacuous"."_year_pair_prev_cnt" as "sales_cnt_diff",
    "young"."_year_pair_curr_cnt" as "curr_yr_cnt",
    2001 as "prev_year",
    2002 as "year_",
    coalesce("vacuous"."i_brand_id","young"."i_brand_id") as "year_pair_i_brand_id",
    coalesce("vacuous"."i_category_id","young"."i_category_id") as "year_pair_i_category_id",
    coalesce("vacuous"."i_class_id","young"."i_class_id") as "year_pair_i_class_id",
    coalesce("vacuous"."i_manufact_id","young"."i_manufact_id") as "year_pair_i_manufact_id"
FROM
    "young"
    FULL JOIN "vacuous" on "young"."i_brand_id" is not distinct from "vacuous"."i_brand_id" AND "young"."i_category_id" is not distinct from "vacuous"."i_category_id" AND "young"."i_class_id" is not distinct from "vacuous"."i_class_id" AND "young"."i_manufact_id" is not distinct from "vacuous"."i_manufact_id")
SELECT
    "sparkling"."prev_year" as "prev_year",
    "sparkling"."year_" as "year_",
    "sparkling"."year_pair_i_brand_id" as "year_pair_i_brand_id",
    "sparkling"."year_pair_i_class_id" as "year_pair_i_class_id",
    "sparkling"."year_pair_i_category_id" as "year_pair_i_category_id",
    "sparkling"."year_pair_i_manufact_id" as "year_pair_i_manufact_id",
    "sparkling"."prev_yr_cnt" as "prev_yr_cnt",
    "sparkling"."curr_yr_cnt" as "curr_yr_cnt",
    "sparkling"."sales_cnt_diff" as "sales_cnt_diff",
    "sparkling"."sales_amt_diff" as "sales_amt_diff"
FROM
    "sparkling"
WHERE
    cast("sparkling"."curr_yr_cnt" as numeric(17,2)) / cast("sparkling"."prev_yr_cnt" as numeric(17,2)) < 0.9

ORDER BY 
    "sparkling"."sales_cnt_diff" asc nulls first,
    "sparkling"."sales_amt_diff" asc nulls first
LIMIT (100)
```
