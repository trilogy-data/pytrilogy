# Query 75

**Status:** `mismatch`

| Stage | Result |
| --- | --- |
| v4 SQL generation | OK |
| v4 execution | OK (100 rows) |
| reference execution | OK (100 rows) |
| results identical | NO |

## Result comparison

v4 rows: 100 (100 distinct)
ref rows: 100 (100 distinct)
only in v4 (showing up to 5 of 5):
  1x  (3901, 2001, 5820, Decimal('-113006.60000000'), -1919, 2002, 2001001, 9, 7, 85)
  1x  (4422, 2001, 6166, Decimal('-48970.54000000'), -1744, 2002, 9006010, 9, 6, 373)
  1x  (4550, 2001, 5860, Decimal('-82309.91000000'), -1310, 2002, 9008008, 9, 8, 535)
  1x  (4607, 2001, 5750, Decimal('-22015.07000000'), -1143, 2002, 2004001, 9, 4, 178)
  1x  (4635, 2001, 5481, Decimal('-5085.95000000'), -846, 2002, 9006010, 9, 1, 190)
only in ref (showing up to 5 of 5):
  1x  (3901, 2001, 5804, Decimal('-113006.60000000'), -1903, 2002, 2001001, 9, 7, 85)
  1x  (4361, 2001, 6166, Decimal('-49562.85000000'), -1805, 2002, 9006010, 9, 6, 373)
  1x  (4550, 2001, 5856, Decimal('-82309.91000000'), -1306, 2002, 9008008, 9, 8, 535)
  1x  (4607, 2001, 5664, Decimal('-22015.07000000'), -1057, 2002, 2004001, 9, 4, 178)
  1x  (5030, 2001, 5826, Decimal('-87415.20000000'), -796, 2002, 9010002, 9, 10, 189)

## SQL size + execution time

| Source | Chars | Lines | Exec (min of 4) |
| --- | --- | --- | --- |
| v4 | 9056 | 192 | 94.30 ms |
| reference | 8613 | 184 | 114.01 ms |
| v4 / ref | 1.05x | 1.04x | 0.83x |

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
thoughtful as (
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
uneven as (
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
quizzical as (
SELECT
    2001 as "prev_year",
    2002 as "year_"
),
vacuous as (
SELECT
    "sales_item_items"."I_BRAND_ID" as "sales_item_brand_id",
    "sales_item_items"."I_CATEGORY_ID" as "sales_item_category_id",
    "sales_item_items"."I_CLASS_ID" as "sales_item_class_id",
    "sales_item_items"."I_MANUFACT_ID" as "sales_item_manufacturer_id",
    "uneven"."sales_date_year" as "sales_date_year",
    "uneven"."sales_ext_sales_price" - coalesce("thoughtful"."sales_return_amount",0.0) as "amt_per_row",
    "uneven"."sales_quantity" - coalesce("thoughtful"."sales_return_quantity",0) as "cnt_per_row"
FROM
    "uneven"
    LEFT OUTER JOIN "thoughtful" on "uneven"."sales_item_id" = "thoughtful"."sales_item_id" AND "uneven"."sales_order_id" = "thoughtful"."sales_order_id" AND "uneven"."sales_sales_channel" = "thoughtful"."sales_sales_channel"
    INNER JOIN "memory"."item" as "sales_item_items" on "uneven"."sales_item_id" = "sales_item_items"."I_ITEM_SK"),
young as (
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
macho as (
SELECT
    "young"."deduped_sales_item_brand_id" as "deduped_sales_item_brand_id",
    "young"."deduped_sales_item_category_id" as "deduped_sales_item_category_id",
    "young"."deduped_sales_item_class_id" as "deduped_sales_item_class_id",
    "young"."deduped_sales_item_manufacturer_id" as "deduped_sales_item_manufacturer_id",
    sum("young"."deduped_amt_per_row") as "_year_pair_curr_amt",
    sum("young"."deduped_cnt_per_row") as "_year_pair_curr_cnt"
FROM
    "young"
WHERE
    "young"."deduped_sales_date_year" = 2002

GROUP BY
    1,
    2,
    3,
    4),
sparkling as (
SELECT
    "young"."deduped_sales_item_brand_id" as "deduped_sales_item_brand_id",
    "young"."deduped_sales_item_category_id" as "deduped_sales_item_category_id",
    "young"."deduped_sales_item_class_id" as "deduped_sales_item_class_id",
    "young"."deduped_sales_item_manufacturer_id" as "deduped_sales_item_manufacturer_id",
    sum("young"."deduped_amt_per_row") as "_year_pair_prev_amt",
    sum("young"."deduped_cnt_per_row") as "_year_pair_prev_cnt"
FROM
    "young"
WHERE
    "young"."deduped_sales_date_year" = 2001

GROUP BY
    1,
    2,
    3,
    4),
kaput as (
SELECT
    "macho"."_year_pair_curr_amt" as "_year_pair_curr_amt",
    "macho"."_year_pair_curr_cnt" as "_year_pair_curr_cnt",
    "macho"."deduped_sales_item_brand_id" as "i_brand_id",
    "macho"."deduped_sales_item_category_id" as "i_category_id",
    "macho"."deduped_sales_item_class_id" as "i_class_id",
    "macho"."deduped_sales_item_manufacturer_id" as "i_manufact_id"
FROM
    "macho"),
late as (
SELECT
    "sparkling"."_year_pair_prev_amt" as "_year_pair_prev_amt",
    "sparkling"."_year_pair_prev_cnt" as "_year_pair_prev_cnt",
    "sparkling"."deduped_sales_item_brand_id" as "i_brand_id",
    "sparkling"."deduped_sales_item_category_id" as "i_category_id",
    "sparkling"."deduped_sales_item_class_id" as "i_class_id",
    "sparkling"."deduped_sales_item_manufacturer_id" as "i_manufact_id"
FROM
    "sparkling"),
divergent as (
SELECT
    "kaput"."_year_pair_curr_amt" - "late"."_year_pair_prev_amt" as "sales_amt_diff",
    "kaput"."_year_pair_curr_cnt" - "late"."_year_pair_prev_cnt" as "sales_cnt_diff",
    "kaput"."_year_pair_curr_cnt" as "curr_yr_cnt",
    "late"."_year_pair_prev_cnt" as "prev_yr_cnt",
    coalesce("kaput"."i_brand_id","late"."i_brand_id") as "year_pair_i_brand_id",
    coalesce("kaput"."i_category_id","late"."i_category_id") as "year_pair_i_category_id",
    coalesce("kaput"."i_class_id","late"."i_class_id") as "year_pair_i_class_id",
    coalesce("kaput"."i_manufact_id","late"."i_manufact_id") as "year_pair_i_manufact_id"
FROM
    "kaput"
    FULL JOIN "late" on "kaput"."i_brand_id" is not distinct from "late"."i_brand_id" AND "kaput"."i_category_id" is not distinct from "late"."i_category_id" AND "kaput"."i_class_id" is not distinct from "late"."i_class_id" AND "kaput"."i_manufact_id" is not distinct from "late"."i_manufact_id")
SELECT
    "quizzical"."prev_year" as "prev_year",
    "quizzical"."year_" as "year_",
    "divergent"."year_pair_i_brand_id" as "year_pair_i_brand_id",
    "divergent"."year_pair_i_class_id" as "year_pair_i_class_id",
    "divergent"."year_pair_i_category_id" as "year_pair_i_category_id",
    "divergent"."year_pair_i_manufact_id" as "year_pair_i_manufact_id",
    "divergent"."prev_yr_cnt" as "prev_yr_cnt",
    "divergent"."curr_yr_cnt" as "curr_yr_cnt",
    "divergent"."sales_cnt_diff" as "sales_cnt_diff",
    "divergent"."sales_amt_diff" as "sales_amt_diff"
FROM
    "divergent"
    LEFT OUTER JOIN "quizzical" on 1=1
WHERE
    cast("divergent"."curr_yr_cnt" as numeric(17,2)) / cast("divergent"."prev_yr_cnt" as numeric(17,2)) < 0.9

ORDER BY 
    "divergent"."sales_cnt_diff" asc nulls first,
    "divergent"."sales_amt_diff" asc nulls first
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
