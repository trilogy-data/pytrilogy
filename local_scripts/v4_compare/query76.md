# Query 76

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
| v4 | 11657 | 272 | 174.95 ms |
| reference | 7468 | 176 | 156.07 ms |
| v4 / ref | 1.56x | 1.55x | 1.12x |

## Preql

```
import physical_sales as ss;
import catalog_sales as cs;
import web_sales as ws;

# Row-grain 0/1 flag that sums to the row count without count()'s COALESCE-to-0 wrapping
# (which breaks the multi-branch coalesce(cnt_a, cnt_b, cnt_c) derive).
auto ss_row_flag <- sum(
    case
        when ss.store.id is null then 1
        else 0
    end
)
    by ss.ticket_number, ss.item.id;
auto ws_row_flag <- sum(
    case
        when ws.ship_customer.id is null then 1
        else 0
    end
)
    by ws.order_number, ws.item.id;
auto cs_row_flag <- sum(
    case
        when cs.customer_address.id is null then 1
        else 0
    end
)
    by cs.order_number, cs.item.id;

rowset q76_results <- where
    ss.store.id is null and ss.ticket_number is not null and ss.date.id is not null
select
    'store' as channel_a,
    'ss_store_sk' as col_name_a,
    ss.date.year as year_a,
    ss.date.quarter as qoy_a,
    ss.item.category as category_a,
    sum(ss_row_flag) as cnt_a,
    sum(ss.ext_sales_price) as amt_a,
merge
where
    ws.ship_customer.id is null and ws.order_number is not null and ws.date.id is not null
select
    'web' as channel_b,
    'ws_ship_customer_sk' as col_name_b,
    ws.date.year as year_b,
    ws.date.quarter as qoy_b,
    ws.item.category as category_b,
    sum(ws_row_flag) as cnt_b,
    sum(ws.ext_sales_price) as amt_b,
merge
where
    cs.customer_address.id is null and cs.order_number is not null and cs.date.id is not null
select
    'catalog' as channel_c,
    'cs_ship_addr_sk' as col_name_c,
    cs.date.year as year_c,
    cs.date.quarter as qoy_c,
    cs.item.category as category_c,
    sum(cs_row_flag) as cnt_c,
    sum(cs.ext_sales_price) as amt_c,
align
    channel: channel_a, channel_b, channel_c
    and col_name: col_name_a, col_name_b, col_name_c
    and d_year: year_a, year_b, year_c
    and d_qoy: qoy_a, qoy_b, qoy_c
    and i_category: category_a, category_b, category_c
derive
    coalesce(q76_results.cnt_a, q76_results.cnt_b, q76_results.cnt_c) -> sales_cnt,
    coalesce(q76_results.amt_a, q76_results.amt_b, q76_results.amt_c) -> sales_amt
;

select
    q76_results.channel,
    q76_results.col_name,
    q76_results.d_year,
    q76_results.d_qoy,
    q76_results.i_category,
    q76_results.sales_cnt,
    q76_results.sales_amt,
order by
    q76_results.channel asc nulls first,
    q76_results.col_name asc nulls first,
    q76_results.d_year asc nulls first,
    q76_results.d_qoy asc nulls first,
    q76_results.i_category asc nulls first
limit 100
;
```

## v4 generated SQL

```sql
WITH 
hard as (
SELECT
    "ws_item_items"."I_CATEGORY" as "ws_item_category"
FROM
    "memory"."item" as "ws_item_items"
GROUP BY
    1),
premium as (
SELECT
    "ws_date_date"."D_QOY" as "ws_date_quarter",
    "ws_date_date"."D_YEAR" as "ws_date_year",
    "ws_item_items"."I_CATEGORY" as "ws_item_category",
    "ws_item_items"."I_ITEM_SK" as "ws_item_id",
    "ws_web_sales"."WS_EXT_SALES_PRICE" as "ws_ext_sales_price",
    "ws_web_sales"."WS_ORDER_NUMBER" as "ws_order_number",
    "ws_web_sales"."WS_SHIP_CUSTOMER_SK" as "ws_ship_customer_id"
FROM
    "memory"."web_sales" as "ws_web_sales"
    INNER JOIN "memory"."date_dim" as "ws_date_date" on "ws_web_sales"."WS_SOLD_DATE_SK" = "ws_date_date"."D_DATE_SK"
    INNER JOIN "memory"."item" as "ws_item_items" on "ws_web_sales"."WS_ITEM_SK" = "ws_item_items"."I_ITEM_SK"
WHERE
    "ws_web_sales"."WS_SHIP_CUSTOMER_SK" is null and "ws_web_sales"."WS_ORDER_NUMBER" is not null and "ws_web_sales"."WS_SOLD_DATE_SK" is not null
),
friendly as (
SELECT
    "ss_item_items"."I_CATEGORY" as "ss_item_category"
FROM
    "memory"."item" as "ss_item_items"
GROUP BY
    1),
abhorrent as (
SELECT
    "ss_date_date"."D_QOY" as "ss_date_quarter",
    "ss_date_date"."D_YEAR" as "ss_date_year",
    "ss_item_items"."I_CATEGORY" as "ss_item_category",
    "ss_item_items"."I_ITEM_SK" as "ss_item_id",
    "ss_store_sales"."SS_EXT_SALES_PRICE" as "ss_ext_sales_price",
    "ss_store_sales"."SS_STORE_SK" as "ss_store_id",
    "ss_store_sales"."SS_TICKET_NUMBER" as "ss_ticket_number"
FROM
    "memory"."store_sales" as "ss_store_sales"
    INNER JOIN "memory"."date_dim" as "ss_date_date" on "ss_store_sales"."SS_SOLD_DATE_SK" = "ss_date_date"."D_DATE_SK"
    INNER JOIN "memory"."item" as "ss_item_items" on "ss_store_sales"."SS_ITEM_SK" = "ss_item_items"."I_ITEM_SK"
WHERE
    "ss_store_sales"."SS_STORE_SK" is null and "ss_store_sales"."SS_TICKET_NUMBER" is not null and "ss_store_sales"."SS_SOLD_DATE_SK" is not null
),
uneven as (
SELECT
    "cs_item_items"."I_CATEGORY" as "cs_item_category"
FROM
    "memory"."item" as "cs_item_items"
GROUP BY
    1),
cheerful as (
SELECT
    "cs_catalog_sales"."CS_EXT_SALES_PRICE" as "cs_ext_sales_price",
    "cs_catalog_sales"."CS_ITEM_SK" as "cs_item_id",
    "cs_catalog_sales"."CS_ORDER_NUMBER" as "cs_order_number",
    "cs_catalog_sales"."CS_SHIP_ADDR_SK" as "cs_customer_address_id",
    "cs_date_date"."D_QOY" as "cs_date_quarter",
    "cs_date_date"."D_YEAR" as "cs_date_year",
    "cs_item_items"."I_CATEGORY" as "cs_item_category"
FROM
    "memory"."catalog_sales" as "cs_catalog_sales"
    INNER JOIN "memory"."date_dim" as "cs_date_date" on "cs_catalog_sales"."CS_SOLD_DATE_SK" = "cs_date_date"."D_DATE_SK"
    INNER JOIN "memory"."item" as "cs_item_items" on "cs_catalog_sales"."CS_ITEM_SK" = "cs_item_items"."I_ITEM_SK"
WHERE
    "cs_catalog_sales"."CS_SHIP_ADDR_SK" is null and "cs_catalog_sales"."CS_ORDER_NUMBER" is not null and "cs_catalog_sales"."CS_SOLD_DATE_SK" is not null
),
juicy as (
SELECT
    :_q76_results_channel_a as "_q76_results_channel_a",
    :_q76_results_channel_b as "_q76_results_channel_b",
    :_q76_results_channel_c as "_q76_results_channel_c",
    :_q76_results_col_name_a as "_q76_results_col_name_a",
    :_q76_results_col_name_b as "_q76_results_col_name_b",
    :_q76_results_col_name_c as "_q76_results_col_name_c"
),
sedate as (
SELECT
    "hard"."ws_item_category" as "_q76_results_category_b"
FROM
    "hard"),
puzzled as (
SELECT
    "premium"."ws_ext_sales_price" as "ws_ext_sales_price",
    "premium"."ws_item_category" as "ws_item_category",
    "premium"."ws_item_id" as "ws_item_id",
    "premium"."ws_order_number" as "ws_order_number",
    CASE
	WHEN "premium"."ws_ship_customer_id" is null THEN 1
	ELSE 0
	END as "ws_row_flag"
FROM
    "premium"),
kaput as (
SELECT
    "friendly"."ss_item_category" as "_q76_results_category_a"
FROM
    "friendly"),
sweltering as (
SELECT
    "abhorrent"."ss_ext_sales_price" as "ss_ext_sales_price",
    "abhorrent"."ss_item_category" as "ss_item_category",
    "abhorrent"."ss_item_id" as "ss_item_id",
    "abhorrent"."ss_ticket_number" as "ss_ticket_number",
    CASE
	WHEN "abhorrent"."ss_store_id" is null THEN 1
	ELSE 0
	END as "ss_row_flag"
FROM
    "abhorrent"),
yummy as (
SELECT
    "uneven"."cs_item_category" as "_q76_results_category_c"
FROM
    "uneven"),
thoughtful as (
SELECT
    "cheerful"."cs_ext_sales_price" as "cs_ext_sales_price",
    "cheerful"."cs_item_category" as "cs_item_category",
    "cheerful"."cs_item_id" as "cs_item_id",
    "cheerful"."cs_order_number" as "cs_order_number",
    CASE
	WHEN "cheerful"."cs_customer_address_id" is null THEN 1
	ELSE 0
	END as "cs_row_flag"
FROM
    "cheerful"),
waggish as (
SELECT
    "premium"."ws_date_quarter" as "ws_date_quarter",
    "premium"."ws_date_year" as "ws_date_year",
    "puzzled"."ws_item_category" as "ws_item_category",
    sum("puzzled"."ws_ext_sales_price") as "_q76_results_amt_b",
    sum("puzzled"."ws_row_flag") as "_q76_results_cnt_b"
FROM
    "puzzled"
    FULL JOIN "premium" on "puzzled"."ws_item_id" = "premium"."ws_item_id" AND "puzzled"."ws_order_number" = "premium"."ws_order_number"
GROUP BY
    1,
    2,
    3),
late as (
SELECT
    "abhorrent"."ss_date_quarter" as "ss_date_quarter",
    "abhorrent"."ss_date_year" as "ss_date_year",
    "sweltering"."ss_item_category" as "ss_item_category",
    sum("sweltering"."ss_ext_sales_price") as "_q76_results_amt_a",
    sum("sweltering"."ss_row_flag") as "_q76_results_cnt_a"
FROM
    "sweltering"
    FULL JOIN "abhorrent" on "sweltering"."ss_item_id" = "abhorrent"."ss_item_id" AND "sweltering"."ss_ticket_number" = "abhorrent"."ss_ticket_number"
GROUP BY
    1,
    2,
    3),
cooperative as (
SELECT
    "cheerful"."cs_date_quarter" as "cs_date_quarter",
    "cheerful"."cs_date_year" as "cs_date_year",
    "thoughtful"."cs_item_category" as "cs_item_category",
    sum("thoughtful"."cs_ext_sales_price") as "_q76_results_amt_c",
    sum("thoughtful"."cs_row_flag") as "_q76_results_cnt_c"
FROM
    "thoughtful"
    FULL JOIN "cheerful" on "thoughtful"."cs_item_id" = "cheerful"."cs_item_id" AND "thoughtful"."cs_order_number" = "cheerful"."cs_order_number"
GROUP BY
    1,
    2,
    3),
puffy as (
SELECT
    "waggish"."_q76_results_amt_b" as "_q76_results_amt_b",
    "waggish"."_q76_results_cnt_b" as "_q76_results_cnt_b",
    "waggish"."ws_date_quarter" as "_q76_results_qoy_b",
    "waggish"."ws_date_year" as "_q76_results_year_b",
    "waggish"."ws_item_category" as "ws_item_category"
FROM
    "waggish"),
scrawny as (
SELECT
    "late"."_q76_results_amt_a" as "_q76_results_amt_a",
    "late"."_q76_results_cnt_a" as "_q76_results_cnt_a",
    "late"."ss_date_quarter" as "_q76_results_qoy_a",
    "late"."ss_date_year" as "_q76_results_year_a",
    "late"."ss_item_category" as "ss_item_category"
FROM
    "late"),
abundant as (
SELECT
    "cooperative"."_q76_results_amt_c" as "_q76_results_amt_c",
    "cooperative"."_q76_results_cnt_c" as "_q76_results_cnt_c",
    "cooperative"."cs_date_quarter" as "_q76_results_qoy_c",
    "cooperative"."cs_date_year" as "_q76_results_year_c",
    "cooperative"."cs_item_category" as "cs_item_category"
FROM
    "cooperative"),
yellow as (
SELECT
    "juicy"."_q76_results_channel_b" as "_q76_results_channel_b",
    "juicy"."_q76_results_channel_b" as "channel",
    "juicy"."_q76_results_col_name_b" as "_q76_results_col_name_b",
    "juicy"."_q76_results_col_name_b" as "col_name",
    "puffy"."_q76_results_amt_b" as "_q76_results_amt_b",
    "puffy"."_q76_results_cnt_b" as "_q76_results_cnt_b",
    "puffy"."_q76_results_qoy_b" as "_q76_results_qoy_b",
    "puffy"."_q76_results_qoy_b" as "d_qoy",
    "puffy"."_q76_results_year_b" as "_q76_results_year_b",
    "puffy"."_q76_results_year_b" as "d_year",
    "sedate"."_q76_results_category_b" as "_q76_results_category_b",
    "sedate"."_q76_results_category_b" as "i_category"
FROM
    "puffy"
    FULL JOIN "sedate" on "puffy"."ws_item_category" is not distinct from "sedate"."_q76_results_category_b"
    FULL JOIN "juicy" on 1=1),
divergent as (
SELECT
    "juicy"."_q76_results_channel_a" as "_q76_results_channel_a",
    "juicy"."_q76_results_channel_a" as "channel",
    "juicy"."_q76_results_col_name_a" as "_q76_results_col_name_a",
    "juicy"."_q76_results_col_name_a" as "col_name",
    "kaput"."_q76_results_category_a" as "_q76_results_category_a",
    "kaput"."_q76_results_category_a" as "i_category",
    "scrawny"."_q76_results_amt_a" as "_q76_results_amt_a",
    "scrawny"."_q76_results_cnt_a" as "_q76_results_cnt_a",
    "scrawny"."_q76_results_qoy_a" as "_q76_results_qoy_a",
    "scrawny"."_q76_results_qoy_a" as "d_qoy",
    "scrawny"."_q76_results_year_a" as "_q76_results_year_a",
    "scrawny"."_q76_results_year_a" as "d_year"
FROM
    "scrawny"
    FULL JOIN "kaput" on "scrawny"."ss_item_category" is not distinct from "kaput"."_q76_results_category_a"
    FULL JOIN "juicy" on 1=1),
vacuous as (
SELECT
    "abundant"."_q76_results_amt_c" as "_q76_results_amt_c",
    "abundant"."_q76_results_cnt_c" as "_q76_results_cnt_c",
    "abundant"."_q76_results_qoy_c" as "_q76_results_qoy_c",
    "abundant"."_q76_results_qoy_c" as "d_qoy",
    "abundant"."_q76_results_year_c" as "_q76_results_year_c",
    "abundant"."_q76_results_year_c" as "d_year",
    "juicy"."_q76_results_channel_c" as "_q76_results_channel_c",
    "juicy"."_q76_results_channel_c" as "channel",
    "juicy"."_q76_results_col_name_c" as "_q76_results_col_name_c",
    "juicy"."_q76_results_col_name_c" as "col_name",
    "yummy"."_q76_results_category_c" as "_q76_results_category_c",
    "yummy"."_q76_results_category_c" as "i_category"
FROM
    "abundant"
    FULL JOIN "yummy" on "abundant"."cs_item_category" is not distinct from "yummy"."_q76_results_category_c"
    FULL JOIN "juicy" on 1=1)
SELECT
    coalesce("divergent"."channel","vacuous"."channel","yellow"."channel") as "q76_results_channel",
    coalesce("divergent"."col_name","vacuous"."col_name","yellow"."col_name") as "q76_results_col_name",
    coalesce("divergent"."d_qoy","vacuous"."d_qoy","yellow"."d_qoy") as "q76_results_d_qoy",
    coalesce("divergent"."d_year","vacuous"."d_year","yellow"."d_year") as "q76_results_d_year",
    coalesce("divergent"."i_category","vacuous"."i_category","yellow"."i_category") as "q76_results_i_category",
    coalesce("divergent"."_q76_results_amt_a","yellow"."_q76_results_amt_b","vacuous"."_q76_results_amt_c") as "q76_results_sales_amt",
    coalesce("divergent"."_q76_results_cnt_a","yellow"."_q76_results_cnt_b","vacuous"."_q76_results_cnt_c") as "q76_results_sales_cnt"
FROM
    "divergent"
    FULL JOIN "yellow" on "divergent"."channel" is not distinct from "yellow"."channel" AND "divergent"."col_name" is not distinct from "yellow"."col_name" AND "divergent"."d_qoy" is not distinct from "yellow"."d_qoy" AND "divergent"."d_year" is not distinct from "yellow"."d_year" AND "divergent"."i_category" is not distinct from "yellow"."i_category"
    FULL JOIN "vacuous" on coalesce("divergent"."channel", "yellow"."channel") = "vacuous"."channel" AND coalesce("divergent"."col_name", "yellow"."col_name") = "vacuous"."col_name" AND coalesce("divergent"."d_qoy", "yellow"."d_qoy") = "vacuous"."d_qoy" AND coalesce("divergent"."d_year", "yellow"."d_year") = "vacuous"."d_year" AND coalesce("divergent"."i_category", "yellow"."i_category") = "vacuous"."i_category"
ORDER BY 
    "q76_results_channel" asc nulls first,
    "q76_results_col_name" asc nulls first,
    "q76_results_d_year" asc nulls first,
    "q76_results_d_qoy" asc nulls first,
    "q76_results_i_category" asc nulls first
LIMIT (100)
```

## Reference SQL (zquery log)

```sql
WITH 
friendly as (
SELECT
    "ws_web_sales"."WS_ITEM_SK" as "ws_item_id",
    "ws_web_sales"."WS_ORDER_NUMBER" as "ws_order_number",
    "ws_web_sales"."WS_SHIP_CUSTOMER_SK" as "ws_ship_customer_id"
FROM
    "memory"."web_sales" as "ws_web_sales"
WHERE
    "ws_web_sales"."WS_SHIP_CUSTOMER_SK" is null and "ws_web_sales"."WS_SOLD_DATE_SK" is not null
),
concerned as (
SELECT
    "ss_store_sales"."SS_ITEM_SK" as "ss_item_id",
    "ss_store_sales"."SS_STORE_SK" as "ss_store_id",
    "ss_store_sales"."SS_TICKET_NUMBER" as "ss_ticket_number"
FROM
    "memory"."store_sales" as "ss_store_sales"
WHERE
    "ss_store_sales"."SS_STORE_SK" is null and "ss_store_sales"."SS_SOLD_DATE_SK" is not null
),
quizzical as (
SELECT
    "cs_catalog_sales"."CS_ITEM_SK" as "cs_item_id",
    "cs_catalog_sales"."CS_ORDER_NUMBER" as "cs_order_number",
    "cs_catalog_sales"."CS_SHIP_ADDR_SK" as "cs_customer_address_id"
FROM
    "memory"."catalog_sales" as "cs_catalog_sales"
WHERE
    "cs_catalog_sales"."CS_SHIP_ADDR_SK" is null and "cs_catalog_sales"."CS_SOLD_DATE_SK" is not null
),
macho as (
SELECT
    "ws_date_date"."D_QOY" as "ws_date_quarter",
    "ws_date_date"."D_YEAR" as "ws_date_year",
    "ws_web_sales"."WS_EXT_SALES_PRICE" as "ws_ext_sales_price",
    "ws_web_sales"."WS_ITEM_SK" as "ws_item_id",
    "ws_web_sales"."WS_ORDER_NUMBER" as "ws_order_number"
FROM
    "memory"."web_sales" as "ws_web_sales"
    INNER JOIN "memory"."date_dim" as "ws_date_date" on "ws_web_sales"."WS_SOLD_DATE_SK" = "ws_date_date"."D_DATE_SK"
WHERE
    "ws_web_sales"."WS_ORDER_NUMBER" is not null and "ws_web_sales"."WS_SOLD_DATE_SK" is not null
),
juicy as (
SELECT
    "ss_date_date"."D_QOY" as "ss_date_quarter",
    "ss_date_date"."D_YEAR" as "ss_date_year",
    "ss_store_sales"."SS_EXT_SALES_PRICE" as "ss_ext_sales_price",
    "ss_store_sales"."SS_ITEM_SK" as "ss_item_id",
    "ss_store_sales"."SS_TICKET_NUMBER" as "ss_ticket_number"
FROM
    "memory"."store_sales" as "ss_store_sales"
    INNER JOIN "memory"."date_dim" as "ss_date_date" on "ss_store_sales"."SS_SOLD_DATE_SK" = "ss_date_date"."D_DATE_SK"
WHERE
    "ss_store_sales"."SS_TICKET_NUMBER" is not null and "ss_store_sales"."SS_SOLD_DATE_SK" is not null
),
thoughtful as (
SELECT
    "cs_catalog_sales"."CS_EXT_SALES_PRICE" as "cs_ext_sales_price",
    "cs_catalog_sales"."CS_ITEM_SK" as "cs_item_id",
    "cs_catalog_sales"."CS_ORDER_NUMBER" as "cs_order_number",
    "cs_date_date"."D_QOY" as "cs_date_quarter",
    "cs_date_date"."D_YEAR" as "cs_date_year"
FROM
    "memory"."catalog_sales" as "cs_catalog_sales"
    INNER JOIN "memory"."date_dim" as "cs_date_date" on "cs_catalog_sales"."CS_SOLD_DATE_SK" = "cs_date_date"."D_DATE_SK"
WHERE
    "cs_catalog_sales"."CS_ORDER_NUMBER" is not null and "cs_catalog_sales"."CS_SOLD_DATE_SK" is not null
),
kaput as (
SELECT
    "friendly"."ws_item_id" as "ws_item_id",
    "friendly"."ws_order_number" as "ws_order_number",
    CASE
	WHEN "friendly"."ws_ship_customer_id" is null THEN 1
	ELSE 0
	END as "ws_row_flag"
FROM
    "friendly"),
young as (
SELECT
    "concerned"."ss_item_id" as "ss_item_id",
    "concerned"."ss_ticket_number" as "ss_ticket_number",
    CASE
	WHEN "concerned"."ss_store_id" is null THEN 1
	ELSE 0
	END as "ss_row_flag"
FROM
    "concerned"),
highfalutin as (
SELECT
    "quizzical"."cs_item_id" as "cs_item_id",
    "quizzical"."cs_order_number" as "cs_order_number",
    CASE
	WHEN "quizzical"."cs_customer_address_id" is null THEN 1
	ELSE 0
	END as "cs_row_flag"
FROM
    "quizzical"),
divergent as (
SELECT
    "macho"."ws_date_quarter" as "d_qoy",
    "macho"."ws_date_year" as "d_year",
    "ws_item_items"."I_CATEGORY" as "i_category",
    :_q76_results_channel_b as "channel",
    :_q76_results_col_name_b as "col_name",
    sum("kaput"."ws_row_flag") as "_q76_results_cnt_b",
    sum("macho"."ws_ext_sales_price") as "_q76_results_amt_b"
FROM
    "kaput"
    INNER JOIN "macho" on "kaput"."ws_item_id" = "macho"."ws_item_id" AND "kaput"."ws_order_number" = "macho"."ws_order_number"
    INNER JOIN "memory"."item" as "ws_item_items" on "kaput"."ws_item_id" = "ws_item_items"."I_ITEM_SK"
GROUP BY
    1,
    2,
    3,
    4,
    5),
sparkling as (
SELECT
    "juicy"."ss_date_quarter" as "d_qoy",
    "juicy"."ss_date_year" as "d_year",
    "ss_item_items"."I_CATEGORY" as "i_category",
    :_q76_results_channel_a as "channel",
    :_q76_results_col_name_a as "col_name",
    sum("juicy"."ss_ext_sales_price") as "_q76_results_amt_a",
    sum("young"."ss_row_flag") as "_q76_results_cnt_a"
FROM
    "young"
    INNER JOIN "juicy" on "young"."ss_item_id" = "juicy"."ss_item_id" AND "young"."ss_ticket_number" = "juicy"."ss_ticket_number"
    INNER JOIN "memory"."item" as "ss_item_items" on "young"."ss_item_id" = "ss_item_items"."I_ITEM_SK"
GROUP BY
    1,
    2,
    3,
    4,
    5),
questionable as (
SELECT
    "cs_item_items"."I_CATEGORY" as "i_category",
    "thoughtful"."cs_date_quarter" as "d_qoy",
    "thoughtful"."cs_date_year" as "d_year",
    :_q76_results_channel_c as "channel",
    :_q76_results_col_name_c as "col_name",
    sum("highfalutin"."cs_row_flag") as "_q76_results_cnt_c",
    sum("thoughtful"."cs_ext_sales_price") as "_q76_results_amt_c"
FROM
    "thoughtful"
    INNER JOIN "highfalutin" on "thoughtful"."cs_item_id" = "highfalutin"."cs_item_id" AND "thoughtful"."cs_order_number" = "highfalutin"."cs_order_number"
    INNER JOIN "memory"."item" as "cs_item_items" on "thoughtful"."cs_item_id" = "cs_item_items"."I_ITEM_SK"
GROUP BY
    1,
    2,
    3,
    4,
    5)
SELECT
    coalesce("divergent"."channel","questionable"."channel","sparkling"."channel") as "q76_results_channel",
    coalesce("divergent"."col_name","questionable"."col_name","sparkling"."col_name") as "q76_results_col_name",
    coalesce("divergent"."d_year","questionable"."d_year","sparkling"."d_year") as "q76_results_d_year",
    coalesce("divergent"."d_qoy","questionable"."d_qoy","sparkling"."d_qoy") as "q76_results_d_qoy",
    coalesce("divergent"."i_category","questionable"."i_category","sparkling"."i_category") as "q76_results_i_category",
    coalesce("sparkling"."_q76_results_cnt_a","divergent"."_q76_results_cnt_b","questionable"."_q76_results_cnt_c") as "q76_results_sales_cnt",
    coalesce("sparkling"."_q76_results_amt_a","divergent"."_q76_results_amt_b","questionable"."_q76_results_amt_c") as "q76_results_sales_amt"
FROM
    "sparkling"
    FULL JOIN "divergent" on "sparkling"."channel" is not distinct from "divergent"."channel" AND "sparkling"."col_name" is not distinct from "divergent"."col_name" AND "sparkling"."d_qoy" is not distinct from "divergent"."d_qoy" AND "sparkling"."d_year" is not distinct from "divergent"."d_year" AND "sparkling"."i_category" is not distinct from "divergent"."i_category"
    FULL JOIN "questionable" on coalesce("sparkling"."channel", "divergent"."channel") = "questionable"."channel" AND coalesce("sparkling"."col_name", "divergent"."col_name") = "questionable"."col_name" AND coalesce("sparkling"."d_qoy", "divergent"."d_qoy") = "questionable"."d_qoy" AND coalesce("sparkling"."d_year", "divergent"."d_year") = "questionable"."d_year" AND coalesce("sparkling"."i_category", "divergent"."i_category") = "questionable"."i_category"
ORDER BY 
    "q76_results_channel" asc nulls first,
    "q76_results_col_name" asc nulls first,
    "q76_results_d_year" asc nulls first,
    "q76_results_d_qoy" asc nulls first,
    "q76_results_i_category" asc nulls first
LIMIT (100)
```
