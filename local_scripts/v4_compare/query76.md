# Query 76

**Status:** `exec_fail`

| Stage | Result |
| --- | --- |
| v4 SQL generation | OK |
| v4 execution | FAILED |
| reference execution | FAILED |

## Result comparison

_at least one side did not produce rows._

## SQL size

| Source | Chars | Lines |
| --- | --- | --- |
| v4 | 13418 | 323 |
| reference | 7609 | 176 |
| v4 / ref | 1.76x | 1.84x |

## Preql

```
import store_sales as ss;
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
scrawny as (
SELECT
    "ws_item_items"."I_CATEGORY" as "ws_item_category",
    "ws_item_items"."I_ITEM_SK" as "ws_item_id"
FROM
    "memory"."item" as "ws_item_items"),
late as (
SELECT
    "ws_web_sales"."WS_EXT_SALES_PRICE" as "ws_ext_sales_price",
    "ws_web_sales"."WS_ITEM_SK" as "ws_item_id",
    "ws_web_sales"."WS_ORDER_NUMBER" as "ws_order_number",
    "ws_web_sales"."WS_SHIP_CUSTOMER_SK" as "ws_ship_customer_id",
    "ws_web_sales"."WS_SOLD_DATE_SK" as "ws_date_id"
FROM
    "memory"."web_sales" as "ws_web_sales"),
sweltering as (
SELECT
    "ws_date_date"."D_DATE_SK" as "ws_date_id",
    "ws_date_date"."D_QOY" as "ws_date_quarter",
    "ws_date_date"."D_YEAR" as "ws_date_year"
FROM
    "memory"."date_dim" as "ws_date_date"),
macho as (
SELECT
    "late"."ws_date_id" as "ws_date_id",
    "late"."ws_ext_sales_price" as "ws_ext_sales_price",
    "late"."ws_item_id" as "ws_item_id",
    "late"."ws_order_number" as "ws_order_number",
    "sweltering"."ws_date_quarter" as "ws_date_quarter",
    "sweltering"."ws_date_year" as "ws_date_year"
FROM
    "late"
    LEFT OUTER JOIN "sweltering" on "late"."ws_date_id" = "sweltering"."ws_date_id"
WHERE
    "late"."ws_ship_customer_id" is null and "late"."ws_order_number" is not null and "late"."ws_date_id" is not null
),
divergent as (
SELECT
    "kaput"."ws_row_flag" as "ws_row_flag",
    "macho"."ws_date_quarter" as "ws_date_quarter",
    "macho"."ws_date_year" as "ws_date_year",
    "macho"."ws_ext_sales_price" as "ws_ext_sales_price",
    "macho"."ws_item_id" as "ws_item_id",
    "macho"."ws_order_number" as "ws_order_number",
    "scrawny"."ws_item_category" as "ws_item_category"
FROM
    "macho"
    INNER JOIN "kaput" on "macho"."ws_item_id" = "kaput"."ws_item_id" AND "macho"."ws_order_number" = "kaput"."ws_order_number"
    INNER JOIN "scrawny" on "macho"."ws_item_id" = "scrawny"."ws_item_id"),
busy as (
SELECT
    "divergent"."ws_date_quarter" as "_q76_results_qoy_b",
    "divergent"."ws_date_quarter" as "d_qoy",
    "divergent"."ws_date_quarter" as "ws_date_quarter",
    "divergent"."ws_date_year" as "_q76_results_year_b",
    "divergent"."ws_date_year" as "d_year",
    "divergent"."ws_date_year" as "ws_date_year",
    "divergent"."ws_item_category" as "_q76_results_category_b",
    "divergent"."ws_item_category" as "i_category",
    "divergent"."ws_item_category" as "ws_item_category",
    :_q76_results_channel_b as "_q76_results_channel_b",
    :_q76_results_channel_b as "channel",
    :_q76_results_col_name_b as "_q76_results_col_name_b",
    :_q76_results_col_name_b as "col_name",
    sum("divergent"."ws_ext_sales_price") as "_q76_results_amt_b",
    sum("divergent"."ws_row_flag") as "_q76_results_cnt_b"
FROM
    "divergent"
GROUP BY
    3,
    6,
    9,
    11,
    13),
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
vacuous as (
SELECT
    "ss_item_items"."I_CATEGORY" as "ss_item_category",
    "ss_item_items"."I_ITEM_SK" as "ss_item_id"
FROM
    "memory"."item" as "ss_item_items"),
yummy as (
SELECT
    "ss_store_sales"."SS_EXT_SALES_PRICE" as "ss_ext_sales_price",
    "ss_store_sales"."SS_ITEM_SK" as "ss_item_id",
    "ss_store_sales"."SS_SOLD_DATE_SK" as "ss_date_id",
    "ss_store_sales"."SS_STORE_SK" as "ss_store_id",
    "ss_store_sales"."SS_TICKET_NUMBER" as "ss_ticket_number"
FROM
    "memory"."store_sales" as "ss_store_sales"),
uneven as (
SELECT
    "ss_date_date"."D_DATE_SK" as "ss_date_id",
    "ss_date_date"."D_QOY" as "ss_date_quarter",
    "ss_date_date"."D_YEAR" as "ss_date_year"
FROM
    "memory"."date_dim" as "ss_date_date"),
juicy as (
SELECT
    "uneven"."ss_date_quarter" as "ss_date_quarter",
    "uneven"."ss_date_year" as "ss_date_year",
    "yummy"."ss_date_id" as "ss_date_id",
    "yummy"."ss_ext_sales_price" as "ss_ext_sales_price",
    "yummy"."ss_item_id" as "ss_item_id",
    "yummy"."ss_ticket_number" as "ss_ticket_number"
FROM
    "yummy"
    LEFT OUTER JOIN "uneven" on "yummy"."ss_date_id" = "uneven"."ss_date_id"
WHERE
    "yummy"."ss_store_id" is null and "yummy"."ss_ticket_number" is not null and "yummy"."ss_date_id" is not null
),
sparkling as (
SELECT
    "juicy"."ss_date_quarter" as "ss_date_quarter",
    "juicy"."ss_date_year" as "ss_date_year",
    "juicy"."ss_ext_sales_price" as "ss_ext_sales_price",
    "juicy"."ss_item_id" as "ss_item_id",
    "juicy"."ss_ticket_number" as "ss_ticket_number",
    "vacuous"."ss_item_category" as "ss_item_category",
    "young"."ss_row_flag" as "ss_row_flag"
FROM
    "juicy"
    INNER JOIN "young" on "juicy"."ss_item_id" = "young"."ss_item_id" AND "juicy"."ss_ticket_number" = "young"."ss_ticket_number"
    INNER JOIN "vacuous" on "juicy"."ss_item_id" = "vacuous"."ss_item_id"),
abhorrent as (
SELECT
    "sparkling"."ss_date_quarter" as "_q76_results_qoy_a",
    "sparkling"."ss_date_quarter" as "d_qoy",
    "sparkling"."ss_date_quarter" as "ss_date_quarter",
    "sparkling"."ss_date_year" as "_q76_results_year_a",
    "sparkling"."ss_date_year" as "d_year",
    "sparkling"."ss_date_year" as "ss_date_year",
    "sparkling"."ss_item_category" as "_q76_results_category_a",
    "sparkling"."ss_item_category" as "i_category",
    "sparkling"."ss_item_category" as "ss_item_category",
    :_q76_results_channel_a as "_q76_results_channel_a",
    :_q76_results_channel_a as "channel",
    :_q76_results_col_name_a as "_q76_results_col_name_a",
    :_q76_results_col_name_a as "col_name",
    sum("sparkling"."ss_ext_sales_price") as "_q76_results_amt_a",
    sum("sparkling"."ss_row_flag") as "_q76_results_cnt_a"
FROM
    "sparkling"
GROUP BY
    3,
    6,
    9,
    11,
    13),
cooperative as (
SELECT
    "cs_item_items"."I_CATEGORY" as "cs_item_category",
    "cs_item_items"."I_ITEM_SK" as "cs_item_id"
FROM
    "memory"."item" as "cs_item_items"),
cheerful as (
SELECT
    "cs_date_date"."D_DATE_SK" as "cs_date_id",
    "cs_date_date"."D_QOY" as "cs_date_quarter",
    "cs_date_date"."D_YEAR" as "cs_date_year"
FROM
    "memory"."date_dim" as "cs_date_date"),
wakeful as (
SELECT
    "cs_catalog_sales"."CS_EXT_SALES_PRICE" as "cs_ext_sales_price",
    "cs_catalog_sales"."CS_ITEM_SK" as "cs_item_id",
    "cs_catalog_sales"."CS_ORDER_NUMBER" as "cs_order_number",
    "cs_catalog_sales"."CS_SHIP_ADDR_SK" as "cs_customer_address_id",
    "cs_catalog_sales"."CS_SOLD_DATE_SK" as "cs_date_id"
FROM
    "memory"."catalog_sales" as "cs_catalog_sales"),
thoughtful as (
SELECT
    "cheerful"."cs_date_quarter" as "cs_date_quarter",
    "cheerful"."cs_date_year" as "cs_date_year",
    "wakeful"."cs_date_id" as "cs_date_id",
    "wakeful"."cs_ext_sales_price" as "cs_ext_sales_price",
    "wakeful"."cs_item_id" as "cs_item_id",
    "wakeful"."cs_order_number" as "cs_order_number"
FROM
    "wakeful"
    LEFT OUTER JOIN "cheerful" on "wakeful"."cs_date_id" = "cheerful"."cs_date_id"
WHERE
    "wakeful"."cs_customer_address_id" is null and "wakeful"."cs_order_number" is not null and "wakeful"."cs_date_id" is not null
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
questionable as (
SELECT
    "cooperative"."cs_item_category" as "cs_item_category",
    "highfalutin"."cs_item_id" as "cs_item_id",
    "highfalutin"."cs_order_number" as "cs_order_number",
    "highfalutin"."cs_row_flag" as "cs_row_flag",
    "thoughtful"."cs_date_quarter" as "cs_date_quarter",
    "thoughtful"."cs_date_year" as "cs_date_year",
    "thoughtful"."cs_ext_sales_price" as "cs_ext_sales_price"
FROM
    "thoughtful"
    INNER JOIN "highfalutin" on "thoughtful"."cs_item_id" = "highfalutin"."cs_item_id" AND "thoughtful"."cs_order_number" = "highfalutin"."cs_order_number"
    INNER JOIN "cooperative" on "thoughtful"."cs_item_id" = "cooperative"."cs_item_id"),
abundant as (
SELECT
    "questionable"."cs_date_quarter" as "_q76_results_qoy_c",
    "questionable"."cs_date_quarter" as "cs_date_quarter",
    "questionable"."cs_date_quarter" as "d_qoy",
    "questionable"."cs_date_year" as "_q76_results_year_c",
    "questionable"."cs_date_year" as "cs_date_year",
    "questionable"."cs_date_year" as "d_year",
    "questionable"."cs_item_category" as "_q76_results_category_c",
    "questionable"."cs_item_category" as "cs_item_category",
    "questionable"."cs_item_category" as "i_category",
    :_q76_results_channel_c as "_q76_results_channel_c",
    :_q76_results_channel_c as "channel",
    :_q76_results_col_name_c as "_q76_results_col_name_c",
    :_q76_results_col_name_c as "col_name",
    sum("questionable"."cs_ext_sales_price") as "_q76_results_amt_c",
    sum("questionable"."cs_row_flag") as "_q76_results_cnt_c"
FROM
    "questionable"
GROUP BY
    2,
    5,
    8,
    11,
    13)
SELECT
    coalesce("abhorrent"."channel","abundant"."channel","busy"."channel") as "q76_results_channel",
    coalesce("abhorrent"."col_name","abundant"."col_name","busy"."col_name") as "q76_results_col_name",
    coalesce("abhorrent"."d_year","abundant"."d_year","busy"."d_year") as "q76_results_d_year",
    coalesce("abhorrent"."d_qoy","abundant"."d_qoy","busy"."d_qoy") as "q76_results_d_qoy",
    coalesce("abhorrent"."i_category","abundant"."i_category","busy"."i_category") as "q76_results_i_category",
    coalesce("abhorrent"."_q76_results_cnt_a","busy"."_q76_results_cnt_b","abundant"."_q76_results_cnt_c") as "q76_results_sales_cnt",
    coalesce("abhorrent"."_q76_results_amt_a","busy"."_q76_results_amt_b","abundant"."_q76_results_amt_c") as "q76_results_sales_amt",
    "abhorrent"."_q76_results_channel_a" as "q76_results_channel_a",
    "abhorrent"."_q76_results_col_name_a" as "q76_results_col_name_a",
    "abhorrent"."_q76_results_year_a" as "q76_results_year_a",
    "abhorrent"."_q76_results_qoy_a" as "q76_results_qoy_a",
    "abhorrent"."_q76_results_category_a" as "q76_results_category_a",
    "abhorrent"."_q76_results_cnt_a" as "q76_results_cnt_a",
    "abhorrent"."_q76_results_amt_a" as "q76_results_amt_a",
    "busy"."_q76_results_channel_b" as "q76_results_channel_b",
    "busy"."_q76_results_col_name_b" as "q76_results_col_name_b",
    "busy"."_q76_results_year_b" as "q76_results_year_b",
    "busy"."_q76_results_qoy_b" as "q76_results_qoy_b",
    "busy"."_q76_results_category_b" as "q76_results_category_b",
    "busy"."_q76_results_cnt_b" as "q76_results_cnt_b",
    "busy"."_q76_results_amt_b" as "q76_results_amt_b",
    "abundant"."_q76_results_channel_c" as "q76_results_channel_c",
    "abundant"."_q76_results_col_name_c" as "q76_results_col_name_c",
    "abundant"."_q76_results_year_c" as "q76_results_year_c",
    "abundant"."_q76_results_qoy_c" as "q76_results_qoy_c",
    "abundant"."_q76_results_category_c" as "q76_results_category_c",
    "abundant"."_q76_results_cnt_c" as "q76_results_cnt_c",
    "abundant"."_q76_results_amt_c" as "q76_results_amt_c"
FROM
    "abhorrent"
    FULL JOIN "busy" on "abhorrent"."channel" is not distinct from "busy"."channel" AND "abhorrent"."col_name" is not distinct from "busy"."col_name" AND "abhorrent"."d_qoy" is not distinct from "busy"."d_qoy" AND "abhorrent"."d_year" is not distinct from "busy"."d_year" AND "abhorrent"."i_category" is not distinct from "busy"."i_category"
    FULL JOIN "abundant" on coalesce("abhorrent"."channel", "busy"."channel") = "abundant"."channel" AND coalesce("abhorrent"."col_name", "busy"."col_name") = "abundant"."col_name" AND coalesce("abhorrent"."d_qoy", "busy"."d_qoy") = "abundant"."d_qoy" AND coalesce("abhorrent"."d_year", "busy"."d_year") = "abundant"."d_year" AND coalesce("abhorrent"."i_category", "busy"."i_category") = "abundant"."i_category"
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
    "ws_web_sales"."WS_SHIP_CUSTOMER_SK" is null and "ws_web_sales"."WS_ORDER_NUMBER" is not null and "ws_web_sales"."WS_SOLD_DATE_SK" is not null
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
    "ss_store_sales"."SS_STORE_SK" is null and "ss_store_sales"."SS_TICKET_NUMBER" is not null and "ss_store_sales"."SS_SOLD_DATE_SK" is not null
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
    "cs_catalog_sales"."CS_SHIP_ADDR_SK" is null and "cs_catalog_sales"."CS_ORDER_NUMBER" is not null and "cs_catalog_sales"."CS_SOLD_DATE_SK" is not null
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
    "macho"
    INNER JOIN "kaput" on "macho"."ws_item_id" = "kaput"."ws_item_id" AND "macho"."ws_order_number" = "kaput"."ws_order_number"
    INNER JOIN "memory"."item" as "ws_item_items" on "macho"."ws_item_id" = "ws_item_items"."I_ITEM_SK"
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
    "juicy"
    INNER JOIN "young" on "juicy"."ss_item_id" = "young"."ss_item_id" AND "juicy"."ss_ticket_number" = "young"."ss_ticket_number"
    INNER JOIN "memory"."item" as "ss_item_items" on "juicy"."ss_item_id" = "ss_item_items"."I_ITEM_SK"
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

## v4 execution error

```
Traceback (most recent call last):
  File "C:\Users\ethan\coding_projects\pytrilogy\local_scripts\discovery_v4_compare.py", line 161, in run_one
    result.v4_rows = execute(con, v4_sql)
                     ~~~~~~~^^^^^^^^^^^^^
  File "C:\Users\ethan\coding_projects\pytrilogy\local_scripts\discovery_v4_compare.py", line 102, in execute
    cursor = con.execute(sql)
_duckdb.ParserException: Parser Error: syntax error at or near ":"

LINE 82:     :_q76_results_channel_b as "_q76_results_channel_b",
             ^
```

## reference execution error

```
Traceback (most recent call last):
  File "C:\Users\ethan\coding_projects\pytrilogy\local_scripts\discovery_v4_compare.py", line 167, in run_one
    result.ref_rows = execute(con, ref_sql)
                      ~~~~~~~^^^^^^^^^^^^^^
  File "C:\Users\ethan\coding_projects\pytrilogy\local_scripts\discovery_v4_compare.py", line 102, in execute
    cursor = con.execute(sql)
_duckdb.ParserException: Parser Error: syntax error at or near ":"

LINE 106:     :_q76_results_channel_b as "channel",
              ^
```
