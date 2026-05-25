# Query 53

**Status:** `exec_fail`

| Stage | Result |
| --- | --- |
| v4 SQL generation | OK |
| v4 execution | FAILED |
| reference execution | OK (100 rows) |

## Result comparison

_at least one side did not produce rows._

## SQL size

| Source | Chars | Lines |
| --- | --- | --- |
| v4 | 4322 | 86 |
| reference | 2518 | 48 |
| v4 / ref | 1.72x | 1.79x |

## Preql

```
import store_sales as store_sales;

auto sum_sales <- sum(store_sales.sales_price) by store_sales.item.manufacturer_id, store_sales.date.quarter;
auto avg_quarterly_sales <- avg(sum_sales) by store_sales.item.manufacturer_id;

where
    store_sales.date.month_seq in (1200, 1201, 1202, 1203, 1204, 1205, 1206, 1207, 1208, 1209, 1210, 1211)
    and store_sales.store.id is not null
    and (
        (
            store_sales.item.category in ('Books', 'Children', 'Electronics')
            and store_sales.item.class in ('personal', 'portable', 'reference', 'self-help')
            and store_sales.item.brand_name in ('scholaramalgamalg #14', 'scholaramalgamalg #7', 'exportiunivamalg #9', 'scholaramalgamalg #9')
        )
        or (
            store_sales.item.category in ('Women', 'Music', 'Men')
            and store_sales.item.class in ('accessories', 'classical', 'fragrances', 'pants')
            and store_sales.item.brand_name in ('amalgimporto #1', 'edu packscholar #1', 'exportiimporto #1', 'importoamalg #1')
        )
    )
select
    store_sales.item.manufacturer_id,
    sum_sales,
    avg_quarterly_sales,
having
    case
            when avg_quarterly_sales > 0 then abs(sum_sales - avg_quarterly_sales) / avg_quarterly_sales
            else null
        end > 0.1

order by
    avg_quarterly_sales asc,
    sum_sales asc,
    store_sales.item.manufacturer_id asc
limit 100
;
```

## v4 generated SQL

```sql
WITH 
wakeful as (
SELECT
    "store_sales_store_sales"."SS_ITEM_SK" as "store_sales_item_id",
    "store_sales_store_sales"."SS_SALES_PRICE" as "store_sales_sales_price",
    "store_sales_store_sales"."SS_SOLD_DATE_SK" as "store_sales_date_id",
    "store_sales_store_sales"."SS_STORE_SK" as "store_sales_store_id",
    "store_sales_store_sales"."SS_TICKET_NUMBER" as "store_sales_ticket_number"
FROM
    "memory"."store_sales" as "store_sales_store_sales"),
highfalutin as (
SELECT
    "store_sales_item_items"."I_BRAND" as "store_sales_item_brand_name",
    "store_sales_item_items"."I_CATEGORY" as "store_sales_item_category",
    "store_sales_item_items"."I_CLASS" as "store_sales_item_class",
    "store_sales_item_items"."I_ITEM_SK" as "store_sales_item_id",
    "store_sales_item_items"."I_MANUFACT_ID" as "store_sales_item_manufacturer_id"
FROM
    "memory"."item" as "store_sales_item_items"),
quizzical as (
SELECT
    "store_sales_date_date"."D_DATE_SK" as "store_sales_date_id",
    "store_sales_date_date"."D_MONTH_SEQ" as "store_sales_date_month_seq",
    "store_sales_date_date"."D_QOY" as "store_sales_date_quarter"
FROM
    "memory"."date_dim" as "store_sales_date_date"),
cheerful as (
SELECT
    "highfalutin"."store_sales_item_brand_name" as "store_sales_item_brand_name",
    "highfalutin"."store_sales_item_category" as "store_sales_item_category",
    "highfalutin"."store_sales_item_class" as "store_sales_item_class",
    "highfalutin"."store_sales_item_manufacturer_id" as "store_sales_item_manufacturer_id",
    "quizzical"."store_sales_date_month_seq" as "store_sales_date_month_seq",
    "quizzical"."store_sales_date_quarter" as "store_sales_date_quarter",
    "wakeful"."store_sales_sales_price" as "store_sales_sales_price",
    "wakeful"."store_sales_store_id" as "store_sales_store_id"
FROM
    "wakeful"
    LEFT OUTER JOIN "quizzical" on "wakeful"."store_sales_date_id" = "quizzical"."store_sales_date_id"
    INNER JOIN "highfalutin" on "wakeful"."store_sales_item_id" = "highfalutin"."store_sales_item_id"
WHERE
    "quizzical"."store_sales_date_month_seq" in (1200,1201,1202,1203,1204,1205,1206,1207,1208,1209,1210,1211) and "wakeful"."store_sales_store_id" is not null and ( ( "highfalutin"."store_sales_item_category" in ('Books','Children','Electronics') and "highfalutin"."store_sales_item_class" in ('personal','portable','reference','self-help') and "highfalutin"."store_sales_item_brand_name" in ('scholaramalgamalg #14','scholaramalgamalg #7','exportiunivamalg #9','scholaramalgamalg #9') ) or ( "highfalutin"."store_sales_item_category" in ('Women','Music','Men') and "highfalutin"."store_sales_item_class" in ('accessories','classical','fragrances','pants') and "highfalutin"."store_sales_item_brand_name" in ('amalgimporto #1','edu packscholar #1','exportiimporto #1','importoamalg #1') ) )
),
thoughtful as (
SELECT
    "cheerful"."store_sales_date_quarter" as "store_sales_date_quarter",
    "cheerful"."store_sales_item_manufacturer_id" as "store_sales_item_manufacturer_id",
    sum("cheerful"."store_sales_sales_price") as "sum_sales"
FROM
    "cheerful"
GROUP BY
    1,
    2),
cooperative as (
SELECT
    "cheerful"."store_sales_item_manufacturer_id" as "store_sales_item_manufacturer_id",
    avg("thoughtful"."sum_sales") as "avg_quarterly_sales"
FROM
    "cheerful"
GROUP BY
    1),
questionable as (
SELECT
    "cooperative"."avg_quarterly_sales" as "avg_quarterly_sales",
    "thoughtful"."store_sales_item_manufacturer_id" as "store_sales_item_manufacturer_id",
    "thoughtful"."sum_sales" as "sum_sales"
FROM
    "cooperative"
    INNER JOIN "thoughtful" on "cooperative"."store_sales_item_manufacturer_id" = "thoughtful"."store_sales_item_manufacturer_id")
SELECT
    "questionable"."store_sales_item_manufacturer_id" as "store_sales_item_manufacturer_id",
    "questionable"."sum_sales" as "sum_sales",
    "questionable"."avg_quarterly_sales" as "avg_quarterly_sales"
FROM
    "questionable"
WHERE
    CASE
	WHEN "questionable"."avg_quarterly_sales" > 0 THEN abs("questionable"."sum_sales" - "questionable"."avg_quarterly_sales") / "questionable"."avg_quarterly_sales"
	ELSE null
	END > 0.1

ORDER BY 
    "questionable"."avg_quarterly_sales" asc,
    "questionable"."sum_sales" asc,
    "questionable"."store_sales_item_manufacturer_id" asc
LIMIT (100)
```

## Reference SQL (zquery log)

```sql
WITH 
cheerful as (
SELECT
    "store_sales_item_items"."I_MANUFACT_ID" as "store_sales_item_manufacturer_id",
    sum("store_sales_store_sales"."SS_SALES_PRICE") as "sum_sales"
FROM
    "memory"."store_sales" as "store_sales_store_sales"
    INNER JOIN "memory"."date_dim" as "store_sales_date_date" on "store_sales_store_sales"."SS_SOLD_DATE_SK" = "store_sales_date_date"."D_DATE_SK"
    INNER JOIN "memory"."item" as "store_sales_item_items" on "store_sales_store_sales"."SS_ITEM_SK" = "store_sales_item_items"."I_ITEM_SK"
WHERE
    "store_sales_date_date"."D_MONTH_SEQ" in (1200,1201,1202,1203,1204,1205,1206,1207,1208,1209,1210,1211) and "store_sales_store_sales"."SS_STORE_SK" is not null and ( ( "store_sales_item_items"."I_CATEGORY" in ('Books','Children','Electronics') and "store_sales_item_items"."I_CLASS" in ('personal','portable','reference','self-help') and "store_sales_item_items"."I_BRAND" in ('scholaramalgamalg #14','scholaramalgamalg #7','exportiunivamalg #9','scholaramalgamalg #9') ) or ( "store_sales_item_items"."I_CATEGORY" in ('Women','Music','Men') and "store_sales_item_items"."I_CLASS" in ('accessories','classical','fragrances','pants') and "store_sales_item_items"."I_BRAND" in ('amalgimporto #1','edu packscholar #1','exportiimporto #1','importoamalg #1') ) )

GROUP BY
    1,
    "store_sales_date_date"."D_QOY"),
cooperative as (
SELECT
    "cheerful"."store_sales_item_manufacturer_id" as "store_sales_item_manufacturer_id",
    avg("cheerful"."sum_sales") as "avg_quarterly_sales"
FROM
    "cheerful"
GROUP BY
    1),
questionable as (
SELECT
    "cheerful"."store_sales_item_manufacturer_id" as "store_sales_item_manufacturer_id",
    "cheerful"."sum_sales" as "sum_sales",
    "cooperative"."avg_quarterly_sales" as "avg_quarterly_sales"
FROM
    "cooperative"
    INNER JOIN "cheerful" on "cooperative"."store_sales_item_manufacturer_id" = "cheerful"."store_sales_item_manufacturer_id"
WHERE
    CASE
	WHEN "cooperative"."avg_quarterly_sales" > 0 THEN abs("cheerful"."sum_sales" - "cooperative"."avg_quarterly_sales") / "cooperative"."avg_quarterly_sales"
	ELSE null
	END > 0.1
)
SELECT
    "questionable"."store_sales_item_manufacturer_id" as "store_sales_item_manufacturer_id",
    "questionable"."sum_sales" as "sum_sales",
    "questionable"."avg_quarterly_sales" as "avg_quarterly_sales"
FROM
    "questionable"
ORDER BY 
    "questionable"."avg_quarterly_sales" asc,
    "questionable"."sum_sales" asc,
    "questionable"."store_sales_item_manufacturer_id" asc
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
_duckdb.BinderException: Binder Error: Referenced table "thoughtful" not found!
Candidate tables: "cheerful"

LINE 57:     avg("thoughtful"."sum_sales") as "avg_quarterly_sales"
                 ^
```
