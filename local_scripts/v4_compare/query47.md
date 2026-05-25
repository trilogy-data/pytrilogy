# Query 47

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
| v4 | 9363 | 157 |
| reference | 5746 | 90 |
| v4 / ref | 1.63x | 1.74x |

## Preql

```
import store_sales as store_sales;

auto sum_sales <- sum(store_sales.sales_price)
    by store_sales.item.category, store_sales.item.brand_name, store_sales.store.name, store_sales.store.company_name, store_sales.date.year, store_sales.date.month_of_year;
auto avg_monthly_sales <- avg(sum_sales)
    by store_sales.item.category, store_sales.item.brand_name, store_sales.store.name, store_sales.store.company_name, store_sales.date.year;
auto psum <- lag(sum_sales, 1)
    over (partition by store_sales.item.category,
            store_sales.item.brand_name,
            store_sales.store.name,
            store_sales.store.company_name
        order by store_sales.date.year asc, store_sales.date.month_of_year asc);
auto nsum <- lead(sum_sales, 1)
    over (partition by store_sales.item.category,
            store_sales.item.brand_name,
            store_sales.store.name,
            store_sales.store.company_name
        order by store_sales.date.year asc, store_sales.date.month_of_year asc);
auto sum_minus_avg <- sum_sales - avg_monthly_sales;

where
    store_sales.store.id is not null
    and (
        store_sales.date.year = 1999
        or (store_sales.date.year = 1998 and store_sales.date.month_of_year = 12)
        or (store_sales.date.year = 2000 and store_sales.date.month_of_year = 1)
    )
select
    store_sales.item.category,
    store_sales.item.brand_name,
    store_sales.store.name,
    store_sales.store.company_name,
    store_sales.date.year,
    store_sales.date.month_of_year,
    avg_monthly_sales,
    sum_sales,
    psum,
    nsum,
    --sum_minus_avg,
having
    store_sales.date.year = 1999
    and avg_monthly_sales > 0
    and case
            when avg_monthly_sales > 0 then abs(sum_sales - avg_monthly_sales) / avg_monthly_sales
            else null
        end > 0.1

order by
    sum_minus_avg asc,
    store_sales.item.category asc,
    store_sales.item.brand_name asc,
    store_sales.store.name asc,
    store_sales.store.company_name asc,
    store_sales.date.year asc,
    store_sales.date.month_of_year asc,
    avg_monthly_sales asc,
    sum_sales asc,
    psum asc,
    nsum asc
limit 100
;
```

## v4 generated SQL

```sql
WITH 
cheerful as (
SELECT
    "store_sales_store_sales"."SS_ITEM_SK" as "store_sales_item_id",
    "store_sales_store_sales"."SS_SALES_PRICE" as "store_sales_sales_price",
    "store_sales_store_sales"."SS_SOLD_DATE_SK" as "store_sales_date_id",
    "store_sales_store_sales"."SS_STORE_SK" as "store_sales_store_id",
    "store_sales_store_sales"."SS_TICKET_NUMBER" as "store_sales_ticket_number"
FROM
    "memory"."store_sales" as "store_sales_store_sales"),
wakeful as (
SELECT
    "store_sales_store_store"."S_COMPANY_NAME" as "store_sales_store_company_name",
    "store_sales_store_store"."S_STORE_NAME" as "store_sales_store_name",
    "store_sales_store_store"."S_STORE_SK" as "store_sales_store_id"
FROM
    "memory"."store" as "store_sales_store_store"),
highfalutin as (
SELECT
    "store_sales_item_items"."I_BRAND" as "store_sales_item_brand_name",
    "store_sales_item_items"."I_CATEGORY" as "store_sales_item_category",
    "store_sales_item_items"."I_ITEM_SK" as "store_sales_item_id"
FROM
    "memory"."item" as "store_sales_item_items"),
quizzical as (
SELECT
    "store_sales_date_date"."D_DATE_SK" as "store_sales_date_id",
    "store_sales_date_date"."D_MOY" as "store_sales_date_month_of_year",
    "store_sales_date_date"."D_YEAR" as "store_sales_date_year"
FROM
    "memory"."date_dim" as "store_sales_date_date"),
thoughtful as (
SELECT
    "cheerful"."store_sales_sales_price" as "store_sales_sales_price",
    "cheerful"."store_sales_store_id" as "store_sales_store_id",
    "highfalutin"."store_sales_item_brand_name" as "store_sales_item_brand_name",
    "highfalutin"."store_sales_item_category" as "store_sales_item_category",
    "quizzical"."store_sales_date_month_of_year" as "store_sales_date_month_of_year",
    "quizzical"."store_sales_date_year" as "store_sales_date_year",
    "wakeful"."store_sales_store_company_name" as "store_sales_store_company_name",
    "wakeful"."store_sales_store_name" as "store_sales_store_name"
FROM
    "cheerful"
    LEFT OUTER JOIN "quizzical" on "cheerful"."store_sales_date_id" = "quizzical"."store_sales_date_id"
    INNER JOIN "highfalutin" on "cheerful"."store_sales_item_id" = "highfalutin"."store_sales_item_id"
    LEFT OUTER JOIN "wakeful" on "cheerful"."store_sales_store_id" = "wakeful"."store_sales_store_id"
WHERE
    "cheerful"."store_sales_store_id" is not null and ( "quizzical"."store_sales_date_year" = 1999 or ( "quizzical"."store_sales_date_year" = 1998 and "quizzical"."store_sales_date_month_of_year" = 12 ) or ( "quizzical"."store_sales_date_year" = 2000 and "quizzical"."store_sales_date_month_of_year" = 1 ) )
),
cooperative as (
SELECT
    "thoughtful"."store_sales_date_month_of_year" as "store_sales_date_month_of_year",
    "thoughtful"."store_sales_date_year" as "store_sales_date_year",
    "thoughtful"."store_sales_item_brand_name" as "store_sales_item_brand_name",
    "thoughtful"."store_sales_item_category" as "store_sales_item_category",
    "thoughtful"."store_sales_store_company_name" as "store_sales_store_company_name",
    "thoughtful"."store_sales_store_name" as "store_sales_store_name",
    sum("thoughtful"."store_sales_sales_price") as "sum_sales"
FROM
    "thoughtful"
GROUP BY
    1,
    2,
    3,
    4,
    5,
    6),
uneven as (
SELECT
    "thoughtful"."store_sales_date_month_of_year" as "store_sales_date_month_of_year",
    "thoughtful"."store_sales_date_year" as "store_sales_date_year",
    "thoughtful"."store_sales_item_brand_name" as "store_sales_item_brand_name",
    "thoughtful"."store_sales_item_category" as "store_sales_item_category",
    "thoughtful"."store_sales_store_company_name" as "store_sales_store_company_name",
    "thoughtful"."store_sales_store_name" as "store_sales_store_name",
    lag("cooperative"."sum_sales", 1) over (partition by "thoughtful"."store_sales_item_category","thoughtful"."store_sales_item_brand_name","thoughtful"."store_sales_store_name","thoughtful"."store_sales_store_company_name" order by "thoughtful"."store_sales_date_year" asc,"thoughtful"."store_sales_date_month_of_year" asc ) as "psum",
    lead("cooperative"."sum_sales", 1) over (partition by "thoughtful"."store_sales_item_category","thoughtful"."store_sales_item_brand_name","thoughtful"."store_sales_store_name","thoughtful"."store_sales_store_company_name" order by "thoughtful"."store_sales_date_year" asc,"thoughtful"."store_sales_date_month_of_year" asc ) as "nsum"
FROM
    "thoughtful"),
questionable as (
SELECT
    "thoughtful"."store_sales_date_year" as "store_sales_date_year",
    "thoughtful"."store_sales_item_brand_name" as "store_sales_item_brand_name",
    "thoughtful"."store_sales_item_category" as "store_sales_item_category",
    "thoughtful"."store_sales_store_company_name" as "store_sales_store_company_name",
    "thoughtful"."store_sales_store_name" as "store_sales_store_name",
    avg("cooperative"."sum_sales") as "avg_monthly_sales"
FROM
    "thoughtful"
GROUP BY
    1,
    2,
    3,
    4,
    5),
abundant as (
SELECT
    "cooperative"."store_sales_date_month_of_year" as "store_sales_date_month_of_year",
    "cooperative"."sum_sales" - "questionable"."avg_monthly_sales" as "sum_minus_avg",
    "cooperative"."sum_sales" as "sum_sales",
    "questionable"."avg_monthly_sales" as "avg_monthly_sales",
    coalesce("cooperative"."store_sales_date_year","questionable"."store_sales_date_year") as "store_sales_date_year",
    coalesce("cooperative"."store_sales_item_brand_name","questionable"."store_sales_item_brand_name") as "store_sales_item_brand_name",
    coalesce("cooperative"."store_sales_item_category","questionable"."store_sales_item_category") as "store_sales_item_category",
    coalesce("cooperative"."store_sales_store_company_name","questionable"."store_sales_store_company_name") as "store_sales_store_company_name",
    coalesce("cooperative"."store_sales_store_name","questionable"."store_sales_store_name") as "store_sales_store_name"
FROM
    "questionable"
    FULL JOIN "cooperative" on "questionable"."store_sales_date_year" = "cooperative"."store_sales_date_year" AND "questionable"."store_sales_item_brand_name" = "cooperative"."store_sales_item_brand_name" AND "questionable"."store_sales_item_category" is not distinct from "cooperative"."store_sales_item_category" AND "questionable"."store_sales_store_company_name" is not distinct from "cooperative"."store_sales_store_company_name" AND "questionable"."store_sales_store_name" is not distinct from "cooperative"."store_sales_store_name"),
yummy as (
SELECT
    "abundant"."avg_monthly_sales" as "avg_monthly_sales",
    "abundant"."sum_minus_avg" as "sum_minus_avg",
    "abundant"."sum_sales" as "sum_sales",
    "uneven"."nsum" as "nsum",
    "uneven"."psum" as "psum",
    coalesce("abundant"."store_sales_date_month_of_year","uneven"."store_sales_date_month_of_year") as "store_sales_date_month_of_year",
    coalesce("abundant"."store_sales_date_year","uneven"."store_sales_date_year") as "store_sales_date_year",
    coalesce("abundant"."store_sales_item_brand_name","uneven"."store_sales_item_brand_name") as "store_sales_item_brand_name",
    coalesce("abundant"."store_sales_item_category","uneven"."store_sales_item_category") as "store_sales_item_category",
    coalesce("abundant"."store_sales_store_company_name","uneven"."store_sales_store_company_name") as "store_sales_store_company_name",
    coalesce("abundant"."store_sales_store_name","uneven"."store_sales_store_name") as "store_sales_store_name"
FROM
    "abundant"
    FULL JOIN "uneven" on "abundant"."store_sales_date_month_of_year" = "uneven"."store_sales_date_month_of_year" AND "abundant"."store_sales_date_year" = "uneven"."store_sales_date_year" AND "abundant"."store_sales_item_brand_name" = "uneven"."store_sales_item_brand_name" AND "abundant"."store_sales_item_category" is not distinct from "uneven"."store_sales_item_category" AND "abundant"."store_sales_store_company_name" is not distinct from "uneven"."store_sales_store_company_name" AND "abundant"."store_sales_store_name" = "uneven"."store_sales_store_name")
SELECT
    "yummy"."store_sales_item_category" as "store_sales_item_category",
    "yummy"."store_sales_item_brand_name" as "store_sales_item_brand_name",
    "yummy"."store_sales_store_name" as "store_sales_store_name",
    "yummy"."store_sales_store_company_name" as "store_sales_store_company_name",
    "yummy"."store_sales_date_year" as "store_sales_date_year",
    "yummy"."store_sales_date_month_of_year" as "store_sales_date_month_of_year",
    "yummy"."avg_monthly_sales" as "avg_monthly_sales",
    "yummy"."sum_sales" as "sum_sales",
    "yummy"."psum" as "psum",
    "yummy"."nsum" as "nsum"
FROM
    "yummy"
WHERE
    "yummy"."store_sales_date_year" = 1999 and "yummy"."avg_monthly_sales" > 0 and CASE
	WHEN "yummy"."avg_monthly_sales" > 0 THEN abs("yummy"."sum_sales" - "yummy"."avg_monthly_sales") / "yummy"."avg_monthly_sales"
	ELSE null
	END > 0.1

ORDER BY 
    "yummy"."sum_minus_avg" asc,
    "yummy"."store_sales_item_category" asc,
    "yummy"."store_sales_item_brand_name" asc,
    "yummy"."store_sales_store_name" asc,
    "yummy"."store_sales_store_company_name" asc,
    "yummy"."store_sales_date_year" asc,
    "yummy"."store_sales_date_month_of_year" asc,
    "yummy"."avg_monthly_sales" asc,
    "yummy"."sum_sales" asc,
    "yummy"."psum" asc,
    "yummy"."nsum" asc
LIMIT (100)
```

## Reference SQL (zquery log)

```sql
WITH 
thoughtful as (
SELECT
    "store_sales_date_date"."D_MOY" as "store_sales_date_month_of_year",
    "store_sales_date_date"."D_YEAR" as "store_sales_date_year",
    "store_sales_item_items"."I_BRAND" as "store_sales_item_brand_name",
    "store_sales_item_items"."I_CATEGORY" as "store_sales_item_category",
    "store_sales_store_store"."S_COMPANY_NAME" as "store_sales_store_company_name",
    "store_sales_store_store"."S_STORE_NAME" as "store_sales_store_name",
    sum("store_sales_store_sales"."SS_SALES_PRICE") as "sum_sales"
FROM
    "memory"."store_sales" as "store_sales_store_sales"
    INNER JOIN "memory"."date_dim" as "store_sales_date_date" on "store_sales_store_sales"."SS_SOLD_DATE_SK" = "store_sales_date_date"."D_DATE_SK"
    INNER JOIN "memory"."item" as "store_sales_item_items" on "store_sales_store_sales"."SS_ITEM_SK" = "store_sales_item_items"."I_ITEM_SK"
    INNER JOIN "memory"."store" as "store_sales_store_store" on "store_sales_store_sales"."SS_STORE_SK" = "store_sales_store_store"."S_STORE_SK"
WHERE
    "store_sales_store_sales"."SS_STORE_SK" is not null and ( "store_sales_date_date"."D_YEAR" = 1999 or ( "store_sales_date_date"."D_YEAR" = 1998 and "store_sales_date_date"."D_MOY" = 12 ) or ( "store_sales_date_date"."D_YEAR" = 2000 and "store_sales_date_date"."D_MOY" = 1 ) )

GROUP BY
    1,
    2,
    3,
    4,
    5,
    6),
abundant as (
SELECT
    "thoughtful"."store_sales_date_year" as "store_sales_date_year",
    "thoughtful"."store_sales_item_brand_name" as "store_sales_item_brand_name",
    "thoughtful"."store_sales_item_category" as "store_sales_item_category",
    "thoughtful"."store_sales_store_company_name" as "store_sales_store_company_name",
    "thoughtful"."store_sales_store_name" as "store_sales_store_name",
    avg("thoughtful"."sum_sales") as "avg_monthly_sales"
FROM
    "thoughtful"
WHERE
    "thoughtful"."store_sales_date_year" = 1999

GROUP BY
    1,
    2,
    3,
    4,
    5),
questionable as (
SELECT
    "thoughtful"."store_sales_date_month_of_year" as "store_sales_date_month_of_year",
    "thoughtful"."store_sales_date_year" as "store_sales_date_year",
    "thoughtful"."store_sales_item_brand_name" as "store_sales_item_brand_name",
    "thoughtful"."store_sales_item_category" as "store_sales_item_category",
    "thoughtful"."store_sales_store_company_name" as "store_sales_store_company_name",
    "thoughtful"."store_sales_store_name" as "store_sales_store_name",
    "thoughtful"."sum_sales" as "sum_sales",
    lag("thoughtful"."sum_sales", 1) over (partition by "thoughtful"."store_sales_item_category","thoughtful"."store_sales_item_brand_name","thoughtful"."store_sales_store_name","thoughtful"."store_sales_store_company_name" order by "thoughtful"."store_sales_date_year" asc,"thoughtful"."store_sales_date_month_of_year" asc ) as "psum",
    lead("thoughtful"."sum_sales", 1) over (partition by "thoughtful"."store_sales_item_category","thoughtful"."store_sales_item_brand_name","thoughtful"."store_sales_store_name","thoughtful"."store_sales_store_company_name" order by "thoughtful"."store_sales_date_year" asc,"thoughtful"."store_sales_date_month_of_year" asc ) as "nsum"
FROM
    "thoughtful")
SELECT
    coalesce("abundant"."store_sales_item_category","questionable"."store_sales_item_category") as "store_sales_item_category",
    coalesce("abundant"."store_sales_item_brand_name","questionable"."store_sales_item_brand_name") as "store_sales_item_brand_name",
    coalesce("abundant"."store_sales_store_name","questionable"."store_sales_store_name") as "store_sales_store_name",
    coalesce("abundant"."store_sales_store_company_name","questionable"."store_sales_store_company_name") as "store_sales_store_company_name",
    coalesce("abundant"."store_sales_date_year","questionable"."store_sales_date_year") as "store_sales_date_year",
    "questionable"."store_sales_date_month_of_year" as "store_sales_date_month_of_year",
    "abundant"."avg_monthly_sales" as "avg_monthly_sales",
    "questionable"."sum_sales" as "sum_sales",
    "questionable"."psum" as "psum",
    "questionable"."nsum" as "nsum"
FROM
    "abundant"
    LEFT OUTER JOIN "questionable" on "abundant"."store_sales_date_year" = "questionable"."store_sales_date_year" AND "abundant"."store_sales_item_brand_name" = "questionable"."store_sales_item_brand_name" AND "abundant"."store_sales_item_category" is not distinct from "questionable"."store_sales_item_category" AND "abundant"."store_sales_store_company_name" is not distinct from "questionable"."store_sales_store_company_name" AND "abundant"."store_sales_store_name" = "questionable"."store_sales_store_name"
WHERE
    coalesce("abundant"."store_sales_date_year","questionable"."store_sales_date_year") = 1999 and "abundant"."avg_monthly_sales" > 0 and CASE
	WHEN "abundant"."avg_monthly_sales" > 0 THEN abs("questionable"."sum_sales" - "abundant"."avg_monthly_sales") / "abundant"."avg_monthly_sales"
	ELSE null
	END > 0.1

ORDER BY 
    "questionable"."sum_sales" - "abundant"."avg_monthly_sales" asc,
    coalesce("abundant"."store_sales_item_category","questionable"."store_sales_item_category") asc,
    coalesce("abundant"."store_sales_item_brand_name","questionable"."store_sales_item_brand_name") asc,
    coalesce("abundant"."store_sales_store_name","questionable"."store_sales_store_name") asc,
    coalesce("abundant"."store_sales_store_company_name","questionable"."store_sales_store_company_name") asc,
    coalesce("abundant"."store_sales_date_year","questionable"."store_sales_date_year") asc,
    "questionable"."store_sales_date_month_of_year" asc,
    "abundant"."avg_monthly_sales" asc,
    "questionable"."sum_sales" asc,
    "questionable"."psum" asc,
    "questionable"."nsum" asc
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
_duckdb.BinderException: Binder Error: Referenced table "cooperative" not found!
Candidate tables: "thoughtful"

LINE 76:     lag("cooperative"."sum_sales", 1) over (partition by "thoughtful...
                 ^
```
