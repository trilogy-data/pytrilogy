# Query 47

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
| v4 | 7403 | 106 | 294.66 ms |
| reference | 5746 | 90 | 205.94 ms |
| v4 / ref | 1.29x | 1.18x | 1.43x |

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
    lag("thoughtful"."sum_sales", 1) over (partition by "thoughtful"."store_sales_item_category","thoughtful"."store_sales_item_brand_name","thoughtful"."store_sales_store_name","thoughtful"."store_sales_store_company_name" order by "thoughtful"."store_sales_date_year" asc,"thoughtful"."store_sales_date_month_of_year" asc ) as "psum",
    lead("thoughtful"."sum_sales", 1) over (partition by "thoughtful"."store_sales_item_category","thoughtful"."store_sales_item_brand_name","thoughtful"."store_sales_store_name","thoughtful"."store_sales_store_company_name" order by "thoughtful"."store_sales_date_year" asc,"thoughtful"."store_sales_date_month_of_year" asc ) as "nsum"
FROM
    "thoughtful"),
uneven as (
SELECT
    "abundant"."avg_monthly_sales" as "avg_monthly_sales",
    "thoughtful"."store_sales_date_month_of_year" as "store_sales_date_month_of_year",
    "thoughtful"."sum_sales" - "abundant"."avg_monthly_sales" as "sum_minus_avg",
    "thoughtful"."sum_sales" as "sum_sales",
    coalesce("abundant"."store_sales_date_year","thoughtful"."store_sales_date_year") as "store_sales_date_year",
    coalesce("abundant"."store_sales_item_brand_name","thoughtful"."store_sales_item_brand_name") as "store_sales_item_brand_name",
    coalesce("abundant"."store_sales_item_category","thoughtful"."store_sales_item_category") as "store_sales_item_category",
    coalesce("abundant"."store_sales_store_company_name","thoughtful"."store_sales_store_company_name") as "store_sales_store_company_name",
    coalesce("abundant"."store_sales_store_name","thoughtful"."store_sales_store_name") as "store_sales_store_name"
FROM
    "abundant"
    LEFT OUTER JOIN "thoughtful" on "abundant"."store_sales_date_year" = "thoughtful"."store_sales_date_year" AND "abundant"."store_sales_item_brand_name" = "thoughtful"."store_sales_item_brand_name" AND "abundant"."store_sales_item_category" is not distinct from "thoughtful"."store_sales_item_category" AND "abundant"."store_sales_store_company_name" is not distinct from "thoughtful"."store_sales_store_company_name" AND "abundant"."store_sales_store_name" is not distinct from "thoughtful"."store_sales_store_name"
WHERE
    coalesce("abundant"."store_sales_date_year","thoughtful"."store_sales_date_year") = 1999 and "abundant"."avg_monthly_sales" > 0
)
SELECT
    coalesce("questionable"."store_sales_item_category","uneven"."store_sales_item_category") as "store_sales_item_category",
    coalesce("questionable"."store_sales_item_brand_name","uneven"."store_sales_item_brand_name") as "store_sales_item_brand_name",
    coalesce("questionable"."store_sales_store_name","uneven"."store_sales_store_name") as "store_sales_store_name",
    coalesce("questionable"."store_sales_store_company_name","uneven"."store_sales_store_company_name") as "store_sales_store_company_name",
    coalesce("questionable"."store_sales_date_year","uneven"."store_sales_date_year") as "store_sales_date_year",
    coalesce("questionable"."store_sales_date_month_of_year","uneven"."store_sales_date_month_of_year") as "store_sales_date_month_of_year",
    "uneven"."avg_monthly_sales" as "avg_monthly_sales",
    "uneven"."sum_sales" as "sum_sales",
    "questionable"."psum" as "psum",
    "questionable"."nsum" as "nsum"
FROM
    "uneven"
    LEFT OUTER JOIN "questionable" on "uneven"."store_sales_date_month_of_year" = "questionable"."store_sales_date_month_of_year" AND "uneven"."store_sales_date_year" = "questionable"."store_sales_date_year" AND "uneven"."store_sales_item_brand_name" = "questionable"."store_sales_item_brand_name" AND "uneven"."store_sales_item_category" is not distinct from "questionable"."store_sales_item_category" AND "uneven"."store_sales_store_company_name" is not distinct from "questionable"."store_sales_store_company_name" AND "uneven"."store_sales_store_name" = "questionable"."store_sales_store_name"
WHERE
    coalesce("questionable"."store_sales_date_year","uneven"."store_sales_date_year") = 1999 and "uneven"."avg_monthly_sales" > 0 and CASE
	WHEN "uneven"."avg_monthly_sales" > 0 THEN abs("uneven"."sum_sales" - "uneven"."avg_monthly_sales") / "uneven"."avg_monthly_sales"
	ELSE null
	END > 0.1

ORDER BY 
    "uneven"."sum_minus_avg" asc,
    coalesce("questionable"."store_sales_item_category","uneven"."store_sales_item_category") asc,
    coalesce("questionable"."store_sales_item_brand_name","uneven"."store_sales_item_brand_name") asc,
    coalesce("questionable"."store_sales_store_name","uneven"."store_sales_store_name") asc,
    coalesce("questionable"."store_sales_store_company_name","uneven"."store_sales_store_company_name") asc,
    coalesce("questionable"."store_sales_date_year","uneven"."store_sales_date_year") asc,
    coalesce("questionable"."store_sales_date_month_of_year","uneven"."store_sales_date_month_of_year") asc,
    "uneven"."avg_monthly_sales" asc,
    "uneven"."sum_sales" asc,
    "questionable"."psum" asc,
    "questionable"."nsum" asc
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
