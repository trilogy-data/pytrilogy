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
| v4 | 7966 | 101 | 167.72 ms |
| reference | 7966 | 101 | 159.04 ms |
| v4 / ref | 1.00x | 1.00x | 1.05x |

## Preql

```
import physical_sales as physical_sales;

auto sum_sales <- sum(physical_sales.sales_price)
    by physical_sales.item.category, physical_sales.item.brand_name, physical_sales.store.name, physical_sales.store.company_name, physical_sales.date.year, physical_sales.date.month_of_year;
auto avg_monthly_sales <- avg(sum_sales)
    by physical_sales.item.category, physical_sales.item.brand_name, physical_sales.store.name, physical_sales.store.company_name, physical_sales.date.year;
auto psum <- lag(sum_sales, 1)
    over (partition by physical_sales.item.category,
            physical_sales.item.brand_name,
            physical_sales.store.name,
            physical_sales.store.company_name
        order by physical_sales.date.year asc, physical_sales.date.month_of_year asc);
auto nsum <- lead(sum_sales, 1)
    over (partition by physical_sales.item.category,
            physical_sales.item.brand_name,
            physical_sales.store.name,
            physical_sales.store.company_name
        order by physical_sales.date.year asc, physical_sales.date.month_of_year asc);
auto sum_minus_avg <- sum_sales - avg_monthly_sales;

where
    physical_sales.store.id is not null
    and (
        physical_sales.date.year = 1999
        or (physical_sales.date.year = 1998 and physical_sales.date.month_of_year = 12)
        or (physical_sales.date.year = 2000 and physical_sales.date.month_of_year = 1)
    )
select
    physical_sales.item.category,
    physical_sales.item.brand_name,
    physical_sales.store.name,
    physical_sales.store.company_name,
    physical_sales.date.year,
    physical_sales.date.month_of_year,
    avg_monthly_sales,
    sum_sales,
    psum,
    nsum,
    --sum_minus_avg,
having
    physical_sales.date.year = 1999
    and avg_monthly_sales > 0
    and case
            when avg_monthly_sales > 0 then abs(sum_sales - avg_monthly_sales) / avg_monthly_sales
            else null
        end > 0.1

order by
    sum_minus_avg asc,
    physical_sales.item.category asc,
    physical_sales.item.brand_name asc,
    physical_sales.store.name asc,
    physical_sales.store.company_name asc,
    physical_sales.date.year asc,
    physical_sales.date.month_of_year asc,
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
    "physical_sales_date_date"."D_MOY" as "physical_sales_date_month_of_year",
    "physical_sales_date_date"."D_YEAR" as "physical_sales_date_year",
    "physical_sales_item_items"."I_BRAND" as "physical_sales_item_brand_name",
    "physical_sales_item_items"."I_CATEGORY" as "physical_sales_item_category",
    "physical_sales_store_store"."S_COMPANY_NAME" as "physical_sales_store_company_name",
    "physical_sales_store_store"."S_STORE_NAME" as "physical_sales_store_name",
    sum("physical_sales_store_sales"."SS_SALES_PRICE") as "sum_sales"
FROM
    "memory"."store_sales" as "physical_sales_store_sales"
    INNER JOIN "memory"."date_dim" as "physical_sales_date_date" on "physical_sales_store_sales"."SS_SOLD_DATE_SK" = "physical_sales_date_date"."D_DATE_SK"
    INNER JOIN "memory"."item" as "physical_sales_item_items" on "physical_sales_store_sales"."SS_ITEM_SK" = "physical_sales_item_items"."I_ITEM_SK"
    INNER JOIN "memory"."store" as "physical_sales_store_store" on "physical_sales_store_sales"."SS_STORE_SK" = "physical_sales_store_store"."S_STORE_SK"
WHERE
    "physical_sales_store_sales"."SS_STORE_SK" is not null and ( "physical_sales_date_date"."D_YEAR" = 1999 or ( "physical_sales_date_date"."D_YEAR" = 1998 and "physical_sales_date_date"."D_MOY" = 12 ) or ( "physical_sales_date_date"."D_YEAR" = 2000 and "physical_sales_date_date"."D_MOY" = 1 ) )

GROUP BY
    1,
    2,
    3,
    4,
    5,
    6),
abundant as (
SELECT
    "thoughtful"."physical_sales_date_month_of_year" as "physical_sales_date_month_of_year",
    "thoughtful"."physical_sales_date_year" as "physical_sales_date_year",
    "thoughtful"."physical_sales_item_brand_name" as "physical_sales_item_brand_name",
    "thoughtful"."physical_sales_item_category" as "physical_sales_item_category",
    "thoughtful"."physical_sales_store_company_name" as "physical_sales_store_company_name",
    "thoughtful"."physical_sales_store_name" as "physical_sales_store_name",
    "thoughtful"."sum_sales" as "sum_sales"
FROM
    "thoughtful"),
uneven as (
SELECT
    "abundant"."physical_sales_date_year" as "physical_sales_date_year",
    "abundant"."physical_sales_item_brand_name" as "physical_sales_item_brand_name",
    "abundant"."physical_sales_item_category" as "physical_sales_item_category",
    "abundant"."physical_sales_store_company_name" as "physical_sales_store_company_name",
    "abundant"."physical_sales_store_name" as "physical_sales_store_name",
    avg("abundant"."sum_sales") as "avg_monthly_sales"
FROM
    "abundant"
GROUP BY
    1,
    2,
    3,
    4,
    5),
yummy as (
SELECT
    "thoughtful"."physical_sales_date_month_of_year" as "physical_sales_date_month_of_year",
    "thoughtful"."sum_sales" - "uneven"."avg_monthly_sales" as "sum_minus_avg",
    "thoughtful"."sum_sales" as "sum_sales",
    "uneven"."avg_monthly_sales" as "avg_monthly_sales",
    coalesce("thoughtful"."physical_sales_date_year","uneven"."physical_sales_date_year") as "physical_sales_date_year",
    coalesce("thoughtful"."physical_sales_item_brand_name","uneven"."physical_sales_item_brand_name") as "physical_sales_item_brand_name",
    coalesce("thoughtful"."physical_sales_item_category","uneven"."physical_sales_item_category") as "physical_sales_item_category",
    coalesce("thoughtful"."physical_sales_store_company_name","uneven"."physical_sales_store_company_name") as "physical_sales_store_company_name",
    coalesce("thoughtful"."physical_sales_store_name","uneven"."physical_sales_store_name") as "physical_sales_store_name",
    lag("thoughtful"."sum_sales", 1) over (partition by coalesce("thoughtful"."physical_sales_item_category","uneven"."physical_sales_item_category"),coalesce("thoughtful"."physical_sales_item_brand_name","uneven"."physical_sales_item_brand_name"),coalesce("thoughtful"."physical_sales_store_name","uneven"."physical_sales_store_name"),coalesce("thoughtful"."physical_sales_store_company_name","uneven"."physical_sales_store_company_name") order by coalesce("thoughtful"."physical_sales_date_year","uneven"."physical_sales_date_year") asc,"thoughtful"."physical_sales_date_month_of_year" asc ) as "psum",
    lead("thoughtful"."sum_sales", 1) over (partition by coalesce("thoughtful"."physical_sales_item_category","uneven"."physical_sales_item_category"),coalesce("thoughtful"."physical_sales_item_brand_name","uneven"."physical_sales_item_brand_name"),coalesce("thoughtful"."physical_sales_store_name","uneven"."physical_sales_store_name"),coalesce("thoughtful"."physical_sales_store_company_name","uneven"."physical_sales_store_company_name") order by coalesce("thoughtful"."physical_sales_date_year","uneven"."physical_sales_date_year") asc,"thoughtful"."physical_sales_date_month_of_year" asc ) as "nsum"
FROM
    "thoughtful"
    INNER JOIN "uneven" on "thoughtful"."physical_sales_date_year" = "uneven"."physical_sales_date_year" AND "thoughtful"."physical_sales_item_brand_name" = "uneven"."physical_sales_item_brand_name" AND "thoughtful"."physical_sales_item_category" is not distinct from "uneven"."physical_sales_item_category" AND "thoughtful"."physical_sales_store_company_name" is not distinct from "uneven"."physical_sales_store_company_name" AND "thoughtful"."physical_sales_store_name" is not distinct from "uneven"."physical_sales_store_name")
SELECT
    coalesce("abundant"."physical_sales_item_category","yummy"."physical_sales_item_category") as "physical_sales_item_category",
    coalesce("abundant"."physical_sales_item_brand_name","yummy"."physical_sales_item_brand_name") as "physical_sales_item_brand_name",
    coalesce("abundant"."physical_sales_store_name","yummy"."physical_sales_store_name") as "physical_sales_store_name",
    coalesce("abundant"."physical_sales_store_company_name","yummy"."physical_sales_store_company_name") as "physical_sales_store_company_name",
    coalesce("abundant"."physical_sales_date_year","yummy"."physical_sales_date_year") as "physical_sales_date_year",
    coalesce("abundant"."physical_sales_date_month_of_year","yummy"."physical_sales_date_month_of_year") as "physical_sales_date_month_of_year",
    "yummy"."avg_monthly_sales" as "avg_monthly_sales",
    "yummy"."sum_sales" as "sum_sales",
    "yummy"."psum" as "psum",
    "yummy"."nsum" as "nsum"
FROM
    "yummy"
    LEFT OUTER JOIN "abundant" on "yummy"."physical_sales_date_month_of_year" = "abundant"."physical_sales_date_month_of_year" AND "yummy"."physical_sales_date_year" = "abundant"."physical_sales_date_year" AND "yummy"."physical_sales_item_brand_name" = "abundant"."physical_sales_item_brand_name" AND "yummy"."physical_sales_item_category" is not distinct from "abundant"."physical_sales_item_category" AND "yummy"."physical_sales_store_company_name" is not distinct from "abundant"."physical_sales_store_company_name" AND "yummy"."physical_sales_store_name" = "abundant"."physical_sales_store_name"
WHERE
    coalesce("abundant"."physical_sales_date_year","yummy"."physical_sales_date_year") = 1999 and "yummy"."avg_monthly_sales" > 0 and CASE
	WHEN "yummy"."avg_monthly_sales" > 0 THEN abs("yummy"."sum_sales" - "yummy"."avg_monthly_sales") / "yummy"."avg_monthly_sales"
	ELSE null
	END > 0.1

ORDER BY 
    "yummy"."sum_minus_avg" asc,
    coalesce("abundant"."physical_sales_item_category","yummy"."physical_sales_item_category") asc,
    coalesce("abundant"."physical_sales_item_brand_name","yummy"."physical_sales_item_brand_name") asc,
    coalesce("abundant"."physical_sales_store_name","yummy"."physical_sales_store_name") asc,
    coalesce("abundant"."physical_sales_store_company_name","yummy"."physical_sales_store_company_name") asc,
    coalesce("abundant"."physical_sales_date_year","yummy"."physical_sales_date_year") asc,
    coalesce("abundant"."physical_sales_date_month_of_year","yummy"."physical_sales_date_month_of_year") asc,
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
    "physical_sales_date_date"."D_MOY" as "physical_sales_date_month_of_year",
    "physical_sales_date_date"."D_YEAR" as "physical_sales_date_year",
    "physical_sales_item_items"."I_BRAND" as "physical_sales_item_brand_name",
    "physical_sales_item_items"."I_CATEGORY" as "physical_sales_item_category",
    "physical_sales_store_store"."S_COMPANY_NAME" as "physical_sales_store_company_name",
    "physical_sales_store_store"."S_STORE_NAME" as "physical_sales_store_name",
    sum("physical_sales_store_sales"."SS_SALES_PRICE") as "sum_sales"
FROM
    "memory"."store_sales" as "physical_sales_store_sales"
    INNER JOIN "memory"."date_dim" as "physical_sales_date_date" on "physical_sales_store_sales"."SS_SOLD_DATE_SK" = "physical_sales_date_date"."D_DATE_SK"
    INNER JOIN "memory"."item" as "physical_sales_item_items" on "physical_sales_store_sales"."SS_ITEM_SK" = "physical_sales_item_items"."I_ITEM_SK"
    INNER JOIN "memory"."store" as "physical_sales_store_store" on "physical_sales_store_sales"."SS_STORE_SK" = "physical_sales_store_store"."S_STORE_SK"
WHERE
    "physical_sales_store_sales"."SS_STORE_SK" is not null and ( "physical_sales_date_date"."D_YEAR" = 1999 or ( "physical_sales_date_date"."D_YEAR" = 1998 and "physical_sales_date_date"."D_MOY" = 12 ) or ( "physical_sales_date_date"."D_YEAR" = 2000 and "physical_sales_date_date"."D_MOY" = 1 ) )

GROUP BY
    1,
    2,
    3,
    4,
    5,
    6),
abundant as (
SELECT
    "thoughtful"."physical_sales_date_month_of_year" as "physical_sales_date_month_of_year",
    "thoughtful"."physical_sales_date_year" as "physical_sales_date_year",
    "thoughtful"."physical_sales_item_brand_name" as "physical_sales_item_brand_name",
    "thoughtful"."physical_sales_item_category" as "physical_sales_item_category",
    "thoughtful"."physical_sales_store_company_name" as "physical_sales_store_company_name",
    "thoughtful"."physical_sales_store_name" as "physical_sales_store_name",
    "thoughtful"."sum_sales" as "sum_sales"
FROM
    "thoughtful"),
uneven as (
SELECT
    "abundant"."physical_sales_date_year" as "physical_sales_date_year",
    "abundant"."physical_sales_item_brand_name" as "physical_sales_item_brand_name",
    "abundant"."physical_sales_item_category" as "physical_sales_item_category",
    "abundant"."physical_sales_store_company_name" as "physical_sales_store_company_name",
    "abundant"."physical_sales_store_name" as "physical_sales_store_name",
    avg("abundant"."sum_sales") as "avg_monthly_sales"
FROM
    "abundant"
GROUP BY
    1,
    2,
    3,
    4,
    5),
yummy as (
SELECT
    "thoughtful"."physical_sales_date_month_of_year" as "physical_sales_date_month_of_year",
    "thoughtful"."sum_sales" - "uneven"."avg_monthly_sales" as "sum_minus_avg",
    "thoughtful"."sum_sales" as "sum_sales",
    "uneven"."avg_monthly_sales" as "avg_monthly_sales",
    coalesce("thoughtful"."physical_sales_date_year","uneven"."physical_sales_date_year") as "physical_sales_date_year",
    coalesce("thoughtful"."physical_sales_item_brand_name","uneven"."physical_sales_item_brand_name") as "physical_sales_item_brand_name",
    coalesce("thoughtful"."physical_sales_item_category","uneven"."physical_sales_item_category") as "physical_sales_item_category",
    coalesce("thoughtful"."physical_sales_store_company_name","uneven"."physical_sales_store_company_name") as "physical_sales_store_company_name",
    coalesce("thoughtful"."physical_sales_store_name","uneven"."physical_sales_store_name") as "physical_sales_store_name",
    lag("thoughtful"."sum_sales", 1) over (partition by coalesce("thoughtful"."physical_sales_item_category","uneven"."physical_sales_item_category"),coalesce("thoughtful"."physical_sales_item_brand_name","uneven"."physical_sales_item_brand_name"),coalesce("thoughtful"."physical_sales_store_name","uneven"."physical_sales_store_name"),coalesce("thoughtful"."physical_sales_store_company_name","uneven"."physical_sales_store_company_name") order by coalesce("thoughtful"."physical_sales_date_year","uneven"."physical_sales_date_year") asc,"thoughtful"."physical_sales_date_month_of_year" asc ) as "psum",
    lead("thoughtful"."sum_sales", 1) over (partition by coalesce("thoughtful"."physical_sales_item_category","uneven"."physical_sales_item_category"),coalesce("thoughtful"."physical_sales_item_brand_name","uneven"."physical_sales_item_brand_name"),coalesce("thoughtful"."physical_sales_store_name","uneven"."physical_sales_store_name"),coalesce("thoughtful"."physical_sales_store_company_name","uneven"."physical_sales_store_company_name") order by coalesce("thoughtful"."physical_sales_date_year","uneven"."physical_sales_date_year") asc,"thoughtful"."physical_sales_date_month_of_year" asc ) as "nsum"
FROM
    "thoughtful"
    INNER JOIN "uneven" on "thoughtful"."physical_sales_date_year" = "uneven"."physical_sales_date_year" AND "thoughtful"."physical_sales_item_brand_name" = "uneven"."physical_sales_item_brand_name" AND "thoughtful"."physical_sales_item_category" is not distinct from "uneven"."physical_sales_item_category" AND "thoughtful"."physical_sales_store_company_name" is not distinct from "uneven"."physical_sales_store_company_name" AND "thoughtful"."physical_sales_store_name" is not distinct from "uneven"."physical_sales_store_name")
SELECT
    coalesce("abundant"."physical_sales_item_category","yummy"."physical_sales_item_category") as "physical_sales_item_category",
    coalesce("abundant"."physical_sales_item_brand_name","yummy"."physical_sales_item_brand_name") as "physical_sales_item_brand_name",
    coalesce("abundant"."physical_sales_store_name","yummy"."physical_sales_store_name") as "physical_sales_store_name",
    coalesce("abundant"."physical_sales_store_company_name","yummy"."physical_sales_store_company_name") as "physical_sales_store_company_name",
    coalesce("abundant"."physical_sales_date_year","yummy"."physical_sales_date_year") as "physical_sales_date_year",
    coalesce("abundant"."physical_sales_date_month_of_year","yummy"."physical_sales_date_month_of_year") as "physical_sales_date_month_of_year",
    "yummy"."avg_monthly_sales" as "avg_monthly_sales",
    "yummy"."sum_sales" as "sum_sales",
    "yummy"."psum" as "psum",
    "yummy"."nsum" as "nsum"
FROM
    "yummy"
    LEFT OUTER JOIN "abundant" on "yummy"."physical_sales_date_month_of_year" = "abundant"."physical_sales_date_month_of_year" AND "yummy"."physical_sales_date_year" = "abundant"."physical_sales_date_year" AND "yummy"."physical_sales_item_brand_name" = "abundant"."physical_sales_item_brand_name" AND "yummy"."physical_sales_item_category" is not distinct from "abundant"."physical_sales_item_category" AND "yummy"."physical_sales_store_company_name" is not distinct from "abundant"."physical_sales_store_company_name" AND "yummy"."physical_sales_store_name" = "abundant"."physical_sales_store_name"
WHERE
    coalesce("abundant"."physical_sales_date_year","yummy"."physical_sales_date_year") = 1999 and "yummy"."avg_monthly_sales" > 0 and CASE
	WHEN "yummy"."avg_monthly_sales" > 0 THEN abs("yummy"."sum_sales" - "yummy"."avg_monthly_sales") / "yummy"."avg_monthly_sales"
	ELSE null
	END > 0.1

ORDER BY 
    "yummy"."sum_minus_avg" asc,
    coalesce("abundant"."physical_sales_item_category","yummy"."physical_sales_item_category") asc,
    coalesce("abundant"."physical_sales_item_brand_name","yummy"."physical_sales_item_brand_name") asc,
    coalesce("abundant"."physical_sales_store_name","yummy"."physical_sales_store_name") asc,
    coalesce("abundant"."physical_sales_store_company_name","yummy"."physical_sales_store_company_name") asc,
    coalesce("abundant"."physical_sales_date_year","yummy"."physical_sales_date_year") asc,
    coalesce("abundant"."physical_sales_date_month_of_year","yummy"."physical_sales_date_month_of_year") asc,
    "yummy"."avg_monthly_sales" asc,
    "yummy"."sum_sales" asc,
    "yummy"."psum" asc,
    "yummy"."nsum" asc
LIMIT (100)
```
