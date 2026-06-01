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
| v4 | 7815 | 106 | 215.57 ms |
| reference | 6059 | 90 | 183.20 ms |
| v4 / ref | 1.29x | 1.18x | 1.18x |

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
    "thoughtful"."physical_sales_date_year" as "physical_sales_date_year",
    "thoughtful"."physical_sales_item_brand_name" as "physical_sales_item_brand_name",
    "thoughtful"."physical_sales_item_category" as "physical_sales_item_category",
    "thoughtful"."physical_sales_store_company_name" as "physical_sales_store_company_name",
    "thoughtful"."physical_sales_store_name" as "physical_sales_store_name",
    avg("thoughtful"."sum_sales") as "avg_monthly_sales"
FROM
    "thoughtful"
WHERE
    "thoughtful"."physical_sales_date_year" = 1999

GROUP BY
    1,
    2,
    3,
    4,
    5),
questionable as (
SELECT
    "thoughtful"."physical_sales_date_month_of_year" as "physical_sales_date_month_of_year",
    "thoughtful"."physical_sales_date_year" as "physical_sales_date_year",
    "thoughtful"."physical_sales_item_brand_name" as "physical_sales_item_brand_name",
    "thoughtful"."physical_sales_item_category" as "physical_sales_item_category",
    "thoughtful"."physical_sales_store_company_name" as "physical_sales_store_company_name",
    "thoughtful"."physical_sales_store_name" as "physical_sales_store_name",
    lag("thoughtful"."sum_sales", 1) over (partition by "thoughtful"."physical_sales_item_category","thoughtful"."physical_sales_item_brand_name","thoughtful"."physical_sales_store_name","thoughtful"."physical_sales_store_company_name" order by "thoughtful"."physical_sales_date_year" asc,"thoughtful"."physical_sales_date_month_of_year" asc ) as "psum",
    lead("thoughtful"."sum_sales", 1) over (partition by "thoughtful"."physical_sales_item_category","thoughtful"."physical_sales_item_brand_name","thoughtful"."physical_sales_store_name","thoughtful"."physical_sales_store_company_name" order by "thoughtful"."physical_sales_date_year" asc,"thoughtful"."physical_sales_date_month_of_year" asc ) as "nsum"
FROM
    "thoughtful"),
uneven as (
SELECT
    "abundant"."avg_monthly_sales" as "avg_monthly_sales",
    "thoughtful"."physical_sales_date_month_of_year" as "physical_sales_date_month_of_year",
    "thoughtful"."sum_sales" - "abundant"."avg_monthly_sales" as "sum_minus_avg",
    "thoughtful"."sum_sales" as "sum_sales",
    coalesce("abundant"."physical_sales_date_year","thoughtful"."physical_sales_date_year") as "physical_sales_date_year",
    coalesce("abundant"."physical_sales_item_brand_name","thoughtful"."physical_sales_item_brand_name") as "physical_sales_item_brand_name",
    coalesce("abundant"."physical_sales_item_category","thoughtful"."physical_sales_item_category") as "physical_sales_item_category",
    coalesce("abundant"."physical_sales_store_company_name","thoughtful"."physical_sales_store_company_name") as "physical_sales_store_company_name",
    coalesce("abundant"."physical_sales_store_name","thoughtful"."physical_sales_store_name") as "physical_sales_store_name"
FROM
    "thoughtful"
    RIGHT OUTER JOIN "abundant" on "thoughtful"."physical_sales_date_year" = "abundant"."physical_sales_date_year" AND "thoughtful"."physical_sales_item_brand_name" = "abundant"."physical_sales_item_brand_name" AND "thoughtful"."physical_sales_item_category" is not distinct from "abundant"."physical_sales_item_category" AND "thoughtful"."physical_sales_store_company_name" is not distinct from "abundant"."physical_sales_store_company_name" AND "thoughtful"."physical_sales_store_name" is not distinct from "abundant"."physical_sales_store_name"
WHERE
    coalesce("abundant"."physical_sales_date_year","thoughtful"."physical_sales_date_year") = 1999 and "abundant"."avg_monthly_sales" > 0
)
SELECT
    coalesce("questionable"."physical_sales_item_category","uneven"."physical_sales_item_category") as "physical_sales_item_category",
    coalesce("questionable"."physical_sales_item_brand_name","uneven"."physical_sales_item_brand_name") as "physical_sales_item_brand_name",
    coalesce("questionable"."physical_sales_store_name","uneven"."physical_sales_store_name") as "physical_sales_store_name",
    coalesce("questionable"."physical_sales_store_company_name","uneven"."physical_sales_store_company_name") as "physical_sales_store_company_name",
    coalesce("questionable"."physical_sales_date_year","uneven"."physical_sales_date_year") as "physical_sales_date_year",
    coalesce("questionable"."physical_sales_date_month_of_year","uneven"."physical_sales_date_month_of_year") as "physical_sales_date_month_of_year",
    "uneven"."avg_monthly_sales" as "avg_monthly_sales",
    "uneven"."sum_sales" as "sum_sales",
    "questionable"."psum" as "psum",
    "questionable"."nsum" as "nsum"
FROM
    "uneven"
    LEFT OUTER JOIN "questionable" on "uneven"."physical_sales_date_month_of_year" = "questionable"."physical_sales_date_month_of_year" AND "uneven"."physical_sales_date_year" = "questionable"."physical_sales_date_year" AND "uneven"."physical_sales_item_brand_name" = "questionable"."physical_sales_item_brand_name" AND "uneven"."physical_sales_item_category" is not distinct from "questionable"."physical_sales_item_category" AND "uneven"."physical_sales_store_company_name" is not distinct from "questionable"."physical_sales_store_company_name" AND "uneven"."physical_sales_store_name" = "questionable"."physical_sales_store_name"
WHERE
    coalesce("questionable"."physical_sales_date_year","uneven"."physical_sales_date_year") = 1999 and "uneven"."avg_monthly_sales" > 0 and CASE
	WHEN "uneven"."avg_monthly_sales" > 0 THEN abs("uneven"."sum_sales" - "uneven"."avg_monthly_sales") / "uneven"."avg_monthly_sales"
	ELSE null
	END > 0.1

ORDER BY 
    "uneven"."sum_minus_avg" asc,
    coalesce("questionable"."physical_sales_item_category","uneven"."physical_sales_item_category") asc,
    coalesce("questionable"."physical_sales_item_brand_name","uneven"."physical_sales_item_brand_name") asc,
    coalesce("questionable"."physical_sales_store_name","uneven"."physical_sales_store_name") asc,
    coalesce("questionable"."physical_sales_store_company_name","uneven"."physical_sales_store_company_name") asc,
    coalesce("questionable"."physical_sales_date_year","uneven"."physical_sales_date_year") asc,
    coalesce("questionable"."physical_sales_date_month_of_year","uneven"."physical_sales_date_month_of_year") asc,
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
    "thoughtful"."physical_sales_date_year" as "physical_sales_date_year",
    "thoughtful"."physical_sales_item_brand_name" as "physical_sales_item_brand_name",
    "thoughtful"."physical_sales_item_category" as "physical_sales_item_category",
    "thoughtful"."physical_sales_store_company_name" as "physical_sales_store_company_name",
    "thoughtful"."physical_sales_store_name" as "physical_sales_store_name",
    avg("thoughtful"."sum_sales") as "avg_monthly_sales"
FROM
    "thoughtful"
WHERE
    "thoughtful"."physical_sales_date_year" = 1999

GROUP BY
    1,
    2,
    3,
    4,
    5),
questionable as (
SELECT
    "thoughtful"."physical_sales_date_month_of_year" as "physical_sales_date_month_of_year",
    "thoughtful"."physical_sales_date_year" as "physical_sales_date_year",
    "thoughtful"."physical_sales_item_brand_name" as "physical_sales_item_brand_name",
    "thoughtful"."physical_sales_item_category" as "physical_sales_item_category",
    "thoughtful"."physical_sales_store_company_name" as "physical_sales_store_company_name",
    "thoughtful"."physical_sales_store_name" as "physical_sales_store_name",
    "thoughtful"."sum_sales" as "sum_sales",
    lag("thoughtful"."sum_sales", 1) over (partition by "thoughtful"."physical_sales_item_category","thoughtful"."physical_sales_item_brand_name","thoughtful"."physical_sales_store_name","thoughtful"."physical_sales_store_company_name" order by "thoughtful"."physical_sales_date_year" asc,"thoughtful"."physical_sales_date_month_of_year" asc ) as "psum",
    lead("thoughtful"."sum_sales", 1) over (partition by "thoughtful"."physical_sales_item_category","thoughtful"."physical_sales_item_brand_name","thoughtful"."physical_sales_store_name","thoughtful"."physical_sales_store_company_name" order by "thoughtful"."physical_sales_date_year" asc,"thoughtful"."physical_sales_date_month_of_year" asc ) as "nsum"
FROM
    "thoughtful")
SELECT
    coalesce("abundant"."physical_sales_item_category","questionable"."physical_sales_item_category") as "physical_sales_item_category",
    coalesce("abundant"."physical_sales_item_brand_name","questionable"."physical_sales_item_brand_name") as "physical_sales_item_brand_name",
    coalesce("abundant"."physical_sales_store_name","questionable"."physical_sales_store_name") as "physical_sales_store_name",
    coalesce("abundant"."physical_sales_store_company_name","questionable"."physical_sales_store_company_name") as "physical_sales_store_company_name",
    coalesce("abundant"."physical_sales_date_year","questionable"."physical_sales_date_year") as "physical_sales_date_year",
    "questionable"."physical_sales_date_month_of_year" as "physical_sales_date_month_of_year",
    "abundant"."avg_monthly_sales" as "avg_monthly_sales",
    "questionable"."sum_sales" as "sum_sales",
    "questionable"."psum" as "psum",
    "questionable"."nsum" as "nsum"
FROM
    "questionable"
    RIGHT OUTER JOIN "abundant" on "questionable"."physical_sales_date_year" = "abundant"."physical_sales_date_year" AND "questionable"."physical_sales_item_brand_name" = "abundant"."physical_sales_item_brand_name" AND "questionable"."physical_sales_item_category" is not distinct from "abundant"."physical_sales_item_category" AND "questionable"."physical_sales_store_company_name" is not distinct from "abundant"."physical_sales_store_company_name" AND "questionable"."physical_sales_store_name" = "abundant"."physical_sales_store_name"
WHERE
    coalesce("abundant"."physical_sales_date_year","questionable"."physical_sales_date_year") = 1999 and "abundant"."avg_monthly_sales" > 0 and CASE
	WHEN "abundant"."avg_monthly_sales" > 0 THEN abs("questionable"."sum_sales" - "abundant"."avg_monthly_sales") / "abundant"."avg_monthly_sales"
	ELSE null
	END > 0.1

ORDER BY 
    "questionable"."sum_sales" - "abundant"."avg_monthly_sales" asc,
    coalesce("abundant"."physical_sales_item_category","questionable"."physical_sales_item_category") asc,
    coalesce("abundant"."physical_sales_item_brand_name","questionable"."physical_sales_item_brand_name") asc,
    coalesce("abundant"."physical_sales_store_name","questionable"."physical_sales_store_name") asc,
    coalesce("abundant"."physical_sales_store_company_name","questionable"."physical_sales_store_company_name") asc,
    coalesce("abundant"."physical_sales_date_year","questionable"."physical_sales_date_year") asc,
    "questionable"."physical_sales_date_month_of_year" asc,
    "abundant"."avg_monthly_sales" asc,
    "questionable"."sum_sales" asc,
    "questionable"."psum" asc,
    "questionable"."nsum" asc
LIMIT (100)
```
