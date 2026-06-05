# Query 57

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
| v4 | 6735 | 102 | 106.87 ms |
| reference | 5356 | 83 | 78.84 ms |
| v4 / ref | 1.26x | 1.23x | 1.36x |

## Preql

```
import catalog_sales as catalog_sales;

auto sum_sales <- sum(catalog_sales.sales_price)
    by catalog_sales.item.category, catalog_sales.item.brand_name, catalog_sales.call_center.name, catalog_sales.date.year, catalog_sales.date.month_of_year;
auto avg_monthly_sales <- avg(sum_sales)
    by catalog_sales.item.category, catalog_sales.item.brand_name, catalog_sales.call_center.name, catalog_sales.date.year;
auto psum <- lag(sum_sales, 1)
    over (partition by catalog_sales.item.category,
            catalog_sales.item.brand_name,
            catalog_sales.call_center.name
        order by catalog_sales.date.year asc, catalog_sales.date.month_of_year asc);
auto nsum <- lead(sum_sales, 1)
    over (partition by catalog_sales.item.category,
            catalog_sales.item.brand_name,
            catalog_sales.call_center.name
        order by catalog_sales.date.year asc, catalog_sales.date.month_of_year asc);
auto sum_minus_avg <- sum_sales - avg_monthly_sales;

where
    catalog_sales.call_center.id is not null
    and (
        catalog_sales.date.year = 1999
        or (catalog_sales.date.year = 1998 and catalog_sales.date.month_of_year = 12)
        or (catalog_sales.date.year = 2000 and catalog_sales.date.month_of_year = 1)
    )
select
    catalog_sales.item.category,
    catalog_sales.item.brand_name,
    catalog_sales.call_center.name,
    catalog_sales.date.year,
    catalog_sales.date.month_of_year,
    avg_monthly_sales,
    sum_sales,
    psum,
    nsum,
    --sum_minus_avg,
having
    catalog_sales.date.year = 1999
    and avg_monthly_sales > 0
    and case
            when avg_monthly_sales > 0 then abs(sum_sales - avg_monthly_sales) / avg_monthly_sales
            else null
        end > 0.1

order by
    sum_minus_avg asc nulls first,
    catalog_sales.item.category asc,
    catalog_sales.item.brand_name asc,
    catalog_sales.call_center.name asc,
    catalog_sales.date.year asc,
    catalog_sales.date.month_of_year asc,
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
    "catalog_sales_call_center_call_center"."CC_NAME" as "catalog_sales_call_center_name",
    "catalog_sales_date_date"."D_MOY" as "catalog_sales_date_month_of_year",
    "catalog_sales_date_date"."D_YEAR" as "catalog_sales_date_year",
    "catalog_sales_item_items"."I_BRAND" as "catalog_sales_item_brand_name",
    "catalog_sales_item_items"."I_CATEGORY" as "catalog_sales_item_category",
    sum("catalog_sales_catalog_sales"."CS_SALES_PRICE") as "sum_sales"
FROM
    "memory"."catalog_sales" as "catalog_sales_catalog_sales"
    INNER JOIN "memory"."date_dim" as "catalog_sales_date_date" on "catalog_sales_catalog_sales"."CS_SOLD_DATE_SK" = "catalog_sales_date_date"."D_DATE_SK"
    INNER JOIN "memory"."item" as "catalog_sales_item_items" on "catalog_sales_catalog_sales"."CS_ITEM_SK" = "catalog_sales_item_items"."I_ITEM_SK"
    INNER JOIN "memory"."call_center" as "catalog_sales_call_center_call_center" on "catalog_sales_catalog_sales"."CS_CALL_CENTER_SK" = "catalog_sales_call_center_call_center"."CC_CALL_CENTER_SK"
WHERE
    "catalog_sales_catalog_sales"."CS_CALL_CENTER_SK" is not null and ( "catalog_sales_date_date"."D_YEAR" = 1999 or ( "catalog_sales_date_date"."D_YEAR" = 1998 and "catalog_sales_date_date"."D_MOY" = 12 ) or ( "catalog_sales_date_date"."D_YEAR" = 2000 and "catalog_sales_date_date"."D_MOY" = 1 ) )

GROUP BY
    1,
    2,
    3,
    4,
    5),
abundant as (
SELECT
    "thoughtful"."catalog_sales_call_center_name" as "catalog_sales_call_center_name",
    "thoughtful"."catalog_sales_date_month_of_year" as "catalog_sales_date_month_of_year",
    "thoughtful"."catalog_sales_date_year" as "catalog_sales_date_year",
    "thoughtful"."catalog_sales_item_brand_name" as "catalog_sales_item_brand_name",
    "thoughtful"."catalog_sales_item_category" as "catalog_sales_item_category",
    "thoughtful"."sum_sales" as "sum_sales",
    lag("thoughtful"."sum_sales", 1) over (partition by "thoughtful"."catalog_sales_item_category","thoughtful"."catalog_sales_item_brand_name","thoughtful"."catalog_sales_call_center_name" order by "thoughtful"."catalog_sales_date_year" asc,"thoughtful"."catalog_sales_date_month_of_year" asc ) as "psum",
    lead("thoughtful"."sum_sales", 1) over (partition by "thoughtful"."catalog_sales_item_category","thoughtful"."catalog_sales_item_brand_name","thoughtful"."catalog_sales_call_center_name" order by "thoughtful"."catalog_sales_date_year" asc,"thoughtful"."catalog_sales_date_month_of_year" asc ) as "nsum"
FROM
    "thoughtful"),
uneven as (
SELECT
    "abundant"."catalog_sales_call_center_name" as "catalog_sales_call_center_name",
    "abundant"."catalog_sales_date_year" as "catalog_sales_date_year",
    "abundant"."catalog_sales_item_brand_name" as "catalog_sales_item_brand_name",
    "abundant"."catalog_sales_item_category" as "catalog_sales_item_category",
    avg("abundant"."sum_sales") as "avg_monthly_sales"
FROM
    "abundant"
WHERE
    "abundant"."catalog_sales_date_year" = 1999

GROUP BY
    1,
    2,
    3,
    4
HAVING
    "avg_monthly_sales" > 0
),
yummy as (
SELECT
    "thoughtful"."catalog_sales_date_month_of_year" as "catalog_sales_date_month_of_year",
    "thoughtful"."sum_sales" - "uneven"."avg_monthly_sales" as "sum_minus_avg",
    "thoughtful"."sum_sales" as "sum_sales",
    "uneven"."avg_monthly_sales" as "avg_monthly_sales",
    coalesce("thoughtful"."catalog_sales_call_center_name","uneven"."catalog_sales_call_center_name") as "catalog_sales_call_center_name",
    coalesce("thoughtful"."catalog_sales_date_year","uneven"."catalog_sales_date_year") as "catalog_sales_date_year",
    coalesce("thoughtful"."catalog_sales_item_brand_name","uneven"."catalog_sales_item_brand_name") as "catalog_sales_item_brand_name",
    coalesce("thoughtful"."catalog_sales_item_category","uneven"."catalog_sales_item_category") as "catalog_sales_item_category"
FROM
    "thoughtful"
    RIGHT OUTER JOIN "uneven" on "thoughtful"."catalog_sales_call_center_name" is not distinct from "uneven"."catalog_sales_call_center_name" AND "thoughtful"."catalog_sales_date_year" = "uneven"."catalog_sales_date_year" AND "thoughtful"."catalog_sales_item_brand_name" = "uneven"."catalog_sales_item_brand_name" AND "thoughtful"."catalog_sales_item_category" is not distinct from "uneven"."catalog_sales_item_category"
WHERE
    coalesce("thoughtful"."catalog_sales_date_year","uneven"."catalog_sales_date_year") = 1999
)
SELECT
    coalesce("abundant"."catalog_sales_item_category","yummy"."catalog_sales_item_category") as "catalog_sales_item_category",
    coalesce("abundant"."catalog_sales_item_brand_name","yummy"."catalog_sales_item_brand_name") as "catalog_sales_item_brand_name",
    coalesce("abundant"."catalog_sales_call_center_name","yummy"."catalog_sales_call_center_name") as "catalog_sales_call_center_name",
    coalesce("abundant"."catalog_sales_date_year","yummy"."catalog_sales_date_year") as "catalog_sales_date_year",
    coalesce("abundant"."catalog_sales_date_month_of_year","yummy"."catalog_sales_date_month_of_year") as "catalog_sales_date_month_of_year",
    "yummy"."avg_monthly_sales" as "avg_monthly_sales",
    "yummy"."sum_sales" as "sum_sales",
    "abundant"."psum" as "psum",
    "abundant"."nsum" as "nsum"
FROM
    "yummy"
    LEFT OUTER JOIN "abundant" on "yummy"."catalog_sales_call_center_name" = "abundant"."catalog_sales_call_center_name" AND "yummy"."catalog_sales_date_month_of_year" = "abundant"."catalog_sales_date_month_of_year" AND "yummy"."catalog_sales_date_year" = "abundant"."catalog_sales_date_year" AND "yummy"."catalog_sales_item_brand_name" = "abundant"."catalog_sales_item_brand_name" AND "yummy"."catalog_sales_item_category" is not distinct from "abundant"."catalog_sales_item_category"
WHERE
    coalesce("abundant"."catalog_sales_date_year","yummy"."catalog_sales_date_year") = 1999 and "yummy"."avg_monthly_sales" > 0 and CASE
	WHEN "yummy"."avg_monthly_sales" > 0 THEN abs("yummy"."sum_sales" - "yummy"."avg_monthly_sales") / "yummy"."avg_monthly_sales"
	ELSE null
	END > 0.1

ORDER BY 
    "yummy"."sum_minus_avg" asc nulls first,
    coalesce("abundant"."catalog_sales_item_category","yummy"."catalog_sales_item_category") asc,
    coalesce("abundant"."catalog_sales_item_brand_name","yummy"."catalog_sales_item_brand_name") asc,
    coalesce("abundant"."catalog_sales_call_center_name","yummy"."catalog_sales_call_center_name") asc,
    coalesce("abundant"."catalog_sales_date_year","yummy"."catalog_sales_date_year") asc,
    coalesce("abundant"."catalog_sales_date_month_of_year","yummy"."catalog_sales_date_month_of_year") asc,
    "yummy"."avg_monthly_sales" asc,
    "yummy"."sum_sales" asc,
    "abundant"."psum" asc,
    "abundant"."nsum" asc
LIMIT (100)
```

## Reference SQL (zquery log)

```sql
WITH 
thoughtful as (
SELECT
    "catalog_sales_call_center_call_center"."CC_NAME" as "catalog_sales_call_center_name",
    "catalog_sales_date_date"."D_MOY" as "catalog_sales_date_month_of_year",
    "catalog_sales_date_date"."D_YEAR" as "catalog_sales_date_year",
    "catalog_sales_item_items"."I_BRAND" as "catalog_sales_item_brand_name",
    "catalog_sales_item_items"."I_CATEGORY" as "catalog_sales_item_category",
    sum("catalog_sales_catalog_sales"."CS_SALES_PRICE") as "sum_sales"
FROM
    "memory"."catalog_sales" as "catalog_sales_catalog_sales"
    INNER JOIN "memory"."date_dim" as "catalog_sales_date_date" on "catalog_sales_catalog_sales"."CS_SOLD_DATE_SK" = "catalog_sales_date_date"."D_DATE_SK"
    INNER JOIN "memory"."item" as "catalog_sales_item_items" on "catalog_sales_catalog_sales"."CS_ITEM_SK" = "catalog_sales_item_items"."I_ITEM_SK"
    INNER JOIN "memory"."call_center" as "catalog_sales_call_center_call_center" on "catalog_sales_catalog_sales"."CS_CALL_CENTER_SK" = "catalog_sales_call_center_call_center"."CC_CALL_CENTER_SK"
WHERE
    "catalog_sales_catalog_sales"."CS_CALL_CENTER_SK" is not null and ( "catalog_sales_date_date"."D_YEAR" = 1999 or ( "catalog_sales_date_date"."D_YEAR" = 1998 and "catalog_sales_date_date"."D_MOY" = 12 ) or ( "catalog_sales_date_date"."D_YEAR" = 2000 and "catalog_sales_date_date"."D_MOY" = 1 ) )

GROUP BY
    1,
    2,
    3,
    4,
    5),
abundant as (
SELECT
    "thoughtful"."catalog_sales_call_center_name" as "catalog_sales_call_center_name",
    "thoughtful"."catalog_sales_date_year" as "catalog_sales_date_year",
    "thoughtful"."catalog_sales_item_brand_name" as "catalog_sales_item_brand_name",
    "thoughtful"."catalog_sales_item_category" as "catalog_sales_item_category",
    avg("thoughtful"."sum_sales") as "avg_monthly_sales"
FROM
    "thoughtful"
WHERE
    "thoughtful"."catalog_sales_date_year" = 1999

GROUP BY
    1,
    2,
    3,
    4),
questionable as (
SELECT
    "thoughtful"."catalog_sales_call_center_name" as "catalog_sales_call_center_name",
    "thoughtful"."catalog_sales_date_month_of_year" as "catalog_sales_date_month_of_year",
    "thoughtful"."catalog_sales_date_year" as "catalog_sales_date_year",
    "thoughtful"."catalog_sales_item_brand_name" as "catalog_sales_item_brand_name",
    "thoughtful"."catalog_sales_item_category" as "catalog_sales_item_category",
    "thoughtful"."sum_sales" as "sum_sales",
    lag("thoughtful"."sum_sales", 1) over (partition by "thoughtful"."catalog_sales_item_category","thoughtful"."catalog_sales_item_brand_name","thoughtful"."catalog_sales_call_center_name" order by "thoughtful"."catalog_sales_date_year" asc,"thoughtful"."catalog_sales_date_month_of_year" asc ) as "psum",
    lead("thoughtful"."sum_sales", 1) over (partition by "thoughtful"."catalog_sales_item_category","thoughtful"."catalog_sales_item_brand_name","thoughtful"."catalog_sales_call_center_name" order by "thoughtful"."catalog_sales_date_year" asc,"thoughtful"."catalog_sales_date_month_of_year" asc ) as "nsum"
FROM
    "thoughtful")
SELECT
    coalesce("abundant"."catalog_sales_item_category","questionable"."catalog_sales_item_category") as "catalog_sales_item_category",
    coalesce("abundant"."catalog_sales_item_brand_name","questionable"."catalog_sales_item_brand_name") as "catalog_sales_item_brand_name",
    coalesce("abundant"."catalog_sales_call_center_name","questionable"."catalog_sales_call_center_name") as "catalog_sales_call_center_name",
    coalesce("abundant"."catalog_sales_date_year","questionable"."catalog_sales_date_year") as "catalog_sales_date_year",
    "questionable"."catalog_sales_date_month_of_year" as "catalog_sales_date_month_of_year",
    "abundant"."avg_monthly_sales" as "avg_monthly_sales",
    "questionable"."sum_sales" as "sum_sales",
    "questionable"."psum" as "psum",
    "questionable"."nsum" as "nsum"
FROM
    "questionable"
    RIGHT OUTER JOIN "abundant" on "questionable"."catalog_sales_call_center_name" = "abundant"."catalog_sales_call_center_name" AND "questionable"."catalog_sales_date_year" = "abundant"."catalog_sales_date_year" AND "questionable"."catalog_sales_item_brand_name" = "abundant"."catalog_sales_item_brand_name" AND "questionable"."catalog_sales_item_category" is not distinct from "abundant"."catalog_sales_item_category"
WHERE
    coalesce("abundant"."catalog_sales_date_year","questionable"."catalog_sales_date_year") = 1999 and "abundant"."avg_monthly_sales" > 0 and CASE
	WHEN "abundant"."avg_monthly_sales" > 0 THEN abs("questionable"."sum_sales" - "abundant"."avg_monthly_sales") / "abundant"."avg_monthly_sales"
	ELSE null
	END > 0.1

ORDER BY 
    "questionable"."sum_sales" - "abundant"."avg_monthly_sales" asc nulls first,
    coalesce("abundant"."catalog_sales_item_category","questionable"."catalog_sales_item_category") asc,
    coalesce("abundant"."catalog_sales_item_brand_name","questionable"."catalog_sales_item_brand_name") asc,
    coalesce("abundant"."catalog_sales_call_center_name","questionable"."catalog_sales_call_center_name") asc,
    coalesce("abundant"."catalog_sales_date_year","questionable"."catalog_sales_date_year") asc,
    "questionable"."catalog_sales_date_month_of_year" asc,
    "abundant"."avg_monthly_sales" asc,
    "questionable"."sum_sales" asc,
    "questionable"."psum" asc,
    "questionable"."nsum" asc
LIMIT (100)
```
