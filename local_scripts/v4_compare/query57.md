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
| v4 | 9612 | 134 | 199.89 ms |
| reference | 5356 | 83 | 117.96 ms |
| v4 / ref | 1.79x | 1.61x | 1.69x |

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
highfalutin as (
SELECT
    "catalog_sales_catalog_sales"."CS_CALL_CENTER_SK" as "catalog_sales_call_center_id",
    "catalog_sales_catalog_sales"."CS_ITEM_SK" as "catalog_sales_item_id",
    "catalog_sales_catalog_sales"."CS_SOLD_DATE_SK" as "catalog_sales_date_id"
FROM
    "memory"."catalog_sales" as "catalog_sales_catalog_sales"
WHERE
    "catalog_sales_catalog_sales"."CS_CALL_CENTER_SK" is not null
),
questionable as (
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
thoughtful as (
SELECT
    "catalog_sales_call_center_call_center"."CC_NAME" as "catalog_sales_call_center_name",
    "catalog_sales_date_date"."D_MOY" as "catalog_sales_date_month_of_year",
    "catalog_sales_date_date"."D_YEAR" as "catalog_sales_date_year",
    "catalog_sales_item_items"."I_BRAND" as "catalog_sales_item_brand_name",
    "catalog_sales_item_items"."I_CATEGORY" as "catalog_sales_item_category"
FROM
    "highfalutin"
    INNER JOIN "memory"."date_dim" as "catalog_sales_date_date" on "highfalutin"."catalog_sales_date_id" = "catalog_sales_date_date"."D_DATE_SK"
    INNER JOIN "memory"."item" as "catalog_sales_item_items" on "highfalutin"."catalog_sales_item_id" = "catalog_sales_item_items"."I_ITEM_SK"
    LEFT OUTER JOIN "memory"."call_center" as "catalog_sales_call_center_call_center" on "highfalutin"."catalog_sales_call_center_id" = "catalog_sales_call_center_call_center"."CC_CALL_CENTER_SK"
WHERE
    ( "catalog_sales_date_date"."D_YEAR" = 1999 or ( "catalog_sales_date_date"."D_YEAR" = 1998 and "catalog_sales_date_date"."D_MOY" = 12 ) or ( "catalog_sales_date_date"."D_YEAR" = 2000 and "catalog_sales_date_date"."D_MOY" = 1 ) )
),
sparkling as (
SELECT
    "questionable"."catalog_sales_call_center_name" as "catalog_sales_call_center_name",
    "questionable"."catalog_sales_date_month_of_year" as "catalog_sales_date_month_of_year",
    "questionable"."catalog_sales_date_year" as "catalog_sales_date_year",
    "questionable"."catalog_sales_item_brand_name" as "catalog_sales_item_brand_name",
    "questionable"."catalog_sales_item_category" as "catalog_sales_item_category",
    lag("questionable"."sum_sales", 1) over (partition by "questionable"."catalog_sales_item_category","questionable"."catalog_sales_item_brand_name","questionable"."catalog_sales_call_center_name" order by "questionable"."catalog_sales_date_year" asc,"questionable"."catalog_sales_date_month_of_year" asc ) as "psum",
    lead("questionable"."sum_sales", 1) over (partition by "questionable"."catalog_sales_item_category","questionable"."catalog_sales_item_brand_name","questionable"."catalog_sales_call_center_name" order by "questionable"."catalog_sales_date_year" asc,"questionable"."catalog_sales_date_month_of_year" asc ) as "nsum"
FROM
    "questionable"),
yummy as (
SELECT
    "questionable"."sum_sales" as "sum_sales",
    coalesce("questionable"."catalog_sales_call_center_name","thoughtful"."catalog_sales_call_center_name") as "catalog_sales_call_center_name",
    coalesce("questionable"."catalog_sales_date_year","thoughtful"."catalog_sales_date_year") as "catalog_sales_date_year",
    coalesce("questionable"."catalog_sales_item_brand_name","thoughtful"."catalog_sales_item_brand_name") as "catalog_sales_item_brand_name",
    coalesce("questionable"."catalog_sales_item_category","thoughtful"."catalog_sales_item_category") as "catalog_sales_item_category"
FROM
    "thoughtful"
    FULL JOIN "questionable" on "thoughtful"."catalog_sales_call_center_name" is not distinct from "questionable"."catalog_sales_call_center_name" AND "thoughtful"."catalog_sales_date_month_of_year" is not distinct from "questionable"."catalog_sales_date_month_of_year" AND "thoughtful"."catalog_sales_date_year" = "questionable"."catalog_sales_date_year" AND "thoughtful"."catalog_sales_item_brand_name" = "questionable"."catalog_sales_item_brand_name" AND "thoughtful"."catalog_sales_item_category" is not distinct from "questionable"."catalog_sales_item_category"
GROUP BY
    1,
    2,
    3,
    4,
    5,
    coalesce("questionable"."catalog_sales_date_month_of_year","thoughtful"."catalog_sales_date_month_of_year")),
vacuous as (
SELECT
    "yummy"."catalog_sales_call_center_name" as "catalog_sales_call_center_name",
    "yummy"."catalog_sales_date_year" as "catalog_sales_date_year",
    "yummy"."catalog_sales_item_brand_name" as "catalog_sales_item_brand_name",
    "yummy"."catalog_sales_item_category" as "catalog_sales_item_category",
    avg("yummy"."sum_sales") as "avg_monthly_sales"
FROM
    "yummy"
GROUP BY
    1,
    2,
    3,
    4),
concerned as (
SELECT
    "questionable"."catalog_sales_date_month_of_year" as "catalog_sales_date_month_of_year",
    "questionable"."sum_sales" - "vacuous"."avg_monthly_sales" as "sum_minus_avg",
    "questionable"."sum_sales" as "sum_sales",
    "vacuous"."avg_monthly_sales" as "avg_monthly_sales",
    coalesce("questionable"."catalog_sales_call_center_name","vacuous"."catalog_sales_call_center_name") as "catalog_sales_call_center_name",
    coalesce("questionable"."catalog_sales_date_year","vacuous"."catalog_sales_date_year") as "catalog_sales_date_year",
    coalesce("questionable"."catalog_sales_item_brand_name","vacuous"."catalog_sales_item_brand_name") as "catalog_sales_item_brand_name",
    coalesce("questionable"."catalog_sales_item_category","vacuous"."catalog_sales_item_category") as "catalog_sales_item_category"
FROM
    "questionable"
    INNER JOIN "vacuous" on "questionable"."catalog_sales_call_center_name" is not distinct from "vacuous"."catalog_sales_call_center_name" AND "questionable"."catalog_sales_date_year" = "vacuous"."catalog_sales_date_year" AND "questionable"."catalog_sales_item_brand_name" = "vacuous"."catalog_sales_item_brand_name" AND "questionable"."catalog_sales_item_category" is not distinct from "vacuous"."catalog_sales_item_category")
SELECT
    coalesce("concerned"."catalog_sales_item_category","sparkling"."catalog_sales_item_category") as "catalog_sales_item_category",
    coalesce("concerned"."catalog_sales_item_brand_name","sparkling"."catalog_sales_item_brand_name") as "catalog_sales_item_brand_name",
    coalesce("concerned"."catalog_sales_call_center_name","sparkling"."catalog_sales_call_center_name") as "catalog_sales_call_center_name",
    coalesce("concerned"."catalog_sales_date_year","sparkling"."catalog_sales_date_year") as "catalog_sales_date_year",
    coalesce("concerned"."catalog_sales_date_month_of_year","sparkling"."catalog_sales_date_month_of_year") as "catalog_sales_date_month_of_year",
    "concerned"."avg_monthly_sales" as "avg_monthly_sales",
    "concerned"."sum_sales" as "sum_sales",
    "sparkling"."psum" as "psum",
    "sparkling"."nsum" as "nsum"
FROM
    "sparkling"
    RIGHT OUTER JOIN "concerned" on "sparkling"."catalog_sales_call_center_name" = "concerned"."catalog_sales_call_center_name" AND "sparkling"."catalog_sales_date_month_of_year" = "concerned"."catalog_sales_date_month_of_year" AND "sparkling"."catalog_sales_date_year" = "concerned"."catalog_sales_date_year" AND "sparkling"."catalog_sales_item_brand_name" = "concerned"."catalog_sales_item_brand_name" AND "sparkling"."catalog_sales_item_category" is not distinct from "concerned"."catalog_sales_item_category"
WHERE
    coalesce("concerned"."catalog_sales_date_year","sparkling"."catalog_sales_date_year") = 1999 and "concerned"."avg_monthly_sales" > 0 and CASE
	WHEN "concerned"."avg_monthly_sales" > 0 THEN abs("concerned"."sum_sales" - "concerned"."avg_monthly_sales") / "concerned"."avg_monthly_sales"
	ELSE null
	END > 0.1

ORDER BY 
    "concerned"."sum_minus_avg" asc nulls first,
    coalesce("concerned"."catalog_sales_item_category","sparkling"."catalog_sales_item_category") asc,
    coalesce("concerned"."catalog_sales_item_brand_name","sparkling"."catalog_sales_item_brand_name") asc,
    coalesce("concerned"."catalog_sales_call_center_name","sparkling"."catalog_sales_call_center_name") asc,
    coalesce("concerned"."catalog_sales_date_year","sparkling"."catalog_sales_date_year") asc,
    coalesce("concerned"."catalog_sales_date_month_of_year","sparkling"."catalog_sales_date_month_of_year") asc,
    "concerned"."avg_monthly_sales" asc,
    "concerned"."sum_sales" asc,
    "sparkling"."psum" asc,
    "sparkling"."nsum" asc
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
