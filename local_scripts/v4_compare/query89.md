# Query 89

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
| v4 | 4123 | 68 | 85.90 ms |
| reference | 4123 | 68 | 91.62 ms |
| v4 / ref | 1.00x | 1.00x | 0.94x |

## Preql

```
import store_sales as store_sales;

auto sum_sales <- sum(store_sales.sales_price)
    by store_sales.item.category, store_sales.item.class, store_sales.item.brand_name, store_sales.store.name, store_sales.store.company_name, store_sales.date.month_of_year;
auto avg_monthly_sales <- avg(sum_sales)
    by store_sales.item.category, store_sales.item.brand_name, store_sales.store.name, store_sales.store.company_name;

where
    store_sales.date.year = 1999
    and store_sales.store.id is not null
    and (
        (
            store_sales.item.category in ('Books', 'Electronics', 'Sports')
            and store_sales.item.class in ('computers', 'stereo', 'football')
        )
        or (
            store_sales.item.category in ('Men', 'Jewelry', 'Women')
            and store_sales.item.class in ('shirts', 'birdal', 'dresses')
        )
    )
select
    store_sales.item.category,
    store_sales.item.class,
    store_sales.item.brand_name,
    store_sales.store.name,
    store_sales.store.company_name,
    store_sales.date.month_of_year,
    sum_sales,
    avg_monthly_sales,
having
    case
            when avg_monthly_sales != 0 then abs(sum_sales - avg_monthly_sales) / avg_monthly_sales
            else null
        end > 0.1

order by
    sum_sales - avg_monthly_sales asc,
    store_sales.store.name asc,
    store_sales.item.category asc,
    store_sales.item.class asc,
    store_sales.item.brand_name asc,
    store_sales.store.company_name asc,
    store_sales.date.month_of_year asc,
    sum_sales asc,
    avg_monthly_sales asc
limit 100
;
```

## v4 generated SQL

```sql
WITH 
thoughtful as (
SELECT
    "store_sales_date_date"."D_MOY" as "store_sales_date_month_of_year",
    "store_sales_item_items"."I_BRAND" as "store_sales_item_brand_name",
    "store_sales_item_items"."I_CATEGORY" as "store_sales_item_category",
    "store_sales_item_items"."I_CLASS" as "store_sales_item_class",
    "store_sales_store_store"."S_COMPANY_NAME" as "store_sales_store_company_name",
    "store_sales_store_store"."S_STORE_NAME" as "store_sales_store_name",
    sum("store_sales_store_sales"."SS_SALES_PRICE") as "sum_sales"
FROM
    "memory"."store_sales" as "store_sales_store_sales"
    INNER JOIN "memory"."date_dim" as "store_sales_date_date" on "store_sales_store_sales"."SS_SOLD_DATE_SK" = "store_sales_date_date"."D_DATE_SK"
    INNER JOIN "memory"."item" as "store_sales_item_items" on "store_sales_store_sales"."SS_ITEM_SK" = "store_sales_item_items"."I_ITEM_SK"
    INNER JOIN "memory"."store" as "store_sales_store_store" on "store_sales_store_sales"."SS_STORE_SK" = "store_sales_store_store"."S_STORE_SK"
WHERE
    "store_sales_date_date"."D_YEAR" = 1999 and "store_sales_store_sales"."SS_STORE_SK" is not null and ( ( "store_sales_item_items"."I_CATEGORY" in ('Books','Electronics','Sports') and "store_sales_item_items"."I_CLASS" in ('computers','stereo','football') ) or ( "store_sales_item_items"."I_CATEGORY" in ('Men','Jewelry','Women') and "store_sales_item_items"."I_CLASS" in ('shirts','birdal','dresses') ) )

GROUP BY
    1,
    2,
    3,
    4,
    5,
    6),
questionable as (
SELECT
    "thoughtful"."store_sales_item_brand_name" as "store_sales_item_brand_name",
    "thoughtful"."store_sales_item_category" as "store_sales_item_category",
    "thoughtful"."store_sales_store_company_name" as "store_sales_store_company_name",
    "thoughtful"."store_sales_store_name" as "store_sales_store_name",
    avg("thoughtful"."sum_sales") as "avg_monthly_sales"
FROM
    "thoughtful"
GROUP BY
    1,
    2,
    3,
    4)
SELECT
    coalesce("questionable"."store_sales_item_category","thoughtful"."store_sales_item_category") as "store_sales_item_category",
    "thoughtful"."store_sales_item_class" as "store_sales_item_class",
    coalesce("questionable"."store_sales_item_brand_name","thoughtful"."store_sales_item_brand_name") as "store_sales_item_brand_name",
    coalesce("questionable"."store_sales_store_name","thoughtful"."store_sales_store_name") as "store_sales_store_name",
    coalesce("questionable"."store_sales_store_company_name","thoughtful"."store_sales_store_company_name") as "store_sales_store_company_name",
    "thoughtful"."store_sales_date_month_of_year" as "store_sales_date_month_of_year",
    "thoughtful"."sum_sales" as "sum_sales",
    "questionable"."avg_monthly_sales" as "avg_monthly_sales"
FROM
    "thoughtful"
    INNER JOIN "questionable" on "thoughtful"."store_sales_item_brand_name" = "questionable"."store_sales_item_brand_name" AND "thoughtful"."store_sales_item_category" is not distinct from "questionable"."store_sales_item_category" AND "thoughtful"."store_sales_store_company_name" is not distinct from "questionable"."store_sales_store_company_name" AND "thoughtful"."store_sales_store_name" is not distinct from "questionable"."store_sales_store_name"
WHERE
    CASE
	WHEN "questionable"."avg_monthly_sales" != 0 THEN abs("thoughtful"."sum_sales" - "questionable"."avg_monthly_sales") / "questionable"."avg_monthly_sales"
	ELSE null
	END > 0.1

ORDER BY 
    "thoughtful"."sum_sales" - "questionable"."avg_monthly_sales" asc,
    coalesce("questionable"."store_sales_store_name","thoughtful"."store_sales_store_name") asc,
    coalesce("questionable"."store_sales_item_category","thoughtful"."store_sales_item_category") asc,
    "thoughtful"."store_sales_item_class" asc,
    coalesce("questionable"."store_sales_item_brand_name","thoughtful"."store_sales_item_brand_name") asc,
    coalesce("questionable"."store_sales_store_company_name","thoughtful"."store_sales_store_company_name") asc,
    "thoughtful"."store_sales_date_month_of_year" asc,
    "thoughtful"."sum_sales" asc,
    "questionable"."avg_monthly_sales" asc
LIMIT (100)
```

## Reference SQL (zquery log)

```sql
WITH 
thoughtful as (
SELECT
    "store_sales_date_date"."D_MOY" as "store_sales_date_month_of_year",
    "store_sales_item_items"."I_BRAND" as "store_sales_item_brand_name",
    "store_sales_item_items"."I_CATEGORY" as "store_sales_item_category",
    "store_sales_item_items"."I_CLASS" as "store_sales_item_class",
    "store_sales_store_store"."S_COMPANY_NAME" as "store_sales_store_company_name",
    "store_sales_store_store"."S_STORE_NAME" as "store_sales_store_name",
    sum("store_sales_store_sales"."SS_SALES_PRICE") as "sum_sales"
FROM
    "memory"."store_sales" as "store_sales_store_sales"
    INNER JOIN "memory"."date_dim" as "store_sales_date_date" on "store_sales_store_sales"."SS_SOLD_DATE_SK" = "store_sales_date_date"."D_DATE_SK"
    INNER JOIN "memory"."item" as "store_sales_item_items" on "store_sales_store_sales"."SS_ITEM_SK" = "store_sales_item_items"."I_ITEM_SK"
    INNER JOIN "memory"."store" as "store_sales_store_store" on "store_sales_store_sales"."SS_STORE_SK" = "store_sales_store_store"."S_STORE_SK"
WHERE
    "store_sales_date_date"."D_YEAR" = 1999 and "store_sales_store_sales"."SS_STORE_SK" is not null and ( ( "store_sales_item_items"."I_CATEGORY" in ('Books','Electronics','Sports') and "store_sales_item_items"."I_CLASS" in ('computers','stereo','football') ) or ( "store_sales_item_items"."I_CATEGORY" in ('Men','Jewelry','Women') and "store_sales_item_items"."I_CLASS" in ('shirts','birdal','dresses') ) )

GROUP BY
    1,
    2,
    3,
    4,
    5,
    6),
questionable as (
SELECT
    "thoughtful"."store_sales_item_brand_name" as "store_sales_item_brand_name",
    "thoughtful"."store_sales_item_category" as "store_sales_item_category",
    "thoughtful"."store_sales_store_company_name" as "store_sales_store_company_name",
    "thoughtful"."store_sales_store_name" as "store_sales_store_name",
    avg("thoughtful"."sum_sales") as "avg_monthly_sales"
FROM
    "thoughtful"
GROUP BY
    1,
    2,
    3,
    4)
SELECT
    coalesce("questionable"."store_sales_item_category","thoughtful"."store_sales_item_category") as "store_sales_item_category",
    "thoughtful"."store_sales_item_class" as "store_sales_item_class",
    coalesce("questionable"."store_sales_item_brand_name","thoughtful"."store_sales_item_brand_name") as "store_sales_item_brand_name",
    coalesce("questionable"."store_sales_store_name","thoughtful"."store_sales_store_name") as "store_sales_store_name",
    coalesce("questionable"."store_sales_store_company_name","thoughtful"."store_sales_store_company_name") as "store_sales_store_company_name",
    "thoughtful"."store_sales_date_month_of_year" as "store_sales_date_month_of_year",
    "thoughtful"."sum_sales" as "sum_sales",
    "questionable"."avg_monthly_sales" as "avg_monthly_sales"
FROM
    "thoughtful"
    INNER JOIN "questionable" on "thoughtful"."store_sales_item_brand_name" = "questionable"."store_sales_item_brand_name" AND "thoughtful"."store_sales_item_category" is not distinct from "questionable"."store_sales_item_category" AND "thoughtful"."store_sales_store_company_name" is not distinct from "questionable"."store_sales_store_company_name" AND "thoughtful"."store_sales_store_name" is not distinct from "questionable"."store_sales_store_name"
WHERE
    CASE
	WHEN "questionable"."avg_monthly_sales" != 0 THEN abs("thoughtful"."sum_sales" - "questionable"."avg_monthly_sales") / "questionable"."avg_monthly_sales"
	ELSE null
	END > 0.1

ORDER BY 
    "thoughtful"."sum_sales" - "questionable"."avg_monthly_sales" asc,
    coalesce("questionable"."store_sales_store_name","thoughtful"."store_sales_store_name") asc,
    coalesce("questionable"."store_sales_item_category","thoughtful"."store_sales_item_category") asc,
    "thoughtful"."store_sales_item_class" asc,
    coalesce("questionable"."store_sales_item_brand_name","thoughtful"."store_sales_item_brand_name") asc,
    coalesce("questionable"."store_sales_store_company_name","thoughtful"."store_sales_store_company_name") asc,
    "thoughtful"."store_sales_date_month_of_year" asc,
    "thoughtful"."sum_sales" asc,
    "questionable"."avg_monthly_sales" asc
LIMIT (100)
```
