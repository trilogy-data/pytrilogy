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
| v4 | 4336 | 68 | 190.46 ms |
| reference | 4336 | 68 | 192.40 ms |
| v4 / ref | 1.00x | 1.00x | 0.99x |

## Preql

```
import physical_sales as physical_sales;

auto sum_sales <- sum(physical_sales.sales_price)
    by physical_sales.item.category, physical_sales.item.class, physical_sales.item.brand_name, physical_sales.store.name, physical_sales.store.company_name, physical_sales.date.month_of_year;
auto avg_monthly_sales <- avg(sum_sales)
    by physical_sales.item.category, physical_sales.item.brand_name, physical_sales.store.name, physical_sales.store.company_name;

where
    physical_sales.date.year = 1999
    and physical_sales.store.id is not null
    and (
        (
            physical_sales.item.category in ('Books', 'Electronics', 'Sports')
            and physical_sales.item.class in ('computers', 'stereo', 'football')
        )
        or (
            physical_sales.item.category in ('Men', 'Jewelry', 'Women')
            and physical_sales.item.class in ('shirts', 'birdal', 'dresses')
        )
    )
select
    physical_sales.item.category,
    physical_sales.item.class,
    physical_sales.item.brand_name,
    physical_sales.store.name,
    physical_sales.store.company_name,
    physical_sales.date.month_of_year,
    sum_sales,
    avg_monthly_sales,
having
    case
            when avg_monthly_sales != 0 then abs(sum_sales - avg_monthly_sales) / avg_monthly_sales
            else null
        end > 0.1

order by
    sum_sales - avg_monthly_sales asc,
    physical_sales.store.name asc,
    physical_sales.item.category asc,
    physical_sales.item.class asc,
    physical_sales.item.brand_name asc,
    physical_sales.store.company_name asc,
    physical_sales.date.month_of_year asc,
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
    "physical_sales_date_date"."D_MOY" as "physical_sales_date_month_of_year",
    "physical_sales_item_items"."I_BRAND" as "physical_sales_item_brand_name",
    "physical_sales_item_items"."I_CATEGORY" as "physical_sales_item_category",
    "physical_sales_item_items"."I_CLASS" as "physical_sales_item_class",
    "physical_sales_store_store"."S_COMPANY_NAME" as "physical_sales_store_company_name",
    "physical_sales_store_store"."S_STORE_NAME" as "physical_sales_store_name",
    sum("physical_sales_store_sales"."SS_SALES_PRICE") as "sum_sales"
FROM
    "memory"."store_sales" as "physical_sales_store_sales"
    INNER JOIN "memory"."date_dim" as "physical_sales_date_date" on "physical_sales_store_sales"."SS_SOLD_DATE_SK" = "physical_sales_date_date"."D_DATE_SK"
    INNER JOIN "memory"."item" as "physical_sales_item_items" on "physical_sales_store_sales"."SS_ITEM_SK" = "physical_sales_item_items"."I_ITEM_SK"
    INNER JOIN "memory"."store" as "physical_sales_store_store" on "physical_sales_store_sales"."SS_STORE_SK" = "physical_sales_store_store"."S_STORE_SK"
WHERE
    "physical_sales_date_date"."D_YEAR" = 1999 and "physical_sales_store_sales"."SS_STORE_SK" is not null and ( ( "physical_sales_item_items"."I_CATEGORY" in ('Books','Electronics','Sports') and "physical_sales_item_items"."I_CLASS" in ('computers','stereo','football') ) or ( "physical_sales_item_items"."I_CATEGORY" in ('Men','Jewelry','Women') and "physical_sales_item_items"."I_CLASS" in ('shirts','birdal','dresses') ) )

GROUP BY
    1,
    2,
    3,
    4,
    5,
    6),
questionable as (
SELECT
    "thoughtful"."physical_sales_item_brand_name" as "physical_sales_item_brand_name",
    "thoughtful"."physical_sales_item_category" as "physical_sales_item_category",
    "thoughtful"."physical_sales_store_company_name" as "physical_sales_store_company_name",
    "thoughtful"."physical_sales_store_name" as "physical_sales_store_name",
    avg("thoughtful"."sum_sales") as "avg_monthly_sales"
FROM
    "thoughtful"
GROUP BY
    1,
    2,
    3,
    4)
SELECT
    coalesce("questionable"."physical_sales_item_category","thoughtful"."physical_sales_item_category") as "physical_sales_item_category",
    "thoughtful"."physical_sales_item_class" as "physical_sales_item_class",
    coalesce("questionable"."physical_sales_item_brand_name","thoughtful"."physical_sales_item_brand_name") as "physical_sales_item_brand_name",
    coalesce("questionable"."physical_sales_store_name","thoughtful"."physical_sales_store_name") as "physical_sales_store_name",
    coalesce("questionable"."physical_sales_store_company_name","thoughtful"."physical_sales_store_company_name") as "physical_sales_store_company_name",
    "thoughtful"."physical_sales_date_month_of_year" as "physical_sales_date_month_of_year",
    "thoughtful"."sum_sales" as "sum_sales",
    "questionable"."avg_monthly_sales" as "avg_monthly_sales"
FROM
    "thoughtful"
    INNER JOIN "questionable" on "thoughtful"."physical_sales_item_brand_name" = "questionable"."physical_sales_item_brand_name" AND "thoughtful"."physical_sales_item_category" is not distinct from "questionable"."physical_sales_item_category" AND "thoughtful"."physical_sales_store_company_name" is not distinct from "questionable"."physical_sales_store_company_name" AND "thoughtful"."physical_sales_store_name" is not distinct from "questionable"."physical_sales_store_name"
WHERE
    CASE
	WHEN "questionable"."avg_monthly_sales" != 0 THEN abs("thoughtful"."sum_sales" - "questionable"."avg_monthly_sales") / "questionable"."avg_monthly_sales"
	ELSE null
	END > 0.1

ORDER BY 
    "thoughtful"."sum_sales" - "questionable"."avg_monthly_sales" asc,
    coalesce("questionable"."physical_sales_store_name","thoughtful"."physical_sales_store_name") asc,
    coalesce("questionable"."physical_sales_item_category","thoughtful"."physical_sales_item_category") asc,
    "thoughtful"."physical_sales_item_class" asc,
    coalesce("questionable"."physical_sales_item_brand_name","thoughtful"."physical_sales_item_brand_name") asc,
    coalesce("questionable"."physical_sales_store_company_name","thoughtful"."physical_sales_store_company_name") asc,
    "thoughtful"."physical_sales_date_month_of_year" asc,
    "thoughtful"."sum_sales" asc,
    "questionable"."avg_monthly_sales" asc
LIMIT (100)
```

## Reference SQL (zquery log)

```sql
WITH 
thoughtful as (
SELECT
    "physical_sales_date_date"."D_MOY" as "physical_sales_date_month_of_year",
    "physical_sales_item_items"."I_BRAND" as "physical_sales_item_brand_name",
    "physical_sales_item_items"."I_CATEGORY" as "physical_sales_item_category",
    "physical_sales_item_items"."I_CLASS" as "physical_sales_item_class",
    "physical_sales_store_store"."S_COMPANY_NAME" as "physical_sales_store_company_name",
    "physical_sales_store_store"."S_STORE_NAME" as "physical_sales_store_name",
    sum("physical_sales_store_sales"."SS_SALES_PRICE") as "sum_sales"
FROM
    "memory"."store_sales" as "physical_sales_store_sales"
    INNER JOIN "memory"."date_dim" as "physical_sales_date_date" on "physical_sales_store_sales"."SS_SOLD_DATE_SK" = "physical_sales_date_date"."D_DATE_SK"
    INNER JOIN "memory"."item" as "physical_sales_item_items" on "physical_sales_store_sales"."SS_ITEM_SK" = "physical_sales_item_items"."I_ITEM_SK"
    INNER JOIN "memory"."store" as "physical_sales_store_store" on "physical_sales_store_sales"."SS_STORE_SK" = "physical_sales_store_store"."S_STORE_SK"
WHERE
    "physical_sales_date_date"."D_YEAR" = 1999 and "physical_sales_store_sales"."SS_STORE_SK" is not null and ( ( "physical_sales_item_items"."I_CATEGORY" in ('Books','Electronics','Sports') and "physical_sales_item_items"."I_CLASS" in ('computers','stereo','football') ) or ( "physical_sales_item_items"."I_CATEGORY" in ('Men','Jewelry','Women') and "physical_sales_item_items"."I_CLASS" in ('shirts','birdal','dresses') ) )

GROUP BY
    1,
    2,
    3,
    4,
    5,
    6),
questionable as (
SELECT
    "thoughtful"."physical_sales_item_brand_name" as "physical_sales_item_brand_name",
    "thoughtful"."physical_sales_item_category" as "physical_sales_item_category",
    "thoughtful"."physical_sales_store_company_name" as "physical_sales_store_company_name",
    "thoughtful"."physical_sales_store_name" as "physical_sales_store_name",
    avg("thoughtful"."sum_sales") as "avg_monthly_sales"
FROM
    "thoughtful"
GROUP BY
    1,
    2,
    3,
    4)
SELECT
    coalesce("questionable"."physical_sales_item_category","thoughtful"."physical_sales_item_category") as "physical_sales_item_category",
    "thoughtful"."physical_sales_item_class" as "physical_sales_item_class",
    coalesce("questionable"."physical_sales_item_brand_name","thoughtful"."physical_sales_item_brand_name") as "physical_sales_item_brand_name",
    coalesce("questionable"."physical_sales_store_name","thoughtful"."physical_sales_store_name") as "physical_sales_store_name",
    coalesce("questionable"."physical_sales_store_company_name","thoughtful"."physical_sales_store_company_name") as "physical_sales_store_company_name",
    "thoughtful"."physical_sales_date_month_of_year" as "physical_sales_date_month_of_year",
    "thoughtful"."sum_sales" as "sum_sales",
    "questionable"."avg_monthly_sales" as "avg_monthly_sales"
FROM
    "thoughtful"
    INNER JOIN "questionable" on "thoughtful"."physical_sales_item_brand_name" = "questionable"."physical_sales_item_brand_name" AND "thoughtful"."physical_sales_item_category" is not distinct from "questionable"."physical_sales_item_category" AND "thoughtful"."physical_sales_store_company_name" is not distinct from "questionable"."physical_sales_store_company_name" AND "thoughtful"."physical_sales_store_name" is not distinct from "questionable"."physical_sales_store_name"
WHERE
    CASE
	WHEN "questionable"."avg_monthly_sales" != 0 THEN abs("thoughtful"."sum_sales" - "questionable"."avg_monthly_sales") / "questionable"."avg_monthly_sales"
	ELSE null
	END > 0.1

ORDER BY 
    "thoughtful"."sum_sales" - "questionable"."avg_monthly_sales" asc,
    coalesce("questionable"."physical_sales_store_name","thoughtful"."physical_sales_store_name") asc,
    coalesce("questionable"."physical_sales_item_category","thoughtful"."physical_sales_item_category") asc,
    "thoughtful"."physical_sales_item_class" asc,
    coalesce("questionable"."physical_sales_item_brand_name","thoughtful"."physical_sales_item_brand_name") asc,
    coalesce("questionable"."physical_sales_store_company_name","thoughtful"."physical_sales_store_company_name") asc,
    "thoughtful"."physical_sales_date_month_of_year" asc,
    "thoughtful"."sum_sales" asc,
    "questionable"."avg_monthly_sales" asc
LIMIT (100)
```
