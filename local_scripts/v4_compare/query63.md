# Query 63

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
| v4 | 2609 | 48 | 11.29 ms |
| reference | 2609 | 48 | 11.16 ms |
| v4 / ref | 1.00x | 1.00x | 1.01x |

## Preql

```
import physical_sales as physical_sales;

auto sum_sales <- sum(physical_sales.sales_price) by physical_sales.item.manager_id, physical_sales.date.month_of_year;
auto avg_monthly_sales <- avg(sum_sales) by physical_sales.item.manager_id;

where
    physical_sales.date.month_seq in (1200, 1201, 1202, 1203, 1204, 1205, 1206, 1207, 1208, 1209, 1210, 1211)
    and physical_sales.store.id is not null
    and (
        (
            physical_sales.item.category in ('Books', 'Children', 'Electronics')
            and physical_sales.item.class in ('personal', 'portable', 'reference', 'self-help')
            and physical_sales.item.brand_name in ('scholaramalgamalg #14', 'scholaramalgamalg #7', 'exportiunivamalg #9', 'scholaramalgamalg #9')
        )
        or (
            physical_sales.item.category in ('Women', 'Music', 'Men')
            and physical_sales.item.class in ('accessories', 'classical', 'fragrances', 'pants')
            and physical_sales.item.brand_name in ('amalgimporto #1', 'edu packscholar #1', 'exportiimporto #1', 'importoamalg #1')
        )
    )
select
    physical_sales.item.manager_id,
    sum_sales,
    avg_monthly_sales,
having
    case
            when avg_monthly_sales > 0 then abs(sum_sales - avg_monthly_sales) / avg_monthly_sales
            else null
        end > 0.1

order by
    physical_sales.item.manager_id asc,
    avg_monthly_sales asc,
    sum_sales asc
limit 100
;
```

## v4 generated SQL

```sql
WITH 
cheerful as (
SELECT
    "physical_sales_item_items"."I_MANAGER_ID" as "physical_sales_item_manager_id",
    sum("physical_sales_store_sales"."SS_SALES_PRICE") as "sum_sales"
FROM
    "memory"."store_sales" as "physical_sales_store_sales"
    INNER JOIN "memory"."date_dim" as "physical_sales_date_date" on "physical_sales_store_sales"."SS_SOLD_DATE_SK" = "physical_sales_date_date"."D_DATE_SK"
    INNER JOIN "memory"."item" as "physical_sales_item_items" on "physical_sales_store_sales"."SS_ITEM_SK" = "physical_sales_item_items"."I_ITEM_SK"
WHERE
    "physical_sales_date_date"."D_MONTH_SEQ" in (1200,1201,1202,1203,1204,1205,1206,1207,1208,1209,1210,1211) and "physical_sales_store_sales"."SS_STORE_SK" is not null and ( ( "physical_sales_item_items"."I_CATEGORY" in ('Books','Children','Electronics') and "physical_sales_item_items"."I_CLASS" in ('personal','portable','reference','self-help') and "physical_sales_item_items"."I_BRAND" in ('scholaramalgamalg #14','scholaramalgamalg #7','exportiunivamalg #9','scholaramalgamalg #9') ) or ( "physical_sales_item_items"."I_CATEGORY" in ('Women','Music','Men') and "physical_sales_item_items"."I_CLASS" in ('accessories','classical','fragrances','pants') and "physical_sales_item_items"."I_BRAND" in ('amalgimporto #1','edu packscholar #1','exportiimporto #1','importoamalg #1') ) )

GROUP BY
    1,
    "physical_sales_date_date"."D_MOY"),
cooperative as (
SELECT
    "cheerful"."physical_sales_item_manager_id" as "physical_sales_item_manager_id",
    avg("cheerful"."sum_sales") as "avg_monthly_sales"
FROM
    "cheerful"
GROUP BY
    1),
questionable as (
SELECT
    "cheerful"."sum_sales" as "sum_sales",
    "cooperative"."avg_monthly_sales" as "avg_monthly_sales",
    coalesce("cheerful"."physical_sales_item_manager_id","cooperative"."physical_sales_item_manager_id") as "physical_sales_item_manager_id"
FROM
    "cheerful"
    INNER JOIN "cooperative" on "cheerful"."physical_sales_item_manager_id" is not distinct from "cooperative"."physical_sales_item_manager_id"
WHERE
    CASE
	WHEN "cooperative"."avg_monthly_sales" > 0 THEN abs("cheerful"."sum_sales" - "cooperative"."avg_monthly_sales") / "cooperative"."avg_monthly_sales"
	ELSE null
	END > 0.1
)
SELECT
    "questionable"."physical_sales_item_manager_id" as "physical_sales_item_manager_id",
    "questionable"."sum_sales" as "sum_sales",
    "questionable"."avg_monthly_sales" as "avg_monthly_sales"
FROM
    "questionable"
ORDER BY 
    "questionable"."physical_sales_item_manager_id" asc,
    "questionable"."avg_monthly_sales" asc,
    "questionable"."sum_sales" asc
LIMIT (100)
```

## Reference SQL (zquery log)

```sql
WITH 
cheerful as (
SELECT
    "physical_sales_item_items"."I_MANAGER_ID" as "physical_sales_item_manager_id",
    sum("physical_sales_store_sales"."SS_SALES_PRICE") as "sum_sales"
FROM
    "memory"."store_sales" as "physical_sales_store_sales"
    INNER JOIN "memory"."date_dim" as "physical_sales_date_date" on "physical_sales_store_sales"."SS_SOLD_DATE_SK" = "physical_sales_date_date"."D_DATE_SK"
    INNER JOIN "memory"."item" as "physical_sales_item_items" on "physical_sales_store_sales"."SS_ITEM_SK" = "physical_sales_item_items"."I_ITEM_SK"
WHERE
    "physical_sales_date_date"."D_MONTH_SEQ" in (1200,1201,1202,1203,1204,1205,1206,1207,1208,1209,1210,1211) and "physical_sales_store_sales"."SS_STORE_SK" is not null and ( ( "physical_sales_item_items"."I_CATEGORY" in ('Books','Children','Electronics') and "physical_sales_item_items"."I_CLASS" in ('personal','portable','reference','self-help') and "physical_sales_item_items"."I_BRAND" in ('scholaramalgamalg #14','scholaramalgamalg #7','exportiunivamalg #9','scholaramalgamalg #9') ) or ( "physical_sales_item_items"."I_CATEGORY" in ('Women','Music','Men') and "physical_sales_item_items"."I_CLASS" in ('accessories','classical','fragrances','pants') and "physical_sales_item_items"."I_BRAND" in ('amalgimporto #1','edu packscholar #1','exportiimporto #1','importoamalg #1') ) )

GROUP BY
    1,
    "physical_sales_date_date"."D_MOY"),
cooperative as (
SELECT
    "cheerful"."physical_sales_item_manager_id" as "physical_sales_item_manager_id",
    avg("cheerful"."sum_sales") as "avg_monthly_sales"
FROM
    "cheerful"
GROUP BY
    1),
questionable as (
SELECT
    "cheerful"."sum_sales" as "sum_sales",
    "cooperative"."avg_monthly_sales" as "avg_monthly_sales",
    coalesce("cheerful"."physical_sales_item_manager_id","cooperative"."physical_sales_item_manager_id") as "physical_sales_item_manager_id"
FROM
    "cheerful"
    INNER JOIN "cooperative" on "cheerful"."physical_sales_item_manager_id" is not distinct from "cooperative"."physical_sales_item_manager_id"
WHERE
    CASE
	WHEN "cooperative"."avg_monthly_sales" > 0 THEN abs("cheerful"."sum_sales" - "cooperative"."avg_monthly_sales") / "cooperative"."avg_monthly_sales"
	ELSE null
	END > 0.1
)
SELECT
    "questionable"."physical_sales_item_manager_id" as "physical_sales_item_manager_id",
    "questionable"."sum_sales" as "sum_sales",
    "questionable"."avg_monthly_sales" as "avg_monthly_sales"
FROM
    "questionable"
ORDER BY 
    "questionable"."physical_sales_item_manager_id" asc,
    "questionable"."avg_monthly_sales" asc,
    "questionable"."sum_sales" asc
LIMIT (100)
```
