# Query 40

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
| v4 | 2293 | 43 | 35.55 ms |
| reference | 1809 | 27 | 17.36 ms |
| v4 / ref | 1.27x | 1.59x | 2.05x |

## Preql

```
import catalog_sales as catalog_sales;
import catalog_returns as catalog_returns;

merge catalog_returns.item.id into ~catalog_sales.item.id;
merge catalog_returns.order_number into ~catalog_sales.order_number;

const cutoff <- '2000-03-11'::date;
const start_date <- '2000-02-10'::date;
const end_date <- '2000-04-10'::date;

where
    catalog_sales.item.current_price between 0.99 and 1.49
    and catalog_sales.sold_date.date between start_date and end_date
select
    catalog_sales.warehouse.state,
    catalog_sales.item.name,
    sum(
            case
                when catalog_sales.sold_date.date < cutoff then catalog_sales.sales_price - coalesce(catalog_returns.refunded_cash, 0.0)
                else 0.0
            end
        ) as sales_before,
    sum(
            case
                when catalog_sales.sold_date.date >= cutoff then catalog_sales.sales_price - coalesce(catalog_returns.refunded_cash, 0.0)
                else 0.0
            end
        ) as sales_after,
order by
    catalog_sales.warehouse.state asc,
    catalog_sales.item.name asc
limit 100
;
```

## v4 generated SQL

```sql
WITH 
cooperative as (
SELECT
    "catalog_returns_catalog_returns"."CR_REFUNDED_CASH" as "catalog_returns_refunded_cash",
    "catalog_sales_catalog_sales"."CS_SALES_PRICE" as "catalog_sales_sales_price",
    "catalog_sales_item_items"."I_ITEM_ID" as "catalog_sales_item_name",
    "catalog_sales_warehouse_warehouse"."w_state" as "catalog_sales_warehouse_state",
    cast("catalog_sales_sold_date_date"."D_DATE" as date) as "catalog_sales_sold_date_date"
FROM
    "memory"."catalog_sales" as "catalog_sales_catalog_sales"
    LEFT OUTER JOIN "memory"."catalog_returns" as "catalog_returns_catalog_returns" on "catalog_sales_catalog_sales"."CS_ITEM_SK" = "catalog_returns_catalog_returns"."CR_ITEM_SK" AND "catalog_sales_catalog_sales"."CS_ORDER_NUMBER" = "catalog_returns_catalog_returns"."CR_ORDER_NUMBER"
    INNER JOIN "memory"."date_dim" as "catalog_sales_sold_date_date" on "catalog_sales_catalog_sales"."CS_SOLD_DATE_SK" = "catalog_sales_sold_date_date"."D_DATE_SK"
    LEFT OUTER JOIN "memory"."warehouse" as "catalog_sales_warehouse_warehouse" on "catalog_sales_catalog_sales"."CS_WAREHOUSE_SK" = "catalog_sales_warehouse_warehouse"."w_warehouse_sk"
    INNER JOIN "memory"."item" as "catalog_sales_item_items" on "catalog_sales_catalog_sales"."CS_ITEM_SK" = "catalog_sales_item_items"."I_ITEM_SK"
WHERE
    "catalog_sales_item_items"."I_CURRENT_PRICE" BETWEEN 0.99 AND 1.49 and cast("catalog_sales_sold_date_date"."D_DATE" as date) BETWEEN :start_date AND :end_date

GROUP BY
    1,
    2,
    3,
    4,
    5)
SELECT
    "cooperative"."catalog_sales_item_name" as "catalog_sales_item_name",
    "cooperative"."catalog_sales_warehouse_state" as "catalog_sales_warehouse_state",
    sum(CASE
	WHEN "cooperative"."catalog_sales_sold_date_date" >= :cutoff THEN "cooperative"."catalog_sales_sales_price" - coalesce("cooperative"."catalog_returns_refunded_cash",0.0)
	ELSE 0.0
	END) as "sales_after",
    sum(CASE
	WHEN "cooperative"."catalog_sales_sold_date_date" < :cutoff THEN "cooperative"."catalog_sales_sales_price" - coalesce("cooperative"."catalog_returns_refunded_cash",0.0)
	ELSE 0.0
	END) as "sales_before"
FROM
    "cooperative"
GROUP BY
    1,
    2
ORDER BY 
    "cooperative"."catalog_sales_warehouse_state" asc,
    "cooperative"."catalog_sales_item_name" asc
LIMIT (100)
```

## Reference SQL (zquery log)

```sql
SELECT
    "catalog_sales_warehouse_warehouse"."w_state" as "catalog_sales_warehouse_state",
    "catalog_sales_item_items"."I_ITEM_ID" as "catalog_sales_item_name",
    sum(CASE
	WHEN cast("catalog_sales_sold_date_date"."D_DATE" as date) < :cutoff THEN "catalog_sales_catalog_sales"."CS_SALES_PRICE" - coalesce("catalog_returns_catalog_returns"."CR_REFUNDED_CASH",0.0)
	ELSE 0.0
	END) as "sales_before",
    sum(CASE
	WHEN cast("catalog_sales_sold_date_date"."D_DATE" as date) >= :cutoff THEN "catalog_sales_catalog_sales"."CS_SALES_PRICE" - coalesce("catalog_returns_catalog_returns"."CR_REFUNDED_CASH",0.0)
	ELSE 0.0
	END) as "sales_after"
FROM
    "memory"."catalog_sales" as "catalog_sales_catalog_sales"
    LEFT OUTER JOIN "memory"."catalog_returns" as "catalog_returns_catalog_returns" on "catalog_sales_catalog_sales"."CS_ITEM_SK" = "catalog_returns_catalog_returns"."CR_ITEM_SK" AND "catalog_sales_catalog_sales"."CS_ORDER_NUMBER" = "catalog_returns_catalog_returns"."CR_ORDER_NUMBER"
    INNER JOIN "memory"."date_dim" as "catalog_sales_sold_date_date" on "catalog_sales_catalog_sales"."CS_SOLD_DATE_SK" = "catalog_sales_sold_date_date"."D_DATE_SK"
    LEFT OUTER JOIN "memory"."warehouse" as "catalog_sales_warehouse_warehouse" on "catalog_sales_catalog_sales"."CS_WAREHOUSE_SK" = "catalog_sales_warehouse_warehouse"."w_warehouse_sk"
    INNER JOIN "memory"."item" as "catalog_sales_item_items" on "catalog_sales_catalog_sales"."CS_ITEM_SK" = "catalog_sales_item_items"."I_ITEM_SK"
WHERE
    "catalog_sales_item_items"."I_CURRENT_PRICE" BETWEEN 0.99 AND 1.49 and cast("catalog_sales_sold_date_date"."D_DATE" as date) BETWEEN :start_date AND :end_date

GROUP BY
    1,
    2
ORDER BY 
    "catalog_sales_warehouse_warehouse"."w_state" asc,
    "catalog_sales_item_items"."I_ITEM_ID" asc
LIMIT (100)
```
