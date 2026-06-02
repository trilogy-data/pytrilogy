# Query 97

**Status:** `match`

| Stage | Result |
| --- | --- |
| v4 SQL generation | OK |
| v4 execution | OK (1 rows) |
| reference execution | OK (1 rows) |
| results identical | YES |

## Result comparison

v4 rows: 1 (1 distinct)
ref rows: 1 (1 distinct)

## SQL size + execution time

| Source | Chars | Lines | Exec (min of 4) |
| --- | --- | --- | --- |
| v4 | 2317 | 55 | 8.55 ms |
| reference | 2309 | 55 | 8.34 ms |
| v4 / ref | 1.00x | 1.00x | 1.02x |

## Preql

```
# Generate counts of (customer, item) pairs that appear only in store sales,
# only in catalog sales, or in both, within a 12-month window.
import all_sales as sales;

rowset pair_presence <- where
    sales.sales_channel in ('STORE', 'CATALOG')
    and sales.date.month_seq between 1200 and 1200 + 11
    and sales.billing_customer.id is not null
    and sales.item.id is not null
select
    sales.billing_customer.id,
    sales.item.id,
    max(
            case
                when sales.sales_channel = 'STORE' then sales.order_id
                else 0
            end
        ) as store_present,
    max(
            case
                when sales.sales_channel = 'CATALOG' then sales.order_id
                else 0
            end
        ) as catalog_present,
;

select
    sum(
            case
                when pair_presence.store_present >= 1 and pair_presence.catalog_present = 0 then 1
                else 0
            end
        ) as store_sale_count,
    sum(
            case
                when pair_presence.store_present = 0 and pair_presence.catalog_present >= 1 then 1
                else 0
            end
        ) as catalog_sale_count,
    sum(
            case
                when pair_presence.store_present >= 1 and pair_presence.catalog_present >= 1 then 1
                else 0
            end
        ) as both_sale_count,
;
```

## v4 generated SQL

```sql
WITH 
cheerful as (
SELECT
    "sales_catalog_sales_unified"."CS_BILL_CUSTOMER_SK" as "sales_billing_customer_id",
    "sales_catalog_sales_unified"."CS_ITEM_SK" as "sales_item_id",
    "sales_catalog_sales_unified"."CS_ORDER_NUMBER" as "sales_order_id",
     'CATALOG'  as "sales_sales_channel"
FROM
    "memory"."catalog_sales" as "sales_catalog_sales_unified"
    INNER JOIN "memory"."date_dim" as "sales_date_date" on "sales_catalog_sales_unified"."CS_SOLD_DATE_SK" = "sales_date_date"."D_DATE_SK"
WHERE
    "sales_catalog_sales_unified"."CS_BILL_CUSTOMER_SK" is not null and "sales_catalog_sales_unified"."CS_ITEM_SK" is not null and "sales_date_date"."D_MONTH_SEQ" BETWEEN 1200 AND 1200 + 11

UNION ALL
SELECT
    "sales_store_sales_unified"."SS_CUSTOMER_SK" as "sales_billing_customer_id",
    "sales_store_sales_unified"."SS_ITEM_SK" as "sales_item_id",
    "sales_store_sales_unified"."SS_TICKET_NUMBER" as "sales_order_id",
     'STORE'  as "sales_sales_channel"
FROM
    "memory"."store_sales" as "sales_store_sales_unified"
    INNER JOIN "memory"."date_dim" as "sales_date_date" on "sales_store_sales_unified"."SS_SOLD_DATE_SK" = "sales_date_date"."D_DATE_SK"
WHERE
    "sales_store_sales_unified"."SS_CUSTOMER_SK" is not null and "sales_store_sales_unified"."SS_ITEM_SK" is not null and "sales_date_date"."D_MONTH_SEQ" BETWEEN 1200 AND 1200 + 11
),
cooperative as (
SELECT
    max(CASE
	WHEN "cheerful"."sales_sales_channel" = 'CATALOG' THEN "cheerful"."sales_order_id"
	ELSE 0
	END) as "_pair_presence_catalog_present",
    max(CASE
	WHEN "cheerful"."sales_sales_channel" = 'STORE' THEN "cheerful"."sales_order_id"
	ELSE 0
	END) as "_pair_presence_store_present"
FROM
    "cheerful"
GROUP BY
    "cheerful"."sales_billing_customer_id",
    "cheerful"."sales_item_id")
SELECT
    sum(CASE
	WHEN "cooperative"."_pair_presence_store_present" >= 1 and "cooperative"."_pair_presence_catalog_present" >= 1 THEN 1
	ELSE 0
	END) as "both_sale_count",
    sum(CASE
	WHEN "cooperative"."_pair_presence_store_present" = 0 and "cooperative"."_pair_presence_catalog_present" >= 1 THEN 1
	ELSE 0
	END) as "catalog_sale_count",
    sum(CASE
	WHEN "cooperative"."_pair_presence_store_present" >= 1 and "cooperative"."_pair_presence_catalog_present" = 0 THEN 1
	ELSE 0
	END) as "store_sale_count"
FROM
    "cooperative"
```

## Reference SQL (zquery log)

```sql
WITH 
cheerful as (
SELECT
    "sales_catalog_sales_unified"."CS_BILL_CUSTOMER_SK" as "sales_billing_customer_id",
    "sales_catalog_sales_unified"."CS_ITEM_SK" as "sales_item_id",
    "sales_catalog_sales_unified"."CS_ORDER_NUMBER" as "sales_order_id",
     'CATALOG'  as "sales_sales_channel"
FROM
    "memory"."catalog_sales" as "sales_catalog_sales_unified"
    INNER JOIN "memory"."date_dim" as "sales_date_date" on "sales_catalog_sales_unified"."CS_SOLD_DATE_SK" = "sales_date_date"."D_DATE_SK"
WHERE
    "sales_catalog_sales_unified"."CS_BILL_CUSTOMER_SK" is not null and "sales_catalog_sales_unified"."CS_ITEM_SK" is not null and "sales_date_date"."D_MONTH_SEQ" BETWEEN 1200 AND 1200 + 11

UNION ALL
SELECT
    "sales_store_sales_unified"."SS_CUSTOMER_SK" as "sales_billing_customer_id",
    "sales_store_sales_unified"."SS_ITEM_SK" as "sales_item_id",
    "sales_store_sales_unified"."SS_TICKET_NUMBER" as "sales_order_id",
     'STORE'  as "sales_sales_channel"
FROM
    "memory"."store_sales" as "sales_store_sales_unified"
    INNER JOIN "memory"."date_dim" as "sales_date_date" on "sales_store_sales_unified"."SS_SOLD_DATE_SK" = "sales_date_date"."D_DATE_SK"
WHERE
    "sales_store_sales_unified"."SS_CUSTOMER_SK" is not null and "sales_store_sales_unified"."SS_ITEM_SK" is not null and "sales_date_date"."D_MONTH_SEQ" BETWEEN 1200 AND 1200 + 11
),
cooperative as (
SELECT
    max(CASE
	WHEN "cheerful"."sales_sales_channel" = 'CATALOG' THEN "cheerful"."sales_order_id"
	ELSE 0
	END) as "pair_presence_catalog_present",
    max(CASE
	WHEN "cheerful"."sales_sales_channel" = 'STORE' THEN "cheerful"."sales_order_id"
	ELSE 0
	END) as "pair_presence_store_present"
FROM
    "cheerful"
GROUP BY
    "cheerful"."sales_billing_customer_id",
    "cheerful"."sales_item_id")
SELECT
    sum(CASE
	WHEN "cooperative"."pair_presence_store_present" >= 1 and "cooperative"."pair_presence_catalog_present" = 0 THEN 1
	ELSE 0
	END) as "store_sale_count",
    sum(CASE
	WHEN "cooperative"."pair_presence_store_present" = 0 and "cooperative"."pair_presence_catalog_present" >= 1 THEN 1
	ELSE 0
	END) as "catalog_sale_count",
    sum(CASE
	WHEN "cooperative"."pair_presence_store_present" >= 1 and "cooperative"."pair_presence_catalog_present" >= 1 THEN 1
	ELSE 0
	END) as "both_sale_count"
FROM
    "cooperative"
```
