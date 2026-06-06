# Query 87

**Status:** `gen_fail`

| Stage | Result |
| --- | --- |
| v4 SQL generation | FAILED |
| reference execution | OK (1 rows) |

## Result comparison

_at least one side did not produce rows._

## SQL size + execution time

| Source | Chars | Lines | Exec (min of 4) |
| --- | --- | --- | --- |
| v4 | 0 | 0 | — |
| reference | 2548 | 65 | 19.22 ms |

## Preql

```
import all_sales as sales;

# For each (customer.last_name, customer.first_name, date.date) tuple,
# count rows present in each channel within month_seq 1200..1211.
auto store_in_window <- sum(
    case
        when sales.sales_channel = 'STORE'
        and sales.date.month_seq between 1200 and 1200 + 11
        and sales.billing_customer.id is not null then 1
        else 0
    end
)
    by sales.billing_customer.last_name, sales.billing_customer.first_name, sales.date.date;
auto catalog_in_window <- sum(
    case
        when sales.sales_channel = 'CATALOG'
        and sales.date.month_seq between 1200 and 1200 + 11
        and sales.billing_customer.id is not null then 1
        else 0
    end
)
    by sales.billing_customer.last_name, sales.billing_customer.first_name, sales.date.date;
auto web_in_window <- sum(
    case
        when sales.sales_channel = 'WEB'
        and sales.date.month_seq between 1200 and 1200 + 11
        and sales.billing_customer.id is not null then 1
        else 0
    end
)
    by sales.billing_customer.last_name, sales.billing_customer.first_name, sales.date.date;

where
    sales.date.month_seq between 1200 and 1200 + 11
select
    sum(
            case
                when store_in_window > 0 and catalog_in_window = 0 and web_in_window = 0 then 1
                else 0
            end
        ) as cnt,
;
```

## v4 generated SQL

_v4 did not produce SQL._

## Reference SQL (zquery log)

```sql
WITH 
thoughtful as (
SELECT
    "sales_catalog_sales_unified"."CS_BILL_CUSTOMER_SK" as "sales_billing_customer_id",
    "sales_catalog_sales_unified"."CS_SOLD_DATE_SK" as "sales_date_id",
     'CATALOG'  as "sales_sales_channel"
FROM
    "memory"."catalog_sales" as "sales_catalog_sales_unified"
UNION ALL
SELECT
    "sales_store_sales_unified"."SS_CUSTOMER_SK" as "sales_billing_customer_id",
    "sales_store_sales_unified"."SS_SOLD_DATE_SK" as "sales_date_id",
     'STORE'  as "sales_sales_channel"
FROM
    "memory"."store_sales" as "sales_store_sales_unified"
UNION ALL
SELECT
    "sales_web_sales_unified"."WS_BILL_CUSTOMER_SK" as "sales_billing_customer_id",
    "sales_web_sales_unified"."WS_SOLD_DATE_SK" as "sales_date_id",
     'WEB'  as "sales_sales_channel"
FROM
    "memory"."web_sales" as "sales_web_sales_unified"),
cooperative as (
SELECT
    "thoughtful"."sales_billing_customer_id" as "sales_billing_customer_id",
    "thoughtful"."sales_date_id" as "sales_date_id",
    "thoughtful"."sales_sales_channel" as "sales_sales_channel"
FROM
    "thoughtful"
GROUP BY
    1,
    2,
    3),
abundant as (
SELECT
    sum(CASE
	WHEN "cooperative"."sales_sales_channel" = 'CATALOG' and "sales_date_date"."D_MONTH_SEQ" BETWEEN 1200 AND 1200 + 11 and "cooperative"."sales_billing_customer_id" is not null THEN 1
	ELSE 0
	END) as "catalog_in_window",
    sum(CASE
	WHEN "cooperative"."sales_sales_channel" = 'STORE' and "sales_date_date"."D_MONTH_SEQ" BETWEEN 1200 AND 1200 + 11 and "cooperative"."sales_billing_customer_id" is not null THEN 1
	ELSE 0
	END) as "store_in_window",
    sum(CASE
	WHEN "cooperative"."sales_sales_channel" = 'WEB' and "sales_date_date"."D_MONTH_SEQ" BETWEEN 1200 AND 1200 + 11 and "cooperative"."sales_billing_customer_id" is not null THEN 1
	ELSE 0
	END) as "web_in_window"
FROM
    "cooperative"
    INNER JOIN "memory"."date_dim" as "sales_date_date" on "cooperative"."sales_date_id" = "sales_date_date"."D_DATE_SK"
    LEFT OUTER JOIN "memory"."customer" as "sales_billing_customer_customers" on "cooperative"."sales_billing_customer_id" = "sales_billing_customer_customers"."C_CUSTOMER_SK"
WHERE
    "sales_date_date"."D_MONTH_SEQ" BETWEEN 1200 AND 1200 + 11

GROUP BY
    "sales_billing_customer_customers"."C_FIRST_NAME",
    "sales_billing_customer_customers"."C_LAST_NAME",
    cast("sales_date_date"."D_DATE" as date))
SELECT
    sum(CASE
	WHEN "abundant"."store_in_window" > 0 and "abundant"."catalog_in_window" = 0 and "abundant"."web_in_window" = 0 THEN 1
	ELSE 0
	END) as "cnt"
FROM
    "abundant"
```

## v4 generation error

```
Traceback (most recent call last):
  File "C:\Users\ethan\coding_projects\pytrilogy\local_scripts\discovery_v4_compare.py", line 256, in generate_v4_sql
    statements = eng.generate_sql(preql_path.read_text())
  File "C:\Program Files\Python313\Lib\functools.py", line 983, in _method
    return dispatch(args[0].__class__).__get__(obj, cls)(*args, **kwargs)
           ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~^^^^^^^^^^^^^^^^^
  File "C:\Users\ethan\coding_projects\pytrilogy\trilogy\executor.py", line 663, in _
    compiled_sql = self.generator.compile_statement(statement)
  File "C:\Users\ethan\coding_projects\pytrilogy\trilogy\dialect\base.py", line 2315, in compile_statement
    raise ValueError(
    ...<2 lines>...
    )
ValueError: Invalid reference string found in query: 
WITH 
quizzical as (
SELECT
    sum(CASE
	WHEN INVALID_REFERENCE_BUG = 'CATALOG' and INVALID_REFERENCE_BUG BETWEEN 1200 AND 1200 + 11 and INVALID_REFERENCE_BUG is not null THEN 1
	ELSE 0
	END) as "catalog_in_window",
    sum(CASE
	WHEN INVALID_REFERENCE_BUG = 'STORE' and INVALID_REFERENCE_BUG BETWEEN 1200 AND 1200 + 11 and INVALID_REFERENCE_BUG is not null THEN 1
	ELSE 0
	END) as "store_in_window",
    sum(CASE
	WHEN INVALID_REFERENCE_BUG = 'WEB' and INVALID_REFERENCE_BUG BETWEEN 1200 AND 1200 + 11 and INVALID_REFERENCE_BUG is not null THEN 1
	ELSE 0
	END) as "web_in_window"

GROUP BY
    INVALID_REFERENCE_BUG)
SELECT
    sum(CASE
	WHEN "quizzical"."store_in_window" > 0 and "quizzical"."catalog_in_window" = 0 and "quizzical"."web_in_window" = 0 THEN 1
	ELSE 0
	END) as "cnt"
FROM
    "quizzical", this should never occur. Please create an issue to report this.
```
