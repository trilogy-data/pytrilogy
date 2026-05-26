# Query 16

**Status:** `exec_fail`

| Stage | Result |
| --- | --- |
| v4 SQL generation | OK |
| v4 execution | FAILED |
| reference execution | OK (1 rows) |

## Result comparison

_at least one side did not produce rows._

## SQL size + execution time

| Source | Chars | Lines | Exec (min of 4) |
| --- | --- | --- | --- |
| v4 | 372 | 8 | — |
| reference | 3713 | 83 | 72.73 ms |
| v4 / ref | 0.10x | 0.10x | — |

## Preql

```
import catalog_sales as cs;
import catalog_returns as cr;

auto multi_warehouse_sales <- cs.order_number ? count(cs.warehouse.id) by cs.order_number > 1;

where
    cs.ship_date.date between '2002-02-01'::date and '2002-04-02'::date
    and cs.customer_address.state = 'GA'
    and cs.call_center.county = 'Williamson County'
    and cs.order_number not in cr.order_number
    and cs.order_number in multi_warehouse_sales
select
    count_distinct(cs.order_number) as order_count,
    sum(cs.ext_ship_cost) as total_shipping_cost,
    sum(cs.net_profit) as total_net_profit,
order by
    order_count desc
limit 100
;
```

## v4 generated SQL

```sql
SELECT
    CASE WHENINVALID_REFERENCE_BUG_<Missing source reference to cs.order_number> IS NOT NULL THEN 1 ELSE 0 END as "order_count",
    INVALID_REFERENCE_BUG_<Missing source reference to cs.ext_ship_cost> as "total_shipping_cost",
    INVALID_REFERENCE_BUG_<Missing source reference to cs.net_profit> as "total_net_profit"

ORDER BY 
    "order_count" desc
LIMIT (100)
```

## Reference SQL (zquery log)

```sql
WITH 
cooperative as (
SELECT
    "cs_catalog_sales"."CS_ORDER_NUMBER" as "cs_order_number",
    "cs_catalog_sales"."CS_WAREHOUSE_SK" as "cs_warehouse_id"
FROM
    "memory"."catalog_sales" as "cs_catalog_sales"
GROUP BY
    1,
    2),
quizzical as (
SELECT
    "cr_catalog_returns"."CR_ORDER_NUMBER" as "cr_order_number"
FROM
    "memory"."catalog_returns" as "cr_catalog_returns"
GROUP BY
    1),
questionable as (
SELECT
    "cooperative"."cs_order_number" as "multi_warehouse_sales"
FROM
    "cooperative"
GROUP BY
    1
HAVING
    count("cooperative"."cs_warehouse_id") > 1
),
abundant as (
SELECT
    "questionable"."multi_warehouse_sales" as "multi_warehouse_sales"
FROM
    "questionable"),
thoughtful as (
SELECT
    "cs_catalog_sales"."CS_ORDER_NUMBER" as "cs_order_number"
FROM
    "memory"."catalog_sales" as "cs_catalog_sales"
    INNER JOIN "memory"."date_dim" as "cs_ship_date_date" on "cs_catalog_sales"."CS_SHIP_DATE_SK" = "cs_ship_date_date"."D_DATE_SK"
    INNER JOIN "memory"."call_center" as "cs_call_center_call_center" on "cs_catalog_sales"."CS_CALL_CENTER_SK" = "cs_call_center_call_center"."CC_CALL_CENTER_SK"
    INNER JOIN "memory"."customer_address" as "cs_customer_address_customer_address" on "cs_catalog_sales"."CS_SHIP_ADDR_SK" = "cs_customer_address_customer_address"."CA_ADDRESS_SK"
WHERE
    cast("cs_ship_date_date"."D_DATE" as date) BETWEEN date '2002-02-01' AND date '2002-04-02' and "cs_call_center_call_center"."CC_COUNTY" = 'Williamson County' and "cs_customer_address_customer_address"."CA_STATE" = 'GA' and "cs_catalog_sales"."CS_ORDER_NUMBER" not in (select quizzical."cr_order_number" from quizzical where quizzical."cr_order_number" is not null) and "cs_catalog_sales"."CS_ORDER_NUMBER" in (select abundant."multi_warehouse_sales" from abundant where abundant."multi_warehouse_sales" is not null)

GROUP BY
    1),
concerned as (
SELECT
    "cs_catalog_sales"."CS_EXT_SHIP_COST" as "cs_ext_ship_cost",
    "cs_catalog_sales"."CS_NET_PROFIT" as "cs_net_profit"
FROM
    "memory"."catalog_sales" as "cs_catalog_sales"
    INNER JOIN "memory"."date_dim" as "cs_ship_date_date" on "cs_catalog_sales"."CS_SHIP_DATE_SK" = "cs_ship_date_date"."D_DATE_SK"
    INNER JOIN "memory"."call_center" as "cs_call_center_call_center" on "cs_catalog_sales"."CS_CALL_CENTER_SK" = "cs_call_center_call_center"."CC_CALL_CENTER_SK"
    INNER JOIN "memory"."customer_address" as "cs_customer_address_customer_address" on "cs_catalog_sales"."CS_SHIP_ADDR_SK" = "cs_customer_address_customer_address"."CA_ADDRESS_SK"
WHERE
    cast("cs_ship_date_date"."D_DATE" as date) BETWEEN date '2002-02-01' AND date '2002-04-02' and "cs_customer_address_customer_address"."CA_STATE" = 'GA' and "cs_call_center_call_center"."CC_COUNTY" = 'Williamson County' and "cs_catalog_sales"."CS_ORDER_NUMBER" not in (select quizzical."cr_order_number" from quizzical where quizzical."cr_order_number" is not null) and "cs_catalog_sales"."CS_ORDER_NUMBER" in (select abundant."multi_warehouse_sales" from abundant where abundant."multi_warehouse_sales" is not null)

GROUP BY
    1,
    2,
    "cs_catalog_sales"."CS_ITEM_SK",
    "cs_catalog_sales"."CS_ORDER_NUMBER"),
vacuous as (
SELECT
    count(distinct "thoughtful"."cs_order_number") as "order_count"
FROM
    "thoughtful"),
young as (
SELECT
    sum("concerned"."cs_ext_ship_cost") as "total_shipping_cost",
    sum("concerned"."cs_net_profit") as "total_net_profit"
FROM
    "concerned")
SELECT
    "vacuous"."order_count" as "order_count",
    "young"."total_shipping_cost" as "total_shipping_cost",
    "young"."total_net_profit" as "total_net_profit"
FROM
    "vacuous"
    FULL JOIN "young" on 1=1
ORDER BY 
    "vacuous"."order_count" desc
LIMIT (100)
```

## v4 execution error

```
Traceback (most recent call last):
  File "C:\Users\ethan\coding_projects\pytrilogy\local_scripts\discovery_v4_compare.py", line 179, in run_one
    result.v4_exec_seconds, result.v4_rows = _time(
                                             ~~~~~^
        lambda: execute(con, v4_sql)
        ^^^^^^^^^^^^^^^^^^^^^^^^^^^^
    )
    ^
  File "C:\Users\ethan\coding_projects\pytrilogy\local_scripts\discovery_v4_compare.py", line 45, in _time
    value = fn()
  File "C:\Users\ethan\coding_projects\pytrilogy\local_scripts\discovery_v4_compare.py", line 180, in <lambda>
    lambda: execute(con, v4_sql)
            ~~~~~~~^^^^^^^^^^^^^
  File "C:\Users\ethan\coding_projects\pytrilogy\local_scripts\discovery_v4_compare.py", line 120, in execute
    cursor = con.execute(sql)
_duckdb.ParserException: Parser Error: syntax error at or near "source"

LINE 2:     CASE WHENINVALID_REFERENCE_BUG_<Missing source reference to cs.order_number> IS NOT NULL THEN 1...
                                                    ^
```
