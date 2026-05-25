# Query 31

**Status:** `exec_fail`

| Stage | Result |
| --- | --- |
| v4 SQL generation | OK |
| v4 execution | FAILED |
| reference execution | OK (44 rows) |

## Result comparison

_at least one side did not produce rows._

## SQL size

| Source | Chars | Lines |
| --- | --- | --- |
| v4 | 6083 | 138 |
| reference | 5451 | 57 |
| v4 / ref | 1.12x | 2.42x |

## Preql

```
import unified_sales as sales;

def channel_qtr(channel, qtr) -> sum(sales.ext_sales_price ? sales.sales_channel = channel and sales.date.quarter = qtr)
    by sales.bill_address.county, sales.date.year;

auto ss_q1 <- @channel_qtr('STORE', 1);
auto ss_q2 <- @channel_qtr('STORE', 2);
auto ss_q3 <- @channel_qtr('STORE', 3);
auto ws_q1 <- @channel_qtr('WEB', 1);
auto ws_q2 <- @channel_qtr('WEB', 2);
auto ws_q3 <- @channel_qtr('WEB', 3);
auto store_q1_q2_increase <- ss_q2 / ss_q1;
auto store_q2_q3_increase <- ss_q3 / ss_q2;
auto web_q1_q2_increase <- ws_q2 / ws_q1;
auto web_q2_q3_increase <- ws_q3 / ws_q2;

where
    sales.date.year = 2000
    and sales.sales_channel in ('STORE', 'WEB')
    and sales.date.quarter in (1, 2, 3)
select
    sales.bill_address.county,
    sales.date.year,
    web_q1_q2_increase,
    store_q1_q2_increase,
    web_q2_q3_increase,
    store_q2_q3_increase,
    --ss_q1,
    --ss_q2,
    --ss_q3,
    --ws_q1,
    --ws_q2,
    --ws_q3,
having
    ss_q1 > 0
    and ss_q2 > 0
    and (
        case
            when ws_q1 > 0 then ws_q2 / ws_q1
            else null
        end > case
            when ss_q1 > 0 then ss_q2 / ss_q1
            else null
        end
    )
    and (
        case
            when ws_q2 > 0 then ws_q3 / ws_q2
            else null
        end > case
            when ss_q2 > 0 then ss_q3 / ss_q2
            else null
        end
    )

order by
    sales.bill_address.county asc nulls first
;
```

## v4 generated SQL

```sql
WITH 
questionable as (
SELECT
    "sales_date_date"."D_DATE_SK" as "sales_date_id",
    "sales_date_date"."D_QOY" as "sales_date_quarter",
    "sales_date_date"."D_YEAR" as "sales_date_year"
FROM
    "memory"."date_dim" as "sales_date_date"),
thoughtful as (
SELECT
    "sales_catalog_sales_unified"."CS_BILL_ADDR_SK" as "sales_bill_address_id",
    "sales_catalog_sales_unified"."CS_SOLD_DATE_SK" as "sales_date_id",
    "sales_catalog_sales_unified"."CS_EXT_SALES_PRICE" as "sales_ext_sales_price",
     'CATALOG'  as "sales_sales_channel"
FROM
    "memory"."catalog_sales" as "sales_catalog_sales_unified"
UNION ALL
SELECT
    "sales_store_sales_unified"."SS_ADDR_SK" as "sales_bill_address_id",
    "sales_store_sales_unified"."SS_SOLD_DATE_SK" as "sales_date_id",
    "sales_store_sales_unified"."SS_EXT_SALES_PRICE" as "sales_ext_sales_price",
     'STORE'  as "sales_sales_channel"
FROM
    "memory"."store_sales" as "sales_store_sales_unified"
UNION ALL
SELECT
    "sales_web_sales_unified"."WS_BILL_ADDR_SK" as "sales_bill_address_id",
    "sales_web_sales_unified"."WS_SOLD_DATE_SK" as "sales_date_id",
    "sales_web_sales_unified"."WS_EXT_SALES_PRICE" as "sales_ext_sales_price",
     'WEB'  as "sales_sales_channel"
FROM
    "memory"."web_sales" as "sales_web_sales_unified"),
cooperative as (
SELECT
    "thoughtful"."sales_bill_address_id" as "sales_bill_address_id",
    "thoughtful"."sales_date_id" as "sales_date_id",
    "thoughtful"."sales_ext_sales_price" as "sales_ext_sales_price",
    "thoughtful"."sales_sales_channel" as "sales_sales_channel"
FROM
    "thoughtful"
GROUP BY
    1,
    2,
    3,
    4),
quizzical as (
SELECT
    "sales_bill_address_customer_address"."CA_ADDRESS_SK" as "sales_bill_address_id",
    "sales_bill_address_customer_address"."CA_COUNTY" as "sales_bill_address_county"
FROM
    "memory"."customer_address" as "sales_bill_address_customer_address"),
abundant as (
SELECT
    "cooperative"."sales_ext_sales_price" as "sales_ext_sales_price",
    "cooperative"."sales_sales_channel" as "sales_sales_channel",
    "questionable"."sales_date_quarter" as "sales_date_quarter",
    "questionable"."sales_date_year" as "sales_date_year",
    "quizzical"."sales_bill_address_county" as "sales_bill_address_county"
FROM
    "cooperative"
    LEFT OUTER JOIN "questionable" on "cooperative"."sales_date_id" = "questionable"."sales_date_id"
    LEFT OUTER JOIN "quizzical" on "cooperative"."sales_bill_address_id" = "quizzical"."sales_bill_address_id"
WHERE
    "questionable"."sales_date_year" = 2000 and "cooperative"."sales_sales_channel" in ('STORE','WEB') and "questionable"."sales_date_quarter" in (1,2,3)

GROUP BY
    1,
    2,
    3,
    4,
    5),
uneven as (
SELECT
    CASE WHEN "abundant"."sales_sales_channel" = 'STORE' and "abundant"."sales_date_quarter" = 1 THEN "abundant"."sales_ext_sales_price" ELSE NULL END as "_virt_filter_ext_sales_price_7655500688085854",
    CASE WHEN "abundant"."sales_sales_channel" = 'STORE' and "abundant"."sales_date_quarter" = 2 THEN "abundant"."sales_ext_sales_price" ELSE NULL END as "_virt_filter_ext_sales_price_281852079532064",
    CASE WHEN "abundant"."sales_sales_channel" = 'STORE' and "abundant"."sales_date_quarter" = 3 THEN "abundant"."sales_ext_sales_price" ELSE NULL END as "_virt_filter_ext_sales_price_729201542198071",
    CASE WHEN "abundant"."sales_sales_channel" = 'WEB' and "abundant"."sales_date_quarter" = 1 THEN "abundant"."sales_ext_sales_price" ELSE NULL END as "_virt_filter_ext_sales_price_8458384044316949",
    CASE WHEN "abundant"."sales_sales_channel" = 'WEB' and "abundant"."sales_date_quarter" = 2 THEN "abundant"."sales_ext_sales_price" ELSE NULL END as "_virt_filter_ext_sales_price_4134243337115957",
    CASE WHEN "abundant"."sales_sales_channel" = 'WEB' and "abundant"."sales_date_quarter" = 3 THEN "abundant"."sales_ext_sales_price" ELSE NULL END as "_virt_filter_ext_sales_price_3771707931091746"
FROM
    "abundant"),
yummy as (
SELECT
    "abundant"."sales_bill_address_county" as "sales_bill_address_county",
    "abundant"."sales_date_year" as "sales_date_year",
    sum("uneven"."_virt_filter_ext_sales_price_281852079532064") as "ss_q2",
    sum("uneven"."_virt_filter_ext_sales_price_3771707931091746") as "ws_q3",
    sum("uneven"."_virt_filter_ext_sales_price_4134243337115957") as "ws_q2",
    sum("uneven"."_virt_filter_ext_sales_price_729201542198071") as "ss_q3",
    sum("uneven"."_virt_filter_ext_sales_price_7655500688085854") as "ss_q1",
    sum("uneven"."_virt_filter_ext_sales_price_8458384044316949") as "ws_q1"
FROM
    "uneven"
GROUP BY
    1,
    2),
juicy as (
SELECT
    "yummy"."sales_bill_address_county" as "sales_bill_address_county",
    "yummy"."sales_date_year" as "sales_date_year",
    "yummy"."ss_q1" as "ss_q1",
    "yummy"."ss_q2" / "yummy"."ss_q1" as "store_q1_q2_increase",
    "yummy"."ss_q2" as "ss_q2",
    "yummy"."ss_q3" / "yummy"."ss_q2" as "store_q2_q3_increase",
    "yummy"."ss_q3" as "ss_q3",
    "yummy"."ws_q1" as "ws_q1",
    "yummy"."ws_q2" / "yummy"."ws_q1" as "web_q1_q2_increase",
    "yummy"."ws_q2" as "ws_q2",
    "yummy"."ws_q3" / "yummy"."ws_q2" as "web_q2_q3_increase",
    "yummy"."ws_q3" as "ws_q3"
FROM
    "yummy")
SELECT
    "juicy"."sales_bill_address_county" as "sales_bill_address_county",
    "juicy"."sales_date_year" as "sales_date_year",
    "juicy"."web_q1_q2_increase" as "web_q1_q2_increase",
    "juicy"."store_q1_q2_increase" as "store_q1_q2_increase",
    "juicy"."web_q2_q3_increase" as "web_q2_q3_increase",
    "juicy"."store_q2_q3_increase" as "store_q2_q3_increase"
FROM
    "juicy"
WHERE
    "juicy"."ss_q1" > 0 and "juicy"."ss_q2" > 0 and ( CASE
	WHEN "juicy"."ws_q1" > 0 THEN "juicy"."ws_q2" / "juicy"."ws_q1"
	ELSE null
	END > CASE
	WHEN "juicy"."ss_q1" > 0 THEN "juicy"."ss_q2" / "juicy"."ss_q1"
	ELSE null
	END ) and ( CASE
	WHEN "juicy"."ws_q2" > 0 THEN "juicy"."ws_q3" / "juicy"."ws_q2"
	ELSE null
	END > CASE
	WHEN "juicy"."ss_q2" > 0 THEN "juicy"."ss_q3" / "juicy"."ss_q2"
	ELSE null
	END )

ORDER BY 
    "juicy"."sales_bill_address_county" asc nulls first
```

## Reference SQL (zquery log)

```sql
WITH 
thoughtful as (
SELECT
    "sales_store_sales_unified"."SS_ADDR_SK" as "sales_bill_address_id",
    "sales_store_sales_unified"."SS_EXT_SALES_PRICE" as "sales_ext_sales_price",
     'STORE'  as "sales_sales_channel",
    "sales_date_date"."D_QOY" as "sales_date_quarter",
    "sales_date_date"."D_YEAR" as "sales_date_year"
FROM
    "memory"."store_sales" as "sales_store_sales_unified"
    INNER JOIN "memory"."date_dim" as "sales_date_date" on "sales_store_sales_unified"."SS_SOLD_DATE_SK" = "sales_date_date"."D_DATE_SK"
WHERE
    "sales_date_date"."D_YEAR" = 2000 and "sales_date_date"."D_QOY" in (1,2,3)

UNION ALL
SELECT
    "sales_web_sales_unified"."WS_BILL_ADDR_SK" as "sales_bill_address_id",
    "sales_web_sales_unified"."WS_EXT_SALES_PRICE" as "sales_ext_sales_price",
     'WEB'  as "sales_sales_channel",
    "sales_date_date"."D_QOY" as "sales_date_quarter",
    "sales_date_date"."D_YEAR" as "sales_date_year"
FROM
    "memory"."web_sales" as "sales_web_sales_unified"
    INNER JOIN "memory"."date_dim" as "sales_date_date" on "sales_web_sales_unified"."WS_SOLD_DATE_SK" = "sales_date_date"."D_DATE_SK"
WHERE
    "sales_date_date"."D_YEAR" = 2000 and "sales_date_date"."D_QOY" in (1,2,3)
)
SELECT
    "sales_bill_address_customer_address"."CA_COUNTY" as "sales_bill_address_county",
    "thoughtful"."sales_date_year" as "sales_date_year",
    sum(CASE WHEN "thoughtful"."sales_sales_channel" = 'WEB' and "thoughtful"."sales_date_quarter" = 2 THEN "thoughtful"."sales_ext_sales_price" ELSE NULL END) / sum(CASE WHEN "thoughtful"."sales_sales_channel" = 'WEB' and "thoughtful"."sales_date_quarter" = 1 THEN "thoughtful"."sales_ext_sales_price" ELSE NULL END) as "web_q1_q2_increase",
    sum(CASE WHEN "thoughtful"."sales_sales_channel" = 'STORE' and "thoughtful"."sales_date_quarter" = 2 THEN "thoughtful"."sales_ext_sales_price" ELSE NULL END) / sum(CASE WHEN "thoughtful"."sales_sales_channel" = 'STORE' and "thoughtful"."sales_date_quarter" = 1 THEN "thoughtful"."sales_ext_sales_price" ELSE NULL END) as "store_q1_q2_increase",
    sum(CASE WHEN "thoughtful"."sales_sales_channel" = 'WEB' and "thoughtful"."sales_date_quarter" = 3 THEN "thoughtful"."sales_ext_sales_price" ELSE NULL END) / sum(CASE WHEN "thoughtful"."sales_sales_channel" = 'WEB' and "thoughtful"."sales_date_quarter" = 2 THEN "thoughtful"."sales_ext_sales_price" ELSE NULL END) as "web_q2_q3_increase",
    sum(CASE WHEN "thoughtful"."sales_sales_channel" = 'STORE' and "thoughtful"."sales_date_quarter" = 3 THEN "thoughtful"."sales_ext_sales_price" ELSE NULL END) / sum(CASE WHEN "thoughtful"."sales_sales_channel" = 'STORE' and "thoughtful"."sales_date_quarter" = 2 THEN "thoughtful"."sales_ext_sales_price" ELSE NULL END) as "store_q2_q3_increase"
FROM
    "thoughtful"
    LEFT OUTER JOIN "memory"."customer_address" as "sales_bill_address_customer_address" on "thoughtful"."sales_bill_address_id" = "sales_bill_address_customer_address"."CA_ADDRESS_SK"
GROUP BY
    1,
    2
HAVING
    sum(CASE WHEN "thoughtful"."sales_sales_channel" = 'STORE' and "thoughtful"."sales_date_quarter" = 1 THEN "thoughtful"."sales_ext_sales_price" ELSE NULL END) > 0 and sum(CASE WHEN "thoughtful"."sales_sales_channel" = 'STORE' and "thoughtful"."sales_date_quarter" = 2 THEN "thoughtful"."sales_ext_sales_price" ELSE NULL END) > 0 and ( CASE
	WHEN sum(CASE WHEN "thoughtful"."sales_sales_channel" = 'WEB' and "thoughtful"."sales_date_quarter" = 1 THEN "thoughtful"."sales_ext_sales_price" ELSE NULL END) > 0 THEN sum(CASE WHEN "thoughtful"."sales_sales_channel" = 'WEB' and "thoughtful"."sales_date_quarter" = 2 THEN "thoughtful"."sales_ext_sales_price" ELSE NULL END) / sum(CASE WHEN "thoughtful"."sales_sales_channel" = 'WEB' and "thoughtful"."sales_date_quarter" = 1 THEN "thoughtful"."sales_ext_sales_price" ELSE NULL END)
	ELSE null
	END > CASE
	WHEN sum(CASE WHEN "thoughtful"."sales_sales_channel" = 'STORE' and "thoughtful"."sales_date_quarter" = 1 THEN "thoughtful"."sales_ext_sales_price" ELSE NULL END) > 0 THEN sum(CASE WHEN "thoughtful"."sales_sales_channel" = 'STORE' and "thoughtful"."sales_date_quarter" = 2 THEN "thoughtful"."sales_ext_sales_price" ELSE NULL END) / sum(CASE WHEN "thoughtful"."sales_sales_channel" = 'STORE' and "thoughtful"."sales_date_quarter" = 1 THEN "thoughtful"."sales_ext_sales_price" ELSE NULL END)
	ELSE null
	END ) and ( CASE
	WHEN sum(CASE WHEN "thoughtful"."sales_sales_channel" = 'WEB' and "thoughtful"."sales_date_quarter" = 2 THEN "thoughtful"."sales_ext_sales_price" ELSE NULL END) > 0 THEN sum(CASE WHEN "thoughtful"."sales_sales_channel" = 'WEB' and "thoughtful"."sales_date_quarter" = 3 THEN "thoughtful"."sales_ext_sales_price" ELSE NULL END) / sum(CASE WHEN "thoughtful"."sales_sales_channel" = 'WEB' and "thoughtful"."sales_date_quarter" = 2 THEN "thoughtful"."sales_ext_sales_price" ELSE NULL END)
	ELSE null
	END > CASE
	WHEN sum(CASE WHEN "thoughtful"."sales_sales_channel" = 'STORE' and "thoughtful"."sales_date_quarter" = 2 THEN "thoughtful"."sales_ext_sales_price" ELSE NULL END) > 0 THEN sum(CASE WHEN "thoughtful"."sales_sales_channel" = 'STORE' and "thoughtful"."sales_date_quarter" = 3 THEN "thoughtful"."sales_ext_sales_price" ELSE NULL END) / sum(CASE WHEN "thoughtful"."sales_sales_channel" = 'STORE' and "thoughtful"."sales_date_quarter" = 2 THEN "thoughtful"."sales_ext_sales_price" ELSE NULL END)
	ELSE null
	END )

ORDER BY 
    "sales_bill_address_customer_address"."CA_COUNTY" asc nulls first
```

## v4 execution error

```
Traceback (most recent call last):
  File "C:\Users\ethan\coding_projects\pytrilogy\local_scripts\discovery_v4_compare.py", line 161, in run_one
    result.v4_rows = execute(con, v4_sql)
                     ~~~~~~~^^^^^^^^^^^^^
  File "C:\Users\ethan\coding_projects\pytrilogy\local_scripts\discovery_v4_compare.py", line 102, in execute
    cursor = con.execute(sql)
_duckdb.BinderException: Binder Error: Referenced table "abundant" not found!
Candidate tables: "uneven"

LINE 84:     "abundant"."sales_bill_address_county" as "sales_bill_addre...
             ^
```
