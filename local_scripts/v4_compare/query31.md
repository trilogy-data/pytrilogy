# Query 31

**Status:** `match`

| Stage | Result |
| --- | --- |
| v4 SQL generation | OK |
| v4 execution | OK (44 rows) |
| reference execution | OK (44 rows) |
| results identical | YES |

## Result comparison

v4 rows: 44 (44 distinct)
ref rows: 44 (44 distinct)

## SQL size + execution time

| Source | Chars | Lines | Exec (min of 4) |
| --- | --- | --- | --- |
| v4 | 3510 | 69 | 127.10 ms |
| reference | 5451 | 57 | 119.61 ms |
| v4 / ref | 0.64x | 1.21x | 1.06x |

## Preql

```
import all_sales as sales;

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
),
questionable as (
SELECT
    "sales_bill_address_customer_address"."CA_COUNTY" as "sales_bill_address_county",
    "thoughtful"."sales_date_year" as "sales_date_year",
    sum(CASE WHEN "thoughtful"."sales_sales_channel" = 'STORE' and "thoughtful"."sales_date_quarter" = 1 THEN "thoughtful"."sales_ext_sales_price" ELSE NULL END) as "ss_q1",
    sum(CASE WHEN "thoughtful"."sales_sales_channel" = 'STORE' and "thoughtful"."sales_date_quarter" = 2 THEN "thoughtful"."sales_ext_sales_price" ELSE NULL END) as "ss_q2",
    sum(CASE WHEN "thoughtful"."sales_sales_channel" = 'STORE' and "thoughtful"."sales_date_quarter" = 3 THEN "thoughtful"."sales_ext_sales_price" ELSE NULL END) as "ss_q3",
    sum(CASE WHEN "thoughtful"."sales_sales_channel" = 'WEB' and "thoughtful"."sales_date_quarter" = 1 THEN "thoughtful"."sales_ext_sales_price" ELSE NULL END) as "ws_q1",
    sum(CASE WHEN "thoughtful"."sales_sales_channel" = 'WEB' and "thoughtful"."sales_date_quarter" = 2 THEN "thoughtful"."sales_ext_sales_price" ELSE NULL END) as "ws_q2",
    sum(CASE WHEN "thoughtful"."sales_sales_channel" = 'WEB' and "thoughtful"."sales_date_quarter" = 3 THEN "thoughtful"."sales_ext_sales_price" ELSE NULL END) as "ws_q3"
FROM
    "thoughtful"
    LEFT OUTER JOIN "memory"."customer_address" as "sales_bill_address_customer_address" on "thoughtful"."sales_bill_address_id" = "sales_bill_address_customer_address"."CA_ADDRESS_SK"
GROUP BY
    1,
    2
HAVING
    "ss_q1" > 0 and "ss_q2" > 0 and ( CASE
	WHEN "ws_q1" > 0 THEN "ws_q2" / "ws_q1"
	ELSE null
	END > CASE
	WHEN "ss_q1" > 0 THEN "ss_q2" / "ss_q1"
	ELSE null
	END ) and ( CASE
	WHEN "ws_q2" > 0 THEN "ws_q3" / "ws_q2"
	ELSE null
	END > CASE
	WHEN "ss_q2" > 0 THEN "ss_q3" / "ss_q2"
	ELSE null
	END )
)
SELECT
    "questionable"."sales_bill_address_county" as "sales_bill_address_county",
    "questionable"."sales_date_year" as "sales_date_year",
    "questionable"."ws_q2" / "questionable"."ws_q1" as "web_q1_q2_increase",
    "questionable"."ss_q2" / "questionable"."ss_q1" as "store_q1_q2_increase",
    "questionable"."ws_q3" / "questionable"."ws_q2" as "web_q2_q3_increase",
    "questionable"."ss_q3" / "questionable"."ss_q2" as "store_q2_q3_increase"
FROM
    "questionable"
ORDER BY 
    "questionable"."sales_bill_address_county" asc nulls first
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
