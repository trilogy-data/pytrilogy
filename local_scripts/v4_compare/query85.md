# Query 85

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
| v4 | 4265 | 40 | 27.58 ms |
| reference | 3830 | 25 | 29.42 ms |
| v4 / ref | 1.11x | 1.60x | 0.94x |

## Preql

```
import web_returns as wr;

where
    wr.web_sales.date.year = 2000
    and (
        (
            wr.refunded_demographic.marital_status = 'M'
            and wr.refunded_demographic.marital_status = wr.returning_demographic.marital_status
            and wr.refunded_demographic.education_status = 'Advanced Degree'
            and wr.refunded_demographic.education_status = wr.returning_demographic.education_status
            and wr.web_sales.sales_price between 100.0 and 150.0
        )
        or (
            wr.refunded_demographic.marital_status = 'S'
            and wr.refunded_demographic.marital_status = wr.returning_demographic.marital_status
            and wr.refunded_demographic.education_status = 'College'
            and wr.refunded_demographic.education_status = wr.returning_demographic.education_status
            and wr.web_sales.sales_price between 50.0 and 100.0
        )
        or (
            wr.refunded_demographic.marital_status = 'W'
            and wr.refunded_demographic.marital_status = wr.returning_demographic.marital_status
            and wr.refunded_demographic.education_status = '2 yr Degree'
            and wr.refunded_demographic.education_status = wr.returning_demographic.education_status
            and wr.web_sales.sales_price between 150.0 and 200.0
        )
    )
    and (
        (
            wr.refunded_address.country = 'United States'
            and wr.refunded_address.state in ('IN', 'OH', 'NJ')
            and wr.web_sales.net_profit between 100 and 200
        )
        or (
            wr.refunded_address.country = 'United States'
            and wr.refunded_address.state in ('WI', 'CT', 'KY')
            and wr.web_sales.net_profit between 150 and 300
        )
        or (
            wr.refunded_address.country = 'United States'
            and wr.refunded_address.state in ('LA', 'IA', 'AR')
            and wr.web_sales.net_profit between 50 and 250
        )
    )
select
    --wr.reason.desc,
    substring(wr.reason.desc, 1, 20) as reason_desc,
    avg(wr.web_sales.quantity) as avg1,
    avg(wr.refunded_cash) as avg2,
    avg(wr.fee) as avg3,
order by
    reason_desc asc,
    avg1 asc,
    avg2 asc,
    avg3 asc
limit 100
;
```

## v4 generated SQL

```sql
WITH 
abundant as (
SELECT
    "wr_reason_reason"."R_REASON_DESC" as "wr_reason_desc",
    avg("wr_web_returns"."WR_FEE") as "avg3",
    avg("wr_web_returns"."WR_REFUNDED_CASH") as "avg2",
    avg("wr_web_sales_web_sales"."WS_QUANTITY") as "avg1"
FROM
    "memory"."web_returns" as "wr_web_returns"
    INNER JOIN "memory"."reason" as "wr_reason_reason" on "wr_web_returns"."WR_REASON_SK" = "wr_reason_reason"."R_REASON_SK"
    INNER JOIN "memory"."web_sales" as "wr_web_sales_web_sales" on "wr_web_returns"."WR_ITEM_SK" = "wr_web_sales_web_sales"."WS_ITEM_SK" AND "wr_web_returns"."WR_ORDER_NUMBER" = "wr_web_sales_web_sales"."WS_ORDER_NUMBER"
    INNER JOIN "memory"."date_dim" as "wr_web_sales_date_date" on "wr_web_sales_web_sales"."WS_SOLD_DATE_SK" = "wr_web_sales_date_date"."D_DATE_SK"
    INNER JOIN "memory"."customer_address" as "wr_refunded_address_customer_address" on "wr_web_returns"."WR_REFUNDED_ADDR_SK" = "wr_refunded_address_customer_address"."CA_ADDRESS_SK"
    INNER JOIN "memory"."customer_demographics" as "wr_refunded_demographic_customer_demographics" on "wr_web_returns"."WR_REFUNDED_CDEMO_SK" = "wr_refunded_demographic_customer_demographics"."CD_DEMO_SK"
    INNER JOIN "memory"."customer_demographics" as "wr_returning_demographic_customer_demographics" on "wr_web_returns"."WR_RETURNING_CDEMO_SK" = "wr_returning_demographic_customer_demographics"."CD_DEMO_SK"
WHERE
    "wr_web_sales_date_date"."D_YEAR" = 2000 and ( ( "wr_refunded_demographic_customer_demographics"."CD_MARITAL_STATUS" = 'M' and "wr_refunded_demographic_customer_demographics"."CD_MARITAL_STATUS" = "wr_returning_demographic_customer_demographics"."CD_MARITAL_STATUS" and "wr_refunded_demographic_customer_demographics"."CD_EDUCATION_STATUS" = 'Advanced Degree' and "wr_refunded_demographic_customer_demographics"."CD_EDUCATION_STATUS" = "wr_returning_demographic_customer_demographics"."CD_EDUCATION_STATUS" and "wr_web_sales_web_sales"."WS_SALES_PRICE" BETWEEN 100.0 AND 150.0 ) or ( "wr_refunded_demographic_customer_demographics"."CD_MARITAL_STATUS" = 'S' and "wr_refunded_demographic_customer_demographics"."CD_MARITAL_STATUS" = "wr_returning_demographic_customer_demographics"."CD_MARITAL_STATUS" and "wr_refunded_demographic_customer_demographics"."CD_EDUCATION_STATUS" = 'College' and "wr_refunded_demographic_customer_demographics"."CD_EDUCATION_STATUS" = "wr_returning_demographic_customer_demographics"."CD_EDUCATION_STATUS" and "wr_web_sales_web_sales"."WS_SALES_PRICE" BETWEEN 50.0 AND 100.0 ) or ( "wr_refunded_demographic_customer_demographics"."CD_MARITAL_STATUS" = 'W' and "wr_refunded_demographic_customer_demographics"."CD_MARITAL_STATUS" = "wr_returning_demographic_customer_demographics"."CD_MARITAL_STATUS" and "wr_refunded_demographic_customer_demographics"."CD_EDUCATION_STATUS" = '2 yr Degree' and "wr_refunded_demographic_customer_demographics"."CD_EDUCATION_STATUS" = "wr_returning_demographic_customer_demographics"."CD_EDUCATION_STATUS" and "wr_web_sales_web_sales"."WS_SALES_PRICE" BETWEEN 150.0 AND 200.0 ) ) and ( ( "wr_refunded_address_customer_address"."CA_COUNTRY" = 'United States' and "wr_refunded_address_customer_address"."CA_STATE" in ('IN','OH','NJ') and "wr_web_sales_web_sales"."WS_NET_PROFIT" BETWEEN 100 AND 200 ) or ( "wr_refunded_address_customer_address"."CA_COUNTRY" = 'United States' and "wr_refunded_address_customer_address"."CA_STATE" in ('WI','CT','KY') and "wr_web_sales_web_sales"."WS_NET_PROFIT" BETWEEN 150 AND 300 ) or ( "wr_refunded_address_customer_address"."CA_COUNTRY" = 'United States' and "wr_refunded_address_customer_address"."CA_STATE" in ('LA','IA','AR') and "wr_web_sales_web_sales"."WS_NET_PROFIT" BETWEEN 50 AND 250 ) )

GROUP BY
    1),
yummy as (
SELECT
    "abundant"."wr_reason_desc" as "wr_reason_desc",
    SUBSTRING("abundant"."wr_reason_desc",1,20) as "reason_desc"
FROM
    "abundant")
SELECT
    "yummy"."reason_desc" as "reason_desc",
    "abundant"."avg1" as "avg1",
    "abundant"."avg2" as "avg2",
    "abundant"."avg3" as "avg3"
FROM
    "yummy"
    FULL JOIN "abundant" on "yummy"."wr_reason_desc" is not distinct from "abundant"."wr_reason_desc"
ORDER BY 
    "yummy"."reason_desc" asc,
    "abundant"."avg1" asc,
    "abundant"."avg2" asc,
    "abundant"."avg3" asc
LIMIT (100)
```

## Reference SQL (zquery log)

```sql
SELECT
    SUBSTRING("wr_reason_reason"."R_REASON_DESC",1,20) as "reason_desc",
    avg("wr_web_sales_web_sales"."WS_QUANTITY") as "avg1",
    avg("wr_web_returns"."WR_REFUNDED_CASH") as "avg2",
    avg("wr_web_returns"."WR_FEE") as "avg3"
FROM
    "memory"."web_returns" as "wr_web_returns"
    INNER JOIN "memory"."reason" as "wr_reason_reason" on "wr_web_returns"."WR_REASON_SK" = "wr_reason_reason"."R_REASON_SK"
    INNER JOIN "memory"."web_sales" as "wr_web_sales_web_sales" on "wr_web_returns"."WR_ITEM_SK" = "wr_web_sales_web_sales"."WS_ITEM_SK" AND "wr_web_returns"."WR_ORDER_NUMBER" = "wr_web_sales_web_sales"."WS_ORDER_NUMBER"
    INNER JOIN "memory"."date_dim" as "wr_web_sales_date_date" on "wr_web_sales_web_sales"."WS_SOLD_DATE_SK" = "wr_web_sales_date_date"."D_DATE_SK"
    INNER JOIN "memory"."customer_address" as "wr_refunded_address_customer_address" on "wr_web_returns"."WR_REFUNDED_ADDR_SK" = "wr_refunded_address_customer_address"."CA_ADDRESS_SK"
    INNER JOIN "memory"."customer_demographics" as "wr_refunded_demographic_customer_demographics" on "wr_web_returns"."WR_REFUNDED_CDEMO_SK" = "wr_refunded_demographic_customer_demographics"."CD_DEMO_SK"
    INNER JOIN "memory"."customer_demographics" as "wr_returning_demographic_customer_demographics" on "wr_web_returns"."WR_RETURNING_CDEMO_SK" = "wr_returning_demographic_customer_demographics"."CD_DEMO_SK"
WHERE
    "wr_web_sales_date_date"."D_YEAR" = 2000 and ( ( "wr_refunded_demographic_customer_demographics"."CD_MARITAL_STATUS" = 'M' and "wr_refunded_demographic_customer_demographics"."CD_MARITAL_STATUS" = "wr_returning_demographic_customer_demographics"."CD_MARITAL_STATUS" and "wr_refunded_demographic_customer_demographics"."CD_EDUCATION_STATUS" = 'Advanced Degree' and "wr_refunded_demographic_customer_demographics"."CD_EDUCATION_STATUS" = "wr_returning_demographic_customer_demographics"."CD_EDUCATION_STATUS" and "wr_web_sales_web_sales"."WS_SALES_PRICE" BETWEEN 100.0 AND 150.0 ) or ( "wr_refunded_demographic_customer_demographics"."CD_MARITAL_STATUS" = 'S' and "wr_refunded_demographic_customer_demographics"."CD_MARITAL_STATUS" = "wr_returning_demographic_customer_demographics"."CD_MARITAL_STATUS" and "wr_refunded_demographic_customer_demographics"."CD_EDUCATION_STATUS" = 'College' and "wr_refunded_demographic_customer_demographics"."CD_EDUCATION_STATUS" = "wr_returning_demographic_customer_demographics"."CD_EDUCATION_STATUS" and "wr_web_sales_web_sales"."WS_SALES_PRICE" BETWEEN 50.0 AND 100.0 ) or ( "wr_refunded_demographic_customer_demographics"."CD_MARITAL_STATUS" = 'W' and "wr_refunded_demographic_customer_demographics"."CD_MARITAL_STATUS" = "wr_returning_demographic_customer_demographics"."CD_MARITAL_STATUS" and "wr_refunded_demographic_customer_demographics"."CD_EDUCATION_STATUS" = '2 yr Degree' and "wr_refunded_demographic_customer_demographics"."CD_EDUCATION_STATUS" = "wr_returning_demographic_customer_demographics"."CD_EDUCATION_STATUS" and "wr_web_sales_web_sales"."WS_SALES_PRICE" BETWEEN 150.0 AND 200.0 ) ) and ( ( "wr_refunded_address_customer_address"."CA_COUNTRY" = 'United States' and "wr_refunded_address_customer_address"."CA_STATE" in ('IN','OH','NJ') and "wr_web_sales_web_sales"."WS_NET_PROFIT" BETWEEN 100 AND 200 ) or ( "wr_refunded_address_customer_address"."CA_COUNTRY" = 'United States' and "wr_refunded_address_customer_address"."CA_STATE" in ('WI','CT','KY') and "wr_web_sales_web_sales"."WS_NET_PROFIT" BETWEEN 150 AND 300 ) or ( "wr_refunded_address_customer_address"."CA_COUNTRY" = 'United States' and "wr_refunded_address_customer_address"."CA_STATE" in ('LA','IA','AR') and "wr_web_sales_web_sales"."WS_NET_PROFIT" BETWEEN 50 AND 250 ) )

GROUP BY
    1,
    "wr_reason_reason"."R_REASON_DESC"
ORDER BY 
    "reason_desc" asc,
    "avg1" asc,
    "avg2" asc,
    "avg3" asc
LIMIT (100)
```
