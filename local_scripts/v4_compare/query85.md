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

## SQL size

| Source | Chars | Lines |
| --- | --- | --- |
| v4 | 8410 | 138 |
| reference | 3830 | 25 |
| v4 / ref | 2.20x | 5.52x |

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
questionable as (
SELECT
    "wr_web_sales_web_sales"."WS_ITEM_SK" as "wr_web_sales_item_id",
    "wr_web_sales_web_sales"."WS_NET_PROFIT" as "wr_web_sales_net_profit",
    "wr_web_sales_web_sales"."WS_ORDER_NUMBER" as "wr_web_sales_order_number",
    "wr_web_sales_web_sales"."WS_QUANTITY" as "wr_web_sales_quantity",
    "wr_web_sales_web_sales"."WS_SALES_PRICE" as "wr_web_sales_sales_price",
    "wr_web_sales_web_sales"."WS_SOLD_DATE_SK" as "wr_web_sales_date_id"
FROM
    "memory"."web_sales" as "wr_web_sales_web_sales"),
cooperative as (
SELECT
    "wr_web_sales_date_date"."D_DATE_SK" as "wr_web_sales_date_id",
    "wr_web_sales_date_date"."D_YEAR" as "wr_web_sales_date_year"
FROM
    "memory"."date_dim" as "wr_web_sales_date_date"),
thoughtful as (
SELECT
    "wr_web_returns"."WR_FEE" as "wr_fee",
    "wr_web_returns"."WR_ITEM_SK" as "wr_web_sales_item_id",
    "wr_web_returns"."WR_ORDER_NUMBER" as "wr_web_sales_order_number",
    "wr_web_returns"."WR_REASON_SK" as "wr_reason_id",
    "wr_web_returns"."WR_REFUNDED_ADDR_SK" as "wr_refunded_address_id",
    "wr_web_returns"."WR_REFUNDED_CASH" as "wr_refunded_cash",
    "wr_web_returns"."WR_REFUNDED_CDEMO_SK" as "wr_refunded_demographic_id",
    "wr_web_returns"."WR_RETURNING_CDEMO_SK" as "wr_returning_demographic_id"
FROM
    "memory"."web_returns" as "wr_web_returns"),
cheerful as (
SELECT
    "wr_returning_demographic_customer_demographics"."CD_DEMO_SK" as "wr_returning_demographic_id",
    "wr_returning_demographic_customer_demographics"."CD_EDUCATION_STATUS" as "wr_returning_demographic_education_status",
    "wr_returning_demographic_customer_demographics"."CD_MARITAL_STATUS" as "wr_returning_demographic_marital_status"
FROM
    "memory"."customer_demographics" as "wr_returning_demographic_customer_demographics"),
wakeful as (
SELECT
    "wr_refunded_demographic_customer_demographics"."CD_DEMO_SK" as "wr_refunded_demographic_id",
    "wr_refunded_demographic_customer_demographics"."CD_EDUCATION_STATUS" as "wr_refunded_demographic_education_status",
    "wr_refunded_demographic_customer_demographics"."CD_MARITAL_STATUS" as "wr_refunded_demographic_marital_status"
FROM
    "memory"."customer_demographics" as "wr_refunded_demographic_customer_demographics"),
highfalutin as (
SELECT
    "wr_refunded_address_customer_address"."CA_ADDRESS_SK" as "wr_refunded_address_id",
    "wr_refunded_address_customer_address"."CA_COUNTRY" as "wr_refunded_address_country",
    "wr_refunded_address_customer_address"."CA_STATE" as "wr_refunded_address_state"
FROM
    "memory"."customer_address" as "wr_refunded_address_customer_address"),
quizzical as (
SELECT
    "wr_reason_reason"."R_REASON_DESC" as "wr_reason_desc",
    "wr_reason_reason"."R_REASON_SK" as "wr_reason_id"
FROM
    "memory"."reason" as "wr_reason_reason"),
abundant as (
SELECT
    "cheerful"."wr_returning_demographic_education_status" as "wr_returning_demographic_education_status",
    "cheerful"."wr_returning_demographic_marital_status" as "wr_returning_demographic_marital_status",
    "cooperative"."wr_web_sales_date_year" as "wr_web_sales_date_year",
    "highfalutin"."wr_refunded_address_country" as "wr_refunded_address_country",
    "highfalutin"."wr_refunded_address_state" as "wr_refunded_address_state",
    "questionable"."wr_web_sales_net_profit" as "wr_web_sales_net_profit",
    "questionable"."wr_web_sales_quantity" as "wr_web_sales_quantity",
    "questionable"."wr_web_sales_sales_price" as "wr_web_sales_sales_price",
    "quizzical"."wr_reason_desc" as "wr_reason_desc",
    "thoughtful"."wr_fee" as "wr_fee",
    "thoughtful"."wr_refunded_cash" as "wr_refunded_cash",
    "wakeful"."wr_refunded_demographic_education_status" as "wr_refunded_demographic_education_status",
    "wakeful"."wr_refunded_demographic_marital_status" as "wr_refunded_demographic_marital_status"
FROM
    "thoughtful"
    INNER JOIN "quizzical" on "thoughtful"."wr_reason_id" = "quizzical"."wr_reason_id"
    INNER JOIN "questionable" on "thoughtful"."wr_web_sales_item_id" = "questionable"."wr_web_sales_item_id" AND "thoughtful"."wr_web_sales_order_number" = "questionable"."wr_web_sales_order_number"
    LEFT OUTER JOIN "cooperative" on "questionable"."wr_web_sales_date_id" = "cooperative"."wr_web_sales_date_id"
    INNER JOIN "highfalutin" on "thoughtful"."wr_refunded_address_id" = "highfalutin"."wr_refunded_address_id"
    INNER JOIN "wakeful" on "thoughtful"."wr_refunded_demographic_id" = "wakeful"."wr_refunded_demographic_id"
    INNER JOIN "cheerful" on "thoughtful"."wr_returning_demographic_id" = "cheerful"."wr_returning_demographic_id"
WHERE
    "cooperative"."wr_web_sales_date_year" = 2000 and ( ( "wakeful"."wr_refunded_demographic_marital_status" = 'M' and "wakeful"."wr_refunded_demographic_marital_status" = "cheerful"."wr_returning_demographic_marital_status" and "wakeful"."wr_refunded_demographic_education_status" = 'Advanced Degree' and "wakeful"."wr_refunded_demographic_education_status" = "cheerful"."wr_returning_demographic_education_status" and "questionable"."wr_web_sales_sales_price" BETWEEN 100.0 AND 150.0 ) or ( "wakeful"."wr_refunded_demographic_marital_status" = 'S' and "wakeful"."wr_refunded_demographic_marital_status" = "cheerful"."wr_returning_demographic_marital_status" and "wakeful"."wr_refunded_demographic_education_status" = 'College' and "wakeful"."wr_refunded_demographic_education_status" = "cheerful"."wr_returning_demographic_education_status" and "questionable"."wr_web_sales_sales_price" BETWEEN 50.0 AND 100.0 ) or ( "wakeful"."wr_refunded_demographic_marital_status" = 'W' and "wakeful"."wr_refunded_demographic_marital_status" = "cheerful"."wr_returning_demographic_marital_status" and "wakeful"."wr_refunded_demographic_education_status" = '2 yr Degree' and "wakeful"."wr_refunded_demographic_education_status" = "cheerful"."wr_returning_demographic_education_status" and "questionable"."wr_web_sales_sales_price" BETWEEN 150.0 AND 200.0 ) ) and ( ( "highfalutin"."wr_refunded_address_country" = 'United States' and "highfalutin"."wr_refunded_address_state" in ('IN','OH','NJ') and "questionable"."wr_web_sales_net_profit" BETWEEN 100 AND 200 ) or ( "highfalutin"."wr_refunded_address_country" = 'United States' and "highfalutin"."wr_refunded_address_state" in ('WI','CT','KY') and "questionable"."wr_web_sales_net_profit" BETWEEN 150 AND 300 ) or ( "highfalutin"."wr_refunded_address_country" = 'United States' and "highfalutin"."wr_refunded_address_state" in ('LA','IA','AR') and "questionable"."wr_web_sales_net_profit" BETWEEN 50 AND 250 ) )

GROUP BY
    1,
    2,
    3,
    4,
    5,
    6,
    7,
    8,
    9,
    10,
    11,
    12,
    13),
yummy as (
SELECT
    "abundant"."wr_reason_desc" as "wr_reason_desc",
    avg("abundant"."wr_fee") as "avg3",
    avg("abundant"."wr_refunded_cash") as "avg2",
    avg("abundant"."wr_web_sales_quantity") as "avg1"
FROM
    "abundant"
GROUP BY
    1),
uneven as (
SELECT
    "abundant"."wr_fee" as "wr_fee",
    "abundant"."wr_reason_desc" as "wr_reason_desc",
    "abundant"."wr_refunded_address_country" as "wr_refunded_address_country",
    "abundant"."wr_refunded_address_state" as "wr_refunded_address_state",
    "abundant"."wr_refunded_cash" as "wr_refunded_cash",
    "abundant"."wr_refunded_demographic_education_status" as "wr_refunded_demographic_education_status",
    "abundant"."wr_refunded_demographic_marital_status" as "wr_refunded_demographic_marital_status",
    "abundant"."wr_returning_demographic_education_status" as "wr_returning_demographic_education_status",
    "abundant"."wr_returning_demographic_marital_status" as "wr_returning_demographic_marital_status",
    "abundant"."wr_web_sales_date_year" as "wr_web_sales_date_year",
    "abundant"."wr_web_sales_net_profit" as "wr_web_sales_net_profit",
    "abundant"."wr_web_sales_quantity" as "wr_web_sales_quantity",
    "abundant"."wr_web_sales_sales_price" as "wr_web_sales_sales_price",
    SUBSTRING("abundant"."wr_reason_desc",1,20) as "reason_desc"
FROM
    "abundant")
SELECT
    "uneven"."reason_desc" as "reason_desc",
    "yummy"."avg1" as "avg1",
    "yummy"."avg2" as "avg2",
    "yummy"."avg3" as "avg3"
FROM
    "uneven"
    FULL JOIN "yummy" on "uneven"."wr_reason_desc" is not distinct from "yummy"."wr_reason_desc"
ORDER BY 
    "uneven"."reason_desc" asc,
    "yummy"."avg1" asc,
    "yummy"."avg2" asc,
    "yummy"."avg3" asc
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
