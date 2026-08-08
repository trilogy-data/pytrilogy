EXPLAIN SELECT
    "ss_return_customer_customers"."C_CUSTOMER_ID" as "ss_return_customer_id"
FROM
    "memory"."store_sales" as "ss_store_sales"
    INNER JOIN "memory"."store_returns" as "ss_store_returns" on "ss_store_sales"."SS_ITEM_SK" = "ss_store_returns"."SR_ITEM_SK" AND "ss_store_sales"."SS_TICKET_NUMBER" = "ss_store_returns"."SR_TICKET_NUMBER"
    INNER JOIN "memory"."date_dim" as "ss_return_date_date" on "ss_store_returns"."SR_RETURNED_DATE_SK" = "ss_return_date_date"."D_DATE_SK"
    INNER JOIN "memory"."store" as "ss_return_store_store" on "ss_store_returns"."SR_STORE_SK" = "ss_return_store_store"."S_STORE_SK"
    LEFT OUTER JOIN "memory"."customer" as "ss_return_customer_customers" on "ss_store_returns"."SR_CUSTOMER_SK" = "ss_return_customer_customers"."C_CUSTOMER_SK"
WHERE
    "ss_return_store_store"."S_STATE" = 'TN' and "ss_return_date_date"."D_YEAR" = 2000

GROUP BY
    1
ORDER BY 
    "ss_return_customer_customers"."C_CUSTOMER_ID" asc
LIMIT (100)