
WITH 
cooperative as (
SELECT
    "store_sales_customer_customers"."C_FIRST_NAME" as "store_sales_customer_first_name",
    "store_sales_customer_customers"."C_LAST_NAME" as "store_sales_customer_last_name",
    "store_sales_customer_customers"."C_PREFERRED_CUST_FLAG" as "store_sales_customer_preferred_cust_flag",
    "store_sales_customer_customers"."C_SALUTATION" as "store_sales_customer_salutation",
    "store_sales_date_date"."D_DOM" as "store_sales_date_day_of_month",
    "store_sales_date_date"."D_YEAR" as "store_sales_date_year",
    "store_sales_household_demographic_household_demographics"."HD_BUY_POTENTIAL" as "store_sales_household_demographic_buy_potential",
    "store_sales_household_demographic_household_demographics"."HD_DEP_COUNT" as "store_sales_household_demographic_dependent_count",
    "store_sales_household_demographic_household_demographics"."HD_VEHICLE_COUNT" as "store_sales_household_demographic_vehicle_count",
    "store_sales_store_sales"."SS_CUSTOMER_SK" as "store_sales_customer_id",
    "store_sales_store_sales"."SS_ITEM_SK" as "store_sales_item_id",
    "store_sales_store_sales"."SS_TICKET_NUMBER" as "store_sales_ticket_number",
    "store_sales_store_store"."S_COUNTY" as "store_sales_store_county"
FROM
    "memory"."store_sales" as "store_sales_store_sales"
    LEFT OUTER JOIN "memory"."date_dim" as "store_sales_date_date" on "store_sales_store_sales"."SS_SOLD_DATE_SK" = "store_sales_date_date"."D_DATE_SK"
    LEFT OUTER JOIN "memory"."store" as "store_sales_store_store" on "store_sales_store_sales"."SS_STORE_SK" = "store_sales_store_store"."S_STORE_SK"
    INNER JOIN "memory"."customer" as "store_sales_customer_customers" on "store_sales_store_sales"."SS_CUSTOMER_SK" = "store_sales_customer_customers"."C_CUSTOMER_SK"
    LEFT OUTER JOIN "memory"."household_demographics" as "store_sales_household_demographic_household_demographics" on "store_sales_store_sales"."SS_HDEMO_SK" = "store_sales_household_demographic_household_demographics"."HD_DEMO_SK"
WHERE
    "store_sales_store_sales"."SS_CUSTOMER_SK" is not null
),
questionable as (
SELECT
    "cooperative"."store_sales_customer_first_name" as "store_sales_customer_first_name",
    "cooperative"."store_sales_customer_id" as "store_sales_customer_id",
    "cooperative"."store_sales_customer_last_name" as "store_sales_customer_last_name",
    "cooperative"."store_sales_customer_preferred_cust_flag" as "store_sales_customer_preferred_cust_flag",
    "cooperative"."store_sales_customer_salutation" as "store_sales_customer_salutation",
    "cooperative"."store_sales_ticket_number" as "store_sales_ticket_number",
    CASE WHEN "cooperative"."store_sales_date_day_of_month" >= 1 and "cooperative"."store_sales_date_day_of_month" <= 2 and ( "cooperative"."store_sales_household_demographic_buy_potential" = '>10000' or "cooperative"."store_sales_household_demographic_buy_potential" = 'Unknown' ) and "cooperative"."store_sales_household_demographic_vehicle_count" > 0 and ( CASE
	WHEN "cooperative"."store_sales_household_demographic_vehicle_count" > 0 THEN ("cooperative"."store_sales_household_demographic_dependent_count" * 1.0) / "cooperative"."store_sales_household_demographic_vehicle_count"
	ELSE null
	END ) > 1 and "cooperative"."store_sales_date_year" in (1999,2000,2001) and "cooperative"."store_sales_store_county" in ('Orange County','Bronx County','Franklin Parish','Williamson County') THEN "cooperative"."store_sales_item_id" ELSE NULL END as "_virt_filter_id_4484877027926973"
FROM
    "cooperative"),
uneven as (
SELECT
    "questionable"."store_sales_customer_id" as "store_sales_customer_id",
    "questionable"."store_sales_ticket_number" as "store_sales_ticket_number",
    count("questionable"."_virt_filter_id_4484877027926973") as "ticket_cnt"
FROM
    "questionable"
GROUP BY
    1,
    2
HAVING
    "ticket_cnt" >= 1 and "ticket_cnt" <= 5
),
abundant as (
SELECT
    "questionable"."store_sales_customer_first_name" as "store_sales_customer_first_name",
    "questionable"."store_sales_customer_id" as "store_sales_customer_id",
    "questionable"."store_sales_customer_last_name" as "store_sales_customer_last_name",
    "questionable"."store_sales_customer_preferred_cust_flag" as "store_sales_customer_preferred_cust_flag",
    "questionable"."store_sales_customer_salutation" as "store_sales_customer_salutation"
FROM
    "questionable"
GROUP BY
    1,
    2,
    3,
    4,
    5)
SELECT
    "abundant"."store_sales_customer_last_name" as "store_sales_customer_last_name",
    "abundant"."store_sales_customer_first_name" as "store_sales_customer_first_name",
    "abundant"."store_sales_customer_salutation" as "store_sales_customer_salutation",
    "abundant"."store_sales_customer_preferred_cust_flag" as "store_sales_customer_preferred_cust_flag",
    "uneven"."store_sales_ticket_number" as "store_sales_ticket_number",
    "uneven"."ticket_cnt" as "ticket_cnt"
FROM
    "uneven"
    INNER JOIN "abundant" on "uneven"."store_sales_customer_id" = "abundant"."store_sales_customer_id"
ORDER BY 
    "uneven"."ticket_cnt" desc,
    "abundant"."store_sales_customer_last_name" asc,
    "uneven"."store_sales_ticket_number" asc,
    "abundant"."store_sales_customer_id" asc