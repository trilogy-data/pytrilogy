
WITH 
quizzical as (
SELECT
    "store_sales_store_sales"."SS_EXT_DISCOUNT_AMT" as "store_sales_ext_discount_amount",
    "store_sales_store_sales"."SS_NET_PAID" as "store_sales_net_paid",
    "store_sales_store_sales"."SS_QUANTITY" as "store_sales_quantity"
FROM
    "memory"."store_sales" as "store_sales_store_sales"
WHERE
    "store_sales_store_sales"."SS_QUANTITY" BETWEEN 1 AND 100
),
cooperative as (
SELECT
    CASE
	WHEN sum(CASE
	WHEN "quizzical"."store_sales_quantity" BETWEEN 81 AND 100 THEN 1
	ELSE 0
	END) > 165306 THEN avg(CASE
	WHEN "quizzical"."store_sales_quantity" BETWEEN 81 AND 100 THEN "quizzical"."store_sales_ext_discount_amount"
	ELSE null
	END)
	ELSE avg(CASE
	WHEN "quizzical"."store_sales_quantity" BETWEEN 81 AND 100 THEN "quizzical"."store_sales_net_paid"
	ELSE null
	END)
	END as "bucket5"
FROM
    "quizzical"),
thoughtful as (
SELECT
    CASE
	WHEN sum(CASE
	WHEN "quizzical"."store_sales_quantity" BETWEEN 61 AND 80 THEN 1
	ELSE 0
	END) > 10097 THEN avg(CASE
	WHEN "quizzical"."store_sales_quantity" BETWEEN 61 AND 80 THEN "quizzical"."store_sales_ext_discount_amount"
	ELSE null
	END)
	ELSE avg(CASE
	WHEN "quizzical"."store_sales_quantity" BETWEEN 61 AND 80 THEN "quizzical"."store_sales_net_paid"
	ELSE null
	END)
	END as "bucket4"
FROM
    "quizzical"),
cheerful as (
SELECT
    CASE
	WHEN sum(CASE
	WHEN "quizzical"."store_sales_quantity" BETWEEN 41 AND 60 THEN 1
	ELSE 0
	END) > 56580 THEN avg(CASE
	WHEN "quizzical"."store_sales_quantity" BETWEEN 41 AND 60 THEN "quizzical"."store_sales_ext_discount_amount"
	ELSE null
	END)
	ELSE avg(CASE
	WHEN "quizzical"."store_sales_quantity" BETWEEN 41 AND 60 THEN "quizzical"."store_sales_net_paid"
	ELSE null
	END)
	END as "bucket3"
FROM
    "quizzical"),
wakeful as (
SELECT
    CASE
	WHEN sum(CASE
	WHEN "quizzical"."store_sales_quantity" BETWEEN 21 AND 40 THEN 1
	ELSE 0
	END) > 122840 THEN avg(CASE
	WHEN "quizzical"."store_sales_quantity" BETWEEN 21 AND 40 THEN "quizzical"."store_sales_ext_discount_amount"
	ELSE null
	END)
	ELSE avg(CASE
	WHEN "quizzical"."store_sales_quantity" BETWEEN 21 AND 40 THEN "quizzical"."store_sales_net_paid"
	ELSE null
	END)
	END as "bucket2"
FROM
    "quizzical"),
highfalutin as (
SELECT
    CASE
	WHEN sum(CASE
	WHEN "quizzical"."store_sales_quantity" BETWEEN 1 AND 20 THEN 1
	ELSE 0
	END) > 74129 THEN avg(CASE
	WHEN "quizzical"."store_sales_quantity" BETWEEN 1 AND 20 THEN "quizzical"."store_sales_ext_discount_amount"
	ELSE null
	END)
	ELSE avg(CASE
	WHEN "quizzical"."store_sales_quantity" BETWEEN 1 AND 20 THEN "quizzical"."store_sales_net_paid"
	ELSE null
	END)
	END as "bucket1"
FROM
    "quizzical")
SELECT
    "highfalutin"."bucket1" as "bucket1",
    "wakeful"."bucket2" as "bucket2",
    "cheerful"."bucket3" as "bucket3",
    "thoughtful"."bucket4" as "bucket4",
    "cooperative"."bucket5" as "bucket5"
FROM
    "highfalutin"
    INNER JOIN "wakeful" on 1=1
    INNER JOIN "cheerful" on 1=1
    INNER JOIN "thoughtful" on 1=1
    INNER JOIN "cooperative" on 1=1