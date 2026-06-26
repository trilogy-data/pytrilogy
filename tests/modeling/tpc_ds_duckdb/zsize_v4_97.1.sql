
WITH 
quizzical as (
SELECT
    "catalog_sales_catalog_sales"."CS_BILL_CUSTOMER_SK" as "customer_id",
    "catalog_sales_catalog_sales"."CS_ITEM_SK" as "item_id",
    "catalog_sales_catalog_sales"."CS_SOLD_DATE_SK" as "catalog_sales_date_id"
FROM
    "memory"."catalog_sales" as "catalog_sales_catalog_sales"
GROUP BY
    1,
    2,
    3),
thoughtful as (
SELECT
    "physical_sales_store_sales"."SS_CUSTOMER_SK" as "customer_id",
    "physical_sales_store_sales"."SS_ITEM_SK" as "item_id",
    "physical_sales_store_sales"."SS_SOLD_DATE_SK" as "physical_sales_date_id"
FROM
    "memory"."store_sales" as "physical_sales_store_sales"
GROUP BY
    1,
    2,
    3)
SELECT
    sum(CASE
	WHEN CASE WHEN "physical_sales_date_date"."D_MONTH_SEQ" BETWEEN 1200 AND 1200 + 11 THEN ((cast(coalesce("quizzical"."customer_id","thoughtful"."customer_id") as string) || '-') || cast(coalesce("quizzical"."item_id","thoughtful"."item_id") as string)) ELSE NULL END is not null and CASE WHEN "catalog_sales_date_date"."D_MONTH_SEQ" BETWEEN 1200 AND 1200 + 11 THEN ((cast(coalesce("quizzical"."customer_id","thoughtful"."customer_id") as string) || '-') || cast(coalesce("quizzical"."item_id","thoughtful"."item_id") as string)) ELSE NULL END is null THEN 1
	ELSE 0
	END) as "store_sale_count",
    sum(CASE
	WHEN CASE WHEN "physical_sales_date_date"."D_MONTH_SEQ" BETWEEN 1200 AND 1200 + 11 THEN ((cast(coalesce("quizzical"."customer_id","thoughtful"."customer_id") as string) || '-') || cast(coalesce("quizzical"."item_id","thoughtful"."item_id") as string)) ELSE NULL END is null and CASE WHEN "catalog_sales_date_date"."D_MONTH_SEQ" BETWEEN 1200 AND 1200 + 11 THEN ((cast(coalesce("quizzical"."customer_id","thoughtful"."customer_id") as string) || '-') || cast(coalesce("quizzical"."item_id","thoughtful"."item_id") as string)) ELSE NULL END is not null THEN 1
	ELSE 0
	END) as "catalog_sale_count",
    sum(CASE
	WHEN CASE WHEN "physical_sales_date_date"."D_MONTH_SEQ" BETWEEN 1200 AND 1200 + 11 THEN ((cast(coalesce("quizzical"."customer_id","thoughtful"."customer_id") as string) || '-') || cast(coalesce("quizzical"."item_id","thoughtful"."item_id") as string)) ELSE NULL END is not null and CASE WHEN "catalog_sales_date_date"."D_MONTH_SEQ" BETWEEN 1200 AND 1200 + 11 THEN ((cast(coalesce("quizzical"."customer_id","thoughtful"."customer_id") as string) || '-') || cast(coalesce("quizzical"."item_id","thoughtful"."item_id") as string)) ELSE NULL END is not null THEN 1
	ELSE 0
	END) as "both_sale_count"
FROM
    "thoughtful"
    FULL JOIN "quizzical" on "thoughtful"."customer_id" is not distinct from "quizzical"."customer_id" AND "thoughtful"."item_id" = "quizzical"."item_id"
    FULL JOIN "memory"."date_dim" as "catalog_sales_date_date" on "quizzical"."catalog_sales_date_id" = "catalog_sales_date_date"."D_DATE_SK"
    FULL JOIN "memory"."date_dim" as "physical_sales_date_date" on "thoughtful"."physical_sales_date_id" = "physical_sales_date_date"."D_DATE_SK"