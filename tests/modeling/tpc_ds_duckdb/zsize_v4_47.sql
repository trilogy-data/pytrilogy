
WITH 
uneven as (
SELECT
    "physical_sales_store_sales"."SS_ITEM_SK" as "physical_sales_item_id",
    "physical_sales_store_sales"."SS_SOLD_DATE_SK" as "physical_sales_date_id",
    "physical_sales_store_sales"."SS_STORE_SK" as "physical_sales_store_id"
FROM
    "memory"."store_sales" as "physical_sales_store_sales"
WHERE
    "physical_sales_store_sales"."SS_STORE_SK" is not null
),
thoughtful as (
SELECT
    "physical_sales_date_date"."D_MOY" as "physical_sales_date_month_of_year",
    "physical_sales_date_date"."D_YEAR" as "physical_sales_date_year",
    "physical_sales_item_items"."I_BRAND" as "physical_sales_item_brand_name",
    "physical_sales_item_items"."I_CATEGORY" as "physical_sales_item_category",
    "physical_sales_store_store"."S_COMPANY_NAME" as "physical_sales_store_company_name",
    "physical_sales_store_store"."S_STORE_NAME" as "physical_sales_store_name",
    sum("physical_sales_store_sales"."SS_SALES_PRICE") as "sum_sales"
FROM
    "memory"."store_sales" as "physical_sales_store_sales"
    INNER JOIN "memory"."date_dim" as "physical_sales_date_date" on "physical_sales_store_sales"."SS_SOLD_DATE_SK" = "physical_sales_date_date"."D_DATE_SK"
    INNER JOIN "memory"."item" as "physical_sales_item_items" on "physical_sales_store_sales"."SS_ITEM_SK" = "physical_sales_item_items"."I_ITEM_SK"
    INNER JOIN "memory"."store" as "physical_sales_store_store" on "physical_sales_store_sales"."SS_STORE_SK" = "physical_sales_store_store"."S_STORE_SK"
WHERE
    "physical_sales_store_sales"."SS_STORE_SK" is not null and ( "physical_sales_date_date"."D_YEAR" = 1999 or ( "physical_sales_date_date"."D_YEAR" = 1998 and "physical_sales_date_date"."D_MOY" = 12 ) or ( "physical_sales_date_date"."D_YEAR" = 2000 and "physical_sales_date_date"."D_MOY" = 1 ) )

GROUP BY
    1,
    2,
    3,
    4,
    5,
    6),
yummy as (
SELECT
    "physical_sales_date_date"."D_MOY" as "physical_sales_date_month_of_year",
    "physical_sales_date_date"."D_YEAR" as "physical_sales_date_year",
    "physical_sales_item_items"."I_BRAND" as "physical_sales_item_brand_name",
    "physical_sales_item_items"."I_CATEGORY" as "physical_sales_item_category",
    "physical_sales_store_store"."S_COMPANY_NAME" as "physical_sales_store_company_name",
    "physical_sales_store_store"."S_STORE_NAME" as "physical_sales_store_name"
FROM
    "uneven"
    INNER JOIN "memory"."date_dim" as "physical_sales_date_date" on "uneven"."physical_sales_date_id" = "physical_sales_date_date"."D_DATE_SK"
    INNER JOIN "memory"."item" as "physical_sales_item_items" on "uneven"."physical_sales_item_id" = "physical_sales_item_items"."I_ITEM_SK"
    LEFT OUTER JOIN "memory"."store" as "physical_sales_store_store" on "uneven"."physical_sales_store_id" = "physical_sales_store_store"."S_STORE_SK"
WHERE
    ( "physical_sales_date_date"."D_YEAR" = 1999 or ( "physical_sales_date_date"."D_YEAR" = 1998 and "physical_sales_date_date"."D_MOY" = 12 ) or ( "physical_sales_date_date"."D_YEAR" = 2000 and "physical_sales_date_date"."D_MOY" = 1 ) ) and "physical_sales_date_date"."D_YEAR" = 1999
),
abundant as (
SELECT
    "thoughtful"."physical_sales_date_month_of_year" as "physical_sales_date_month_of_year",
    "thoughtful"."physical_sales_date_year" as "physical_sales_date_year",
    "thoughtful"."physical_sales_item_brand_name" as "physical_sales_item_brand_name",
    "thoughtful"."physical_sales_item_category" as "physical_sales_item_category",
    "thoughtful"."physical_sales_store_company_name" as "physical_sales_store_company_name",
    "thoughtful"."physical_sales_store_name" as "physical_sales_store_name",
    lag("thoughtful"."sum_sales", 1) over (partition by "thoughtful"."physical_sales_item_category","thoughtful"."physical_sales_item_brand_name","thoughtful"."physical_sales_store_name","thoughtful"."physical_sales_store_company_name" order by "thoughtful"."physical_sales_date_year" asc,"thoughtful"."physical_sales_date_month_of_year" asc ) as "psum",
    lead("thoughtful"."sum_sales", 1) over (partition by "thoughtful"."physical_sales_item_category","thoughtful"."physical_sales_item_brand_name","thoughtful"."physical_sales_store_name","thoughtful"."physical_sales_store_company_name" order by "thoughtful"."physical_sales_date_year" asc,"thoughtful"."physical_sales_date_month_of_year" asc ) as "nsum"
FROM
    "thoughtful"),
juicy as (
SELECT
    "yummy"."physical_sales_date_month_of_year" as "physical_sales_date_month_of_year",
    "yummy"."physical_sales_date_year" as "physical_sales_date_year",
    "yummy"."physical_sales_item_brand_name" as "physical_sales_item_brand_name",
    "yummy"."physical_sales_item_category" as "physical_sales_item_category",
    "yummy"."physical_sales_store_company_name" as "physical_sales_store_company_name",
    "yummy"."physical_sales_store_name" as "physical_sales_store_name"
FROM
    "yummy"
WHERE
    "yummy"."physical_sales_date_year" = 1999
),
vacuous as (
SELECT
    "thoughtful"."sum_sales" as "sum_sales",
    coalesce("juicy"."physical_sales_date_year","thoughtful"."physical_sales_date_year") as "physical_sales_date_year",
    coalesce("juicy"."physical_sales_item_brand_name","thoughtful"."physical_sales_item_brand_name") as "physical_sales_item_brand_name",
    coalesce("juicy"."physical_sales_item_category","thoughtful"."physical_sales_item_category") as "physical_sales_item_category",
    coalesce("juicy"."physical_sales_store_company_name","thoughtful"."physical_sales_store_company_name") as "physical_sales_store_company_name",
    coalesce("juicy"."physical_sales_store_name","thoughtful"."physical_sales_store_name") as "physical_sales_store_name"
FROM
    "thoughtful"
    FULL JOIN "juicy" on "thoughtful"."physical_sales_date_month_of_year" = "juicy"."physical_sales_date_month_of_year" AND "thoughtful"."physical_sales_date_year" = "juicy"."physical_sales_date_year" AND "thoughtful"."physical_sales_item_brand_name" = "juicy"."physical_sales_item_brand_name" AND "thoughtful"."physical_sales_item_category" is not distinct from "juicy"."physical_sales_item_category" AND "thoughtful"."physical_sales_store_company_name" is not distinct from "juicy"."physical_sales_store_company_name" AND "thoughtful"."physical_sales_store_name" = "juicy"."physical_sales_store_name"
WHERE
    coalesce("juicy"."physical_sales_date_year","thoughtful"."physical_sales_date_year") = 1999

GROUP BY
    1,
    2,
    3,
    4,
    5,
    6,
    coalesce("juicy"."physical_sales_date_month_of_year","thoughtful"."physical_sales_date_month_of_year")),
young as (
SELECT
    "vacuous"."physical_sales_date_year" as "physical_sales_date_year",
    "vacuous"."physical_sales_item_brand_name" as "physical_sales_item_brand_name",
    "vacuous"."physical_sales_item_category" as "physical_sales_item_category",
    "vacuous"."physical_sales_store_company_name" as "physical_sales_store_company_name",
    "vacuous"."physical_sales_store_name" as "physical_sales_store_name",
    avg("vacuous"."sum_sales") as "avg_monthly_sales"
FROM
    "vacuous"
WHERE
    "vacuous"."physical_sales_date_year" = 1999

GROUP BY
    1,
    2,
    3,
    4,
    5
HAVING
    "avg_monthly_sales" > 0
),
sparkling as (
SELECT
    "thoughtful"."physical_sales_date_month_of_year" as "physical_sales_date_month_of_year",
    "thoughtful"."sum_sales" - "young"."avg_monthly_sales" as "sum_minus_avg",
    "thoughtful"."sum_sales" as "sum_sales",
    "young"."avg_monthly_sales" as "avg_monthly_sales",
    coalesce("thoughtful"."physical_sales_date_year","young"."physical_sales_date_year") as "physical_sales_date_year",
    coalesce("thoughtful"."physical_sales_item_brand_name","young"."physical_sales_item_brand_name") as "physical_sales_item_brand_name",
    coalesce("thoughtful"."physical_sales_item_category","young"."physical_sales_item_category") as "physical_sales_item_category",
    coalesce("thoughtful"."physical_sales_store_company_name","young"."physical_sales_store_company_name") as "physical_sales_store_company_name",
    coalesce("thoughtful"."physical_sales_store_name","young"."physical_sales_store_name") as "physical_sales_store_name"
FROM
    "thoughtful"
    RIGHT OUTER JOIN "young" on "thoughtful"."physical_sales_date_year" = "young"."physical_sales_date_year" AND "thoughtful"."physical_sales_item_brand_name" = "young"."physical_sales_item_brand_name" AND "thoughtful"."physical_sales_item_category" is not distinct from "young"."physical_sales_item_category" AND "thoughtful"."physical_sales_store_company_name" is not distinct from "young"."physical_sales_store_company_name" AND "thoughtful"."physical_sales_store_name" = "young"."physical_sales_store_name"
WHERE
    coalesce("thoughtful"."physical_sales_date_year","young"."physical_sales_date_year") = 1999
)
SELECT
    coalesce("abundant"."physical_sales_item_category","sparkling"."physical_sales_item_category") as "physical_sales_item_category",
    coalesce("abundant"."physical_sales_item_brand_name","sparkling"."physical_sales_item_brand_name") as "physical_sales_item_brand_name",
    coalesce("abundant"."physical_sales_store_name","sparkling"."physical_sales_store_name") as "physical_sales_store_name",
    coalesce("abundant"."physical_sales_store_company_name","sparkling"."physical_sales_store_company_name") as "physical_sales_store_company_name",
    coalesce("abundant"."physical_sales_date_year","sparkling"."physical_sales_date_year") as "physical_sales_date_year",
    coalesce("abundant"."physical_sales_date_month_of_year","sparkling"."physical_sales_date_month_of_year") as "physical_sales_date_month_of_year",
    "sparkling"."avg_monthly_sales" as "avg_monthly_sales",
    "sparkling"."sum_sales" as "sum_sales",
    "abundant"."psum" as "psum",
    "abundant"."nsum" as "nsum"
FROM
    "sparkling"
    LEFT OUTER JOIN "abundant" on "sparkling"."physical_sales_date_month_of_year" = "abundant"."physical_sales_date_month_of_year" AND "sparkling"."physical_sales_date_year" = "abundant"."physical_sales_date_year" AND "sparkling"."physical_sales_item_brand_name" = "abundant"."physical_sales_item_brand_name" AND "sparkling"."physical_sales_item_category" is not distinct from "abundant"."physical_sales_item_category" AND "sparkling"."physical_sales_store_company_name" is not distinct from "abundant"."physical_sales_store_company_name" AND "sparkling"."physical_sales_store_name" = "abundant"."physical_sales_store_name"
WHERE
    coalesce("abundant"."physical_sales_date_year","sparkling"."physical_sales_date_year") = 1999 and "sparkling"."avg_monthly_sales" > 0 and CASE
	WHEN "sparkling"."avg_monthly_sales" > 0 THEN abs("sparkling"."sum_sales" - "sparkling"."avg_monthly_sales") / "sparkling"."avg_monthly_sales"
	ELSE null
	END > 0.1

ORDER BY 
    "sparkling"."sum_minus_avg" asc,
    coalesce("abundant"."physical_sales_item_category","sparkling"."physical_sales_item_category") asc,
    coalesce("abundant"."physical_sales_item_brand_name","sparkling"."physical_sales_item_brand_name") asc,
    coalesce("abundant"."physical_sales_store_name","sparkling"."physical_sales_store_name") asc,
    coalesce("abundant"."physical_sales_store_company_name","sparkling"."physical_sales_store_company_name") asc,
    coalesce("abundant"."physical_sales_date_year","sparkling"."physical_sales_date_year") asc,
    coalesce("abundant"."physical_sales_date_month_of_year","sparkling"."physical_sales_date_month_of_year") asc,
    "sparkling"."avg_monthly_sales" asc,
    "sparkling"."sum_sales" asc,
    "abundant"."psum" asc,
    "abundant"."nsum" asc
LIMIT (100)