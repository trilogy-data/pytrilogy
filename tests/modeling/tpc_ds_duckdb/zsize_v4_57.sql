
WITH 
highfalutin as (
SELECT
    "catalog_sales_catalog_sales"."CS_CALL_CENTER_SK" as "catalog_sales_call_center_id",
    "catalog_sales_catalog_sales"."CS_ITEM_SK" as "catalog_sales_item_id",
    "catalog_sales_catalog_sales"."CS_SOLD_DATE_SK" as "catalog_sales_date_id"
FROM
    "memory"."catalog_sales" as "catalog_sales_catalog_sales"
WHERE
    "catalog_sales_catalog_sales"."CS_CALL_CENTER_SK" is not null
),
abundant as (
SELECT
    "catalog_sales_call_center_call_center"."CC_NAME" as "catalog_sales_call_center_name",
    "catalog_sales_date_date"."D_MOY" as "catalog_sales_date_month_of_year",
    "catalog_sales_date_date"."D_YEAR" as "catalog_sales_date_year",
    "catalog_sales_item_items"."I_BRAND" as "catalog_sales_item_brand_name",
    "catalog_sales_item_items"."I_CATEGORY" as "catalog_sales_item_category",
    sum("catalog_sales_catalog_sales"."CS_SALES_PRICE") as "sum_sales"
FROM
    "memory"."catalog_sales" as "catalog_sales_catalog_sales"
    INNER JOIN "memory"."date_dim" as "catalog_sales_date_date" on "catalog_sales_catalog_sales"."CS_SOLD_DATE_SK" = "catalog_sales_date_date"."D_DATE_SK"
    INNER JOIN "memory"."item" as "catalog_sales_item_items" on "catalog_sales_catalog_sales"."CS_ITEM_SK" = "catalog_sales_item_items"."I_ITEM_SK"
    INNER JOIN "memory"."call_center" as "catalog_sales_call_center_call_center" on "catalog_sales_catalog_sales"."CS_CALL_CENTER_SK" = "catalog_sales_call_center_call_center"."CC_CALL_CENTER_SK"
WHERE
    "catalog_sales_catalog_sales"."CS_CALL_CENTER_SK" is not null and ( "catalog_sales_date_date"."D_YEAR" = 1999 or ( "catalog_sales_date_date"."D_YEAR" = 1998 and "catalog_sales_date_date"."D_MOY" = 12 ) or ( "catalog_sales_date_date"."D_YEAR" = 2000 and "catalog_sales_date_date"."D_MOY" = 1 ) )

GROUP BY
    1,
    2,
    3,
    4,
    5),
thoughtful as (
SELECT
    "catalog_sales_call_center_call_center"."CC_NAME" as "catalog_sales_call_center_name",
    "catalog_sales_date_date"."D_MOY" as "catalog_sales_date_month_of_year",
    "catalog_sales_date_date"."D_YEAR" as "catalog_sales_date_year",
    "catalog_sales_item_items"."I_BRAND" as "catalog_sales_item_brand_name",
    "catalog_sales_item_items"."I_CATEGORY" as "catalog_sales_item_category"
FROM
    "highfalutin"
    INNER JOIN "memory"."date_dim" as "catalog_sales_date_date" on "highfalutin"."catalog_sales_date_id" = "catalog_sales_date_date"."D_DATE_SK"
    INNER JOIN "memory"."item" as "catalog_sales_item_items" on "highfalutin"."catalog_sales_item_id" = "catalog_sales_item_items"."I_ITEM_SK"
    LEFT OUTER JOIN "memory"."call_center" as "catalog_sales_call_center_call_center" on "highfalutin"."catalog_sales_call_center_id" = "catalog_sales_call_center_call_center"."CC_CALL_CENTER_SK"
WHERE
    ( "catalog_sales_date_date"."D_YEAR" = 1999 or ( "catalog_sales_date_date"."D_YEAR" = 1998 and "catalog_sales_date_date"."D_MOY" = 12 ) or ( "catalog_sales_date_date"."D_YEAR" = 2000 and "catalog_sales_date_date"."D_MOY" = 1 ) ) and "catalog_sales_date_date"."D_YEAR" = 1999
),
abhorrent as (
SELECT
    "abundant"."catalog_sales_call_center_name" as "catalog_sales_call_center_name",
    "abundant"."catalog_sales_date_month_of_year" as "catalog_sales_date_month_of_year",
    "abundant"."catalog_sales_date_year" as "catalog_sales_date_year",
    "abundant"."catalog_sales_item_brand_name" as "catalog_sales_item_brand_name",
    "abundant"."catalog_sales_item_category" as "catalog_sales_item_category",
    lag("abundant"."sum_sales", 1) over (partition by "abundant"."catalog_sales_item_category","abundant"."catalog_sales_item_brand_name","abundant"."catalog_sales_call_center_name" order by "abundant"."catalog_sales_date_year" asc,"abundant"."catalog_sales_date_month_of_year" asc ) as "psum",
    lead("abundant"."sum_sales", 1) over (partition by "abundant"."catalog_sales_item_category","abundant"."catalog_sales_item_brand_name","abundant"."catalog_sales_call_center_name" order by "abundant"."catalog_sales_date_year" asc,"abundant"."catalog_sales_date_month_of_year" asc ) as "nsum"
FROM
    "abundant"),
cooperative as (
SELECT
    "thoughtful"."catalog_sales_call_center_name" as "catalog_sales_call_center_name",
    "thoughtful"."catalog_sales_date_month_of_year" as "catalog_sales_date_month_of_year",
    "thoughtful"."catalog_sales_date_year" as "catalog_sales_date_year",
    "thoughtful"."catalog_sales_item_brand_name" as "catalog_sales_item_brand_name",
    "thoughtful"."catalog_sales_item_category" as "catalog_sales_item_category"
FROM
    "thoughtful"
WHERE
    "thoughtful"."catalog_sales_date_year" = 1999
),
juicy as (
SELECT
    "abundant"."sum_sales" as "sum_sales",
    coalesce("abundant"."catalog_sales_call_center_name","cooperative"."catalog_sales_call_center_name") as "catalog_sales_call_center_name",
    coalesce("abundant"."catalog_sales_date_year","cooperative"."catalog_sales_date_year") as "catalog_sales_date_year",
    coalesce("abundant"."catalog_sales_item_brand_name","cooperative"."catalog_sales_item_brand_name") as "catalog_sales_item_brand_name",
    coalesce("abundant"."catalog_sales_item_category","cooperative"."catalog_sales_item_category") as "catalog_sales_item_category"
FROM
    "abundant"
    FULL JOIN "cooperative" on "abundant"."catalog_sales_call_center_name" = "cooperative"."catalog_sales_call_center_name" AND "abundant"."catalog_sales_date_month_of_year" = "cooperative"."catalog_sales_date_month_of_year" AND "abundant"."catalog_sales_date_year" = "cooperative"."catalog_sales_date_year" AND "abundant"."catalog_sales_item_brand_name" = "cooperative"."catalog_sales_item_brand_name" AND "abundant"."catalog_sales_item_category" is not distinct from "cooperative"."catalog_sales_item_category"
WHERE
    coalesce("abundant"."catalog_sales_date_year","cooperative"."catalog_sales_date_year") = 1999

GROUP BY
    1,
    2,
    3,
    4,
    5,
    coalesce("abundant"."catalog_sales_date_month_of_year","cooperative"."catalog_sales_date_month_of_year")),
concerned as (
SELECT
    "juicy"."catalog_sales_call_center_name" as "catalog_sales_call_center_name",
    "juicy"."catalog_sales_date_year" as "catalog_sales_date_year",
    "juicy"."catalog_sales_item_brand_name" as "catalog_sales_item_brand_name",
    "juicy"."catalog_sales_item_category" as "catalog_sales_item_category",
    avg("juicy"."sum_sales") as "avg_monthly_sales"
FROM
    "juicy"
WHERE
    "juicy"."catalog_sales_date_year" = 1999

GROUP BY
    1,
    2,
    3,
    4
HAVING
    "avg_monthly_sales" > 0
),
young as (
SELECT
    "abundant"."catalog_sales_date_month_of_year" as "catalog_sales_date_month_of_year",
    "abundant"."sum_sales" - "concerned"."avg_monthly_sales" as "sum_minus_avg",
    "abundant"."sum_sales" as "sum_sales",
    "concerned"."avg_monthly_sales" as "avg_monthly_sales",
    coalesce("abundant"."catalog_sales_call_center_name","concerned"."catalog_sales_call_center_name") as "catalog_sales_call_center_name",
    coalesce("abundant"."catalog_sales_date_year","concerned"."catalog_sales_date_year") as "catalog_sales_date_year",
    coalesce("abundant"."catalog_sales_item_brand_name","concerned"."catalog_sales_item_brand_name") as "catalog_sales_item_brand_name",
    coalesce("abundant"."catalog_sales_item_category","concerned"."catalog_sales_item_category") as "catalog_sales_item_category"
FROM
    "abundant"
    RIGHT OUTER JOIN "concerned" on "abundant"."catalog_sales_call_center_name" = "concerned"."catalog_sales_call_center_name" AND "abundant"."catalog_sales_date_year" = "concerned"."catalog_sales_date_year" AND "abundant"."catalog_sales_item_brand_name" = "concerned"."catalog_sales_item_brand_name" AND "abundant"."catalog_sales_item_category" is not distinct from "concerned"."catalog_sales_item_category"
WHERE
    coalesce("abundant"."catalog_sales_date_year","concerned"."catalog_sales_date_year") = 1999
)
SELECT
    coalesce("abhorrent"."catalog_sales_item_category","young"."catalog_sales_item_category") as "catalog_sales_item_category",
    coalesce("abhorrent"."catalog_sales_item_brand_name","young"."catalog_sales_item_brand_name") as "catalog_sales_item_brand_name",
    coalesce("abhorrent"."catalog_sales_call_center_name","young"."catalog_sales_call_center_name") as "catalog_sales_call_center_name",
    coalesce("abhorrent"."catalog_sales_date_year","young"."catalog_sales_date_year") as "catalog_sales_date_year",
    coalesce("abhorrent"."catalog_sales_date_month_of_year","young"."catalog_sales_date_month_of_year") as "catalog_sales_date_month_of_year",
    "young"."avg_monthly_sales" as "avg_monthly_sales",
    "young"."sum_sales" as "sum_sales",
    "abhorrent"."psum" as "psum",
    "abhorrent"."nsum" as "nsum"
FROM
    "abhorrent"
    RIGHT OUTER JOIN "young" on "abhorrent"."catalog_sales_call_center_name" = "young"."catalog_sales_call_center_name" AND "abhorrent"."catalog_sales_date_month_of_year" = "young"."catalog_sales_date_month_of_year" AND "abhorrent"."catalog_sales_date_year" = "young"."catalog_sales_date_year" AND "abhorrent"."catalog_sales_item_brand_name" = "young"."catalog_sales_item_brand_name" AND "abhorrent"."catalog_sales_item_category" is not distinct from "young"."catalog_sales_item_category"
WHERE
    coalesce("abhorrent"."catalog_sales_date_year","young"."catalog_sales_date_year") = 1999 and "young"."avg_monthly_sales" > 0 and CASE
	WHEN "young"."avg_monthly_sales" > 0 THEN abs("young"."sum_sales" - "young"."avg_monthly_sales") / "young"."avg_monthly_sales"
	ELSE null
	END > 0.1

ORDER BY 
    "young"."sum_minus_avg" asc nulls first,
    coalesce("abhorrent"."catalog_sales_item_category","young"."catalog_sales_item_category") asc,
    coalesce("abhorrent"."catalog_sales_item_brand_name","young"."catalog_sales_item_brand_name") asc,
    coalesce("abhorrent"."catalog_sales_call_center_name","young"."catalog_sales_call_center_name") asc,
    coalesce("abhorrent"."catalog_sales_date_year","young"."catalog_sales_date_year") asc,
    coalesce("abhorrent"."catalog_sales_date_month_of_year","young"."catalog_sales_date_month_of_year") asc,
    "young"."avg_monthly_sales" asc,
    "young"."sum_sales" asc,
    "abhorrent"."psum" asc,
    "abhorrent"."nsum" asc
LIMIT (100)