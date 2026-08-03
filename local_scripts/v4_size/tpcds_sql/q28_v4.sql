
WITH 
quizzical as (
SELECT
    "store_sales_store_sales"."SS_COUPON_AMT" as "store_sales_coupon_amt",
    "store_sales_store_sales"."SS_LIST_PRICE" as "store_sales_list_price",
    "store_sales_store_sales"."SS_QUANTITY" as "store_sales_quantity",
    "store_sales_store_sales"."SS_WHOLESALE_COST" as "store_sales_wholesale_cost"
FROM
    "memory"."store_sales" as "store_sales_store_sales"
WHERE
    "store_sales_store_sales"."SS_QUANTITY" BETWEEN 0 AND 30
),
wakeful as (
SELECT
    CASE
	WHEN "quizzical"."store_sales_quantity" BETWEEN 0 AND 5 THEN 1
	WHEN "quizzical"."store_sales_quantity" BETWEEN 6 AND 10 THEN 2
	WHEN "quizzical"."store_sales_quantity" BETWEEN 11 AND 15 THEN 3
	WHEN "quizzical"."store_sales_quantity" BETWEEN 16 AND 20 THEN 4
	WHEN "quizzical"."store_sales_quantity" BETWEEN 21 AND 25 THEN 5
	WHEN "quizzical"."store_sales_quantity" BETWEEN 26 AND 30 THEN 6
	ELSE null
	END as "bucket_id",
    avg(CASE
	WHEN CASE
	WHEN "quizzical"."store_sales_quantity" BETWEEN 0 AND 5 THEN 1
	WHEN "quizzical"."store_sales_quantity" BETWEEN 6 AND 10 THEN 2
	WHEN "quizzical"."store_sales_quantity" BETWEEN 11 AND 15 THEN 3
	WHEN "quizzical"."store_sales_quantity" BETWEEN 16 AND 20 THEN 4
	WHEN "quizzical"."store_sales_quantity" BETWEEN 21 AND 25 THEN 5
	WHEN "quizzical"."store_sales_quantity" BETWEEN 26 AND 30 THEN 6
	ELSE null
	END = 1 and ( "quizzical"."store_sales_list_price" BETWEEN 8 AND 18 or "quizzical"."store_sales_coupon_amt" BETWEEN 459 AND 1459 or "quizzical"."store_sales_wholesale_cost" BETWEEN 57 AND 77 ) THEN "quizzical"."store_sales_list_price"
	WHEN CASE
	WHEN "quizzical"."store_sales_quantity" BETWEEN 0 AND 5 THEN 1
	WHEN "quizzical"."store_sales_quantity" BETWEEN 6 AND 10 THEN 2
	WHEN "quizzical"."store_sales_quantity" BETWEEN 11 AND 15 THEN 3
	WHEN "quizzical"."store_sales_quantity" BETWEEN 16 AND 20 THEN 4
	WHEN "quizzical"."store_sales_quantity" BETWEEN 21 AND 25 THEN 5
	WHEN "quizzical"."store_sales_quantity" BETWEEN 26 AND 30 THEN 6
	ELSE null
	END = 2 and ( "quizzical"."store_sales_list_price" BETWEEN 90 AND 100 or "quizzical"."store_sales_coupon_amt" BETWEEN 2323 AND 3323 or "quizzical"."store_sales_wholesale_cost" BETWEEN 31 AND 51 ) THEN "quizzical"."store_sales_list_price"
	WHEN CASE
	WHEN "quizzical"."store_sales_quantity" BETWEEN 0 AND 5 THEN 1
	WHEN "quizzical"."store_sales_quantity" BETWEEN 6 AND 10 THEN 2
	WHEN "quizzical"."store_sales_quantity" BETWEEN 11 AND 15 THEN 3
	WHEN "quizzical"."store_sales_quantity" BETWEEN 16 AND 20 THEN 4
	WHEN "quizzical"."store_sales_quantity" BETWEEN 21 AND 25 THEN 5
	WHEN "quizzical"."store_sales_quantity" BETWEEN 26 AND 30 THEN 6
	ELSE null
	END = 3 and ( "quizzical"."store_sales_list_price" BETWEEN 142 AND 152 or "quizzical"."store_sales_coupon_amt" BETWEEN 12214 AND 13214 or "quizzical"."store_sales_wholesale_cost" BETWEEN 79 AND 99 ) THEN "quizzical"."store_sales_list_price"
	WHEN CASE
	WHEN "quizzical"."store_sales_quantity" BETWEEN 0 AND 5 THEN 1
	WHEN "quizzical"."store_sales_quantity" BETWEEN 6 AND 10 THEN 2
	WHEN "quizzical"."store_sales_quantity" BETWEEN 11 AND 15 THEN 3
	WHEN "quizzical"."store_sales_quantity" BETWEEN 16 AND 20 THEN 4
	WHEN "quizzical"."store_sales_quantity" BETWEEN 21 AND 25 THEN 5
	WHEN "quizzical"."store_sales_quantity" BETWEEN 26 AND 30 THEN 6
	ELSE null
	END = 4 and ( "quizzical"."store_sales_list_price" BETWEEN 135 AND 145 or "quizzical"."store_sales_coupon_amt" BETWEEN 6071 AND 7071 or "quizzical"."store_sales_wholesale_cost" BETWEEN 38 AND 58 ) THEN "quizzical"."store_sales_list_price"
	WHEN CASE
	WHEN "quizzical"."store_sales_quantity" BETWEEN 0 AND 5 THEN 1
	WHEN "quizzical"."store_sales_quantity" BETWEEN 6 AND 10 THEN 2
	WHEN "quizzical"."store_sales_quantity" BETWEEN 11 AND 15 THEN 3
	WHEN "quizzical"."store_sales_quantity" BETWEEN 16 AND 20 THEN 4
	WHEN "quizzical"."store_sales_quantity" BETWEEN 21 AND 25 THEN 5
	WHEN "quizzical"."store_sales_quantity" BETWEEN 26 AND 30 THEN 6
	ELSE null
	END = 5 and ( "quizzical"."store_sales_list_price" BETWEEN 122 AND 132 or "quizzical"."store_sales_coupon_amt" BETWEEN 836 AND 1836 or "quizzical"."store_sales_wholesale_cost" BETWEEN 17 AND 37 ) THEN "quizzical"."store_sales_list_price"
	WHEN CASE
	WHEN "quizzical"."store_sales_quantity" BETWEEN 0 AND 5 THEN 1
	WHEN "quizzical"."store_sales_quantity" BETWEEN 6 AND 10 THEN 2
	WHEN "quizzical"."store_sales_quantity" BETWEEN 11 AND 15 THEN 3
	WHEN "quizzical"."store_sales_quantity" BETWEEN 16 AND 20 THEN 4
	WHEN "quizzical"."store_sales_quantity" BETWEEN 21 AND 25 THEN 5
	WHEN "quizzical"."store_sales_quantity" BETWEEN 26 AND 30 THEN 6
	ELSE null
	END = 6 and ( "quizzical"."store_sales_list_price" BETWEEN 154 AND 164 or "quizzical"."store_sales_coupon_amt" BETWEEN 7326 AND 8326 or "quizzical"."store_sales_wholesale_cost" BETWEEN 7 AND 27 ) THEN "quizzical"."store_sales_list_price"
	ELSE null
	END) as "lp_avg",
    count(CASE
	WHEN CASE
	WHEN "quizzical"."store_sales_quantity" BETWEEN 0 AND 5 THEN 1
	WHEN "quizzical"."store_sales_quantity" BETWEEN 6 AND 10 THEN 2
	WHEN "quizzical"."store_sales_quantity" BETWEEN 11 AND 15 THEN 3
	WHEN "quizzical"."store_sales_quantity" BETWEEN 16 AND 20 THEN 4
	WHEN "quizzical"."store_sales_quantity" BETWEEN 21 AND 25 THEN 5
	WHEN "quizzical"."store_sales_quantity" BETWEEN 26 AND 30 THEN 6
	ELSE null
	END = 1 and ( "quizzical"."store_sales_list_price" BETWEEN 8 AND 18 or "quizzical"."store_sales_coupon_amt" BETWEEN 459 AND 1459 or "quizzical"."store_sales_wholesale_cost" BETWEEN 57 AND 77 ) THEN "quizzical"."store_sales_list_price"
	WHEN CASE
	WHEN "quizzical"."store_sales_quantity" BETWEEN 0 AND 5 THEN 1
	WHEN "quizzical"."store_sales_quantity" BETWEEN 6 AND 10 THEN 2
	WHEN "quizzical"."store_sales_quantity" BETWEEN 11 AND 15 THEN 3
	WHEN "quizzical"."store_sales_quantity" BETWEEN 16 AND 20 THEN 4
	WHEN "quizzical"."store_sales_quantity" BETWEEN 21 AND 25 THEN 5
	WHEN "quizzical"."store_sales_quantity" BETWEEN 26 AND 30 THEN 6
	ELSE null
	END = 2 and ( "quizzical"."store_sales_list_price" BETWEEN 90 AND 100 or "quizzical"."store_sales_coupon_amt" BETWEEN 2323 AND 3323 or "quizzical"."store_sales_wholesale_cost" BETWEEN 31 AND 51 ) THEN "quizzical"."store_sales_list_price"
	WHEN CASE
	WHEN "quizzical"."store_sales_quantity" BETWEEN 0 AND 5 THEN 1
	WHEN "quizzical"."store_sales_quantity" BETWEEN 6 AND 10 THEN 2
	WHEN "quizzical"."store_sales_quantity" BETWEEN 11 AND 15 THEN 3
	WHEN "quizzical"."store_sales_quantity" BETWEEN 16 AND 20 THEN 4
	WHEN "quizzical"."store_sales_quantity" BETWEEN 21 AND 25 THEN 5
	WHEN "quizzical"."store_sales_quantity" BETWEEN 26 AND 30 THEN 6
	ELSE null
	END = 3 and ( "quizzical"."store_sales_list_price" BETWEEN 142 AND 152 or "quizzical"."store_sales_coupon_amt" BETWEEN 12214 AND 13214 or "quizzical"."store_sales_wholesale_cost" BETWEEN 79 AND 99 ) THEN "quizzical"."store_sales_list_price"
	WHEN CASE
	WHEN "quizzical"."store_sales_quantity" BETWEEN 0 AND 5 THEN 1
	WHEN "quizzical"."store_sales_quantity" BETWEEN 6 AND 10 THEN 2
	WHEN "quizzical"."store_sales_quantity" BETWEEN 11 AND 15 THEN 3
	WHEN "quizzical"."store_sales_quantity" BETWEEN 16 AND 20 THEN 4
	WHEN "quizzical"."store_sales_quantity" BETWEEN 21 AND 25 THEN 5
	WHEN "quizzical"."store_sales_quantity" BETWEEN 26 AND 30 THEN 6
	ELSE null
	END = 4 and ( "quizzical"."store_sales_list_price" BETWEEN 135 AND 145 or "quizzical"."store_sales_coupon_amt" BETWEEN 6071 AND 7071 or "quizzical"."store_sales_wholesale_cost" BETWEEN 38 AND 58 ) THEN "quizzical"."store_sales_list_price"
	WHEN CASE
	WHEN "quizzical"."store_sales_quantity" BETWEEN 0 AND 5 THEN 1
	WHEN "quizzical"."store_sales_quantity" BETWEEN 6 AND 10 THEN 2
	WHEN "quizzical"."store_sales_quantity" BETWEEN 11 AND 15 THEN 3
	WHEN "quizzical"."store_sales_quantity" BETWEEN 16 AND 20 THEN 4
	WHEN "quizzical"."store_sales_quantity" BETWEEN 21 AND 25 THEN 5
	WHEN "quizzical"."store_sales_quantity" BETWEEN 26 AND 30 THEN 6
	ELSE null
	END = 5 and ( "quizzical"."store_sales_list_price" BETWEEN 122 AND 132 or "quizzical"."store_sales_coupon_amt" BETWEEN 836 AND 1836 or "quizzical"."store_sales_wholesale_cost" BETWEEN 17 AND 37 ) THEN "quizzical"."store_sales_list_price"
	WHEN CASE
	WHEN "quizzical"."store_sales_quantity" BETWEEN 0 AND 5 THEN 1
	WHEN "quizzical"."store_sales_quantity" BETWEEN 6 AND 10 THEN 2
	WHEN "quizzical"."store_sales_quantity" BETWEEN 11 AND 15 THEN 3
	WHEN "quizzical"."store_sales_quantity" BETWEEN 16 AND 20 THEN 4
	WHEN "quizzical"."store_sales_quantity" BETWEEN 21 AND 25 THEN 5
	WHEN "quizzical"."store_sales_quantity" BETWEEN 26 AND 30 THEN 6
	ELSE null
	END = 6 and ( "quizzical"."store_sales_list_price" BETWEEN 154 AND 164 or "quizzical"."store_sales_coupon_amt" BETWEEN 7326 AND 8326 or "quizzical"."store_sales_wholesale_cost" BETWEEN 7 AND 27 ) THEN "quizzical"."store_sales_list_price"
	ELSE null
	END) as "lp_cnt",
    count(distinct CASE
	WHEN CASE
	WHEN "quizzical"."store_sales_quantity" BETWEEN 0 AND 5 THEN 1
	WHEN "quizzical"."store_sales_quantity" BETWEEN 6 AND 10 THEN 2
	WHEN "quizzical"."store_sales_quantity" BETWEEN 11 AND 15 THEN 3
	WHEN "quizzical"."store_sales_quantity" BETWEEN 16 AND 20 THEN 4
	WHEN "quizzical"."store_sales_quantity" BETWEEN 21 AND 25 THEN 5
	WHEN "quizzical"."store_sales_quantity" BETWEEN 26 AND 30 THEN 6
	ELSE null
	END = 1 and ( "quizzical"."store_sales_list_price" BETWEEN 8 AND 18 or "quizzical"."store_sales_coupon_amt" BETWEEN 459 AND 1459 or "quizzical"."store_sales_wholesale_cost" BETWEEN 57 AND 77 ) THEN "quizzical"."store_sales_list_price"
	WHEN CASE
	WHEN "quizzical"."store_sales_quantity" BETWEEN 0 AND 5 THEN 1
	WHEN "quizzical"."store_sales_quantity" BETWEEN 6 AND 10 THEN 2
	WHEN "quizzical"."store_sales_quantity" BETWEEN 11 AND 15 THEN 3
	WHEN "quizzical"."store_sales_quantity" BETWEEN 16 AND 20 THEN 4
	WHEN "quizzical"."store_sales_quantity" BETWEEN 21 AND 25 THEN 5
	WHEN "quizzical"."store_sales_quantity" BETWEEN 26 AND 30 THEN 6
	ELSE null
	END = 2 and ( "quizzical"."store_sales_list_price" BETWEEN 90 AND 100 or "quizzical"."store_sales_coupon_amt" BETWEEN 2323 AND 3323 or "quizzical"."store_sales_wholesale_cost" BETWEEN 31 AND 51 ) THEN "quizzical"."store_sales_list_price"
	WHEN CASE
	WHEN "quizzical"."store_sales_quantity" BETWEEN 0 AND 5 THEN 1
	WHEN "quizzical"."store_sales_quantity" BETWEEN 6 AND 10 THEN 2
	WHEN "quizzical"."store_sales_quantity" BETWEEN 11 AND 15 THEN 3
	WHEN "quizzical"."store_sales_quantity" BETWEEN 16 AND 20 THEN 4
	WHEN "quizzical"."store_sales_quantity" BETWEEN 21 AND 25 THEN 5
	WHEN "quizzical"."store_sales_quantity" BETWEEN 26 AND 30 THEN 6
	ELSE null
	END = 3 and ( "quizzical"."store_sales_list_price" BETWEEN 142 AND 152 or "quizzical"."store_sales_coupon_amt" BETWEEN 12214 AND 13214 or "quizzical"."store_sales_wholesale_cost" BETWEEN 79 AND 99 ) THEN "quizzical"."store_sales_list_price"
	WHEN CASE
	WHEN "quizzical"."store_sales_quantity" BETWEEN 0 AND 5 THEN 1
	WHEN "quizzical"."store_sales_quantity" BETWEEN 6 AND 10 THEN 2
	WHEN "quizzical"."store_sales_quantity" BETWEEN 11 AND 15 THEN 3
	WHEN "quizzical"."store_sales_quantity" BETWEEN 16 AND 20 THEN 4
	WHEN "quizzical"."store_sales_quantity" BETWEEN 21 AND 25 THEN 5
	WHEN "quizzical"."store_sales_quantity" BETWEEN 26 AND 30 THEN 6
	ELSE null
	END = 4 and ( "quizzical"."store_sales_list_price" BETWEEN 135 AND 145 or "quizzical"."store_sales_coupon_amt" BETWEEN 6071 AND 7071 or "quizzical"."store_sales_wholesale_cost" BETWEEN 38 AND 58 ) THEN "quizzical"."store_sales_list_price"
	WHEN CASE
	WHEN "quizzical"."store_sales_quantity" BETWEEN 0 AND 5 THEN 1
	WHEN "quizzical"."store_sales_quantity" BETWEEN 6 AND 10 THEN 2
	WHEN "quizzical"."store_sales_quantity" BETWEEN 11 AND 15 THEN 3
	WHEN "quizzical"."store_sales_quantity" BETWEEN 16 AND 20 THEN 4
	WHEN "quizzical"."store_sales_quantity" BETWEEN 21 AND 25 THEN 5
	WHEN "quizzical"."store_sales_quantity" BETWEEN 26 AND 30 THEN 6
	ELSE null
	END = 5 and ( "quizzical"."store_sales_list_price" BETWEEN 122 AND 132 or "quizzical"."store_sales_coupon_amt" BETWEEN 836 AND 1836 or "quizzical"."store_sales_wholesale_cost" BETWEEN 17 AND 37 ) THEN "quizzical"."store_sales_list_price"
	WHEN CASE
	WHEN "quizzical"."store_sales_quantity" BETWEEN 0 AND 5 THEN 1
	WHEN "quizzical"."store_sales_quantity" BETWEEN 6 AND 10 THEN 2
	WHEN "quizzical"."store_sales_quantity" BETWEEN 11 AND 15 THEN 3
	WHEN "quizzical"."store_sales_quantity" BETWEEN 16 AND 20 THEN 4
	WHEN "quizzical"."store_sales_quantity" BETWEEN 21 AND 25 THEN 5
	WHEN "quizzical"."store_sales_quantity" BETWEEN 26 AND 30 THEN 6
	ELSE null
	END = 6 and ( "quizzical"."store_sales_list_price" BETWEEN 154 AND 164 or "quizzical"."store_sales_coupon_amt" BETWEEN 7326 AND 8326 or "quizzical"."store_sales_wholesale_cost" BETWEEN 7 AND 27 ) THEN "quizzical"."store_sales_list_price"
	ELSE null
	END) as "lp_cntd"
FROM
    "quizzical"
GROUP BY
    1),
highfalutin as (
SELECT
    CASE
	WHEN "quizzical"."store_sales_quantity" BETWEEN 0 AND 5 THEN 1
	WHEN "quizzical"."store_sales_quantity" BETWEEN 6 AND 10 THEN 2
	WHEN "quizzical"."store_sales_quantity" BETWEEN 11 AND 15 THEN 3
	WHEN "quizzical"."store_sales_quantity" BETWEEN 16 AND 20 THEN 4
	WHEN "quizzical"."store_sales_quantity" BETWEEN 21 AND 25 THEN 5
	WHEN "quizzical"."store_sales_quantity" BETWEEN 26 AND 30 THEN 6
	ELSE null
	END as "bucket_id"
FROM
    "quizzical"),
cheerful as (
SELECT
    "highfalutin"."bucket_id" as "bucket_id",
    CASE WHEN "highfalutin"."bucket_id" = 1 THEN "wakeful"."lp_avg" ELSE NULL END as "_virt_filter_lp_avg_9807684414024497",
    CASE WHEN "highfalutin"."bucket_id" = 1 THEN "wakeful"."lp_cntd" ELSE NULL END as "_virt_filter_lp_cntd_5327557161933280",
    CASE WHEN "highfalutin"."bucket_id" = 1 THEN coalesce("wakeful"."lp_cnt",0) ELSE NULL END as "_virt_filter_lp_cnt_2224516900884374",
    CASE WHEN "highfalutin"."bucket_id" = 2 THEN "wakeful"."lp_avg" ELSE NULL END as "_virt_filter_lp_avg_2861588206160358",
    CASE WHEN "highfalutin"."bucket_id" = 2 THEN "wakeful"."lp_cntd" ELSE NULL END as "_virt_filter_lp_cntd_7846943073851735",
    CASE WHEN "highfalutin"."bucket_id" = 2 THEN coalesce("wakeful"."lp_cnt",0) ELSE NULL END as "_virt_filter_lp_cnt_6858115141264011",
    CASE WHEN "highfalutin"."bucket_id" = 3 THEN "wakeful"."lp_avg" ELSE NULL END as "_virt_filter_lp_avg_2850673729313768",
    CASE WHEN "highfalutin"."bucket_id" = 3 THEN "wakeful"."lp_cntd" ELSE NULL END as "_virt_filter_lp_cntd_5736325163174944",
    CASE WHEN "highfalutin"."bucket_id" = 3 THEN coalesce("wakeful"."lp_cnt",0) ELSE NULL END as "_virt_filter_lp_cnt_894357222675893",
    CASE WHEN "highfalutin"."bucket_id" = 4 THEN "wakeful"."lp_avg" ELSE NULL END as "_virt_filter_lp_avg_4880359626544039",
    CASE WHEN "highfalutin"."bucket_id" = 4 THEN "wakeful"."lp_cntd" ELSE NULL END as "_virt_filter_lp_cntd_7690745280661633",
    CASE WHEN "highfalutin"."bucket_id" = 4 THEN coalesce("wakeful"."lp_cnt",0) ELSE NULL END as "_virt_filter_lp_cnt_3577078747089769",
    CASE WHEN "highfalutin"."bucket_id" = 5 THEN "wakeful"."lp_avg" ELSE NULL END as "_virt_filter_lp_avg_284549201004209",
    CASE WHEN "highfalutin"."bucket_id" = 5 THEN "wakeful"."lp_cntd" ELSE NULL END as "_virt_filter_lp_cntd_3739216114026085",
    CASE WHEN "highfalutin"."bucket_id" = 5 THEN coalesce("wakeful"."lp_cnt",0) ELSE NULL END as "_virt_filter_lp_cnt_1394773238248231",
    CASE WHEN "highfalutin"."bucket_id" = 6 THEN "wakeful"."lp_avg" ELSE NULL END as "_virt_filter_lp_avg_9446332470240154",
    CASE WHEN "highfalutin"."bucket_id" = 6 THEN "wakeful"."lp_cntd" ELSE NULL END as "_virt_filter_lp_cntd_3811294399709285",
    CASE WHEN "highfalutin"."bucket_id" = 6 THEN coalesce("wakeful"."lp_cnt",0) ELSE NULL END as "_virt_filter_lp_cnt_8966209673081292"
FROM
    "highfalutin"
    INNER JOIN "wakeful" on "highfalutin"."bucket_id" is not distinct from "wakeful"."bucket_id"),
cooperative as (
SELECT
    "cheerful"."_virt_filter_lp_avg_284549201004209" as "_virt_filter_lp_avg_284549201004209",
    "cheerful"."_virt_filter_lp_avg_2850673729313768" as "_virt_filter_lp_avg_2850673729313768",
    "cheerful"."_virt_filter_lp_avg_2861588206160358" as "_virt_filter_lp_avg_2861588206160358",
    "cheerful"."_virt_filter_lp_avg_4880359626544039" as "_virt_filter_lp_avg_4880359626544039",
    "cheerful"."_virt_filter_lp_avg_9446332470240154" as "_virt_filter_lp_avg_9446332470240154",
    "cheerful"."_virt_filter_lp_avg_9807684414024497" as "_virt_filter_lp_avg_9807684414024497",
    "cheerful"."_virt_filter_lp_cnt_1394773238248231" as "_virt_filter_lp_cnt_1394773238248231",
    "cheerful"."_virt_filter_lp_cnt_2224516900884374" as "_virt_filter_lp_cnt_2224516900884374",
    "cheerful"."_virt_filter_lp_cnt_3577078747089769" as "_virt_filter_lp_cnt_3577078747089769",
    "cheerful"."_virt_filter_lp_cnt_6858115141264011" as "_virt_filter_lp_cnt_6858115141264011",
    "cheerful"."_virt_filter_lp_cnt_894357222675893" as "_virt_filter_lp_cnt_894357222675893",
    "cheerful"."_virt_filter_lp_cnt_8966209673081292" as "_virt_filter_lp_cnt_8966209673081292",
    "cheerful"."_virt_filter_lp_cntd_3739216114026085" as "_virt_filter_lp_cntd_3739216114026085",
    "cheerful"."_virt_filter_lp_cntd_3811294399709285" as "_virt_filter_lp_cntd_3811294399709285",
    "cheerful"."_virt_filter_lp_cntd_5327557161933280" as "_virt_filter_lp_cntd_5327557161933280",
    "cheerful"."_virt_filter_lp_cntd_5736325163174944" as "_virt_filter_lp_cntd_5736325163174944",
    "cheerful"."_virt_filter_lp_cntd_7690745280661633" as "_virt_filter_lp_cntd_7690745280661633",
    "cheerful"."_virt_filter_lp_cntd_7846943073851735" as "_virt_filter_lp_cntd_7846943073851735"
FROM
    "cheerful"
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
    13,
    14,
    15,
    16,
    17,
    18,
    "cheerful"."bucket_id")
SELECT
    max("cooperative"."_virt_filter_lp_avg_9807684414024497") as "B1_LP",
    max("cooperative"."_virt_filter_lp_cnt_2224516900884374") as "B1_CNT",
    max("cooperative"."_virt_filter_lp_cntd_5327557161933280") as "B1_CNTD",
    max("cooperative"."_virt_filter_lp_avg_2861588206160358") as "B2_LP",
    max("cooperative"."_virt_filter_lp_cnt_6858115141264011") as "B2_CNT",
    max("cooperative"."_virt_filter_lp_cntd_7846943073851735") as "B2_CNTD",
    max("cooperative"."_virt_filter_lp_avg_2850673729313768") as "B3_LP",
    max("cooperative"."_virt_filter_lp_cnt_894357222675893") as "B3_CNT",
    max("cooperative"."_virt_filter_lp_cntd_5736325163174944") as "B3_CNTD",
    max("cooperative"."_virt_filter_lp_avg_4880359626544039") as "B4_LP",
    max("cooperative"."_virt_filter_lp_cnt_3577078747089769") as "B4_CNT",
    max("cooperative"."_virt_filter_lp_cntd_7690745280661633") as "B4_CNTD",
    max("cooperative"."_virt_filter_lp_avg_284549201004209") as "B5_LP",
    max("cooperative"."_virt_filter_lp_cnt_1394773238248231") as "B5_CNT",
    max("cooperative"."_virt_filter_lp_cntd_3739216114026085") as "B5_CNTD",
    max("cooperative"."_virt_filter_lp_avg_9446332470240154") as "B6_LP",
    max("cooperative"."_virt_filter_lp_cnt_8966209673081292") as "B6_CNT",
    max("cooperative"."_virt_filter_lp_cntd_3811294399709285") as "B6_CNTD"
FROM
    "cooperative"
LIMIT (100)