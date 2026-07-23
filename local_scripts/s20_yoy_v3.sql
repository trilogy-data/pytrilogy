
WITH 
yummy as (
SELECT
    "web_sales"."customer" as "csk",
    "web_sales"."id" as "ws_id",
    "web_sales"."net" as "ws_net",
    "web_sales"."year" as "ws_year"
FROM
    (
select 1 as id, 1 as customer, 2001 as year, 100.0 as net
union all select 2, 1, 2002, 300.0
union all select 3, 2, 2001, 100.0
union all select 4, 2, 2002, 50.0
union all select 5, 3, 2001, 100.0
union all select 6, 3, 2002, 200.0
) as "web_sales"
WHERE
    ("web_sales"."year" is not null and "web_sales"."year" in (2001,2002))
),
quizzical as (
SELECT
    "customers"."cid" as "cid",
    "customers"."csk" as "csk",
    "customers"."first_name" as "first_name",
    "customers"."last_name" as "last_name"
FROM
    (
select 1 as csk, 'A' as cid, 'John' as first_name, 'Doe' as last_name
union all
select 2, 'B', 'John', 'Doe'
union all
select 3, 'C', 'Jane', 'Smith'
) as "customers"),
highfalutin as (
SELECT
    "store_sales"."customer" as "csk",
    "store_sales"."id" as "ss_id",
    "store_sales"."net" as "ss_net",
    "store_sales"."year" as "ss_year"
FROM
    (
select 1 as id, 1 as customer, 2001 as year, 100.0 as net
union all select 2, 1, 2002, 100.0
union all select 3, 2, 2001, 100.0
union all select 4, 2, 2002, 100.0
union all select 5, 3, 2001, 100.0
union all select 6, 3, 2002, 100.0
) as "store_sales"
WHERE
    ("store_sales"."year" is not null and "store_sales"."year" in (2001,2002))
),
juicy as (
SELECT
    "yummy"."csk" as "csk",
    "yummy"."ws_year" as "ws_year",
    sum("yummy"."ws_net") as "_wb_wb_tot"
FROM
    "yummy"
GROUP BY
    1,
    2),
wakeful as (
SELECT
    "highfalutin"."csk" as "csk",
    "highfalutin"."ss_year" as "ss_year",
    sum("highfalutin"."ss_net") as "_st_st_tot"
FROM
    "highfalutin"
GROUP BY
    1,
    2),
vacuous as (
SELECT
    "juicy"."_wb_wb_tot" as "_wb_wb_tot",
    "juicy"."ws_year" as "_wb_w_yr",
    "quizzical"."cid" as "_wb_w_cid",
    "quizzical"."csk" as "_wb_w_csk",
    "quizzical"."first_name" as "_wb_w_fn",
    "quizzical"."last_name" as "_wb_w_ln"
FROM
    "juicy"
    INNER JOIN "quizzical" on "juicy"."csk" = "quizzical"."csk"),
cheerful as (
SELECT
    "quizzical"."cid" as "_st_s_cid",
    "quizzical"."csk" as "_st_s_csk",
    "quizzical"."first_name" as "_st_s_fn",
    "quizzical"."last_name" as "_st_s_ln",
    "wakeful"."_st_st_tot" as "_st_st_tot",
    "wakeful"."ss_year" as "_st_s_yr"
FROM
    "wakeful"
    INNER JOIN "quizzical" on "wakeful"."csk" = "quizzical"."csk"),
concerned as (
SELECT
    "vacuous"."_wb_w_csk" as "wb_w_csk",
    "vacuous"."_wb_w_yr" as "wb_w_yr",
    "vacuous"."_wb_wb_tot" as "wb_wb_tot"
FROM
    "vacuous"),
thoughtful as (
SELECT
    "cheerful"."_st_s_cid" as "st_s_cid",
    "cheerful"."_st_s_csk" as "st_s_csk",
    "cheerful"."_st_s_fn" as "st_s_fn",
    "cheerful"."_st_s_ln" as "st_s_ln",
    "cheerful"."_st_s_yr" as "st_s_yr",
    "cheerful"."_st_st_tot" as "st_st_tot"
FROM
    "cheerful"),
young as (
SELECT
    "concerned"."wb_w_csk" as "wb_w_csk",
    "concerned"."wb_w_yr" as "wb_w_yr",
    "concerned"."wb_wb_tot" as "wb_wb_tot",
    CASE WHEN "concerned"."wb_w_yr" = 2001 THEN "concerned"."wb_wb_tot" ELSE NULL END as "_virt_filter_wb_tot_8931540061554244",
    CASE WHEN "concerned"."wb_w_yr" = 2002 THEN "concerned"."wb_wb_tot" ELSE NULL END as "_virt_filter_wb_tot_9652995580675536"
FROM
    "concerned"),
cooperative as (
SELECT
    "thoughtful"."st_s_cid" as "st_s_cid",
    "thoughtful"."st_s_csk" as "st_s_csk",
    "thoughtful"."st_s_fn" as "st_s_fn",
    "thoughtful"."st_s_ln" as "st_s_ln",
    "thoughtful"."st_s_yr" as "st_s_yr",
    "thoughtful"."st_st_tot" as "st_st_tot"
FROM
    "thoughtful"
WHERE
    "thoughtful"."st_s_cid" is not null
),
sparkling as (
SELECT
    "young"."_virt_filter_wb_tot_8931540061554244" as "_virt_filter_wb_tot_8931540061554244",
    "young"."_virt_filter_wb_tot_9652995580675536" as "_virt_filter_wb_tot_9652995580675536",
    "young"."wb_w_csk" as "wb_w_csk",
    "young"."wb_wb_tot" as "wb_wb_tot"
FROM
    "young"
GROUP BY
    1,
    2,
    3,
    4),
questionable as (
SELECT
    "cooperative"."st_s_cid" as "st_s_cid",
    "cooperative"."st_s_csk" as "st_s_csk",
    "cooperative"."st_s_fn" as "st_s_fn",
    "cooperative"."st_s_ln" as "st_s_ln",
    "cooperative"."st_s_yr" as "st_s_yr",
    "cooperative"."st_st_tot" as "st_st_tot",
    CASE WHEN "cooperative"."st_s_yr" = 2001 THEN "cooperative"."st_st_tot" ELSE NULL END as "_virt_filter_st_tot_3577771660166312",
    CASE WHEN "cooperative"."st_s_yr" = 2002 THEN "cooperative"."st_st_tot" ELSE NULL END as "_virt_filter_st_tot_9882851076675156"
FROM
    "cooperative"),
abhorrent as (
SELECT
    "sparkling"."_virt_filter_wb_tot_8931540061554244" as "_virt_filter_wb_tot_8931540061554244",
    "sparkling"."_virt_filter_wb_tot_9652995580675536" as "_virt_filter_wb_tot_9652995580675536",
    "sparkling"."wb_wb_tot" as "wb_wb_tot",
    "thoughtful"."st_s_cid" as "st_s_cid",
    "thoughtful"."st_s_fn" as "st_s_fn",
    "thoughtful"."st_s_ln" as "st_s_ln"
FROM
    "sparkling"
    FULL JOIN "thoughtful" on "sparkling"."wb_w_csk" = "thoughtful"."st_s_csk"
WHERE
    "thoughtful"."st_s_cid" is not null
),
abundant as (
SELECT
    "questionable"."_virt_filter_st_tot_3577771660166312" as "_virt_filter_st_tot_3577771660166312",
    "questionable"."_virt_filter_st_tot_9882851076675156" as "_virt_filter_st_tot_9882851076675156",
    "questionable"."st_s_cid" as "st_s_cid",
    "questionable"."st_s_fn" as "st_s_fn",
    "questionable"."st_s_ln" as "st_s_ln",
    "questionable"."st_st_tot" as "st_st_tot"
FROM
    "questionable"
GROUP BY
    1,
    2,
    3,
    4,
    5,
    6),
sweltering as (
SELECT
    "abhorrent"."st_s_cid" as "st_s_cid",
    "abhorrent"."st_s_fn" as "st_s_fn",
    "abhorrent"."st_s_ln" as "st_s_ln",
    sum("abhorrent"."_virt_filter_wb_tot_8931540061554244") as "_virt_agg_sum_9853764948188113",
    sum("abhorrent"."_virt_filter_wb_tot_9652995580675536") as "_virt_agg_sum_4137858187504676"
FROM
    "abhorrent"
GROUP BY
    1,
    2,
    3),
uneven as (
SELECT
    "abundant"."st_s_cid" as "st_s_cid",
    "abundant"."st_s_fn" as "st_s_fn",
    "abundant"."st_s_ln" as "st_s_ln",
    sum("abundant"."_virt_filter_st_tot_3577771660166312") as "_virt_agg_sum_9400619314901898",
    sum("abundant"."_virt_filter_st_tot_9882851076675156") as "_virt_agg_sum_8547237941805449"
FROM
    "abundant"
GROUP BY
    1,
    2,
    3)
SELECT
    "sweltering"."st_s_cid" as "customer_code",
    "sweltering"."st_s_fn" as "first_name_out",
    "sweltering"."st_s_ln" as "last_name_out"
FROM
    "sweltering"
    LEFT OUTER JOIN "uneven" on "sweltering"."st_s_cid" = "uneven"."st_s_cid" AND "sweltering"."st_s_fn" = "uneven"."st_s_fn" AND "sweltering"."st_s_ln" = "uneven"."st_s_ln"
WHERE
    "uneven"."_virt_agg_sum_9400619314901898" > 0 and "sweltering"."_virt_agg_sum_9853764948188113" > 0 and "uneven"."_virt_agg_sum_8547237941805449" is not null and "sweltering"."_virt_agg_sum_4137858187504676" is not null and ( "sweltering"."_virt_agg_sum_4137858187504676" / "sweltering"."_virt_agg_sum_9853764948188113" ) > ( "uneven"."_virt_agg_sum_8547237941805449" / "uneven"."_virt_agg_sum_9400619314901898" )

ORDER BY 
    "customer_code" asc nulls first
---- ROWS ----
('A', 'John', 'Doe')
('C', 'Jane', 'Smith')
