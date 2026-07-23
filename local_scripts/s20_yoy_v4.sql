
WITH 
young as (
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
cheerful as (
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
sparkling as (
SELECT
    "young"."csk" as "csk",
    "young"."ws_year" as "ws_year",
    sum("young"."ws_net") as "_wb_wb_tot"
FROM
    "young"
GROUP BY
    1,
    2),
highfalutin as (
SELECT
    "quizzical"."cid" as "cid",
    "quizzical"."first_name" as "first_name",
    "quizzical"."last_name" as "last_name"
FROM
    "quizzical"
GROUP BY
    1,
    2,
    3),
thoughtful as (
SELECT
    "cheerful"."csk" as "csk",
    "cheerful"."ss_year" as "ss_year",
    sum("cheerful"."ss_net") as "_st_st_tot"
FROM
    "cheerful"
GROUP BY
    1,
    2),
abhorrent as (
SELECT
    "sparkling"."_wb_wb_tot" as "_wb_wb_tot",
    "sparkling"."csk" as "_wb_w_csk",
    "sparkling"."csk" as "csk",
    "sparkling"."ws_year" as "_wb_w_yr",
    "sparkling"."ws_year" as "ws_year"
FROM
    "sparkling"),
concerned as (
SELECT
    "highfalutin"."cid" as "_wb_w_cid",
    "highfalutin"."first_name" as "_wb_w_fn",
    "highfalutin"."last_name" as "_wb_w_ln"
FROM
    "highfalutin"),
wakeful as (
SELECT
    "highfalutin"."cid" as "_st_s_cid",
    "highfalutin"."first_name" as "_st_s_fn",
    "highfalutin"."last_name" as "_st_s_ln"
FROM
    "highfalutin"),
cooperative as (
SELECT
    "thoughtful"."_st_st_tot" as "_st_st_tot",
    "thoughtful"."csk" as "_st_s_csk",
    "thoughtful"."csk" as "csk",
    "thoughtful"."ss_year" as "_st_s_yr",
    "thoughtful"."ss_year" as "ss_year"
FROM
    "thoughtful"),
sweltering as (
SELECT
    "abhorrent"."_wb_w_csk" as "_wb_w_csk",
    "abhorrent"."_wb_w_yr" as "_wb_w_yr",
    "abhorrent"."_wb_wb_tot" as "_wb_wb_tot",
    "concerned"."_wb_w_cid" as "_wb_w_cid",
    "concerned"."_wb_w_fn" as "_wb_w_fn",
    "concerned"."_wb_w_ln" as "_wb_w_ln"
FROM
    "concerned"
    FULL JOIN "abhorrent" on 1=1
GROUP BY
    1,
    2,
    3,
    4,
    5,
    6),
questionable as (
SELECT
    "cooperative"."_st_s_csk" as "_st_s_csk",
    "cooperative"."_st_s_yr" as "_st_s_yr",
    "cooperative"."_st_st_tot" as "_st_st_tot",
    "wakeful"."_st_s_cid" as "_st_s_cid",
    "wakeful"."_st_s_fn" as "_st_s_fn",
    "wakeful"."_st_s_ln" as "_st_s_ln"
FROM
    "wakeful"
    FULL JOIN "cooperative" on 1=1
GROUP BY
    1,
    2,
    3,
    4,
    5,
    6),
late as (
SELECT
    "sweltering"."_wb_w_csk" as "wb_w_csk",
    "sweltering"."_wb_w_yr" as "wb_w_yr",
    "sweltering"."_wb_wb_tot" as "wb_wb_tot"
FROM
    "sweltering"),
abundant as (
SELECT
    "questionable"."_st_s_cid" as "st_s_cid",
    "questionable"."_st_s_csk" as "st_s_csk",
    "questionable"."_st_s_fn" as "st_s_fn",
    "questionable"."_st_s_ln" as "st_s_ln",
    "questionable"."_st_s_yr" as "st_s_yr",
    "questionable"."_st_st_tot" as "st_st_tot"
FROM
    "questionable"),
macho as (
SELECT
    "late"."wb_w_csk" as "wb_w_csk",
    "late"."wb_w_yr" as "wb_w_yr",
    "late"."wb_wb_tot" as "wb_wb_tot",
    CASE WHEN "late"."wb_w_yr" = 2001 THEN "late"."wb_wb_tot" ELSE NULL END as "_virt_filter_wb_tot_8931540061554244",
    CASE WHEN "late"."wb_w_yr" = 2002 THEN "late"."wb_wb_tot" ELSE NULL END as "_virt_filter_wb_tot_9652995580675536"
FROM
    "late"),
uneven as (
SELECT
    "abundant"."st_s_cid" as "st_s_cid",
    "abundant"."st_s_csk" as "st_s_csk",
    "abundant"."st_s_fn" as "st_s_fn",
    "abundant"."st_s_ln" as "st_s_ln",
    "abundant"."st_s_yr" as "st_s_yr",
    "abundant"."st_st_tot" as "st_st_tot"
FROM
    "abundant"
WHERE
    "abundant"."st_s_cid" is not null
),
scrawny as (
SELECT
    "macho"."_virt_filter_wb_tot_8931540061554244" as "_virt_filter_wb_tot_8931540061554244",
    "macho"."_virt_filter_wb_tot_9652995580675536" as "_virt_filter_wb_tot_9652995580675536",
    "macho"."wb_wb_tot" as "wb_wb_tot",
    "uneven"."st_s_cid" as "st_s_cid",
    "uneven"."st_s_fn" as "st_s_fn",
    "uneven"."st_s_ln" as "st_s_ln",
    "uneven"."st_st_tot" as "st_st_tot",
    coalesce("macho"."wb_w_csk","uneven"."st_s_csk") as "st_s_csk",
    coalesce("macho"."wb_w_csk","uneven"."st_s_csk") as "wb_w_csk",
    coalesce("macho"."wb_w_yr","uneven"."st_s_yr") as "st_s_yr",
    coalesce("macho"."wb_w_yr","uneven"."st_s_yr") as "wb_w_yr"
FROM
    "macho"
    FULL JOIN "uneven" on "macho"."wb_w_csk" is not distinct from "uneven"."st_s_csk" AND "macho"."wb_w_yr" is not distinct from "uneven"."st_s_yr"),
yummy as (
SELECT
    "uneven"."st_s_cid" as "st_s_cid",
    "uneven"."st_s_csk" as "st_s_csk",
    "uneven"."st_s_fn" as "st_s_fn",
    "uneven"."st_s_ln" as "st_s_ln",
    "uneven"."st_s_yr" as "st_s_yr",
    "uneven"."st_st_tot" as "st_st_tot",
    CASE WHEN "uneven"."st_s_yr" = 2001 THEN "uneven"."st_st_tot" ELSE NULL END as "_virt_filter_st_tot_3577771660166312",
    CASE WHEN "uneven"."st_s_yr" = 2002 THEN "uneven"."st_st_tot" ELSE NULL END as "_virt_filter_st_tot_9882851076675156"
FROM
    "uneven"),
friendly as (
SELECT
    "scrawny"."_virt_filter_wb_tot_8931540061554244" as "_virt_filter_wb_tot_8931540061554244",
    "scrawny"."_virt_filter_wb_tot_9652995580675536" as "_virt_filter_wb_tot_9652995580675536",
    "scrawny"."st_s_cid" as "st_s_cid",
    "scrawny"."st_s_fn" as "st_s_fn",
    "scrawny"."st_s_ln" as "st_s_ln",
    "scrawny"."wb_wb_tot" as "wb_wb_tot"
FROM
    "scrawny"
GROUP BY
    1,
    2,
    3,
    4,
    5,
    6),
juicy as (
SELECT
    "yummy"."_virt_filter_st_tot_3577771660166312" as "_virt_filter_st_tot_3577771660166312",
    "yummy"."_virt_filter_st_tot_9882851076675156" as "_virt_filter_st_tot_9882851076675156",
    "yummy"."st_s_cid" as "st_s_cid",
    "yummy"."st_s_fn" as "st_s_fn",
    "yummy"."st_s_ln" as "st_s_ln",
    "yummy"."st_st_tot" as "st_st_tot"
FROM
    "yummy"
GROUP BY
    1,
    2,
    3,
    4,
    5,
    6),
kaput as (
SELECT
    "friendly"."st_s_cid" as "st_s_cid",
    "friendly"."st_s_fn" as "st_s_fn",
    "friendly"."st_s_ln" as "st_s_ln",
    sum("friendly"."_virt_filter_wb_tot_8931540061554244") as "_virt_agg_sum_9853764948188113",
    sum("friendly"."_virt_filter_wb_tot_9652995580675536") as "_virt_agg_sum_4137858187504676"
FROM
    "friendly"
GROUP BY
    1,
    2,
    3),
vacuous as (
SELECT
    "juicy"."st_s_cid" as "st_s_cid",
    "juicy"."st_s_fn" as "st_s_fn",
    "juicy"."st_s_ln" as "st_s_ln",
    sum("juicy"."_virt_filter_st_tot_3577771660166312") as "_virt_agg_sum_9400619314901898",
    sum("juicy"."_virt_filter_st_tot_9882851076675156") as "_virt_agg_sum_8547237941805449"
FROM
    "juicy"
GROUP BY
    1,
    2,
    3),
divergent as (
SELECT
    "kaput"."_virt_agg_sum_4137858187504676" as "_virt_agg_sum_4137858187504676",
    "kaput"."_virt_agg_sum_9853764948188113" as "_virt_agg_sum_9853764948188113",
    "kaput"."st_s_cid" as "customer_code",
    "kaput"."st_s_cid" as "st_s_cid",
    "kaput"."st_s_fn" as "first_name_out",
    "kaput"."st_s_fn" as "st_s_fn",
    "kaput"."st_s_ln" as "last_name_out",
    "kaput"."st_s_ln" as "st_s_ln"
FROM
    "kaput"),
busy as (
SELECT
    "divergent"."_virt_agg_sum_4137858187504676" as "_virt_agg_sum_4137858187504676",
    "divergent"."_virt_agg_sum_9853764948188113" as "_virt_agg_sum_9853764948188113",
    "vacuous"."_virt_agg_sum_8547237941805449" as "_virt_agg_sum_8547237941805449",
    "vacuous"."_virt_agg_sum_9400619314901898" as "_virt_agg_sum_9400619314901898",
    coalesce("divergent"."customer_code","divergent"."st_s_cid") as "customer_code",
    coalesce("divergent"."first_name_out","divergent"."st_s_fn") as "first_name_out",
    coalesce("divergent"."last_name_out","divergent"."st_s_ln") as "last_name_out"
FROM
    "divergent"
    LEFT OUTER JOIN "vacuous" on "divergent"."customer_code" = "vacuous"."st_s_cid" AND "divergent"."first_name_out" = "vacuous"."st_s_fn" AND "divergent"."last_name_out" = "vacuous"."st_s_ln")
SELECT
    "busy"."customer_code" as "customer_code",
    "busy"."first_name_out" as "first_name_out",
    "busy"."last_name_out" as "last_name_out"
FROM
    "busy"
WHERE
    "busy"."_virt_agg_sum_9400619314901898" > 0 and "busy"."_virt_agg_sum_9853764948188113" > 0 and "busy"."_virt_agg_sum_8547237941805449" is not null and "busy"."_virt_agg_sum_4137858187504676" is not null and ( "busy"."_virt_agg_sum_4137858187504676" / "busy"."_virt_agg_sum_9853764948188113" ) > ( "busy"."_virt_agg_sum_8547237941805449" / "busy"."_virt_agg_sum_9400619314901898" )

ORDER BY 
    "busy"."customer_code" asc nulls first
---- ROWS ----
('A', 'John', 'Doe')
('B', 'John', 'Doe')
('C', 'Jane', 'Smith')
