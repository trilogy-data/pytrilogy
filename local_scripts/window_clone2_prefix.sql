
WITH 
highfalutin as (
SELECT
    unnest(:_virt_4116103647639195) as "nums"
),
wakeful as (
SELECT
    lag("highfalutin"."nums") over (order by "highfalutin"."nums" asc ) as "filtered"
FROM
    "highfalutin"),
cheerful as (
SELECT
    "wakeful"."filtered" as "filtered"
FROM
    "wakeful"
GROUP BY
    1),
quizzical as (
SELECT
    "orders"."revenue" as "revenue"
FROM
    (
select 1 order_id, 1 store_id, 1 product_id, DATETIME  '1992-09-20 11:30:00.123456789' order_timestamp, 10.00 revenue
union all
select 2, 1, 2, DATETIME   '1992-09-20 11:30:00.123456789', 10.0
union all
select 3, 2, 1, DATETIME   '1992-09-20 11:30:00.123456789', 5.0
union all
select 4, 2, 2, DATETIME   '1992-09-20 11:30:00.123456789', 5.0
) as "orders"
WHERE
    exists (select 1 from cheerful where cheerful."filtered" is not distinct from "orders"."order_id")
),
thoughtful as (
SELECT
    "orders"."order_id" as "order_id"
FROM
    (
select 1 order_id, 1 store_id, 1 product_id, DATETIME  '1992-09-20 11:30:00.123456789' order_timestamp, 10.00 revenue
union all
select 2, 1, 2, DATETIME   '1992-09-20 11:30:00.123456789', 10.0
union all
select 3, 2, 1, DATETIME   '1992-09-20 11:30:00.123456789', 5.0
union all
select 4, 2, 2, DATETIME   '1992-09-20 11:30:00.123456789', 5.0
) as "orders"
WHERE
    exists (select 1 from cheerful where cheerful."filtered" is not distinct from "orders"."order_id")
),
uneven as (
SELECT
    "thoughtful"."order_id" as "order_id",
    unnest(:_virt_4116103647639195) as "nums"
FROM
    "thoughtful"),
cooperative as (
SELECT
    unnest(:_virt_4116103647639195) as "nums"
FROM
    "thoughtful"),
questionable as (
SELECT
    "cooperative"."nums" as "nums"
FROM
    "cooperative"
GROUP BY
    1),
abundant as (
SELECT
    "questionable"."nums" as "nums",
    lag("questionable"."nums") over (order by "questionable"."nums" asc ) as "filtered"
FROM
    "questionable")
SELECT
    "abundant"."filtered" as "filtered",
    "uneven"."order_id" as "order_id"
FROM
    "uneven"
    INNER JOIN "abundant" on "uneven"."nums" = "abundant"."nums"
GROUP BY
    1,
    2
ORDER BY 
    "abundant"."filtered" asc,
    "uneven"."order_id" asc