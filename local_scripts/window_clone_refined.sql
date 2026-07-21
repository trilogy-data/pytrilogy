
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
thoughtful as (
SELECT
    "orders"."o" as "order_id"
FROM
    "orders"
WHERE
    exists (select 1 from cheerful where cheerful."filtered" is not distinct from "orders"."o")
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