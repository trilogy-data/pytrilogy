# Scoped INNER join = set intersection — behavior matrix

**State:** de-collapse (pseudonyms) + WHERE injection + removed the pseudonym exclusion in `generate_candidates_restrictive` + datasource nullability flags.

Intent: `inner join a = b` declares the two concepts identical; the output reflects only the **intersection** of the two sets. NULL matches NULL (`is not distinct from`).

> Source: `tests/test_scoped_inner_join_intersection.py`

## Value-set joins (value lives on the fact) — ALL PASS

left `{1,2,3}` right `{2,3,4}` → `{2,3}`; null variants below.

| query | intended | actual |
|-------|----------|--------|
| `select lval inner join lval = rval;` | `{2,3}` | `[(2,), (3,)]` |
| `select rval inner join lval = rval;` | `{2,3}` | `[(2,), (3,)]` |
| `select lval, rval inner join lval = rval;` | `(2,2),(3,3)` | `[(2, 2), (3, 3)]` |
| `select count(l_id) as n inner join lval = rval;` | `2` | `[(2,)]` |

<details><summary><code>select lval inner join lval = rval;</code> — intended <code>{2,3}</code>, actual <code>[(2,), (3,)]</code></summary>

```sql
WITH 
quizzical as (
SELECT
    "lefts"."v" as "lval"
FROM
    (select 1 id, 1 v union all select 2 id, 2 v union all select 3 id, 3 v) as "lefts"
GROUP BY
    1),
wakeful as (
SELECT
    "rights"."v" as "rval"
FROM
    (select 1 id, 2 v union all select 2 id, 3 v union all select 3 id, 4 v) as "rights")
SELECT
    "quizzical"."lval" as "lval"
FROM
    "wakeful"
    INNER JOIN "quizzical" on "wakeful"."rval" = "quizzical"."lval"
WHERE
    ( "quizzical"."lval" is not distinct from "wakeful"."rval" )

GROUP BY
    1,
    "wakeful"."rval"
```
</details>

<details><summary><code>select rval inner join lval = rval;</code> — intended <code>{2,3}</code>, actual <code>[(2,), (3,)]</code></summary>

```sql
WITH 
quizzical as (
SELECT
    "lefts"."v" as "lval"
FROM
    (select 1 id, 1 v union all select 2 id, 2 v union all select 3 id, 3 v) as "lefts"
GROUP BY
    1),
wakeful as (
SELECT
    "rights"."v" as "rval"
FROM
    (select 1 id, 2 v union all select 2 id, 3 v union all select 3 id, 4 v) as "rights")
SELECT
    "wakeful"."rval" as "rval"
FROM
    "wakeful"
    INNER JOIN "quizzical" on "wakeful"."rval" = "quizzical"."lval"
WHERE
    ( "quizzical"."lval" is not distinct from "wakeful"."rval" )

GROUP BY
    1,
    "quizzical"."lval"
```
</details>

<details><summary><code>select lval, rval inner join lval = rval;</code> — intended <code>(2,2),(3,3)</code>, actual <code>[(2, 2), (3, 3)]</code></summary>

```sql
WITH 
quizzical as (
SELECT
    "lefts"."v" as "lval"
FROM
    (select 1 id, 1 v union all select 2 id, 2 v union all select 3 id, 3 v) as "lefts"
GROUP BY
    1),
wakeful as (
SELECT
    "rights"."v" as "rval"
FROM
    (select 1 id, 2 v union all select 2 id, 3 v union all select 3 id, 4 v) as "rights")
SELECT
    "quizzical"."lval" as "lval",
    "wakeful"."rval" as "rval"
FROM
    "wakeful"
    INNER JOIN "quizzical" on "wakeful"."rval" = "quizzical"."lval"
WHERE
    ( "quizzical"."lval" is not distinct from "wakeful"."rval" )

GROUP BY
    1,
    2
```
</details>

<details><summary><code>select count(l_id) as n inner join lval = rval;</code> — intended <code>2</code>, actual <code>[(2,)]</code></summary>

```sql
WITH 
quizzical as (
SELECT
    "lefts"."id" as "l_id",
    "lefts"."v" as "lval"
FROM
    (select 1 id, 1 v union all select 2 id, 2 v union all select 3 id, 3 v) as "lefts"),
highfalutin as (
SELECT
    "rights"."v" as "rval"
FROM
    (select 1 id, 2 v union all select 2 id, 3 v union all select 3 id, 4 v) as "rights"),
wakeful as (
SELECT
    "quizzical"."l_id" as "l_id"
FROM
    "highfalutin"
    INNER JOIN "quizzical" on "highfalutin"."rval" = "quizzical"."lval"
WHERE
    ( "quizzical"."lval" is not distinct from "highfalutin"."rval" )

GROUP BY
    1)
SELECT
    count("wakeful"."l_id") as "n"
FROM
    "wakeful"
```
</details>

## Value-set — null on BOTH sides — PASS

left `{1,2,NULL}` right `{2,4,NULL}` → `{2,NULL}` (NULL matches NULL). Columns marked `?lval`/`?rval`.

| query | intended | actual |
|-------|----------|--------|
| `select lval inner join lval = rval;` | `{2, NULL}` | `[(2,), (None,)]` |
| `select count(l_id) as n inner join lval = rval;` | `2` | `[(2,)]` |

<details><summary><code>select lval inner join lval = rval;</code> — intended <code>{2, NULL}</code>, actual <code>[(2,), (None,)]</code></summary>

```sql
WITH 
quizzical as (
SELECT
    "lefts"."v" as "lval"
FROM
    (select 1 id, 1 v union all select 2 id, 2 v union all select 3 id, cast(null as int) v) as "lefts"
GROUP BY
    1),
wakeful as (
SELECT
    "rights"."v" as "rval"
FROM
    (select 1 id, 2 v union all select 2 id, 4 v union all select 3 id, cast(null as int) v) as "rights")
SELECT
    "quizzical"."lval" as "lval"
FROM
    "wakeful"
    INNER JOIN "quizzical" on "wakeful"."rval" is not distinct from "quizzical"."lval"
WHERE
    ( "quizzical"."lval" is not distinct from "wakeful"."rval" )

GROUP BY
    1,
    "wakeful"."rval"
```
</details>

<details><summary><code>select count(l_id) as n inner join lval = rval;</code> — intended <code>2</code>, actual <code>[(2,)]</code></summary>

```sql
WITH 
quizzical as (
SELECT
    "lefts"."id" as "l_id",
    "lefts"."v" as "lval"
FROM
    (select 1 id, 1 v union all select 2 id, 2 v union all select 3 id, cast(null as int) v) as "lefts"),
highfalutin as (
SELECT
    "rights"."v" as "rval"
FROM
    (select 1 id, 2 v union all select 2 id, 4 v union all select 3 id, cast(null as int) v) as "rights"),
wakeful as (
SELECT
    "quizzical"."l_id" as "l_id"
FROM
    "highfalutin"
    INNER JOIN "quizzical" on "highfalutin"."rval" is not distinct from "quizzical"."lval"
WHERE
    ( "quizzical"."lval" is not distinct from "highfalutin"."rval" )

GROUP BY
    1)
SELECT
    count("wakeful"."l_id") as "n"
FROM
    "wakeful"
```
</details>

## Value-set — null on ONE side — PASS

left `{1,2,NULL}` right `{2,3,4}` → `{2}` (left NULL has no match).

| query | intended | actual |
|-------|----------|--------|
| `select lval inner join lval = rval;` | `{2}` | `[(2,)]` |
| `select count(l_id) as n inner join lval = rval;` | `1` | `[(1,)]` |

<details><summary><code>select lval inner join lval = rval;</code> — intended <code>{2}</code>, actual <code>[(2,)]</code></summary>

```sql
WITH 
quizzical as (
SELECT
    "lefts"."v" as "lval"
FROM
    (select 1 id, 1 v union all select 2 id, 2 v union all select 3 id, cast(null as int) v) as "lefts"
GROUP BY
    1),
wakeful as (
SELECT
    "rights"."v" as "rval"
FROM
    (select 1 id, 2 v union all select 2 id, 3 v union all select 3 id, 4 v) as "rights")
SELECT
    "quizzical"."lval" as "lval"
FROM
    "quizzical"
    INNER JOIN "wakeful" on "quizzical"."lval" = "wakeful"."rval"
WHERE
    ( "quizzical"."lval" is not distinct from "wakeful"."rval" )

GROUP BY
    1,
    "wakeful"."rval"
```
</details>

<details><summary><code>select count(l_id) as n inner join lval = rval;</code> — intended <code>1</code>, actual <code>[(1,)]</code></summary>

```sql
WITH 
quizzical as (
SELECT
    "lefts"."id" as "l_id",
    "lefts"."v" as "lval"
FROM
    (select 1 id, 1 v union all select 2 id, 2 v union all select 3 id, cast(null as int) v) as "lefts"),
highfalutin as (
SELECT
    "rights"."v" as "rval"
FROM
    (select 1 id, 2 v union all select 2 id, 3 v union all select 3 id, 4 v) as "rights"),
wakeful as (
SELECT
    "quizzical"."l_id" as "l_id"
FROM
    "quizzical"
    INNER JOIN "highfalutin" on "quizzical"."lval" = "highfalutin"."rval"
WHERE
    ( "quizzical"."lval" is not distinct from "highfalutin"."rval" )

GROUP BY
    1)
SELECT
    count("wakeful"."l_id") as "n"
FROM
    "wakeful"
```
</details>

## Property-attribute join, SINGLE key (dim attribute) — PASS

`lweek`/`rweek` are attributes of separate date dims, domains `{1,2,3}` vs `{2,3,4}` → `{2,3}`.

| query | intended | actual |
|-------|----------|--------|
| `select lweek inner join lweek = rweek;` | `{2,3}` | `[(2,), (3,)]` |

<details><summary><code>select lweek inner join lweek = rweek;</code> — intended <code>{2,3}</code>, actual <code>[(2,), (3,)]</code></summary>

```sql
WITH 
quizzical as (
SELECT
    "ldays"."w" as "lweek"
FROM
    (select 1 d, 1 w union all select 2 d, 2 w union all select 3 d, 3 w) as "ldays"
GROUP BY
    1),
wakeful as (
SELECT
    "rdays"."w" as "rweek"
FROM
    (select 2 d, 2 w union all select 3 d, 3 w union all select 4 d, 4 w) as "rdays")
SELECT
    "quizzical"."lweek" as "lweek"
FROM
    "wakeful"
    INNER JOIN "quizzical" on "wakeful"."rweek" = "quizzical"."lweek"
WHERE
    ( "quizzical"."lweek" is not distinct from "wakeful"."rweek" )

GROUP BY
    1,
    "wakeful"."rweek"
```
</details>

## Property-attribute join, TWO keys (item + week) — PASSES (anchored)

cs/inv facts joined on item AND a dim `week`. cs item1 sold wk100 & wk200; inv item1 stocked ONLY wk100. The week key is sourced from the standalone date dim, so cs's own week is overridden and item1/week200 leaks.

| query | intended | actual |
|-------|----------|--------|
| `select cs_item, cs_week, count(cs_id) as n inner join cs_item = inv_item inner join cs_week = inv_week;` | `(1,100,1),(2,100,1)` | `[(1, 100, 1), (2, 100, 1)]` |

<details><summary><code>select cs_item, cs_week, count(cs_id) as n inner join cs_item = inv_item inner join cs_week = inv_week;</code> — intended <code>(1,100,1),(2,100,1)</code>, actual <code>[(1, 100, 1), (2, 100, 1)]</code></summary>

```sql
WITH 
quizzical as (
SELECT
    "cs"."d" as "ld_id",
    "cs"."id" as "cs_id",
    "cs"."it" as "cs_item"
FROM
    (select 1 id, 1 it, 100 d union all select 2 id, 1 it, 200 d union all select 3 id, 2 it, 100 d) as "cs"),
highfalutin as (
SELECT
    "inv"."d" as "rd_id",
    "inv"."it" as "inv_item"
FROM
    (select 10 id, 1 it, 100 d union all select 11 id, 2 it, 100 d) as "inv"),
wakeful as (
SELECT
    "ldays"."d" as "ld_id",
    "ldays"."w" as "cs_week"
FROM
    (select 100 d, 100 w union all select 200 d, 200 w) as "ldays"),
cheerful as (
SELECT
    "rdays"."d" as "rd_id",
    "rdays"."w" as "inv_week"
FROM
    (select 100 d, 100 w union all select 200 d, 200 w) as "rdays"),
thoughtful as (
SELECT
    "cheerful"."inv_week" as "inv_week",
    "highfalutin"."inv_item" as "inv_item",
    "quizzical"."cs_id" as "cs_id",
    "quizzical"."cs_item" as "cs_item",
    "wakeful"."cs_week" as "cs_week"
FROM
    "wakeful"
    INNER JOIN "quizzical" on "wakeful"."ld_id" = "quizzical"."ld_id"
    INNER JOIN "highfalutin" on "quizzical"."cs_item" = "highfalutin"."inv_item"
    INNER JOIN "cheerful" on "highfalutin"."rd_id" = "cheerful"."rd_id"
WHERE
    ( "quizzical"."cs_item" is not distinct from "highfalutin"."inv_item" ) and ( "wakeful"."cs_week" is not distinct from "cheerful"."inv_week" )

GROUP BY
    1,
    2,
    3,
    4,
    5)
SELECT
    "thoughtful"."cs_item" as "cs_item",
    "thoughtful"."cs_week" as "cs_week",
    count("thoughtful"."cs_id") as "n"
FROM
    "thoughtful"
GROUP BY
    1,
    2
```
</details>

## Rowset <-> rowset intersection — FAILS (xfail)

the two sets are ROWSET outputs joined on their output; still routed through the guard/collapse.

| query | intended | actual |
|-------|----------|--------|
| `select both.name order by both.name;` | `['Smith']` | `UnresolvableQueryException: Cannot resolve cross-rowset INNER join store_names.name = cat_names.na` |

<details><summary><code>select both.name order by both.name;</code> — intended <code>['Smith']</code>, actual <code>UnresolvableQueryException: Cannot resolve cross-rowset INNER join store_names.name = cat_names.na</code></summary>

```sql
-- UnresolvableQueryException: Cannot resolve cross-rowset INNER join store_names.name = cat_names.name: it intersects two independent rowsets but the collapse dropped store_names.name, silen
```
</details>
