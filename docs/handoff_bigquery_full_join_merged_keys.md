# Follow-up: merged join keys render as expressions, and BigQuery cannot plan them

Status: **shipped workaround, open follow-up.** The production break is fixed;
what is filed here is the better fix nobody needs today.

## What shipped (2026-08-17)

BigQuery rejects a `FULL OUTER JOIN` whose ON clause it cannot reduce to either
a hash key or a constant, with

    FULL OUTER JOIN cannot be used without a condition that is an equality of
    fields from both sides of the join.

Take that message as a description of the accepted set and you will get the
rule wrong in both directions. It is narrower: a top-level `X = Y` reduces to a
key whatever X and Y are, and a constant reduces to a cross product, so both
are accepted. What is not accepted is the base dialect's null-safe expansion
`(l = r or (l is null and r is null))` — an OR that BigQuery reads back as a
key only while both operands are plain column references, an expression on
either side being enough to lose it. `IS NOT DISTINCT FROM` is rejected
outright.

Both directions of that are load-bearing. A *non-nullable* merged key renders
as the bare `coalesce(a.x, b.x) = c.x` and is accepted, so it is deliberately
left alone (tpc-ds q97 on BigQuery is exactly that shape); a keyless FULL join
renders `on 1=1` and is likewise fine.

Verified against real BigQuery tables (UNNEST/constant *inputs* are not subject
to the restriction, so a scratch repro over `unnest([1,2,3])` passes and proves
nothing) — `tests/engine/bigquery/test_bigquery_full_join_keys_live.py` re-runs
this table:

| ON clause | accepted | null-matches |
| --- | --- | --- |
| `a.x = b.y` | yes | no |
| `(a.x = b.y or (a.x is null and b.y is null))` | yes | yes |
| `(coalesce(a.x, c.x) = b.y or (… is null and b.y is null))` | **no** | — |
| `a.x is not distinct from b.y` | **no** | — |
| `TO_JSON_STRING(coalesce(a.x, c.x)) = TO_JSON_STRING(b.y)` | yes | yes |
| `coalesce(a.x, -1) = coalesce(b.y, -1)` | yes | yes |
| `1=1`, `true` | yes | n/a (cross product) |
| `struct(a.x as v, a.x is null as n) = struct(b.y as v, b.y is null as n)` | yes | **no** |

The struct row is worth keeping: struct equality *is* accepted by the FULL-join
planner, and it is not null-safe, and the obvious repair does not work. BigQuery
compares structs field-wise and a NULL field poisons the comparison to NULL, so
pairing the value with an `IS NULL` boolean changes nothing — `NULL AND TRUE` is
`NULL`. Making it work means keeping the NULL out of the value field
(`struct(coalesce(l, sentinel), …)`), at which point the sentinel is doing the
work, the struct is packaging, and the sentinel needs to be type-appropriate and
provably non-colliding.

So `BigqueryDialect.NULL_WRAPPER` encodes only the keys that would not compile —
FULL join, nullable key, at least one non-field operand — as
`TO_JSON_STRING(l) = TO_JSON_STRING(r)`. Null-safe for free (NULL encodes as
bare `null`, the string `'null'` encodes quoted, so they cannot collide across
types). `NULL_WRAPPER` now takes the join type as a fourth argument for this;
every other dialect ignores it.

Known asymmetry with `=`: encoded FLOAT64s compare textually, so NaN matches
itself and `-0.0` stops matching `0.0`. A float join key is pathological, and
the alternative is a query that does not run.

## How it surfaced

`thelook-daily-sales` on prod, run `c426ac95-a2e9-5675-a1db-6be6299c7550`,
2026-08-17 04:29 UTC:

```
Failed to refresh datasource 'sales_reporting' (33 stale partitions):
BadRequest: 400 FULL OUTER JOIN cannot be used without a condition that is an
equality of fields from both sides of the join.
```

One of the 16 FULL joins in that rebuild was the offender — the one whose key is
merged across three row-preserving sources:

```sql
FULL JOIN `divergent` on (coalesce(`young`.`order_item_order_id`,
  `abhorrent`.`order_item_order_id`, `vacuous`.`order_item_order_id`)
  = `divergent`.`order_item_order_id` or (coalesce(…) is null and … is null))
```

Reproducible with no cloud dependency at all via
`tests/dialect/test_bigquery_full_join_keys.py`, which carries a model small
enough to read. The live test asserts the rejections as well as the
acceptances: a relaxed rule would leave the encoding as pure cost, and the
message's wording is misleading enough that the accepted set is worth pinning
rather than reasoning about. Against the real thing,
`trilogy refresh sales_reporting.preql --dry-run` in
`trilogy-cloud/demo_models/thelook_ecommerce` and a `bq query --dry_run` of the
statement it prints.

The other five thelook jobs were checked with `--force` (they were up to date,
so a plain dry run compiles nothing) and are all field-keyed. `sales_reporting`
was the only one exposed.

## The follow-up

Two things, either of which would make the encoding dead code. Neither is
urgent — the workaround is correct, and it fires on one join in one query.

**1. Hoist a merged key into its producing CTE.** `coalesce(a.x, b.x, c.x)` in
an ON clause is a value the consumer already selects as a column
(`charming.order_item_order_id` is literally that COALESCE, one line above the
join that recomputes it). Projecting it in the parent and joining on the column
would make both sides fields, which fixes this for BigQuery *and* is the shape
every engine plans better — the expression is opaque to clustering and to any
key-ordering the source could have offered. This is the real fix; it is a
planner change, so it wants its own investigation of which CTE is entitled to
materialize the key and whether doing so changes row counts anywhere.

**2. Extend `SimplifyNullSafeJoins` past INNER joins.** The rule strips a
redundant `Modifier.NULLABLE` only on INNER joins, deliberately: an outer align
wants the null-safe form so unmatched NULL keys group together, and nullability
tracking on intermediate projection CTEs under-reports there. But the keys in
this query are grain keys (`order_item.order.id`, `…product.id`) that no branch
can null, and a provably non-null FULL-join key could render plain `=` — no
expansion, no encoding, no question. That needs the nullability proof to hold up
on the FULL path first, which is exactly what the current restriction says it
does not.

Whichever lands, delete the `jointype is JoinType.FULL` branch in
`trilogy/dialect/bigquery.py` and keep the test — it should then assert the
query compiles *without* `TO_JSON_STRING`.
