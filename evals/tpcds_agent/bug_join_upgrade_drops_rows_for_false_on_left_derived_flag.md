# Bug: `flag = false` / `flag != true` on a LEFT-join-derived boolean silently returns 0 rows (join wrongly upgraded to INNER)

**Status:** FIXED 2026-06-07 (found via enriched eval q78; root cause of the `is_returned` agent thrash).
Fix: `trilogy/core/optimizations/join_upgrade.py` — `_opaque_binding_addresses` excludes concepts
whose datasource column binding is a `RawColumnExpr`/null-opaque function (structurally non-null on
unmatched rows) from the side-forcing proof in `_downgrade`/`_downgrade_base_join`. The non-null
proof itself was correct; the bug was inferring "concept non-null ⟹ join side matched" for a
CASE-derived flag. Verified e2e on TPC-DS sf=1: `is_returned = false` 0→11859 (== `is not true`).
Tests: `test_raw_derived_flag_eq_false_keeps_left_outer` / `test_plain_column_flag_eq_false_still_upgrades`.
Note: `= true` now also stays LEFT OUTER (correct results; sound de-opt — can't analyze raw CASE to
keep INNER). The `is not true` idiom remains the documented "not returned" form.
**Severity:** high — **silent wrong results, no error.** A filter that should select the
"flag is false" population returns 0 rows because the LEFT join that *produces* the false value is
upgraded to INNER, dropping exactly those rows. `is_returned` is load-bearing across the return-flag
queries (q76, q78, q83…), so this recurs and is impossible for an agent to diagnose.

## Symptom (verified on `raw.web_sales`)

`is_returned` is derived from a web_returns column:
`raw('''CASE WHEN WR_ORDER_NUMBER IS NOT NULL THEN 1 else 0 END''')` (a LEFT-joined column — NULL for
the not-returned rows). Filtering by it:

| Trilogy filter | join emitted to `web_returns` | rows | correct? |
|---|---|---|---|
| `is_returned = true`     | INNER | 7137 | ✓ (want the matched rows) |
| `is_returned is true`    | INNER | 7137 | ✓ |
| `is_returned = false`    | **INNER** | **0** | ✗ (should be 9229) |
| `is_returned != true`    | **INNER** | **0** | ✗ (should be 9229) |
| `is_returned is not true`| LEFT OUTER | 9229 | ✓ |

The not-returned rows have NO web_returns match. The LEFT→INNER **join upgrade** fires for *every*
comparison on `is_returned`, but it is only sound for `= true`/`is true` (which genuinely require the
match to exist). For `= false`/`!= true`, the predicate is satisfied **exactly by the unmatched
(LEFT-null) rows** — so promoting to INNER drops the entire result set → 0 rows, no error.

## Root cause

The LEFT→INNER join-upgrade gate treats any equality/inequality on a flag whose lineage flows through
a LEFT join as "proves the join key non-null → safe to INNER-join." That holds for `IS TRUE`/`= true`
but is **inverted** for `= false`/`!= true`/`<> true`: those predicates are TRUE on the null-derived
side, so the join must stay LEFT. (Cf. `project_join_upgrade_partial_key` / the "`X IS True` proves
non-null" gate — this is the missing dual: `X = false` / `X is not true` must NOT promote.)

## Deterministic reproduction

`raw.web_sales` workspace model (`evals/tpcds_agent/results/20260607-133609/workspace`, or any
ingested TPC-DS web model where `is_returned` derives from `WR_ORDER_NUMBER`):
```trilogy
import raw.web_sales as web;
where web.date.year = 2000 and web.is_returned = false
select web.item.id, count(web.order_number) as n;
```
→ 0 rows (wrong). The generated SQL `INNER JOIN "web_returns" ... WHERE ... CASE WHEN WR_ORDER_NUMBER
IS NOT NULL THEN 1 ELSE 0 END = False`. Swapping the filter to `is not true` emits `LEFT OUTER JOIN`
and returns 9229. Diff the two generated SQLs to see the join-type flip — that is the whole bug.

NOTE: this is NOT the integer-vs-bool cast (a separate red herring — `CASE…1/0` mapped to a `bool`
concept compares fine; `0 = False` is `true` in DuckDB). Changing the flag to `THEN True else False`
does NOT fix this — the join upgrade still fires and still drops the rows.

## Suggested fix

In the LEFT→INNER upgrade gate: a predicate on a LEFT-derived nullable flag may only promote the join
when it is satisfiable ONLY by the matched side (`IS TRUE`, `= true`). A predicate that is satisfiable
by the null/unmatched side (`= false`, `!= true`, `<> true`, `IS NOT TRUE`, `IS NULL`) must keep the
join LEFT. Equivalently: only `IS TRUE`-class predicates prove the join non-null; the negative/false
class proves the opposite and must block the upgrade.

## Agentic impact

This is the root of the q78 `is_returned` thrash (~40 turns). The agent tried `!= true`, `= false`,
`is null`, `is not true` and got mutually inconsistent results (0 / 0 / 0 / 9229) with no error — it
correctly called it *"strange behavior"* but had no way to know the join silently changed. Until
fixed, the only working "not returned" idioms are `is not true` (false-or-null) and, on a true
True/NULL flag, `is null`; document that, but the real fix is the join-upgrade gate.
