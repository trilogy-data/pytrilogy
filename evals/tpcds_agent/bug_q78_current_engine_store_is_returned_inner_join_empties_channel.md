# q78 (CURRENT engine, run `20260706-001731`, 2,287,759 tok / FAIL) — strip-notnull fix IS effective; the live driver is the store `is_returned` INNER-join collapsing the never-returned universe → silent 0-row result → agent rationalizes it as correct

Run: `evals/tpcds_agent/results/20260706-001731` (q78). 2,287,759 tokens, FAIL.
Prior run `20260705-200535` was 2,018,366 tok/FAIL — no improvement. The agent's final
`workspace/query78.preql` is a NEW formulation vs the earlier docs: three `with … as`
store/web/catalog rowsets combined with `union join` on (year,item,customer), final
select with ratio/having/order. NOT the `union(...)`-rowset filtered-aggregate form that
`bug_q78_regression_4p8M_strip_notnull_drops_nullable_customer.md` analyzed.

## Verdict: (c) agent thrashing on a SILENT wrong (near-empty) result, rooted in a MODEL defect — NOT the strip-notnull residual (a), NOT a strip-notnull framework bug (b)

### The StripRedundantNotNull fix is EFFECTIVE for this shape — all three `<customer> IS NOT NULL` filters survive

Generated SQL (via `make_scoring_engine` against the run workspace, read-only) keeps every
authored customer filter in its arm CTE:

```
store  (young):       WHERE D_YEAR=2000 and "ss_store_sales"."SS_CUSTOMER_SK" is not null and ...
web    (kaput):       WHERE D_YEAR=2000 and coalesce("...C_CUSTOMER_SK","...WS_BILL_CUSTOMER_SK") is not null and ...
catalog(cooperative): WHERE D_YEAR=2000 and coalesce("...C_CUSTOMER_SK","...CS_BILL_CUSTOMER_SK") is not null and ...
```

Toggling `CONFIG.optimizations.strip_redundant_not_null` True↔False leaves customer filters
at **3/3 both ways** — the pass no longer touches these guards. `_unfiltered_nullable_addresses`
(source-tree walk added by the prior fix) correctly recovers the base-column nullability of
`customer.id`, so the 2026-07-05 fix holds. The output has **zero null-customer rows**. The
earlier smoking gun (silent disappearance of the customer filters → null-customer rows) is
GONE. So this run's 2.3M churn is NOT (a) and NOT (b).

## Live driver: the store arm's `is_returned` INNER-joins store_returns, collapsing 486,964 never-returned groups to 1,853

The store arm renders (unlike web/catalog which LEFT-join their returns table):

```
young as ( ... FROM "store_sales"
  INNER JOIN "store_returns" on SS_ITEM_SK=SR_ITEM_SK and SS_TICKET_NUMBER=SR_TICKET_NUMBER ...
  WHERE D_YEAR=2000 and SS_CUSTOMER_SK is not null and SR_RETURN_TIME_SK IS NOT NULL != True )
```

`INNER JOIN store_returns` keeps only store lines that HAVE a matching return row, then
`SR_RETURN_TIME_SK IS NOT NULL != True` keeps the sliver whose return-time is NULL. This is
the exact inverse of "never returned." Measured against the run DB:

- correct never-returned store (year,item,customer) groups for 2000: **486,964**
- agent's INNER-join store arm returns: **1,853** (~263x undercount)
- web arm: 128,097 groups; catalog similar.

The 1,853 distorted store groups do not overlap the web/catalog universes, so the final
`having other_qty > 0` join yields **0 rows**. The agent executes cleanly (~400ms, no error,
no hang) and — with no failure signal — explicitly rationalizes the empty result as correct
(conversation msgs 6003 / 6009: "The query produces 0 rows — this is the correct answer …
no customer bought the same item both from a store and from web/catalog"). It spent the
whole budget A/B-testing formulations against a silently wrong empty result.

## Root cause (file:line) — MODEL asymmetry, faithfully compiled

`tests/modeling/tpc_ds_duckdb/store_sales.preql:91` (identical in the run's
`workspace/raw/store_sales.preql:91`):

```
raw('''SR_RETURN_TIME_SK IS NOT NULL'''): is_returned,   # bound ON the store_returns datasource, NON-nullable
```

`is_returned` is sourceable ONLY from `store_returns` and is non-nullable there, so any
query needing it — including the negative filter `ss.is_returned != true` — forces an INNER
join to `store_returns`, which structurally drops every never-returned store line.

Contrast `web_sales.preql:47/55/101` (and catalog analogously), which bind a **nullable
padded FK on the SALES side** and derive the flag from it:

```
_returned_order_number int?,                       # line 47: nullable, on web_sales
auto is_returned <- _returned_order_number is not null;   # line 55; comment: '"not returned" works as = false / is not true / is null'
WR_ORDER_NUMBER: _returned_order_number,           # line 101: NULL-padded by the LEFT join
```

That pattern LEFT-joins the returns table and evaluates the flag on padded (NULL) rows →
`is_returned = False` for non-returned lines, so `!= true` correctly keeps them. The store
model never got this treatment; its `is_returned` cannot express "not returned." Canonical
`query78.preql` sidesteps it entirely via the unified `all_sales` model
(`LEFT OUTER JOIN cheerful … WHERE sales_is_returned is null`); canonical generates valid
never-returned SQL (couldn't execute here only due to a `memory.*`-schema address mismatch
between the canonical models and this run's raw-ingested DB — unrelated to the verdict).

## Classification

**(c) agent thrashing, driven by a MODEL defect (store `is_returned` modeling), NOT a
strip-notnull residual and NOT a strip-notnull framework bug.** The framework compiles both
models faithfully; the store/web asymmetry is the fault. Severity high for the agent budget:
a silently near-empty channel with no error is exactly what denies a correction cue.

Fix options (do NOT apply per task): re-model store `is_returned` like web/catalog — a
nullable padded FK (`_returned_ticket_number int?`) on `store_sales` backing
`auto is_returned <- _returned_ticket_number is not null` — so `!= true` LEFT-joins and
keeps never-returned rows. Secondary/optional: consider whether the framework should refuse
or warn when a non-nullable datasource-only boolean is used in a negating filter (it can
never be false), rather than silently INNER-joining.
