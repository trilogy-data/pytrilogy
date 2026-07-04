# q67 token sink — RecursionError on a named window concept partitioned by a `grouping()`-derived concept

Run: `evals/tpcds_agent/results/20260703-212501/` — q67 burned **1,360,195 tokens** and FAILED
(final submitted query runs but `result set differs from reference`). Prior runs: ~174–272K.

## Symptom (what the agent saw)

The agent authored q67 the idiomatic way — naming the pieces as `auto` concepts:

```
auto summed_sales <- sum(coalesce(ss.ext_sales_price, 0));
auto cat_for_rank <- case when grouping(ss.item.category) = 1 then '~~GRAND_TOTAL~~'
                     else coalesce(ss.item.category, '~~NULL_CATEGORY~~') end;
auto rnk <- rank(ss.item.class, ...) over (partition by cat_for_rank order by summed_sales desc);
select ... rnk as within_category_rank
having within_category_rank <= 100
by rollup (ss.item.category, ss.item.class, ...);
```

Every run of this shape returned the opaque **`Resolution error ... query could not be
planned; this is a bug.`** (the CLI wrapper for an uncaught `RecursionError`). The agent hit
it ~5 times, then pivoted to a rowset workaround that tripped a *second* framework error
(`Binder Error: GROUPING statement cannot be used without groups`), thrashed, and finally
settled for an inline-partition version that runs but is semantically wrong (100 rows, fails).

## Reproductions (self-contained, `import raw.store_sales as ss;`)

### Bug #1 — RecursionError (the actual token sink). Minimal body:

```
auto part <- case when grouping(ss.item.category)=1 then 'GT' else ss.item.category end;
auto rnk  <- rank(ss.item.class) over (partition by part order by sum(ss.quantity) desc);
select ss.item.category, ss.item.class, rnk as r
by rollup (ss.item.category, ss.item.class);
```
→ `RecursionError: Recursion error building concept local.r with grain
Grain<local.part, ss.item.class> and lineage alias(ref:local.rnk).`

### Bug #2 — codegen (secondary, hit during the workaround):

```
with rd as where ss.date.year=2000
select ss.item.category, ss.item.class, sum(coalesce(ss.ext_sales_price,0)) as summed_sales
by rollup (ss.item.category, ss.item.class);
auto g_cat <- grouping(rd.category);
select rd.category, rd.class, rd.summed_sales, g_cat as gc limit 50;
```
→ `(_duckdb.BinderException) Binder Error: GROUPING statement cannot be used without groups`.
`grouping()` over a rowset output in a *downstream select with no rollup clause* renders a
groupless `SELECT grouping(...)`. Should be a clean trilogy validation error (cf. the existing
q70 WHERE-grouping guard), not raw DuckDB. Not the sink, but reinforced the agent's confusion.

## Trigger matrix — Bug #1 (toggle one ingredient; `[X]` = recurses, `[.]` = OK)

| # | named `auto rnk` | rank key arg `rank(k)` | partition = grouping-derived named concept | consumed via alias `rnk as r` | result |
|---|---|---|---|---|---|
| canon | — bare `rank()`, inline partition, `by rollup ()` | | | | `[.]` 100 rows |
| A | yes | no | no (raw col) | yes | `[.]` |
| B | yes | no | yes | yes | `[.]` |
| C | yes | no | no (raw col) | yes | `[.]` |
| D1 | no (inline window) | yes | yes | — | `[.]` |
| D2 | yes | yes | **no (raw col)** | yes | `[.]` |
| D3 | yes | **no (bare)** | yes | yes | `[.]` |
| M4 | yes | yes | yes | **no (direct, not aliased)** | `[.]` |
| M2 | yes | yes | partition = bare `grouping()` (no case wrapper) | yes | `[.]` |
| M3 | yes | yes | plain case, **no grouping()** | yes | `[.]` |
| N1 | yes | yes | grouping case **inlined** into `partition by` (no named `part`) | yes | `[.]` |
| **M1/D4/D5/D6** | **yes** | **yes** | **yes (named concept whose lineage contains grouping())** | **yes** | **`[X]` recurses** |

Recursion fires ONLY with the full conjunction: a **named** window concept `rnk` +
an **explicit rank key arg** + `partition by` a **named** concept whose lineage wraps
`grouping()` + `rnk` **consumed through an alias**. It does NOT require HAVING, and does
NOT even require the rollup clause (D5 recurses without it) — it is purely a grain-resolution
loop, independent of the rank filter. Inlining the grouping-case directly into `partition by`
(N1, and the canonical query) sidesteps it entirely.

## Root cause — `trilogy/parsing/common.py`, `concepts_to_grain_concepts_ordered`

The per-concept grain classifier has three branches:
- **line 725–734 (ALIAS):** an aliased concept contributes its *source* to the grain —
  `preconcepts.append(source ...)`. For `rnk as r` the source is the WINDOW concept `rnk`,
  appended **as-is**.
- **line 735–744 (WINDOW):** a directly-referenced window contributes its *keys* (partition +
  anchor) instead of itself, to avoid the window's `order by sum()` being grouped by the
  window output (a build recursion).
- **line 745–764 (BASIC-wrapping-window + `_references_grouping`):** the q70 guard, which
  demotes a grouping()-referencing CASE-over-window to the nested window's keys.

The gap: when a window is consumed **through an alias**, the ALIAS branch appends the source
window directly and **never routes it through the WINDOW-key substitution** (735) nor the
grouping guard (745). Its partition key `part` (a `grouping()`-derived BASIC) then enters the
grain as a plain key. Building `part` requires the rollup select grain (grouping() resolves its
`by` to the whole select), which contains the very window that partitions by `part` → grain
self-reference → `RecursionError`. The two existing guards only cover grouping() on the window
*output*; grouping() reachable via a window **partition-key concept** consumed via an alias is
unguarded. (Fix direction, for a later session: in the ALIAS branch, if the source is a WINDOW
— or the BASIC-wrapping-window / grouping case — recurse it through the same substitution
rather than appending it raw.)

## Regression?

**Not from the newest join refactor.** The offending ALIAS/WINDOW grain branches were reshaped
in `beb04190ac [Feat] Joins` (2026-06-08); the grouping guard is `489ffe7e5e` (2026-06-29). The
latest suspects (`dcc62ed78 union_checkpoint`, `956e7303b hacky_joins`, `5f5c956e2`) touch
join_resolution / group_node, **not** this grain code. The q67 spike is therefore best read as:
a genuine, pre-existing grain-recursion **gap** in the q70 grouping-partition guard family that
this run's agent walked into by choosing the named-concept authoring style (multi-arg
`rank(...)` + named grouping partition + aliased rank). Prior runs stayed cheap because the
agent happened to author the inline-partition / bare-rank form (like the canonical) that dodges
the conjunction. The framework obstacle is real: a reasonable construct is rejected with an
opaque "this is a bug", giving the agent no actionable signal.

## Canonical reference — builds & matches on current engine

`tests/modeling/tpc_ds_duckdb/query67.preql` uses `by rollup ()` (empty), a bare
`rank() over (partition by ss.item.category order by sumsales desc)`, HAVING before the rollup
clause. Confirmed it builds and returns 100 rows on this workspace engine (grand-total rows have
NULL dims). It avoids the bug precisely because it uses neither a named grouping-partition
concept nor a multi-arg named-and-aliased rank.

## Classification

- Bug #1: **real framework bug** (uncaught RecursionError → opaque "this is a bug"). The token sink.
- Bug #2: **validation/codegen gap** (grouping() outside a rollup select → raw DuckDB binder error; should be a clean trilogy error). Secondary.
