# Bug: nested membership over a two-source-aggregate filter → uncaught `INVALID_REFERENCE_BUG` sentinel crash (q64)

**Status:** FIXED 2026-06-26 — see "Fix" below. Minimal deterministic repro retained.

## Fix

`add_existence_sources` (`trilogy/core/processing/node_generators/filter_node.py`) silently swallowed
the case where a filter node's existence subquery (a membership RHS like `cat_qual_item`) couldn't be
sourced — it `return None`d but the caller ignored the return and rendered the filter anyway, leaving
the dangling `INVALID_REFERENCE_BUG` existence CTE. It now returns a `bool`, and `gen_filter_node`
returns `None` when sourcing fails, so the search reports the same clean `UnresolvableQueryException`
as the equivalent direct membership. (The two-source agg comparison is genuinely unsourceable —
`sum(cs.*) by cs.item.text_id` vs `sum(cr.*) by cr.item.text_id` are unmergeable grains — so a clean
unresolvable error is the correct outcome; making it *resolvable* is a separate, larger task.)
Regression tests: `tests/modeling/tpc_ds_duckdb/test_non_benchmark_queries.py::test_q64_nested_membership_two_source_agg_clean_error`
(+ `_single_source_agg_compiles` guards against over-bailing).

**Surfaced by:** TPC-DS q64 enriched eval (run `20260626-183016`). Reported by the harness as
`Unexpected error in query64.preql: Invalid reference string found in query` — an **uncaught
`ValueError`**, not a clean user error.
**Severity:** HIGH — internal invariant violation; the renderer emits the `INVALID_REFERENCE_BUG`
sentinel as a CTE name and `strict_mode` raises `ValueError`.

## Symptom

The rendered SQL contains the sentinel where a real CTE alias belongs:

```sql
... THEN "cs_item_items"."I_ITEM_ID" ELSE NULL END
    in (select INVALID_REFERENCE_BUG."cat_qual_item"
        from INVALID_REFERENCE_BUG
        where INVALID_REFERENCE_BUG."cat_qual_item" is not null)
```

The **inner** existence subquery (for `cat_qual_item`) never got a materialized source CTE — its
alias resolved to `BASE_INVALID` (`"INVALID_REFERENCE_BUG"`, `trilogy/dialect/base.py:247`), tripping
the guard at `base.py:2386`.

## Minimal reproducing query (deterministic)

```trilogy
import raw.store_sales as ss;
import raw.catalog_sales as cs;
import raw.catalog_returns as cr;

auto qual_item    <- cs.item.text_id ? cs.item.current_price between 65 and 74;
auto cat_ext_list <- sum(cs.ext_list_price) by cs.item.text_id;      -- agg over cs
auto cat_refund   <- sum(coalesce(cr.refunded_cash, 0)) by cr.item.text_id;  -- agg over cr (DIFFERENT source)
auto cat_qual_item <- cs.item.text_id ? cat_ext_list > cat_refund;   -- filter compares the two-source aggs
auto final_item   <- qual_item ? qual_item in cat_qual_item;         -- NESTED membership

where ss.item.text_id in final_item
select ss.item.text_id, count(ss.line_item) as cnt;
```

## Bisection (what's load-bearing)

| variant | result |
|---|---|
| `where ss.k in cat_qual_item` (DIRECT membership, two-source agg) | clean `UnresolvableQueryException: Could not resolve connections` |
| nested membership, **single-source** agg filter (`cat_ext_list > 100`) | **OK** (compiles) |
| **nested membership + two-source agg filter** (above) | **`ValueError: Invalid reference string` crash** |

So BOTH are required: (1) a **nested** membership — `final_item` is itself a filtered concept whose
condition is `<x> in cat_qual_item`, then consumed via `ss.k in final_item`; AND (2) the inner
`cat_qual_item` is filtered by a comparison of **two aggregates sourced from different facts**
(`sum(cs.…) by item` vs `sum(cr.…) by item`, which must be JOINED to evaluate). The `2 *` multiplier
in the original is irrelevant. The marital filter and the wide dimension-column select are irrelevant
(repro is a 2-column select).

## Likely fix area

The inner membership's existence subquery needs its RHS concept (`cat_qual_item`) materialized as a
CTE before the outer membership references it. When the RHS is a single-source filter it sources
fine; when it requires a **multi-source join** (cs⋈cr to compare the two aggregates) AND it is reached
**one level down** (through `final_item`'s nested `in`), the existence source is never planned, so the
subquery's CTE alias stays `BASE_INVALID`. Inspect existence/`append_existence_check` sourcing for a
membership whose RHS is a join-requiring filtered concept reached transitively through another
membership — likely in `v4_helper/source_planning.py` / `strategy_builder.py` (the
`INVALID_REFERENCE_BUG` dangling-CTE family). At minimum, the direct-membership path's clean
`UnresolvableQueryException` should also cover the nested path instead of crashing.

## Repro harness

```python
import sys; sys.path.insert(0, 'evals')
from common import scoring
from pathlib import Path
ws = Path('evals/tpcds_agent/results/20260626-183016/workspace')
eng = scoring.make_scoring_engine(ws / 'tpcds.duckdb', ws, 'tpcds')
eng.generate_sql(open('repro.preql').read())   # ValueError: Invalid reference string found in query
```
