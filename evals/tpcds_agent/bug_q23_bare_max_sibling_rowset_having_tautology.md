# Bug: WHERE/HAVING inconsistency for a bare aggregate over a rowset column — the WHERE `_degenerate_aggregate_cograin` exception deviates from grain-inheritance intent

**Status:** FIXED 2026-07-06 · **Class:** semantic inconsistency (silent) · **Found:** 2026-07-06 via TPC-DS q23
(enriched leg, run `20260706-115356`, 3.7M-token sink; #1 by token spend).
**Resolution:** `_degenerate_aggregate_cograin` + `_rowset_derived` and the resolution-grain
override removed from `trilogy/core/models/build.py`; a bare aggregate now co-grains uniformly
per grain inheritance in WHERE and HAVING alike.
`tests/test_where_scalar_aggregate_degenerate_cograin.py` rewritten to pin the co-grain intent
(degenerate WHERE/HAVING tautology cases return all rows; `by *` and the grained-select-output
form pin global). Syntax [213] now says to move `by` outside the aggregate (`max(x) by *`).

## Language intent (owner-stated, 2026-07-06)

A bare aggregate (**no `by`**) is **grain-polymorphic**: it co-grains to the consuming/select
grain ("grain inheritance"). A named aggregate **with** a grain (`by *`, `by k`) is **pinned** to
that grain. Consequences that follow directly from the rule:

- `auto g <- max(rs.x)` referenced in a select/filter grained by `k` ⇒ `g` = `max(rs.x)` **grouped
  by `k`**. If `rs.x` is already at grain `k`, each group holds one row, so `max(rs.x)==rs.x` and a
  gate `rs.x > 0.5*g` collapses to `rs.x > 0.5*rs.x` — **true for every positive row**. That
  tautology is the *correct application of the rule*, **not a bug**. To take a global reduction the
  author must write `max(rs.x) by *`.
- Standalone `select g` returns the global max only because the query's grain is global — same rule,
  no special case.
- **Aggregate outputs of a SELECT are always grained** (to that select's grain), so they do NOT
  re-grain when referenced elsewhere. The canonical `query23.preql` is correct for this reason — it
  wraps the max as a grained output, `rowset max_total <- select max(customer_total_in_window) as
  cmax` (grain = global, no grouping key) — not because of any special-casing. Referencing
  `max_total.cmax` in another rowset's WHERE **or** HAVING yields the global value today, no
  exception involved (verified: both = 1687).

So the previously-suspected "HAVING mis-codegens" framing is WRONG: **HAVING is behaving to intent.**

The two correct authorings for a global reduction are therefore: `max(rs.x) by *` (pinned grain), or
the grained-select-output form `rowset m <- select max(rs.x) as mx; ... > 0.5*m.mx`. The agent's
error was the third, polymorphic form `auto g <- max(rs.x)` (no `by`, not a select output).

## The actual defect: WHERE deviates

An earlier q23 fix (memory `project_q23_degenerate_where_cograin_scalar_max`) added
`_degenerate_aggregate_cograin`, which — **only on the WHERE/filter path** — detects "all aggregate
inputs are rowset columns functionally determined by the grain" and silently resolves the aggregate
as a **global single-row scalar** instead of co-graining. That special-cases away the grain-
inheritance rule. Result: the *same* construct answers differently by clause.

## Trigger matrix (model = customer_totals rowset at cust_id grain; "co-grain" answer = 90858 of ~90858 custs; "global" answer = 1687)

| # | form | today | per intent |
|---|---|---|---|
| 1 | bare `max`, sibling-rowset **HAVING** | 90858 (co-grain) | ✅ correct (co-grain) |
| 2 | `max ... by *`, either clause | 1687 (global) | ✅ correct (pinned global) |
| 3 | bare `max`, sibling-rowset/flat **WHERE** | **1687 (global)** | ❌ **should be 90858 (co-grain)** — exception fires |
| 4 | bare `max`, coarser select grain (per-year) | per-group max | ✅ correct (meaningful co-grain, exception does not fire) |

Repro harness (any TPC-DS workspace):
```python
import sys; sys.path.insert(0,'evals'); from pathlib import Path; from common import scoring
ws=Path('.../workspace'); eng=scoring.make_scoring_engine(ws/'tpcds.duckdb', ws, 'tpcds')
rows=lambda b: list(eng.execute_raw_sql(eng.generate_sql(b)[-1]).fetchall())
head="""import raw.store_sales as ss;
rowset ct <- where ss.customer.id is not null
select ss.customer.id as cust_id, sum(ss.quantity*ss.sales_price) as lifetime_total;"""
W = head+"\nauto g<-max(ct.lifetime_total);\nrowset b<-where ct.lifetime_total>0.5*g select ct.cust_id as cid;\nselect b.cid;"
H = head+"\nauto g<-max(ct.lifetime_total);\nrowset b<-select ct.cust_id as cid having ct.lifetime_total>0.5*g;\nselect b.cid;"
len({r[0] for r in rows(W)})  # 1687 today (exception)  -> should be 90858 per intent
len({r[0] for r in rows(H)})  # 90858 today             -> already correct per intent
```

## Root cause (file:line)

`build.py:2988` — `if base.is_aggregate and self._degenerate_aggregate_cograin(base, resolution_grain): resolution_grain = Grain()`. This clause forces the global grain for the degenerate WHERE case.
`_degenerate_aggregate_cograin` (L2854) is gated to `self.aggregate_grain is not None` (L2866), i.e.
the WHERE/filter co-grain factory only, which is exactly why WHERE diverges and HAVING does not.

## Fix direction

Remove the L2988 clause (and, if unused elsewhere, `_degenerate_aggregate_cograin` + its helper
uses) so a bare aggregate co-grains uniformly per grain inheritance in WHERE and HAVING alike.
Verify the non-degenerate co-grain paths are untouched (they already co-grain correctly — case 4 /
`NON_DEGENERATE*` tests) and only the degenerate WHERE assertions change.

**Fix scope is narrow — only the bare-polymorphic path.** Verified against the run workspace:
the grained aggregate-select-output form (`rowset mx <- select max(ct.lifetime_total) as cmax; ...
> 0.5*mx.cmax`) already returns the correct global (1687) in **both** WHERE and HAVING today,
because that output is grained and does not re-grain. Removing the exception must NOT change it —
it never went through `_degenerate_aggregate_cograin`. Only `auto g <- max(rs.x)` (bare, no `by`,
not a select output) is affected, and it *should* change (WHERE 1687→90858 to match HAVING).

## Consequences the implementer MUST handle

1. **Guard test inverts.** `tests/test_where_scalar_aggregate_degenerate_cograin.py` asserts the
   *global-scalar* behavior (its docstring: "must instead resolve as a global single-row scalar").
   Under the corrected intent its degenerate cases must flip: `test_where_scalar_max_top_level`,
   `_wrapped_rowset`, `_inline` change from `[(2,)]` to **all rows** `[(1,),(2,),(3,)]`; the
   NON_DEGENERATE / `SCALAR_ALONE` cases stay. Rewrite (don't delete) the file to encode the
   co-grain intent + a `by *` case that yields the global answer.
2. **q23 authoring changes.** The natural bare `max(customer_totals.lifetime_total)` becomes a
   silent tautology (returns every customer). The correct enriched `.preql` / agent answer must use
   `max(customer_totals.lifetime_total) by *`. Update any q23 candidate/reference relying on the
   bare form, and consider agent-facing guidance that a global reduction needs `by *`.
3. **Check coupled follow-ups.** commit `e48aef371` (applier-required validation) + the ROWSET
   injection gate in `_resolve_condition_disposition` were landed alongside the exception — confirm
   they are independent of the global-scalar collapse before removing it.

## Open sub-question for owner (silent footgun)

Per intent, `rs.x > 0.5*max(rs.x)` at the same grain **silently returns every row**. Intent treats
that as author error (use `by *`). Optional hardening: a lint/warning (not an error) when a bare
aggregate's inputs are provably FD-determined by the co-grain — surfaces the no-op without violating
the rule. Not required for the fix; flagged because this shape is a natural mistake and its only
current detector is the token bar (it burned 3.7M on q23).

## Provenance
- Run `evals/tpcds_agent/results/20260706-115356` (q23 FAIL, ref_rows=4 cand_rows=100).
- Trajectory `agent_log.q23.conversation.txt` msgs ~34–36: agent tried `max(x by *)` → `Syntax
  [213]` → misread the message and dropped `by *` (landing on the bare form) instead of moving `by`
  outside the parens. Secondary agent-facing win: `[213]` should say "move `by` outside the
  aggregate: `max(x) by *`" when `by` appears inside an aggregate's args.
- Kin (the exception being reverted): memory `project_q23_degenerate_where_cograin_scalar_max`.
