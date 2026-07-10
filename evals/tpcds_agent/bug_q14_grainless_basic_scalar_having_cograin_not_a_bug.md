# q14 token sink (2,014,440 tok, fail) — NO framework bug; grainless BASIC-over-aggregate co-grains to the select grain (q23 family)

Run: `evals/tpcds_agent/results/20260705-200535` — q14 burned **2,014,440 tokens / 48 calls, SCORED FAIL** (silent empty-ish result).

## Verdict
**NOT a framework bug.** The agent's `overall_avg` threshold is a *bare* (un-pinned)
BASIC-over-aggregates scalar `sum(x?c)/greatest(sum(1?c),1)`. A bare aggregate is
grain-polymorphic: consumed in a `channel`-grained select/HAVING it co-grains to that
grain, so the threshold becomes a **per-channel** average, not the global average the
reference computes. This is the same design as q23
([[project_q23_bare_max_sibling_rowset_having_tautology]]) — the obstacle is agent
authoring against documented grain-polymorphic semantics, not the engine.

The original write-up's "misrouted finer-dim semijoin → silent 0 rows / BinderException"
root cause is **STALE**. It described run 20260705, *before* the 2026-07-06 fixes
([[project_q44_basic_scalar_granularity_having_misroute_fixed]] and the q23 carveout
removal). On the current engine the grainless BASIC co-grains cleanly — no semijoin
misroute, no binder error.

Canonical `tests/modeling/tpc_ds_duckdb/query14.preql` builds and matches reference
100/100 on the current engine (`pytest ...::test_fourteen` passes). It expresses the
global average correctly: `rowset avg_sales <- select avg(...) as average_sales` (a
grained rowset scalar, never re-grains), and gates HAVING at leaf grain inside a
separate rowset before rolling up.

## Current-HEAD behavior (verified against results/20260705-200535 model, sf=0.1)
Minimal reductions of the agent form (no rollup / no membership needed to show the effect):

| case | threshold expression | pop. | rows | generated shape |
|---|---|---|---|---|
| F (agent form) | `sum(x?c)/greatest(sum(1?c),1)` bare | cross (`?1999-2001`) | **3** | `gavg` GROUP BY channel, `total_sales INNER JOIN threshold ON channel=channel` |
| F_min | bare `avg(x)` | same | **3** | `total_sales > avg(x)` co-grained to channel |
| by-* | `sum(x?c) by * / greatest(sum(1?c) by *,1)` | cross | **3** | genuine global scalar, cross-joined `on 1=1` |
| F3 | `sum(x)/greatest(sum(1),1)` bare | same | **0** | degenerate self-reference (see below) |

F/F_min return real rows and the SQL is valid and semantically consistent: the threshold
is computed **per channel** because it was written un-pinned. The agent read the 0-ish
result of its *full* query (with rollup + membership + this per-grain threshold) as
plausibly-empty and stopped — the footgun is the silent co-grain, not a wrong plan.

## Why the agent's full q14 is wrong (authoring, not framework)
TPC-DS q14 compares each `(channel, brand, class, category)` bucket against the **global**
average `avg(quantity*list_price)` over all 1999-2001 sales. The agent's bare threshold
co-grains to the L0 select grain `(channel, brand, class, category)`, so it compares each
bucket against (roughly) its own average — a near-tautology / wrong population. Fix on the
authoring side: pin the threshold global with `by *`, or compute it as a grained rowset
scalar (`rowset avg_sales <- select avg(...) as average_sales`) exactly as the canonical
query does.

## Residual (minor, separate, degenerate) — F3 same-population
When the threshold reuses the *same* aggregate as the compared output at the *same*
population (`sum(x)/greatest(sum(1),1)` with the select outputting `sum(x)`), the planner
sources the threshold's `sum(1)` from the already-channel-collapsed CTE, so `sum(1)`
materializes as literal `1` → HAVING `total_sales > total_sales/1` is always false → 0 rows.
This is a degenerate self-referential construct the agent did **not** write (agent used the
cross-population `?1999-2001` form, case F, which works). It is arguably a double-aggregation
quirk of co-graining an aggregate over an already-aggregated CTE, but it is not the filed
symptom and does not change the verdict. Worth a separate lint/investigation only if it
recurs on a non-degenerate query.

## Classification
guidance / agent — grainless BASIC-over-aggregate correctly co-grains to the consuming
grain (same rule as bare `max`/`sum`); author must pin the global with `by *` or a grained
rowset scalar. Framework unchanged. Possible guidance win below.

## Possible guidance win
Same footgun class as q23: a bare aggregate (or a BASIC wrapping only bare aggregates) used
as a HAVING threshold at the same grain as its own inputs silently dissolves toward
identity / per-group. Candidate hints:
1. **Docs/preamble convention**: "A threshold that should be a single global number
   (an overall average/total/max compared against per-group values) must be pinned with
   `by *` or written as a grainless rowset scalar (`rowset t <- select avg(...) as a`).
   A bare aggregate in HAVING co-grains to the group and becomes a per-group value."
2. **Lint/Syntax hint**: when a bare aggregate (or BASIC-over-bare-aggregates, no `by`) is
   consumed in a HAVING/WHERE whose grain is a superset of (or equal to) the aggregate's
   own input grain, warn that it will co-grain (dissolve toward identity) and suggest
   `by *` or a rowset scalar. Covers q14 + q23 + q30/q81 avg-scope twins.
