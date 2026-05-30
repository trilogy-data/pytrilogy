# Engine bug handoff: recursion / stack-overflow building cross-model additive aggregates with a multi-key `by` grain

## Summary

Certain valid queries crash the engine at **concept-build time** (before SQL
generation) with either:

- `Unexpected error: Recursion error building concept <name> with grain <Grain<...>> and lineage add(sum(...),sum(...)). This is likely due to a circular reference.` (Trilogy's own recursion guard), or
- `Unexpected error: maximum recursion depth exceeded` (Python `RecursionError`, same root cause hitting the interpreter limit first).

This was the single most common failure in the TPC-DS agent eval base leg
(**12 occurrences**, run `20260530-201159`) and also appears in the enriched leg
(`20260530-194955`, e.g. q14). It blocks q02, q04, q14 and is a robustness issue:
the query is well-formed and should either execute or return a clean modeling
error, not stack-overflow.

## Minimal, deterministic repro

Trigger = a derived concept that **adds two aggregates from two different
(merged) models** and is grouped by a **multi-key `by` grain**.

```
merge ws.sold_date.date_sk into ~cs.sold_date.date_sk;
select
  ws.sold_date.week_seq,
  sum(ws.ext_sales_price ? ws.sold_date.dow=0)
    + sum(cs.ext_sales_price ? cs.sold_date.dow=0)
    by ws.sold_date.week_seq, ws.sold_date.dow as sun
where ws.sold_date.week_seq between 5270 and 5272
limit 20;
```

Run against an ingested TPC-DS model (see Setup):

```bash
trilogy run --import raw.web_sales:ws --import raw.catalog_sales:cs "<query above>" duckdb
```

### What narrows it (tested)

Holding everything else fixed and varying only the `by` grain:

| `by` grain | result |
|---|---|
| `by ws.sold_date.week_seq` (single key) | ✅ runs (3 rows) |
| `by (ws.sold_date.week_seq - 53)` (single expr) | ✅ runs (3 rows) |
| `by ws.sold_date.week_seq, ws.sold_date.dow` (two keys) | ❌ recursion |
| `by ((ws.sold_date.week_seq - 53), ws.sold_date.dow)` (expr + key) | ❌ recursion |

So the trigger is a **multi-key grain on a cross-model additive aggregate**, not
the arithmetic expression. A single-model additive aggregate, or a single-key
grain, resolves fine. (Not yet tested: whether a multi-key grain recurses for a
*single*-model additive aggregate, or for a non-additive cross-model aggregate —
worth checking to fully bound the trigger.)

### Smoking gun: spurious inferred grain

The reported grain references concepts **not in the query at all**:

```
Recursion error building concept local.sun_02 with grain
Grain<cs.net_paid_inc_ship_tax, cs.sold_date.date_sk, ws.net_paid_inc_ship>
and lineage add(sum(<Filter: ref:ws.ext_sales_price ...>),
                sum(<Filter: ref:cs.ext_sales_price ...>))
```

`net_paid_inc_ship_tax` / `net_paid_inc_ship` are never referenced. The grain
computed for the `add(sum(A_from_ws), sum(B_from_cs))` node appears to pull in
unrelated concepts from both merged models, and resolving that bogus grain loops.
Strong hypothesis: **grain inference for an additive combination of aggregates
sourced from two different merged models is wrong**, and the wrong grain feeds a
circular build dependency.

## Where to look

- Grain computation for a binary op (`add`) whose two sides are aggregates with
  their own `by` grains over *different* model namespaces — likely in
  `Grain.from_concepts` / the aggregate-grain merge logic, and the concept-build
  recursion guard that emits "Recursion error building concept ... with grain".
- The fix should make the inferred grain the (deduplicated) union of the
  explicit `by` keys, not pull in source-model output concepts like
  `net_paid_inc_ship_tax`.
- Add the recursion guard's "with grain <X> and lineage <Y>" message as a
  regression assertion once fixed, plus a passing test for the minimal repro.

## Affected eval queries

- **q02** (`20260530-201159`): day-of-week pivot — `sum(ws...) + sum(cs...) by (week_seq, dow)` across merged web+catalog sales. Direct hit.
- **q04** (`20260530-201159`): multi-channel customer year-totals across 3 merged models; intermediate drafts hit `maximum recursion depth exceeded` (the final on-disk `query04.preql` happens to run — the recursion was on earlier drafts, so reproduce from the conversation log, not the saved file).
- **q14** (`20260530-194955`, enriched): brand/class/category across 3 channels.

## Setup (getting a model to repro against)

The repro needs the ingested TPC-DS model (`raw/web_sales.preql`,
`raw/catalog_sales.preql`, etc.) and a DuckDB with TPC-DS data. Easiest paths:

1. Reuse an existing eval workspace if still present:
   `evals/tpcds_agent/results/20260530-201159/workspace/` (has `raw/` + `tpcds.duckdb` + `trilogy.toml`). `cd` there and run the repro command.
2. Or regenerate: build a TPC-DS DuckDB (`evals/common/db.py` / the eval harness at sf=0.1), then `trilogy ingest --all` to populate `raw/`.

The recursion is at build/resolution time, so it triggers regardless of data
volume; any populated TPC-DS schema works.

- Observed on `trilogy 0.3.275`.
- The error surfaces as the agent-tool message `Recursion error building concept ...` and, on the raw CLI, the same or `maximum recursion depth exceeded`.
