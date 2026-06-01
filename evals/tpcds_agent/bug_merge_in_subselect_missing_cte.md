# Engine bug handoff: `merge` + `in`-subselect on the merged key generates SQL referencing a non-existent CTE (DuckDB `Binder Error: Referenced table ... not found`)

> **ROOT-CAUSED, FIX PENDING SIGN-OFF** (follow-up pass). Confirmed still
> reproduces on the current tree. Root cause: the `merge ... into ~...`
> canonicalizes the `in`-subselect's RHS so the existence concept's `.address`
> collides with the LHS per-row concept's address (`store_sales.ticket_number`).
> In `trilogy/core/processing/nodes/base_node.py::resolve_concept_map` the
> existence-only parent (the GROUP CTE `thoughtful`) is order-sensitively allowed
> to win the row column's `source_map` slot, so the fact CTE (`cheerful`) never
> materializes the per-row column, and `existence_source_map` is left unpopulated.
> The renderer at `dialect/base.py:1379` then faithfully emits
> `thoughtful.ticket_number` in a `FROM` that only has `cheerful`.
>
> The minimal fix (make `resolve_concept_map` existence-aware so a row address
> never resolves to an existence-only parent when a row parent provides it, and
> populate `existence_source_map`) touches the most central source-map routing
> used by *every* query, and per project norms needs the full tpc_ds + tpch agent
> sweep to validate — not a partial unit run. Given this is a single, arguably
> degenerate query (`X in X` after merge is a tautological filter) with a clean
> workaround (drop the merge), the change is held for explicit sign-off rather
> than landed unilaterally. See `syntax_error_followups.md` resolution notes.

## Summary

When a query (a) `merge`s a key from one model into another and (b) filters with
an `in`-subselect on that merged key inside an aggregate, the generated SQL
references a CTE alias that was never emitted. DuckDB rejects it:

```
(_duckdb.BinderException) Binder Error: Referenced table "cooperative" not found!
Candidate tables: "cheerful"
```

The referenced alias and the candidate alias are both engine-generated random CTE
names; the subselect points at one that isn't in scope. This is a SQL-generation
bug (wrong/missing CTE in the emitted plan), not a user error — the same query
without the `merge` compiles and runs.

Cluster: TPC-DS agent eval base leg `20260601-025817_base`, **q24** (graded
`other`; the `questionable not found` variant). The agent never recovered.

## Minimal, deterministic repro

A **single** merge plus a **single** `in`-subselect on the merged key is enough:

```
import raw.store_sales as store_sales;
import raw.store_returns as store_returns;

merge store_returns.ticket_number into ~store_sales.ticket_number;

auto sub_total <- sum(store_sales.net_paid
    ? store_sales.ticket_number in store_returns.ticket_number
) by store_sales.customer.last_name, store_sales.store.store_name;

select store_sales.customer.last_name, store_sales.store.store_name, sub_total
limit 10;
```

```bash
trilogy run repro.preql duckdb
```

→ `Binder Error: Referenced table "thoughtful" not found! Candidate tables: "cheerful"`

### What narrows it (tested against `20260601-025817_base/workspace`)

| variant | result |
|---|---|
| no `merge`, `in`-subselect on the same key | ✅ runs (10 rows) |
| 1 `merge` + 1 `in`-subselect on the merged key | ❌ Binder Error (missing CTE) |
| 2 `merge`s + 2 `in`-subselects (q24's actual form) | ❌ Binder Error (missing CTE) |

So the `merge` statement is the trigger: with the merge present, the `in`-subselect
on the merged key compiles to a reference to a CTE that the plan doesn't emit
under that name. Without the merge, the `in` filter resolves correctly.

## Where to look

- The merged-key path that lowers `X in Y` (a subselect/semijoin) when `X` and `Y`
  are linked by a `merge ... into ~...` equivalence. The subselect's `FROM`/`IN`
  target is bound to a CTE name that either (a) is never materialized, or (b) is
  materialized under a different generated alias than the reference uses — an
  alias-assignment / CTE-emission mismatch in the merged-key branch.
- Likely in the merge-resolution → node-graph → CTE-naming stage (where random CTE
  names like `cooperative`/`cheerful` are assigned). Compare the set of emitted CTE
  names against the set referenced by `in`-subselect targets.

## Suggested regression assertion once fixed

- The 1-merge repro above runs and returns rows.
- Generic invariant: every table/CTE referenced in the emitted SQL exists in the
  `WITH` list (a cheap post-render check would catch this whole class, like the
  existing `INVALID_REFERENCE_BUG not in sql` guards).

## Agent workaround (what eventually passed)

Drop the `merge` entirely and rely on the bare `in`-subselect across the two
models (store_sales / store_returns already share imported dimensions), e.g.
`store_sales.ticket_number in store_returns.ticket_number` with no merge
statement. The on-disk `query24.preql` in the run workspace uses this form and
runs clean.

## Setup

Reuse `evals/tpcds_agent/results/20260601-025817_base/workspace/` (`raw/`,
`tpcds.duckdb`, `trilogy.toml`); `cd` there and run the repro.

- Observed on `trilogy 0.3.275`.
